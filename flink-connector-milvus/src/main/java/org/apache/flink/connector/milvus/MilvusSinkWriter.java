package org.apache.flink.connector.milvus;

import com.google.gson.JsonObject;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Writer for {@link MilvusSink} that buffers records and flushes them to Milvus in batches.
 *
 * <p>Write semantics are determined by the record itself:
 * <ul>
 *   <li>{@link RowKind#INSERT} / {@link RowKind#UPDATE_AFTER} → Milvus upsert</li>
 *   <li>{@link RowKind#UPDATE_BEFORE} / {@link RowKind#DELETE} → Milvus delete by primary key</li>
 * </ul>
 * For non-{@link RowData} input (DataStream API), all records are treated as upserts.
 *
 * @param <T> the type of input records
 */
public class MilvusSinkWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MilvusSinkWriter.class);

    private final MilvusSinkConfig config;
    private final MilvusRecordConverter<T> converter;
    private final MilvusConnectionProvider connectionProvider;

    private final Object flushLock = new Object();
    private final List<JsonObject> upsertBuffer = new ArrayList<>();
    private final List<JsonObject> deleteBuffer = new ArrayList<>();

    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> flushTask;

    public MilvusSinkWriter(MilvusSinkConfig config, MilvusRecordConverter<T> converter) {
        this(config, converter, new MilvusConnectionProvider(config));
    }

    /** Package-private constructor for testing. */
    MilvusSinkWriter(MilvusSinkConfig config, MilvusRecordConverter<T> converter,
                     MilvusConnectionProvider connectionProvider) {
        this.config = config;
        this.converter = converter;
        this.connectionProvider = connectionProvider;

        connectionProvider.getOrCreateClient();

        if (config.getFlushIntervalMs() > 0) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "milvus-sink-flusher");
                t.setDaemon(true);
                return t;
            });
            flushTask = scheduler.scheduleAtFixedRate(
                    this::scheduledFlush,
                    config.getFlushIntervalMs(),
                    config.getFlushIntervalMs(),
                    TimeUnit.MILLISECONDS);
        } else {
            scheduler = null;
            flushTask = null;
        }
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        try {
            // Determine operation from RowKind for RowData; default to upsert for other types.
            if (element instanceof RowData) {
                RowKind kind = ((RowData) element).getRowKind();
                JsonObject record = converter.convert(element);
                synchronized (flushLock) {
                    if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
                        upsertBuffer.add(record);
                    } else {
                        // UPDATE_BEFORE and DELETE both remove the record by primary key
                        deleteBuffer.add(record);
                    }
                    if (upsertBuffer.size() + deleteBuffer.size() >= config.getBatchSize()) {
                        flushBuffers();
                    }
                }
            } else {
                JsonObject record = converter.convert(element);
                synchronized (flushLock) {
                    upsertBuffer.add(record);
                    if (upsertBuffer.size() >= config.getBatchSize()) {
                        flushBuffers();
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to convert and buffer record", e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        synchronized (flushLock) {
            if (!upsertBuffer.isEmpty() || !deleteBuffer.isEmpty()) {
                flushBuffers();
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (flushTask != null) {
            flushTask.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        synchronized (flushLock) {
            if (!upsertBuffer.isEmpty() || !deleteBuffer.isEmpty()) {
                try {
                    flushBuffers();
                } catch (Exception e) {
                    LOG.warn("Error flushing remaining records on close", e);
                }
            }
        }
        connectionProvider.close();
    }

    private void scheduledFlush() {
        synchronized (flushLock) {
            if (!upsertBuffer.isEmpty() || !deleteBuffer.isEmpty()) {
                try {
                    flushBuffers();
                } catch (Exception e) {
                    LOG.error("Error during scheduled flush", e);
                }
            }
        }
    }

    /**
     * Flushes buffered records to Milvus with retry.
     * Must be called with {@code flushLock} held.
     */
    private void flushBuffers() throws IOException {
        if (upsertBuffer.isEmpty() && deleteBuffer.isEmpty()) {
            return;
        }

        List<JsonObject> toUpsert = new ArrayList<>(upsertBuffer);
        List<JsonObject> toDelete = new ArrayList<>(deleteBuffer);
        upsertBuffer.clear();
        deleteBuffer.clear();

        LOG.debug("Flushing {} upsert and {} delete records to collection '{}'",
                toUpsert.size(), toDelete.size(), config.getCollectionName());

        int attempts = 0;
        while (true) {
            try {
                doFlush(toUpsert, toDelete);
                LOG.debug("Flush succeeded after {} attempt(s)", attempts + 1);
                return;
            } catch (Exception e) {
                attempts++;
                if (attempts > config.getMaxRetries()) {
                    throw new IOException(
                            "Failed to flush records after " + attempts + " attempt(s)", e);
                }
                LOG.warn("Flush attempt {}/{} failed, retrying in {}ms",
                        attempts, config.getMaxRetries(), config.getRetryIntervalMs(), e);
                try {
                    Thread.sleep(config.getRetryIntervalMs());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during retry", ie);
                }
            }
        }
    }

    private void doFlush(List<JsonObject> toUpsert, List<JsonObject> toDelete) {
        MilvusClientV2 client = connectionProvider.getOrCreateClient();

        if (!toUpsert.isEmpty()) {
            var upsertBuilder = UpsertReq.builder()
                    .collectionName(config.getCollectionName())
                    .data(toUpsert);
            if (config.getPartitionName() != null && !config.getPartitionName().isEmpty()) {
                upsertBuilder.partitionName(config.getPartitionName());
            }
            client.upsert(upsertBuilder.build());
        }

        if (!toDelete.isEmpty()) {
            List<Object> ids = new ArrayList<>();
            for (JsonObject obj : toDelete) {
                if (obj.has("id")) {
                    ids.add(obj.get("id").getAsLong());
                }
            }
            if (!ids.isEmpty()) {
                var deleteBuilder = DeleteReq.builder()
                        .collectionName(config.getCollectionName())
                        .ids(ids);
                if (config.getPartitionName() != null && !config.getPartitionName().isEmpty()) {
                    deleteBuilder.partitionName(config.getPartitionName());
                }
                client.delete(deleteBuilder.build());
            }
        }
    }
}
