package org.apache.flink.connector.milvus;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Serializable;

/** Manages the lifecycle of a {@link MilvusClientV2} connection. */
public class MilvusConnectionProvider implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MilvusConnectionProvider.class);

    private final MilvusSinkConfig config;
    private transient MilvusClientV2 client;

    public MilvusConnectionProvider(MilvusSinkConfig config) {
        this.config = config;
    }

    /** Returns the Milvus client, creating it if necessary. */
    public MilvusClientV2 getOrCreateClient() {
        if (client == null) {
            LOG.info("Creating Milvus client for uri={}, database={}", config.getUri(), config.getDatabaseName());
            ConnectConfig connectConfig;
            if (config.getToken() != null && !config.getToken().isEmpty()) {
                connectConfig = ConnectConfig.builder()
                        .uri(config.getUri())
                        .dbName(config.getDatabaseName())
                        .token(config.getToken())
                        .build();
            } else {
                connectConfig = ConnectConfig.builder()
                        .uri(config.getUri())
                        .dbName(config.getDatabaseName())
                        .build();
            }

            client = new MilvusClientV2(connectConfig);
            LOG.info("Milvus client created successfully");
        }
        return client;
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
                LOG.info("Milvus client closed");
            } catch (Exception e) {
                LOG.warn("Error closing Milvus client", e);
            } finally {
                client = null;
            }
        }
    }
}
