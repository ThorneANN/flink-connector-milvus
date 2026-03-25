/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.milvus;

/**
 * This class is a placeholder for the {@code flink-sql-connector-milvus} fat jar.
 *
 * <p>This module repackages {@code flink-connector-milvus} together with all its dependencies
 * (Milvus SDK, gRPC, Netty, etc.) into a single shaded jar that can be dropped directly into a
 * Flink cluster's {@code lib/} or {@code plugins/} directory without any classpath conflicts.
 *
 * <p>For the actual connector implementation see the {@code flink-connector-milvus} module.
 */
public class DummyDocs {}
