/*
 *    Copyright 2020 Thisaru Guruge
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.thisaru.kafka.localcluster.servers;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Creates a local Kafka server used for testing purposes.
 */
public class LocalKafkaServer {
    Logger logger = LoggerFactory.getLogger(LocalKafkaServer.class);

    private KafkaServerStartable kafkaServer;

    /**
     * Creates a Kafka Server instance.
     *
     * @param properties - Properties of the Kafka server, in addition to the default configurations
     */
    public LocalKafkaServer(Properties properties) {
        KafkaConfig kafkaConfig = KafkaConfig.fromProps(properties);
        kafkaServer = new KafkaServerStartable(kafkaConfig);
    }

    /**
     * Starts the Kafka server.
     */
    public void start() throws RuntimeException {
        logger.info("Starting Kafka server");
        try {
            this.kafkaServer.startup();
            logger.info("Kafka server successfully started.");
        } catch (Throwable throwable) {
            logger.error("Failed to start the Kafka server.", throwable);
            throw new RuntimeException(throwable);
        }
    }

    /**
     * Stops the Kafka Server.
     */
    public void stop() throws RuntimeException {
        logger.info("Stopping Kafka server");
        try {
            this.kafkaServer.shutdown();
            logger.info("Kafka server successfully stopped.");
        } catch (Throwable throwable) {
            logger.error("Failed to stop the Kafka server.", throwable);
            throw new RuntimeException(throwable);
        }
    }
}
