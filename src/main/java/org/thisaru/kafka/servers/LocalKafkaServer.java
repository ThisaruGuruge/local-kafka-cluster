/*
 * Author: Thisaru Guruge
 * Website: https://ThisaruGuruge.github.io
 *
 * This software is under Apache 2.0 License.
 *
 * Copyright (c) 2020 | All Rights Reserved
 */
package org.thisaru.kafka.servers;

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
