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

import kafka.metrics.KafkaMetricsReporter;
import kafka.metrics.KafkaMetricsReporter$;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Seq;

import java.util.Properties;

/**
 * Creates a local Kafka server used for testing purposes.
 */
public class LocalKafkaServer {
    private static final String prefix = "kafka-thread";
    Logger logger = LoggerFactory.getLogger(LocalKafkaServer.class);

    private final KafkaServer kafkaServer;

    /**
     * Creates a Kafka Server instance.
     *
     * @param properties - Properties of the Kafka server, in addition to the default configurations
     */
    public LocalKafkaServer(Properties properties) {
        KafkaConfig kafkaConfig = KafkaConfig.fromProps(properties);
        Seq<KafkaMetricsReporter> reporters = KafkaMetricsReporter$.MODULE$.startReporters(
                new VerifiableProperties(properties));
        this.kafkaServer = new KafkaServer(kafkaConfig, Time.SYSTEM, Option.apply(prefix), reporters);
    }

    /**
     * Starts the Kafka server.
     */
    public void start() {
        logger.info("Starting Kafka server");
        this.kafkaServer.startup();
        logger.info("Kafka server successfully started.");
    }

    /**
     * Stops the Kafka Server.
     */
    public void stop() {
        logger.info("Stopping Kafka server");
        this.kafkaServer.shutdown();
        logger.info("Kafka server successfully stopped.");
    }
}
