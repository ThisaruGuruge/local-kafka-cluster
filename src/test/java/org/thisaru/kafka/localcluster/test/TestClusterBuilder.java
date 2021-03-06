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

package org.thisaru.kafka.localcluster.test;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.thisaru.kafka.localcluster.KafkaCluster;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Test(description = "Tests to the cluster building functionality")
public class TestClusterBuilder {

    @Test
    public void testDeleteLogsOnExit() throws IOException {
        String dataDir = "/tmp/kafka-cluster-test-1";
        KafkaCluster kafkaCluster = new KafkaCluster(dataDir)
                .withZooKeeper(2181)
                .withBroker("PLAINTEXT", 9092)
                .start();

        kafkaCluster.stop(true);
        Assert.assertFalse(Files.exists(Paths.get(dataDir)));
    }

    @Test
    public void testMessageProducingAndConsuming() throws IOException, ExecutionException, InterruptedException {
        String message = "Hello, world";
        String dataDir = "/tmp/kafka-cluster-test-2";
        String serializer = StringSerializer.class.getName();
        String deserializer = StringDeserializer.class.getName();
        String groupId = "test-group";
        String topic = "test-topic";
        List<String> topics = Collections.singletonList(topic);

        KafkaCluster kafkaCluster = new KafkaCluster(dataDir)
                .withZooKeeper(2182)
                .withBroker("PLAINTEXT", 9093)
                .withConsumer(deserializer, deserializer, groupId, topics)
                .withProducer(serializer, serializer)
                .start();

        kafkaCluster.createTopic(topic, 3, 1);
        kafkaCluster.sendMessage(topic, message);
        String receivedMessage = kafkaCluster.consumeMessage(2000);
        Assert.assertEquals(receivedMessage, message);
    }
}
