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

import org.testng.Assert;
import org.testng.annotations.Test;
import org.thisaru.kafka.localcluster.KafkaCluster;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Test(description = "Tests to the cluster building functionality")
public class TestClusterBuilder {

    @Test
    public void testDeleteLogsOnExit() throws IOException {
        String dataDir = "/tmp/kafka-cluster-test";
        KafkaCluster kafkaCluster = new KafkaCluster(dataDir, null)
                .withZooKeeper(2181, null)
                .withBroker("PLAINTEXT", 9092, null)
                .start();

        kafkaCluster.stop(true);
        Assert.assertFalse(Files.exists(Paths.get(dataDir)));
    }
}
