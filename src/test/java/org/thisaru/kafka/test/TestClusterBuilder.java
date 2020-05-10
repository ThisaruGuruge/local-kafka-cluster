/*
 * Author: Thisaru Guruge
 * Website: https://ThisaruGuruge.github.io
 *
 * This software is under Apache 2.0 License.
 *
 * Copyright (c) 2020 | All Rights Reserved
 */

package org.thisaru.kafka.test;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.thisaru.kafka.KafkaCluster;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Test(description = "Tests to the cluster building functionality")
public class TestClusterBuilder {

    @Test
    public void testDeleteLogsOnExit() throws IOException {
        String dataDir = "/tmp/kafka-cluster-test";
        KafkaCluster kafkaCluster = new KafkaCluster(dataDir, null)
                .withZookeeper(2181, null)
                .withBroker("PLAINTEXT", 9092, null)
                .start();

        kafkaCluster.stop(true);
        Assert.assertFalse(Files.exists(Paths.get(dataDir)));
    }
}
