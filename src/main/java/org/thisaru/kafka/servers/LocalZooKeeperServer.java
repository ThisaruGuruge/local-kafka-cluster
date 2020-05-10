/*
 * Author: Thisaru Guruge
 * Website: https://ThisaruGuruge.github.io
 *
 * This software is under Apache 2.0 License.
 *
 * Copyright (c) 2020 | All Rights Reserved
 */

package org.thisaru.kafka.servers;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Creates a local Zookeeper server for testing purposes. Specially in unit tests / integration tests requiring Kafka.
 */
public class LocalZooKeeperServer {
    Logger logger = LoggerFactory.getLogger(LocalZooKeeperServer.class);

    private ZooKeeperServerMain zooKeeperServer;

    /**
     * Create and runs a local ZooKeeper instance. This runs in a separate thread.
     *
     * @param properties - Properties of the ZooKeeper server, in addition to the default configurations
     */
    public LocalZooKeeperServer(Properties properties) {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        zooKeeperServer = new ZooKeeperServerMain();
        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);

        new Thread(() -> {
            try {
                zooKeeperServer.runFromConfig(configuration);
                logger.info("Zookeeper started");
            } catch (Exception e) {
                logger.error("Zookeeper startup failed.", e);
            }
        }).start();
    }
}
