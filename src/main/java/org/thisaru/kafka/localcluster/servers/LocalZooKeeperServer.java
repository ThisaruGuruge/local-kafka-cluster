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

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Creates a local ZooKeeper server for testing purposes. Specially in unit tests / integration tests requiring Kafka.
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
                logger.info("ZooKeeper started");
            } catch (Exception e) {
                logger.error("ZooKeeper startup failed.", e);
            }
        }).start();
    }
}
