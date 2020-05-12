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

package org.thisaru.kafka.localcluster;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.thisaru.kafka.localcluster.servers.LocalKafkaServer;
import org.thisaru.kafka.localcluster.servers.LocalZooKeeperServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.thisaru.kafka.localcluster.utils.Constants.KAFKA_PROP;
import static org.thisaru.kafka.localcluster.utils.Constants.KAFKA_SUFFIX;
import static org.thisaru.kafka.localcluster.utils.Constants.ZOOKEEPER_PROP;
import static org.thisaru.kafka.localcluster.utils.Constants.ZOOKEEPER_SUFFIX;

/**
 * Builds a Kafka cluster with a ZooKeeper, a KafkaServer(s) (At least one required), KafkaConsumer
 * (Optional), KafkaProducer (Optional).
 */
public class KafkaCluster {

    // Broker configs
    private static final String zookeeperConnectConfig = "zookeeper.connect";
    private static final String logDirConfig = "log.dirs";
    private static final String listenersConfig = "listeners";
    private static final String brokerIdConfig = "broker.id";

    // ZooKeeper configs
    private static final String dataDirConfig = "dataDir";
    private static final String portConfig = "clientPort";

    private KafkaConsumer<?, ?> kafkaConsumer = null;
    private KafkaProducer<?, ?> kafkaProducer = null;
    private final String dataDir;

    private LocalZooKeeperServer zookeeper;
    private List<LocalKafkaServer> brokerList;

    private Properties defaultZooKeeperProperties;
    private Properties defaultKafkaProperties;
    private int zookeeperPort = 2181;
    private int brokerPort = 9092;
    private String host = "localhost";
    private String bootstrapServer = host + ":" + brokerPort;

    /**
     * Initiates the building of the Kafka cluster.
     *
     * @param dataDir - The root directory to keep ZooKeeper and Kafka logs
     * @throws IOException - If the default properties files are missing
     */
    public KafkaCluster(String dataDir) throws IOException {
        this.dataDir = dataDir;
        initializeDefaultProperties();
        this.brokerList = new ArrayList<>();
    }

    /**
     * Initiates the building of the Kafka cluster.
     *
     * @param dataDir - The root directory to keep ZooKeeper and Kafka logs
     * @param host    - Host of the Kafka Cluster. Default value is {@code localhost}
     * @throws IOException - If the default properties files are missing
     */
    public KafkaCluster(String dataDir, String host) throws IOException {
        this.dataDir = dataDir;
        if (host != null) {
            this.host = host;
        }
        initializeDefaultProperties();
        this.brokerList = new ArrayList<>();
    }

    /**
     * Initiates the building of the Kafka cluster.
     *
     * @param dataDir           - The root directory to keep ZooKeeper and Kafka logs
     * @param host              - Host of the Kafka Cluster. Default value is {@code localhost}
     * @param resourceDirectory - Path to the resources directory where default ZooKeeper and Kafka properties at
     * @throws IOException - If the default properties files are missing
     */
    public KafkaCluster(String dataDir, String host, String resourceDirectory) throws IOException {
        this.dataDir = dataDir;
        if (host != null) {
            this.host = host;
        }
        initializeDefaultProperties(resourceDirectory);
        this.brokerList = new ArrayList<>();
    }

    /**
     * Add a ZooKeeper server to the cluster.
     *
     * @param port             - Client port ({@code clientPort}) of the ZooKeeper
     * @return - {@code KafkaCluster} with the ZooKeeper
     */
    public KafkaCluster withZooKeeper(int port) {
        return withZooKeeper(port, null);
    }

    /**
     * Add a ZooKeeper server to the cluster.
     *
     * @param port             - Client port ({@code clientPort}) of the ZooKeeper
     * @param customProperties - Properties of the ZooKeeper, in addition to the default values
     * @return - {@code KafkaCluster} with the ZooKeeper
     */
    public KafkaCluster withZooKeeper(int port, Properties customProperties) {
        if (this.zookeeper != null) {
            throw new IllegalStateException("ZooKeeper is already running");
        }
        Properties properties = new Properties();
        this.zookeeperPort = port;
        properties.putAll(defaultZooKeeperProperties);
        if (customProperties != null) {
            properties.putAll(customProperties);
        }
        String zookeeperDataDir = Paths.get(dataDir, ZOOKEEPER_SUFFIX).toString();
        properties.setProperty(dataDirConfig, zookeeperDataDir);
        properties.setProperty(portConfig, Integer.toString(port));

        this.zookeeper = new LocalZooKeeperServer(properties);
        return this;
    }

    /**
     * Adds a Kafka broker the to the cluster.
     *
     * @param protocol         - The security protocol used in the Broker
     * @param port             - The port number of the broker
     * @return - {@code KafkaCluster} with the Kafka broker
     */
    public KafkaCluster withBroker(String protocol, int port) {
        return withBroker(protocol, port, null);
    }

    /**
     * Adds a Kafka broker the to the cluster.
     *
     * @param protocol         - The security protocol used in the Broker
     * @param port             - The port number of the broker
     * @param customProperties - Properties of the Kafka broker, in addition to the default values
     * @return - {@code KafkaCluster} with the Kafka broker
     */
    public KafkaCluster withBroker(String protocol, int port, Properties customProperties) {
        if (this.zookeeper == null) {
            throw new IllegalStateException("ZooKeeper is not initialized");
        }
        Properties properties = new Properties();
        properties.putAll(defaultKafkaProperties);
        if (customProperties != null) {
            properties.putAll(customProperties);
        }

        if (properties.getProperty(listenersConfig) == null) {
            String listeners = protocol + "://" + this.host + ":" + port;
            properties.setProperty(listenersConfig, listeners);
        }

        String kafkaDataDir = Paths.get(dataDir, KAFKA_SUFFIX).toString();
        String zookeeperConfig = this.host + ":" + this.zookeeperPort;
        properties.setProperty(zookeeperConnectConfig, zookeeperConfig);
        properties.setProperty(logDirConfig, kafkaDataDir);
        // Assign next broker index as the broker ID
        properties.setProperty(brokerIdConfig, Integer.toString(brokerList.size()));
        LocalKafkaServer kafkaServer = new LocalKafkaServer(properties);
        this.brokerList.add(kafkaServer);
        this.brokerPort = port;
        return this;
    }

    /**
     * Adds a Kafka consumer to the cluster. This should be added if messages needs to be consumed using the cluster.
     *
     * @param keyDeserializer   - The key deserializer ({@code key.deserializer}) of the consumer
     * @param valueDeserializer - The value deserializer ({@code value.deserializer}) of the consumer
     * @param groupId           - The consumer group ID ({@code group.id})
     * @param topics            - List of topics to subscribe
     * @return - {@code KafkaCluster} with the Kafka consumer
     */
    public KafkaCluster withConsumer(String keyDeserializer, String valueDeserializer, String groupId,
                                     List<String> topics) {
        this.bootstrapServer = this.host + ":" + this.brokerPort;
        String maximumMessagesPerPoll = Integer.toString(1);
        String offsetCommit = "earliest";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Consumer one message at a time. Call this again to consume more.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maximumMessagesPerPoll);
        // We don't want to miss any messages, which were sent before the consumer started.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetCommit);
        this.kafkaConsumer = new KafkaConsumer(properties);
        this.kafkaConsumer.subscribe(topics);
        return this;
    }

    /**
     * Adds a Kafka producer to the cluster. This should be added if messages needs to be sent using the cluster.
     *
     * @param keySerializer   - The key serializer ({@code key.serializer}) of the producer
     * @param valueSerializer - The value serializer ({@code value.serializer}) of the producer
     * @return - {@code KafkaCluster} with the Kafka consumer
     */
    public KafkaCluster withProducer(String keySerializer, String valueSerializer) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        // Stop batching the messages since we need to send messages ASAP in the tests.
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(0));
        this.kafkaProducer = new KafkaProducer(properties);
        return this;
    }

    /**
     * Starts the {@code KafkaCluster}. This actually starts the Kafka server, since the zookeeper is already started
     * and running in a differenet thread.
     *
     * @return - The started {@code KafkaCluster}
     */
    public KafkaCluster start() {
        if (this.zookeeper == null) {
            throw new IllegalStateException("ZooKeeper is not started");
        } else if (this.brokerList.isEmpty()) {
            throw new IllegalStateException("No brokers added");
        }
        for (LocalKafkaServer kafkaServer : brokerList) {
            kafkaServer.start();
        }
        return this;
    }

    /**
     * Stops the {@code KafkaCluster}. This can also delete the kafka logs.
     *
     * @param deleteLogs - Deletes the Kafka logs and the ZooKeeper logs if set to {@code true}.
     */
    public void stop(boolean deleteLogs) {
        if (this.kafkaConsumer != null) {
            this.kafkaConsumer.close();
        }
        if (this.kafkaProducer != null) {
            this.kafkaProducer.close();
        }
        for (LocalKafkaServer kafkaServer : brokerList) {
            kafkaServer.stop();
        }

        if (deleteLogs) {
            deleteDirectory(new File(dataDir));
        }
    }

    /**
     * Creates a topic in the provided {@code KafkaCluster}.
     *
     * @param topic             - Name of the topic
     * @param partitions        - Number of partitions for the topic
     * @param replicationFactor - Replication factor of the topic
     */
    public void createTopic(String topic, int partitions, int replicationFactor) {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        AdminClient adminClient = AdminClient.create(properties);
        NewTopic newTopic = new NewTopic(topic, partitions, (short) replicationFactor);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(newTopic));
        await().atMost(10000, TimeUnit.MILLISECONDS).until(() -> createTopicsResult.all().isDone());
        adminClient.close();
    }

    /**
     * Consumes single message from the {@code KafkaCluster}.
     *
     * @param timeout - The timeout duration for the consumer {@code poll}
     * @return - String value of the message
     */
    public String consumeMessage(long timeout) {
        if (this.kafkaConsumer == null) {
            throw new IllegalStateException("Kafka Consumer is not initialized");
        }
        Duration duration = Duration.ofMillis(timeout);
        ConsumerRecords<?, ?> records = this.kafkaConsumer.poll(duration);
        for (Object record : records) {
            ConsumerRecord<?, ?> consumerRecord = (ConsumerRecord<?, ?>) record;
            return consumerRecord.value().toString();
        }
        return null;
    }

    /**
     * Sends a message to the {@code KafkaCluster}. This blocks the sending function and waits for it to return, since
     * this is used in tests. Not recommended in production.
     *
     * @param topic - Topic to send the message
     * @param key   - Key of the Kafka message
     * @param value - Value of the Kafka message
     * @throws ExecutionException   - If an exception occurred while sending the message
     * @throws InterruptedException - If a concurrency exception occurred while sending the message
     */
    public void sendMessage(String topic, Object key, Object value) throws ExecutionException, InterruptedException {
        if (this.kafkaProducer == null) {
            throw new IllegalStateException("Kafka Producer is not initialized");
        }
        ProducerRecord producerRecord = new ProducerRecord<>(topic, key, value);
        // Since this is for tests, block until producer sends the message.
        this.kafkaProducer.send(producerRecord).get();
    }

    /**
     * Sends a message to the {@code KafkaCluster}. This blocks the sending function and waits for it to return, since
     * this is used in tests. Not recommended in production.
     *
     * @param topic - Topic to send the message
     * @param value - Value of the Kafka message
     * @throws ExecutionException   - If an exception occurred while sending the message
     * @throws InterruptedException - If a concurrency exception occurred while sending the message
     */
    public void sendMessage(String topic, Object value) throws ExecutionException, InterruptedException {
        if (this.kafkaProducer == null) {
            throw new IllegalStateException("Kafka Producer is not initialized");
        }
        ProducerRecord producerRecord = new ProducerRecord<>(topic, value);
        // Since this is for tests, block until producer sends the message
        this.kafkaProducer.send(producerRecord).get();
    }

    private void deleteDirectory(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File content : contents) {
                deleteDirectory(content);
            }
        }
        if (!file.delete()) {
            file.deleteOnExit();
        }
    }

    private void initializeDefaultProperties() throws IOException {
        defaultZooKeeperProperties = new Properties();
        defaultKafkaProperties = new Properties();
        defaultZooKeeperProperties.load(Class.class.getResourceAsStream(ZOOKEEPER_PROP));
        defaultKafkaProperties.load(Class.class.getResourceAsStream(KAFKA_PROP));
    }

    private void initializeDefaultProperties(String resourceDirectory) throws IOException {
        defaultZooKeeperProperties = new Properties();
        defaultKafkaProperties = new Properties();
        InputStream zookeeperPropertiesStream = new FileInputStream(
                (Paths.get(resourceDirectory, "zookeeper.properties")).toString());
        defaultZooKeeperProperties.load(zookeeperPropertiesStream);
        InputStream kafkaPropertiesStream = new FileInputStream(
                Paths.get(resourceDirectory, "server.properties").toString());
        defaultKafkaProperties.load(kafkaPropertiesStream);
    }
}
