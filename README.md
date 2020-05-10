# Local Kafka Cluster
This is a local kafka cluster setup used for testing purposes.
The Kafka cluster have
- A ZooKeeper Server
- One or more Kafka Servers
- An optional Kafka Producer
- An optional Kafka Consumer

> This is only intended to be used for testing, and does not recommend to use in production.

# Examples

To create a simple Kafka cluster, you can use the following method. The Kafka cluster constructor takes two paramters. 
- `dataDir` - The root directory to keep ZooKeeper and the Kafka logs.
- `host` - Host for the ZooKeeper and the Kafka servers. Default value is `localhost`.

Then the `withZooKeeper()` method will create and start a ZooKeeper. The created ZooKeeper will run on a different
 thread. This method takes two parameters.
 - `port` - The client port of the ZooKeeper.
 - `properties` - The additional properties for the ZooKeeper. The default ZooKeeper properties will be added by
  default. Pass any extra properties, if required.
  
  Then the `withBroker()`  method will create a Kafka server. It takes three parameters.
  - `securityProtocol` - This is just to add the listener name. Other security parameters should be passed through
   the `properties` parameter.
   - `port` -  This will set the listener port of the broker.
   - `properties` - The additional properties for the Kafka server. The default Kafka server properties will be added by
                      default. Pass any extra properties, if required.
   
   Finally. calling `start()` method will start the Kafka server.                   
```java
public class TestKafkaCluster {
    public void createKafkaCluster() throws IOException {
        String dataDir = "/tmp/kafka-cluster-test";
        KafkaCluster kafkaCluster = new KafkaCluster(dataDir, null)
                                    .withZookeeper(2181, null)
                                    .withBroker("PLAINTEXT", 9092, null)
                                    .start();
    }
}
```
