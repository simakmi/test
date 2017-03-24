package ru.smi.test.kafka;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Main {

    public static void main(String... args) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileReader("src/main/resources/conf/client.properties"));

        String bootstrapServers = properties.getProperty("bootstrap.servers");
        if (bootstrapServers == null) {
            System.err.println("Property 'bootstrap.servers' not found");
            System.exit(1);
        }

        String groupId = properties.getProperty("group.id");
        if (groupId == null) {
            System.err.println("Property 'group.id' not found");
            System.exit(1);
        }

        String topicName = properties.getProperty("topic.name");
        if (topicName == null) {
            System.err.println("Property 'topic.name' not found");
            System.exit(1);
        }


        Producer producer = new Producer(bootstrapServers, groupId, topicName);
        producer.write();

        Consumer consumer = new Consumer(bootstrapServers, groupId, topicName);
        String timeout = properties.getProperty("read.timeout");
        if (timeout != null) {
            consumer.setReadTimeout(Integer.parseInt(timeout));
        }
        consumer.read();
    }

}
