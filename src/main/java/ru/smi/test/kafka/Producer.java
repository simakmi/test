package ru.smi.test.kafka;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

    private final KafkaProducer<String, String> producer;
    private final String topicName;

    public Producer(String bootstrapServers, String groupId, String topicName) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", groupId);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
        this.topicName = topicName;
    }

    public void write() throws IOException {
        List<String> commands = Files.readAllLines(Paths.get("src/main/resources/raw_commands.txt"), Charset.forName("UTF-8"));
        for (String command : commands) {
            producer.send(new ProducerRecord<String, String>(topicName, null, command));
        }
        producer.close();
    }

}
