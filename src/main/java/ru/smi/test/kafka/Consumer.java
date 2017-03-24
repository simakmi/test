package ru.smi.test.kafka;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.axibase.tsd.network.PlainCommand;

import ru.smi.test.axibase.network.PlainCommandDeserializer;

public class Consumer {

    private final KafkaConsumer<String, PlainCommand> consumer;
    private int readTimeout = 10;

    public Consumer(String bootstrapServers, String groupId, String topicName) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", groupId);
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", PlainCommandDeserializer.class.getName());
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topicName));
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }
    
    public void read() {
        Thread thread = new Thread(new Runnable() {
            
            @Override
            public void run() {

                try {
                    int iteractionCount = 0;
                    while (iteractionCount < 10) {
                        ConsumerRecords<String, PlainCommand> records = consumer.poll(TimeUnit.SECONDS.toMillis(readTimeout));
                        PlainCommand command;
                        for (ConsumerRecord<String, PlainCommand> record : records) {
                            command = record.value();
                            System.out.println("Command: " + (command == null ? "null" : command.compose()));
                        }
                        iteractionCount++;
                        consumer.commitSync();
                    }
                } catch (WakeupException e) {
                    // ignore for shutdown
                } finally {
                    consumer.close();
                }
            }
        }
            );
        thread.start();
    }

}
