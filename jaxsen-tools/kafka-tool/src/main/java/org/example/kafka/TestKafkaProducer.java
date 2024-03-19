package org.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author mayongjie
 * @date 2024/03/18 18:21
 **/
@Slf4j
public class TestKafkaProducer implements AutoCloseable{

    /**
     * 生产者
     */
    private KafkaProducer<String, String> kafkaProducer;

    public TestKafkaProducer() {
        Properties properties = new Properties();
        String consumerGroup = "test";
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        int max = 1024 * 1024 * 1024;
        properties.put("max.request.size", max);
        properties.put("buffer.memory", max);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.239.83:9092");
//        properties.put("security.protocol", "SASL_PLAINTEXT");
//        properties.put("sasl.mechanism", "PLAIN");
//        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-kafka\";");
        kafkaProducer = new KafkaProducer<>(properties);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            log.info(consumer.listTopics().keySet().toString());
            System.out.println(consumer.listTopics().keySet());
        }
    }

    public void send(int count) throws Exception{
        int i = 0;
        for (i = 0; i < count;i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test_myj_22222", 0, null, "test-" + i);
            Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
            RecordMetadata recordMetadata = null;
            try {
                recordMetadata = send.get();
                System.out.println(recordMetadata.offset());
            } catch (Exception e) {
                log.error("error", e);
            }
            TimeUnit.SECONDS.sleep(0);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
