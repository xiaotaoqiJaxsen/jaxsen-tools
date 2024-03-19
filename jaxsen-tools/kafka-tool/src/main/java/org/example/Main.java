package org.example;

import org.example.kafka.TestAdminClient;
import org.example.kafka.TestKafkaProducer;

public class Main {
    public static void main(String[] args) throws Exception {
//        try (TestKafkaProducer producer = new TestKafkaProducer()) {
//            producer.send(10);
//        }

        try (TestAdminClient adminClient = new TestAdminClient()) {
            adminClient.getConfigs();
        }
    }
}