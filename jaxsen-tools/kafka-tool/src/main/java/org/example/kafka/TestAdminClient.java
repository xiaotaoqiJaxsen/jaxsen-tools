package org.example.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author mayongjie
 * @date 2024/03/19 15:17
 **/
public class TestAdminClient implements AutoCloseable{

    private AdminClient adminClient;

    public TestAdminClient() {
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
        adminClient = AdminClient.create(properties);
    }

    public void getConfigs() {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");

        // 获取 Broker 配置的详细信息
        try {
            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singleton(resource));
            Map<ConfigResource, Config> configs = describeConfigsResult.all().get();

            // 遍历并打印配置信息
            for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
                ConfigResource configResource = entry.getKey();
                Config config = entry.getValue();

                System.out.println("Resource: " + configResource);
                System.out.println("Configs: " + config);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void close() throws Exception {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
