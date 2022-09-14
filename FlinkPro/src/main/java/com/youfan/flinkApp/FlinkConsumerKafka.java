package com.youfan.flinkApp;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkConsumerKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String kafkaServer = "iZrioqk6b370kwZ:9092";
        Properties properties = new Properties();
        //封装kafka的连接地址
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        //指定消费者id
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "wu");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("aaatest", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> streamSource = env.addSource(kafkaConsumer);
        streamSource.print();
        env.execute();
    }
}
