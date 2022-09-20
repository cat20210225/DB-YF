package com.youfan.flinkApp;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.youfan.udf.MyDebezium;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class ToKafka {
    public static void main(String[] args) throws Exception {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);


            //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
            //2.1 开启Checkpoint,每隔5秒钟做一次CK
            env.enableCheckpointing(5000L);
            //2.2 指定CK的一致性语义
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            //2.3 设置任务关闭的时候保留最后一次CK数据
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            //2.4 指定从CK自动重启策略
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
            //2.5 设置状态后端
            //env.setStateBackend(new RocksDBStateBackend("file:///usr/local/flink-1.13.5/ck"));
            //env.setStateBackend(new MemoryStateBackend());
            // MemoryStateBackend（内存状态后端）
            // FsStateBackend（文件系统状态后端 hdfs）
            // RocksDBStateBackend（RocksDB状态后端）
            env.setStateBackend(new FsStateBackend("hdfs://iZrioqk6b370kwZ:8020/flinkCDC"));
            //2.6 设置访问HDFS的用户名
            System.setProperty("HADOOP_USER_NAME", "root");
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
            env.getCheckpointConfig().setCheckpointTimeout(10000);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);


            SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                    .hostname("120.26.1.207")
                    .port(17477)
                    .database("test") // monitor sqlserver database
                    .tableList("dbo.Mytest") // monitor products table
                    .username("scm")
                    .password("HoIhrR64kTwZnCz9")
                    .startupOptions(StartupOptions.initial())
                    .deserializer(new MyDebezium()) // converts SourceRecord to JSON String
                    .build();


            DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

//            SingleOutputStreamOperator<String> mapStream = dataStreamSource.map(new MapFunction<String, String>() {
//                @Override
//                public String map(String value) throws Exception {
//                    JSONObject jsonObject = JSONObject.parseObject(value);
//
//                    JSONObject json = new JSONObject();
//                    json.put("database",jsonObject.getString("database"));
//                    json.put("id",JSONObject.parseObject(jsonObject.getString("data")).getString("Id"));
//                    json.put("age",JSONObject.parseObject(jsonObject.getString("data")).getInteger("Age"));
//                    json.put("name",JSONObject.parseObject(jsonObject.getString("data")).getString("Name"));
//                    json.put("type",jsonObject.getString("type"));
//                    json.put("table",jsonObject.getString("table"));
//
//
//                    return json.toString();
//                }
//            });

        dataStreamSource.print("转换数据");

        dataStreamSource.addSink(new FlinkKafkaProducer<String>("iZrioqk6b370kwZ:9092","bbbtest",new SimpleStringSchema()));

//        mapStream.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<String>() {
//            @Override
//            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
//                return new ProducerRecord<>(JSONObject.parseObject(element).getString("table"),element.getBytes());
//            }
//        }));


            env.execute();

    }
}
