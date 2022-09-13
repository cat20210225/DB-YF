package com.youfan;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.Validator;
import com.youfan.udf.MyDebezium1;
import io.debezium.connector.sqlserver.SqlServerConnector;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.hadoop.fs.Path;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        System.setProperty("HADOOP_USER_NAME", "root");

//        Properties properties = new Properties();
//        properties.setProperty("debezium.snapshot.locking.mode", "none");


        SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                .hostname("120.26.1.207")
                .port(17477)
                .database("UFO_SCM_ZJFanKa") // monitor sqlserver database
                .tableList("dbo.ro_salesorder") // monitor products table
                .username("scm")
                .password("HoIhrR64kTwZnCz9")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDebezium1()) // converts SourceRecord to JSON String
//              .debeziumProperties(properties)
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource.print();
//        String outputPath = "hdfs://121.41.82.106:8020/flink/flinkcdc";
//        BucketingSink hadoopSink = new BucketingSink<String>(outputPath);
//        // 使用表名命名存储区
//        hadoopSink.setBucketer(new Bucketer() {
//            @Override
//            public Path getBucketPath(Clock clock, Path path, Object o) {
//                String table = JSONObject.parseObject((String) o).getString("table");
//                return new Path(outputPath + "/" + table);
//            }
//        });
//        // 下述两种条件满足其一时，创建新的块文件
//        // 条件1.设置块大小为100MB
//        hadoopSink.setBatchSize(1024 * 1024 * 100);
//        // 条件2.设置时间间隔20min
//        hadoopSink.setBatchRolloverInterval(20 * 60 * 1000);
//        //设置的是检查两次检查桶不活跃的情况的周期
//        hadoopSink.setInactiveBucketCheckInterval(5L);
//        //设置的是关闭不活跃桶的阈值,多久时间没有数据写入就关闭桶
//        hadoopSink.setInactiveBucketThreshold(10L);
//        hadoopSink.setPartPrefix("ods");
//        hadoopSink.setPartSuffix(".txt");
//
//        dataStreamSource.addSink(hadoopSink);
        env.execute();
    }
}
