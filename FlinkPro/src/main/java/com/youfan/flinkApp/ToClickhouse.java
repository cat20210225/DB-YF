package com.youfan.flinkApp;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.youfan.bean.TestBean;
import com.youfan.udf.MyDebezium;
import com.youfan.udf.MyDebezium1;
import com.youfan.util.ClickHouseUtil;
import org.apache.avro.ValidateAll;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.expressions.CurrentTime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

public class ToClickhouse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


//        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
//        //2.1 开启Checkpoint,每隔1分钟做一次CK
//        env.enableCheckpointing(60000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次C+K数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
//        //2.5 设置状态后端
//        //env.setStateBackend(new RocksDBStateBackend("file:///usr/local/flink-1.13.5/ck"));
//        //env.setStateBackend(new MemoryStateBackend());
//        // MemoryStateBackend（内存状态后端）
//        // FsStateBackend（文件系统状态后端 hdfs）
//        // RocksDBStateBackend（RocksDB状态后端）
//        SimpleDateFormat sdf1= new SimpleDateFormat("yyyy-MM-dd-HH");
//        String date = sdf1.format(new Date());
//        String path = "hdfs://iZrioqk6b370kwZ:8020/flink/clickhouse/ck/"+date;
//        env.setStateBackend(new FsStateBackend(path));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "root");
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);


        SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                .hostname("120.26.1.207")
                .port(17477)
                .database("test") // monitor sqlserver database
                .tableList("dbo.flink_test") // monitor products table
                .username("scm")
                .password("HoIhrR64kTwZnCz9")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDebezium()) // converts SourceRecord to JSON String
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        SingleOutputStreamOperator<TestBean> data = dataStreamSource.map(new MapFunction<String, TestBean>() {
            @Override
            public TestBean map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONObject data = jsonObject.getJSONObject("data");
                TestBean testBean = JSONObject.parseObject(data.toString(), TestBean.class);
                Date date = new Date();
                testBean.setDate(date);
                return testBean;
            }
        });

        data.print();

// todo
        data.addSink(
                ClickHouseUtil.getJdbcSink("insert into flink_test values(?,?,?,?,?,?)"));


        env.execute();
    }
}
