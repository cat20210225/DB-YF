package com.youfan.flinkApp;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.youfan.udf.MyDebezium1;
import io.debezium.data.Envelope;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;

public class ToPG {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔3秒做一次CK
        env.enableCheckpointing(5000L);
        //2.2 指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次C+K数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        //2.5 设置状态后端
        SimpleDateFormat sdf1= new SimpleDateFormat("yyyy-MM-dd-HH");
        String date = sdf1.format(new Date());
        String path = "hdfs://iZrioqk6b370kwZ:8020/flink/test/ck/"+date;
        env.setStateBackend(new FsStateBackend(path));
        //2.6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "root");
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointTimeout(30000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);

        SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                .hostname("120.26.1.207")
                .port(17477)
                .database("UFO_SCM_ZJFanKa") // monitor sqlserver database
                .tableList("dbo.FX_TaskNotice") // monitor products table
                .username("scm")
                .password("HoIhrR64kTwZnCz9")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDebezium1()) // converts SourceRecord to JSON String
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        SingleOutputStreamOperator<String> mapSource = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String id = jsonObject.getString("Id");
                JSONObject map = new JSONObject();
                map.put("id", id);

                return map.toString();
            }
        });

        mapSource.print();

        mapSource.addSink(new FlinkKafkaProducer<String>("iZrioqk6b370kwZ:9092","flink_test",new SimpleStringSchema()));

//        dataStreamSource.addSink(new RichSinkFunction<String>() {
//
//            private PreparedStatement pstm;
//            private Connection conn;
//            private String sql;
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                String jdbc = "org.postgresql.Driver";
//                String url = "jdbc:postgresql://121.41.82.106:45565/postgres";
//                String user = "postgres";
//                String password = "Qweasd987@#";
//                Class.forName(jdbc);
//                conn = DriverManager.getConnection(url, user, password);
//            }
//
//            @Override
//            public void invoke(String value, Context context) throws Exception {
//                //insert into test.mysql_hive(id,name,age,money,todate,ts) values(?,?,?,?,?,?)
//                //建表  create table if not exist....
//                JSONObject jsonObject = JSONObject.parseObject(value);
//                Set<String> set = jsonObject.keySet();
//                Collection<Object> values = jsonObject.values();
//
//                sql = "insert into db_model.tenant_test(\"" + StringUtils.join(set,"\",\"")
//                        + "\") values ('" + StringUtils.join(values,"','") +"')";
////             String sql1= "insert into db_ods.ods_tmp_test values ('" + StringUtils.join(values,"','") +"')";
//
//                pstm = conn.prepareStatement(sql);
//                pstm.execute();
//
//            }
//
//            @Override
//            public void close() throws Exception {
//                if (pstm != null) {
//                    try {
//                        pstm.close();
//                    } catch (SQLException e) {
//
//                    }
//                }
//                if (conn != null) {
//                    try {
//                        conn.close();
//                    } catch (SQLException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        });

        env.execute();

    }
}
