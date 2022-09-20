package com.youfan.flinkApp;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class FlinkConsumerKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔2分钟做一次CK
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(5));
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
        //超时时间3分钟
        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.SECONDS.toMillis(30));
        //两个checkpoint间隔最小为500毫秒（第一个ck结束后至少过3分钟才开始下一个ck）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);

        String kafkaServer = "iZrioqk6b370kwZ:9092";
        Properties properties = new Properties();
        //封装kafka的连接地址
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        //指定消费者id
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("flink_test", new SimpleStringSchema(), properties);
//        kafkaConsumer.setStartFromEarliest();
//        kafkaConsumer.setStartFromLatest();
        DataStreamSource<String> streamSource = env.addSource(kafkaConsumer);

        streamSource.print();

        SingleOutputStreamOperator<String> mapSource = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String id = jsonObject.getString("id");
                jsonObject.put("id", id + "f");
                return jsonObject.toString();
            }
        });

        mapSource.addSink(new RichSinkFunction<String>() {

            private PreparedStatement pstm;
            private Connection conn;
            private String sql;
            @Override
            public void open(Configuration parameters) throws Exception {
                String jdbc = "org.postgresql.Driver";
                String url = "jdbc:postgresql://121.41.82.106:45565/postgres";
                String user = "postgres";
                String password = "Qweasd987@#";
                Class.forName(jdbc);
                conn = DriverManager.getConnection(url, user, password);
            }

            @Override
            public void invoke(String value, Context context) throws Exception {
                //insert into test.mysql_hive(id,name,age,money,todate,ts) values(?,?,?,?,?,?)
                //建表  create table if not exist....
                JSONObject jsonObject = JSONObject.parseObject(value);
                Set<String> set = jsonObject.keySet();
                Collection<Object> values = jsonObject.values();

                sql = "insert into db_model.flink_test(\"" + StringUtils.join(set,"\",\"")
                        + "\") values ('" + StringUtils.join(values,"','") +"')";
//             String sql1= "insert into db_ods.ods_tmp_test values ('" + StringUtils.join(values,"','") +"')";

                pstm = conn.prepareStatement(sql);
                pstm.execute();

            }

            @Override
            public void close() throws Exception {
                if (pstm != null) {
                    try {
                        pstm.close();
                    } catch (SQLException e) {

                    }
                }
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        env.execute();
    }
}
