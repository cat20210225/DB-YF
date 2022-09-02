package com.youfan.flinkTest;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

public class ToPG {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

//        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
//        //2.1 开启Checkpoint,每隔30秒做一次CK
//        env.enableCheckpointing(30000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次C+K数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
//        //2.5 设置状态后端
//        SimpleDateFormat sdf1= new SimpleDateFormat("yyyy-MM-dd-HH");
//        String date = sdf1.format(new Date());
//        String path = "hdfs://iZrioqk6b370kwZ:8020/data/flinkcdc/ck/"+date;
//        env.setStateBackend(new FsStateBackend(path));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "root");
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);


        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("121.41.82.106")
                .port(3306)
                .username("root")
                .password("!QAZ@WSX")
                .databaseList("linkbi_decision_pro")
                .tableList("linkbi_decision_pro.tenant")
                .startupOptions(StartupOptions.initial())

                .deserializer(new DebeziumDeserializationSchema<String>() {
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> out) throws Exception {
                        HashMap<String, Object> hashMap = new HashMap<>();

                        Struct struct = (Struct) sourceRecord.value();
                        Struct source = struct.getStruct("source");

                        //获取操作类型
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        //获取数据本身
//                        Struct struct = (Struct)sourceRecord.value();
                        Struct after = struct.getStruct("after");
                        Struct before = struct.getStruct("before");
        /*
            1，同时存在 beforeStruct 跟 afterStruct数据的话，就代表是update的数据
             2,只存在 beforeStruct 就是delete数据
             3，只存在 afterStruct数据 就是insert数据
        */

                        if (after != null) {
                            //insert
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                hashMap.put(field.name(), after.get(field.name()));
                            }
                        }else if (before !=null){
                            //delete
                            Schema schema = before.schema();
                            for (Field field : schema.fields()) {
                                hashMap.put(field.name(), before.get(field.name()));
                            }
                        }else if(before !=null && after !=null){
                            //update
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                hashMap.put(field.name(), after.get(field.name()));
                            }
                        }

                        Gson gson = new Gson();
                        out.collect(gson.toJson(hashMap));
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .build();

        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Mysql-source");

//        streamSource.print("mysql");
        SingleOutputStreamOperator<String> filterSource = streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String dbStr = jsonObject.getString("DbStr");
                boolean flag = false;
                if (!dbStr.equals("")&&!dbStr.equals("#")){
                    String[] split = dbStr.split(";");
                    for (String s : split) {
                        if (s.equals("Server\u003d120.26.1.207,17477")){
                            flag=true;
                        }
                    }
                }
                return flag;
            }
        });
//        filterSource.print("filter");

        SingleOutputStreamOperator<String> mapSource = filterSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);

                String dbStr = jsonObject.getString("DbStr");
                String[] split = dbStr.split(";");
                jsonObject.put("IP", split[1].split("=")[1].split(",")[0]);
                jsonObject.put("Port", split[1].split("=")[1].split(",")[1]);
                jsonObject.put("User_id", split[2].split("=")[1]);
                jsonObject.put("Password", split[3].split("=")[1]);
                jsonObject.put("DataBase", split[4].split("=")[1]);
                jsonObject.remove("DbStr");

                return jsonObject.toJSONString();
            }
        });

//        mapSource.print("map");

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

                sql = "insert into db_model.tenant_test(\"" + StringUtils.join(set,"\",\"")
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
