package com.youfan.flinkApp;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.youfan.udf.MyDebezium1;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Flink_Flume_Hdfs {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);


        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5分钟做一次CK
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(5));
        //2.2 指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次C+K数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 60000L));
        //2.5 设置状态后端
        SimpleDateFormat sdf1= new SimpleDateFormat("yyyy-MM-dd-HH");
        String date = sdf1.format(new Date());
        String path = "hdfs://121.41.82.106:9002/flume/ck/"+date;
        env.setStateBackend(new FsStateBackend(path));
        //2.6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "root");
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //超时时间8分钟
        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(8));
        //两个checkpoint间隔最小为5分钟（第一个ck结束后至少过1分钟才开始下一个ck）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(5));

        //todo 广播流
        SourceFunction<String> pgSource = PostgreSQLSource.<String>builder()
                .hostname("121.41.82.106")
                .port(45565)
                .database("postgres") // monitor postgres database
                .schemaList("db_model")  // monitor inventory schema
                .tableList("db_model.tenant_test","db_model.dict_tenant_data_source_test") // monitor products table
                .username("postgres")
                .password("Qweasd987@#")
                .decodingPluginName("pgoutput")
                .deserializer(new MyDebezium1()) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> pgStream = env.addSource(pgSource);

        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("guid",String.class,String.class);
        BroadcastStream<String> broadcastStream = pgStream.broadcast(mapStateDescriptor);


//        todo 加载主流配置文件
        ArrayList<String> list = new ArrayList<>();
        String uri = "hdfs://121.41.82.106:9002/files/useripconf2.txt";
        org.apache.hadoop.conf.Configuration conf = new Configuration();
//        conf.set("dfs.client.use.datanode.hostname", "true");
        String user ="root";
        FileSystem fs = FileSystem.get(URI.create(uri),conf,user);
        FSDataInputStream in = fs.open(new org.apache.hadoop.fs.Path(uri));

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line=reader.readLine()) != null) {
            list.add(line);
        }
//        fs.close();
        in.close();

        List<DataSourcePro> dataBaseNameList = new ArrayList<>();

        for (String s : list) {
            String[] split = s.split(" ");
            DataSourcePro dataSourcePro = new DataSourcePro();
            dataSourcePro.setIp(split[0]);
            dataSourcePro.setPort(Integer.parseInt(split[1]));
            dataSourcePro.setDb(split[2]);
            dataSourcePro.setTb(split[3]);
            dataSourcePro.setUser(split[4]);
            dataSourcePro.setPwd(split[5]);
            dataBaseNameList.add(dataSourcePro);
        }

        DataStream<String> unionds = null;
        //todo 主流
        for(DataSourcePro dataSourcePro:dataBaseNameList) {
            SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                    .hostname(dataSourcePro.getIp())
                    .port(dataSourcePro.getPort())
                    .database(dataSourcePro.getDb())
                    .tableList(dataSourcePro.getTb())
                    .username(dataSourcePro.getUser())
                    .password(dataSourcePro.getPwd())
                    .startupOptions(StartupOptions.latest())
                    .deserializer(new MyDebezium1())
                    .build();
            DataStreamSource<String> streamSource = env.addSource(sourceFunction);

            if (unionds != null) {
                unionds = streamSource.union(unionds);
            } else {
                unionds = env.addSource(sourceFunction);
            }
        }

        BroadcastConnectedStream<String, String> connectDS = unionds.connect(broadcastStream);
        SingleOutputStreamOperator<String> processDS = connectDS.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                JSONObject jsonObject = JSONObject.parseObject(value);
                String database = jsonObject.getString("database");
                String guid = broadcastState.get(database);
                String tenantid = broadcastState.get(database+"_db");
                if (guid != null) {
                    jsonObject.put("database", guid);
//                    out.collect(jsonObject.toJSONString());
                }
                if (tenantid != null){
                    jsonObject.put("tenantid", tenantid);
                }
                out.collect(jsonObject.toJSONString());
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String dataBase = jsonObject.getString("DataBase");
                String db_name = jsonObject.getString("db_name");
                String tenant_id = jsonObject.getString("tenant_id");
                String guid = jsonObject.getString("guid");
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                broadcastState.put(dataBase, guid);
                broadcastState.put(db_name+"_db",tenant_id);

            }
        });

//        processDS.print("转换数据");

        processDS.addSink(new FlinkKafkaProducer<String>("iZrioqk6b370kwZ:9092","bbbtest",new SimpleStringSchema()));

//        unionds.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<String>() {
//            @Override
//            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
//                return new ProducerRecord<>(JSONObject.parseObject(element).getString("table"),element.getBytes());
//            }
//        }));


//        String log = new String("body".getBytes(), StandardCharsets.UTF_8);

        env.execute();


    }
}
