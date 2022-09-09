package com.youfan.flinkTest;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.youfan.udf.MyDebezium1;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class ToHdfs {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);


        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔30秒做一次CK
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(8));
        //2.2 指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次C+K数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 30000L));
        //2.5 设置状态后端
        SimpleDateFormat sdf1= new SimpleDateFormat("yyyy-MM-dd-HH");
        String date = sdf1.format(new Date());
        String path = "hdfs://121.41.82.106:8020/flink/flinkcdc/ck/"+date;
        env.setStateBackend(new FsStateBackend(path));
        //2.6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "root");
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //超时时间30秒
        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(10));
        //两个checkpoint间隔最小为500毫秒（第一个ck结束后至少过5毫秒才开始下一个ck）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(4));

        //todo 广播流
        SourceFunction<String> pgSource = PostgreSQLSource.<String>builder()
                .hostname("121.41.82.106")
                .port(45565)
                .database("postgres") // monitor postgres database
                .schemaList("db_model")  // monitor inventory schema
                .tableList("db_model.tenant_test") // monitor products table
                .username("postgres")
                .password("Qweasd987@#")
                .decodingPluginName("pgoutput")
                .deserializer(new MyDebezium1()) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> pgStream = env.addSource(pgSource);

        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("guid", String.class, String.class);
        BroadcastStream<String> broadcastStream = pgStream.broadcast(mapStateDescriptor);

        String outputPath = "hdfs://121.41.82.106:8020/flink/flinkcdc";
        BucketingSink hadoopSink = new BucketingSink<String>(outputPath);
        // 使用表名命名存储区
        hadoopSink.setBucketer(new Bucketer() {
            @Override
            public org.apache.hadoop.fs.Path getBucketPath(Clock clock, org.apache.hadoop.fs.Path path, Object o) {
                String table = JSONObject.parseObject((String) o).getString("table");
                return new Path(outputPath + "/" + table);
            }
        });
        // 下述两种条件满足其一时，创建新的块文件
        // 条件1.设置块大小为128MB
        hadoopSink.setBatchSize(1024 * 1024 * 128);
        // 条件2.设置时间间隔1个小时
        hadoopSink.setBatchRolloverInterval(60 * 60 * 1000);
        //设置的是检查两次检查桶不活跃的情况的周期
        hadoopSink.setInactiveBucketCheckInterval(TimeUnit.MINUTES.toMillis(2));
        //设置的是关闭不活跃桶的阈值,多久时间没有数据写入就关闭桶
        hadoopSink.setInactiveBucketThreshold(TimeUnit.MINUTES.toMillis(6));
        hadoopSink.setPartPrefix("ods");
        hadoopSink.setPartSuffix(".txt");

//        todo 加载主流配置文件
        ArrayList<String> list = new ArrayList<>();
        String uri = "hdfs://121.41.82.106:8020/flink/flinkcdc/useripconf3.txt";
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
                    .startupOptions(StartupOptions.initial())
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
                    if (guid != null) {
                        jsonObject.put("database", guid);
                        out.collect(jsonObject.toJSONString());
                    } else {
                        out.collect(jsonObject.toJSONString());
                    }

                }

                @Override
                public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String dataBase = jsonObject.getString("DataBase");
                    String guid = jsonObject.getString("guid");
                    BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                    broadcastState.put(dataBase, guid);

                }
            });
            processDS.addSink(hadoopSink);

        env.execute();
    }

}

