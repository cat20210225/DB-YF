package com.youfan;

import com.alibaba.fastjson.JSONObject;
import com.sun.istack.Nullable;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.youfan.flinkTest.DataSourcePro;
import com.youfan.udf.MyDebezium;
import com.youfan.udf.MyDebezium1;
import com.youfan.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestCollect {
    public static void main(String[] args) throws Exception {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(3);


            //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
            //2.1 开启Checkpoint,每隔5分钟做一次CK
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(5));
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
        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(8));
        //两个checkpoint间隔最小为500毫秒（第一个ck结束后至少过5毫秒才开始下一个ck）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(3));


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

        unionds.print("转换数据");  

        unionds.addSink(new FlinkKafkaProducer<String>("iZrioqk6b370kwZ:9092","bbbtest",new SimpleStringSchema()));

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
