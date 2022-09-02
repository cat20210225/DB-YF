package com.youfan.flinkTest;

import com.youfan.util.SinkUtil;
import com.youfan.util.SourceUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Mutiple {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.1 开启Checkpoint,每隔1分钟做一次CK
        env.enableCheckpointing(60000L);
        //2.2 指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次C+K数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        SimpleDateFormat sdf1= new SimpleDateFormat("yyyy-MM-dd-HH");
        String date = sdf1.format(new Date());
        String path = "hdfs://iZrioqk6b370kwZ:8020/data/flinkcdc/ck/"+date;
        env.setStateBackend(new FsStateBackend(path));
        //2.6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "root");
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        SourceFunction sourceFunction1 = SourceUtil.getConfig("120.26.1.207",17477,"test","dbo.Mytest","scm","HoIhrR64kTwZnCz9");

        SourceFunction sourceFunction2 = SourceUtil.getConfig("120.26.1.207",17477,"test","dbo.configuration","scm","HoIhrR64kTwZnCz9");

        DataStreamSource<String> dataStreamSource1 = env.addSource(sourceFunction1);
        DataStreamSource<String> dataStreamSource2 = env.addSource(sourceFunction2);

        dataStreamSource1.print();
        dataStreamSource2.print();

        dataStreamSource1.sinkTo(SinkUtil.getSink());
        dataStreamSource2.sinkTo(SinkUtil.getSink());
        env.execute();




    }
}
