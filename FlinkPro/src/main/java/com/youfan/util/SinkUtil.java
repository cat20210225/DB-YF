package com.youfan.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkUtil {
    public static FileSink<String> getSink(){

        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("ods")
                .withPartSuffix(".txt")
                .build();

        String outputPath = "hdfs://iZrioqk6b370kwZ:8020/data/flinkcdc";

        final FileSink<String> sink = FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        //  时长 滚动切割
                        .withRolloverInterval(TimeUnit.HOURS.toMillis(1))
                        // 空闲，滚动切割
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                        // 大小 滚动切割,
                        .withMaxPartSize(1024 * 1024 * 128)
                        .build())
                // 按自定义字段划分目录
                .withBucketAssigner(new BucketAssigner<String, String>() {
                    @Override
                    public String getBucketId(String element, Context context) {

//                        SimpleDateFormat sdf1= new SimpleDateFormat("yyyy-MM-dd--HH");
////                            SimpleDateFormat sdf2 = new SimpleDateFormat("HH");
//                        String date = sdf1.format(new Date());
                        // date/account/dataSourceId/result.txt
                        try {
                            // 重点！！！ 根据内容，自定义路径位置

                            String table = JSONObject.parseObject(element).getString("table");

                            return table;
                        } catch (NumberFormatException e) {
                            return "unknow-source";
                        }
                    }

                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return SimpleVersionedStringSerializer.INSTANCE;
                    }
                })
                .withOutputFileConfig(config)
                // 判断是否结束文件 的间隔时间
                .withBucketCheckInterval(TimeUnit.MINUTES.toMillis(1))
                .build();


        return sink;
    }
}
