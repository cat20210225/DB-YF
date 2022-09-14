package com.youfan.flinkApp;

import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.youfan.udf.MyDebezium;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class TestSqlServer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        ArrayList<String> list = new ArrayList<>();
        String uri = "hdfs://121.41.82.106:8020/data/flinkcdc/useripconf.txt";
        Configuration conf = new Configuration();
//        conf.set("dfs.client.use.datanode.hostname", "true");
        String user ="root";
        FileSystem fs = FileSystem.get(URI.create(uri),conf,user);
        FSDataInputStream in = fs.open(new Path(uri));

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line=reader.readLine()) != null) {
            list.add(line);
        }
//        fs.close();
        in.close();

        List<DataSourcePro> dataBaseNameList = new ArrayList<>();

        for (String s : list) {
            String[] split = s.split(",");
                DataSourcePro dataSourcePro = new DataSourcePro();
                dataSourcePro.setIp(split[0]);
                dataSourcePro.setPort(Integer.parseInt(split[1]));
                dataSourcePro.setDb(split[2]);
                dataSourcePro.setTb(split[3]);
                dataSourcePro.setUser(split[4]);
                dataSourcePro.setPwd(split[5]);
                dataBaseNameList.add(dataSourcePro);
        }



        for(DataSourcePro dataSourcePro:dataBaseNameList){
                SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                        .hostname(dataSourcePro.getIp())
                        .port(dataSourcePro.getPort())
                        .database(dataSourcePro.getDb())
                        .tableList(dataSourcePro.getTb())
                        .username(dataSourcePro.getUser())
                        .password(dataSourcePro.getPwd())
                        .startupOptions(StartupOptions.initial())
                        .deserializer(new MyDebezium())
                        .build();
            DataStreamSource<String> streamSource = env.addSource(sourceFunction);

            streamSource.print();
        }


        env.execute();
    }

}
