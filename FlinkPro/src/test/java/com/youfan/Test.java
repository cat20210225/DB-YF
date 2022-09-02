package com.youfan;

import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.Validator;
import io.debezium.connector.sqlserver.SqlServerConnector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        Properties properties = new Properties();
//        properties.setProperty("debezium.snapshot.locking.mode", "none");


//        SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
//                .hostname("120.26.1.207")
//                .port(17477)
//                .database("test") // monitor sqlserver database
//                .tableList("dbo.Mytest") // monitor products table
//                .username("scm")
//                .password("HoIhrR64kTwZnCz9")
//                .startupOptions(StartupOptions.initial())
//                .deserializer(new MyDebezium1()) // converts SourceRecord to JSON String
////              .debeziumProperties(properties)
//                .build();
//
//
//        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
//
//        dataStreamSource.print();

        String DATABASE_SERVER_NAME = "sqlserver_transaction_log_source";
        String databaseList = "test"+","+"UFO_SCM_GDMNK";
        String tableList = "dbo.Mytest"+","+"dbo.FX_BasicData";

        Properties props = new Properties();
        props.setProperty("connector.class", SqlServerConnector.class.getCanonicalName());
        props.setProperty("database.server.name", DATABASE_SERVER_NAME);
        props.setProperty("database.hostname","120.26.1.207");
        props.setProperty("database.user","scm");
        props.setProperty("database.password","HoIhrR64kTwZnCz9");
        props.setProperty("database.port","17477");
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.dbname", String.join(",",databaseList));
        props.setProperty("table.include.list", String.join(",", tableList));

        DataStreamSource<String> streamSource = env.addSource(new DebeziumSourceFunction<>(new StringDebeziumDeserializationSchema(), props, null, new Validator() {
            @Override
            public void validate() {
            }
        }));
        env.execute();
    }
}
