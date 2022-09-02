package com.youfan.util;

import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.youfan.udf.MyDebezium1;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SourceUtil {


    public static SourceFunction<String> getConfig(String hostname, int port, String database, String username, String password) {

        SourceFunction<String> configSource = SqlServerSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .database(database) // monitor sqlserver database
//                .tableList(table) // monitor products table
                .username(username)
                .password(password)
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDebezium1()) // converts SourceRecord to JSON String
                .build();

        return configSource;
    }
    public static SourceFunction<String> getConfig(String hostname, int port, String database, String table, String username, String password) {

        SourceFunction<String> configSource = SqlServerSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .database(database) // monitor sqlserver database
                .tableList(table) // monitor products table
                .username(username)
                .password(password)
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDebezium1()) // converts SourceRecord to JSON String
                .build();

        return configSource;
    }

}
