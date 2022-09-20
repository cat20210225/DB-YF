package com.youfan.common;

/**
 * Desc: 项目常用配置
 */
public class GmallConfig {
    public static final String HBASE_SCHEMA="GMALL2021_REALTIME";
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop102:8123/default";

    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}