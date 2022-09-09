package com.youfan.app

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingTask {
  def main(args: Array[String]): Unit = {

//    System.setProperty("HADOOP_USER_NAME","root")
    //准备streamContext
    val conf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .registerKryoClasses()
    val ssc = new StreamingContext(conf,Seconds(10))
    //准备读取kafka参数
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "iZrioqk6b370kwZ:9092",
      //组号，多次执行时需要将指针重置或者更新组号
      ConsumerConfig.GROUP_ID_CONFIG -> "spark",
      //每次允许拉取最大的消息数量
//      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "1000",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      //设置指针偏移重置从开始的数据开始
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      //设置开启自动提交
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"

    )

    //读取kafka数据
    val ds=KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set("aaatest"),kafkaParams))

//    ds.print()
//    ds.map(_.value()).print()
      ds.map(_.value()).flatMap(_.split(":"))
      .map((_,1)).reduceByKey(_+_)
      .print()

    val sdf1 = new SimpleDateFormat("yyyy-MM-dd-HH")
    val date = sdf1.format(new Date)
    val path = "hdfs://121.41.82.106:8020/spark/ck/" + date

//    spark中缓存和ck
//    ds.cache()
//    ds.checkpoint()

      ssc.start()
      ssc.awaitTermination()
  }
}
