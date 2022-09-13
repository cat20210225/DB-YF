package com.youfan.app

import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
//  --设置虚拟用户访问Hadoop
//    System.setProperty("HADOOP_USER_NAME","root")
//todo （也叫闭包检查）用到driver端对象（也就是算子外new的对象），需要序列化，用case（样例类）即可
    val conf = new SparkConf().setAppName("aa").setMaster("local[*]")

//序列化的优化，用KryoSerializer代替Java序列化
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("E:\\idea\\DB-YF\\sparkck")

    val value = sc.textFile("E:\\idea\\DB-YF\\test.txt")
      /**
       *
       */
//    val rdd = sc.textFile("hdfs://lijie:9000/checkpoint0727/c1a51ee9-1daf-4169-991e-b290f88bac20/rdd-0/part-00000").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    /**
     * 偏函数写法map {
     * case a => (a, 1)
     * }
     */
    val value1 = value.flatMap(_.split(" ")).map {
      case a => (a, 1)
    }
//todo DAG有向无环图，当从缓存中获取数据，可以从web页面看到，缓存前的stag是灰色的（也就是没有从头计算，没有走stag阶段，直接从stag获取数据了）
//    val value2 = value.flatMap(_.split(" ")).map((_, 2))
//todo cache默认就是放内存，底层用的还是persist
    value1.cache()
//    todo persist，可以指定存储类型（磁盘还是内存）
//    value1.persist()
//    todo checkpoint只能存磁盘
    value1.checkpoint()
    value1.foreach(println)
    //coalesce算子指定分区数，保存到哪
    value1.coalesce(1).saveAsTextFile("E:\\idea\\DB-YF\\sparkoutput")
    value1.unpersist()
    sc.stop()

  }
}
