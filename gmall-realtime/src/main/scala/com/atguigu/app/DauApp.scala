package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    //1获取SparkConf
    val sc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    //2获取StreamingContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    //3获取kafka数据
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    //4将数据转换为样例类并补全logdate和loghour字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    kafkaDstream.mapPartitions(partition =>{
      partition.map(record =>{
        //a 将数据转换为样例类
        val startUplog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        //b 补全logdate和loghour字段
        val ts = startUplog.ts
        //c 将数据格式化为yyyy-MM-dd HH
         val dateHourStr: String = sdf.format(new Date(ts))
        //d 补全logdate
      startUplog.logDate =  dateHourStr.split(" ")(0)
      startUplog.logHour = dateHourStr.split(" ")(1)
      startUplog
      })
    })
    //5跨批次去重

    //6批次内去重

    //7将去重后的mid写入redis
 Dauhandler.saveMidToRedis(startUpLogDstream)
    //8 将明细写入Hbase

    9 开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
