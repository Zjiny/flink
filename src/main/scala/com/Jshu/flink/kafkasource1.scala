package com.Jshu.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer


object kafkasource1 {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //导入隐式转换

    import org.apache.flink.streaming.api.scala._


    //kafka参数

    val kafka = "Jshu1:9092,Jshu2:9092,Jshu3:9092"

    val topic = "first"

    val groupid = "flink"

    val deserializeClass = classOf[StringDeserializer].getName

    //封装kafka参数

    val properties = new Properties()

    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid)

    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,deserializeClass)

    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,deserializeClass)

    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka)

    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

    val ds0: DataStream[String] = streamEnv.addSource(new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),properties))


    ds0.print()

    streamEnv.execute()

  }

}
