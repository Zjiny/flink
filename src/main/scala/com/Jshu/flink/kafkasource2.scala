package com.Jshu.flink

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer

object kafkasource2 {

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

    //

    streamEnv.addSource(new FlinkKafkaConsumer[(String,String)](topic,new KafkaDeserializationSchema[(String, String)] {

      //是否是结束流
      override def isEndOfStream(t: (String, String)): Boolean = false


      //反序列化

      override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {


        (new String(consumerRecord.key()),new String(consumerRecord.value()))

      }


      //指定flink中的类型

      override def getProducedType: TypeInformation[(String, String)] = {


        createTuple2TypeInformation(createTypeInformation[String],createTypeInformation[String])

      }
    },properties))


  }




}
