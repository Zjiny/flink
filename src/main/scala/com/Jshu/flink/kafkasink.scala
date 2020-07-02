package com.Jshu.flink

import java.lang
import java.util.Properties

import akka.remote.serialization.StringSerializer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer

object kafkasink {


  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //导入隐式转换

    import org.apache.flink.streaming.api.scala._


    val kafka = "Jshu1:9092,Jshu2:9092,Jshu3:9092"

    val topic = "second"


    val deserializeClass = classOf[StringSerializer].getName

    //封装kafka参数

    val properties = new Properties()

    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,deserializeClass)

    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,deserializeClass)

    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka)


    val ds0: DataStream[String] = streamEnv.socketTextStream("Jshu1",6666)


    val ds1: DataStream[(String, Int)] = ds0.map((_,1))


    //(value) 数据

   // ds1.addSink(new FlinkKafkaProducer[String]("Jshu1:9092,Jshu2:9092,Jshu3:9092","first",new SimpleStringSchema()))




    //（k,v）

    ds1.addSink(new FlinkKafkaProducer[(String,Int)](
      topic,

      new KafkaSerializationSchema[(String, Int)] {
        override def serialize(element: (String, Int), timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {

          new ProducerRecord[Array[Byte], Array[Byte]](topic,element._1.getBytes(),element._2.toString.getBytes())


        }
      },

      properties,

      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    ))

    streamEnv.execute()





  }

}
