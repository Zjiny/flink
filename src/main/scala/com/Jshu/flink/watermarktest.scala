package com.Jshu.flink

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object watermarktest {


  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    streamEnv.setParallelism(1)

    streamEnv.getConfig.setAutoWatermarkInterval(500
    )





    //读取数据

    val ds0: DataStream[String] = streamEnv.socketTextStream("Jshu1",6666)



  }

}
