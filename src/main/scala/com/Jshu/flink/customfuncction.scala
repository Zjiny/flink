package com.Jshu.flink

import java.lang

import org.apache.flink.api.common.functions.AggregateFunction



//导入隐式转换

import org.apache.flink.streaming.api.scala._

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector




object customfuncction {



  def main(args: Array[String]): Unit = {


    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    streamEnv.setParallelism(1)



    //读取数据

    val ds0: DataStream[String] = streamEnv.socketTextStream("Jshu1",6666)


    ds0.map((_,1)).keyBy(0).window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(3))).
      aggregate(new myaggregate).print()
  }




}






