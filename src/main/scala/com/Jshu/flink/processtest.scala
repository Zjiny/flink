package com.Jshu.flink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object processtest {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    streamEnv.setParallelism(1)


    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //导入隐式转换

    import org.apache.flink.streaming.api.scala._

    //读取数据

    val ds0: DataStream[String] = streamEnv.socketTextStream("Jshu1",6666)


    ds0.map((_,1)).keyBy(_._1).timeWindow(Time.seconds(10)).

      process(new ProcessWindowFunction[(String,Int),(String,Int),String,TimeWindow] {


        //第一次窗口关闭时触发，以后窗口关闭不会触发

        override def open(parameters: Configuration): Unit = {


          print("---------------")



        }

        //每个key调用一次

        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {

          out.collect((key,elements.size))

        }


      }).print()



    streamEnv.execute()



  }

}
