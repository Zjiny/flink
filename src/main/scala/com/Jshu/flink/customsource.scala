package com.Jshu.flink

import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

object customsource {


  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)


    //导入隐式转换

    import org.apache.flink.streaming.api.scala._


    val ds0: DataStream[String] = streamEnv.addSource(new SourceFunction[String] {

      //定义flag

      var running = true

      override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {

        while (running) {


          (1 to 10).map(x => {

            x + "-------" + "jshu" + System.currentTimeMillis()
          }).foreach(

            sourceContext.collect(_)
          )


          Thread.sleep(2000)

        }


      }


      //数据流停止

      override def cancel(): Unit = {

        running = false


      }
    })


    val ds1: SplitStream[String] = ds0.split(str => {
      if (Integer.parseInt(str.charAt(0).toString) < 5) {

        Seq("<5")

      }
      else {

        Seq(">=5")
      }
    })




    val low: DataStream[String] = ds1.select("<5")

    val high: DataStream[String] = ds1.select(">=5")



    low.print("low")

    high.print("high")

    streamEnv.execute()

  }





}
