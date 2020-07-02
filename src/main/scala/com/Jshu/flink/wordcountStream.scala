package com.Jshu.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

object wordcountStream {

  def main(args: Array[String]): Unit = {

    //初始化流式计算的环境

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //导入隐式转换

    import org.apache.flink.streaming.api.scala._



    //读取数据

    val ds0: DataStream[String] = streamEnv.socketTextStream("Jshu1",6666)

    //数据处理

    val ds1: DataStream[(String, Int)] = ds0.map((_,1)).keyBy(0).reduce((x,y)=>{
      (x._1,x._2+y._2)
    })

    ds1.print("结果")

    //程序启动

    streamEnv.execute("wordcount")



  }

}
