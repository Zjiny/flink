package com.Jshu.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object connected {

  def main(args: Array[String]): Unit = {


    //初始化流式计算的环境

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    streamEnv.setParallelism(1)

    //导入隐式转换

    import org.apache.flink.streaming.api.scala._


    val ds1: DataStream[(String, Int)] = streamEnv.fromElements(("jshu1",1),("Jshu2",2))

    val ds2: DataStream[Int] = streamEnv.fromElements(1,2,3)

    val ds3: ConnectedStreams[(String, Int), Int] = ds1.connect(ds2)


    val ds4: DataStream[(String, Int)] = ds3.map(t=>(t._1,t._2),("lm",_))

    ds4.print()

    streamEnv.execute()
  }

}
