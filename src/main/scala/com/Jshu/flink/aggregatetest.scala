package com.Jshu.flink

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object aggregatetest {

  def main(args: Array[String]): Unit = {


    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //导入隐式转换

    import org.apache.flink.streaming.api.scala._

    //读取数据

    val ds0: DataStream[String] = streamEnv.socketTextStream("Jshu1",6666)


    ds0.map((_,1)).keyBy(_._1).window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(3))).

      aggregate(new AggregateFunction[(String,Int),Int,Int] {

        override def createAccumulator(): Int = 0

        override def add(value: (String, Int), accumulator: Int): Int = value._2 + accumulator

        override def getResult(accumulator: Int): Int = accumulator

        override def merge(a: Int, b: Int): Int = a + b

      },new WindowFunction[Int,(String,Int),String,TimeWindow] {

        override def apply(key: String, window: TimeWindow, input: Iterable[Int], out: Collector[(String, Int)]): Unit = {

          out.collect((key,input.iterator.next()))
        }

      })


    streamEnv.execute()

  }








}

class myaggregate extends AggregateFunction[(String,Int),(String,Int),(String,Int)]{


  override def createAccumulator(): (String, Int) = (null,0)

  override def add(in: (String, Int), acc: (String, Int)): (String, Int) = (in._1,in._2 + acc._2)

  override def getResult(acc: (String, Int)): (String, Int) = acc

  override def merge(acc: (String, Int), acc1: (String, Int)): (String, Int) = (acc1._1,acc._2 + acc1._2)
}

