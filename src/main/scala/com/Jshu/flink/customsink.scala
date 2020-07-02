package com.Jshu.flink






import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object customsink {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //导入隐式转换

    import org.apache.flink.streaming.api.scala._


    streamEnv.setParallelism(1)





    val ds0: DataStream[String] = streamEnv.socketTextStream("Jshu1",6666)





    ds0.addSink(new mysqlsink)

    streamEnv.execute()

  }

}

class mysqlsink extends RichSinkFunction[String]{

  var con:Connection=_

  var preobj:PreparedStatement =_

  override def open(parameters: Configuration): Unit = {

    //加载驱动

    Class.forName("com.mysql.jdbc.Driver")

    //创建连接

    con = DriverManager.getConnection("jdbc:mysql://Jshu1:3306/jshu","root","123456")

    //获取预编译对象

    preobj = con.prepareStatement("insert into bigdata (name) values(?)")

  }


  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {

      preobj.setString(1,value)

      preobj.executeUpdate()

  }

  override def close(): Unit = {

    preobj.close()

    con.close()


  }
}
class myprocessFunction extends KeyedProcessFunction[String,(String,Int),String]{

  //上一次数据的状态值

  val lasttemp: ValueState[Int] = getRuntimeContext.getState[Int](new ValueStateDescriptor[Int]("lasttemp",classOf[Int]))


  //定时器时间

  val lasttime: ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("lasttemp",classOf[Long]))



  override def processElement(value: (String, Int), ctx: KeyedProcessFunction[String, (String, Int), String]#Context, out: Collector[String]): Unit =  {





    //注册定时器

    //1.如果定时器不存在

    if(lasttime.value() == 0){

      ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+10000L)


      lasttemp.update(value._2)

    }
    else {

      // 如果定时器存在，并且上次的数据的状态值大于当前数据

      if(lasttime.value() > 0 && lasttemp.value() > value._2 ){

        //删除定时器，清空状态，更新数据状态值

        ctx.timerService().deleteEventTimeTimer(lasttime.value())

        lasttime.clear()

        lasttemp.update(value._2)
      }



    }



  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Int), String]#OnTimerContext, out: Collector[String]): Unit = {





  }
}