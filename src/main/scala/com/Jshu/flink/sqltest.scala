package com.Jshu.flink

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

object sqltest {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv,settings)

    import org.apache.flink.table.api.scala._

    import org.apache.flink.streaming.api.scala._



    val ds0: DataStream[info] = streamEnv.socketTextStream("Jshu1",6666).map(line => {

      val arr: Array[String] = line.split(",")

      info(arr(0), Integer.parseInt(arr(1)))

    })

    //注册一张表

    tableEnv.registerDataStream("info",ds0,'name,'age)






    //得到table对象

    val t1: Table = tableEnv.fromDataStream(ds0)

    //val tableSource = new CsvTableSource("./test1.txt",Array("name","age"),Array(Types.STRING,Types.INT))



    val t2: Table = tableEnv.sqlQuery(s"select name from $t1")

    //  注册表

//    tableEnv.registerTableSource("info",tableSource)


//    val t1: Table = tableEnv.sqlQuery("select name ,sum(age) from info group by name")


    tableEnv.toRetractStream[Row](t2).print()


    tableEnv.execute("job1")



  }

}
case class info(name:String,age:Int)
