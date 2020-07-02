package com.Jshu.flink

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.sources.{CsvTableSource, TableSource}
import org.apache.flink.types.Row

object tabletest {


  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv,settings)

    import org.apache.flink.table.api.scala._

    import org.apache.flink.streaming.api.scala._


    val ds0: DataStream[String] = streamEnv.socketTextStream("Jshu1",6666)

    val ds1: DataStream[info] = ds0.map(line => {

      val arr: Array[String] = line.split(" ")

      info(arr(0), Integer.parseInt(arr(1)))

    })

    val table: Table = tableEnv.fromDataStream(ds1)


    table.window(Tumble.over("5.second").on("call").as("window"))


    val table2: Table = table.groupBy('name).select('name,'age.sum)


    tableEnv.toRetractStream[Row](table2).filter(_._1 == true).print()



//    tableEnv.registerDataStream("student",ds1)
//
//    val table: Table = tableEnv.scan("student")
//
//    table.printSchema()
//
    streamEnv.execute()





    //批处理

//    val tableSource = new CsvTableSource("./test1.txt",Array("name","age"),Array(Types.STRING,Types.INT))






  }

}


