package com.Jshu.flink

import java.net.URL

import org.apache.flink.api.scala.ExecutionEnvironment


object wordcountbatch {

  def main(args: Array[String]): Unit = {

    //创建Env

    val executionEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //导入依赖

    import org.apache.flink.api.scala._

    //获取资源路径
    val url: URL = getClass.getResource("/kafka.properties")


    val ds0: DataSet[String] = executionEnv.readTextFile(url.toString)

    ds0.map((_,1)).groupBy(0).sum(1).print()


  }

}
