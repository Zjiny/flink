import java.io.FileInputStream
import java.net.URL
import java.util.Properties

import scala.tools.nsc.interpreter.InputStream

object PropertyUtils {

  def main(args: Array[String]): Unit = {

    val properties: Properties = PropertyUtils.load("kafka.properties")


    println(properties.getProperty("kafka.bootstrap.servers"))

  }

  def load(propertyName:String):Properties={

    val pro = new Properties()

    pro.load(this.getClass.getResourceAsStream(propertyName))

    pro



  }

}
