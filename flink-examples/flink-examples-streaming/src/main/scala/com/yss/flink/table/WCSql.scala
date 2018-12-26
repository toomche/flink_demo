package com.yss.flink.table

import com.yss.flink.table.entity.WC
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

/**
  * common processing of table api include:
  * 1.register a table
  * 2.query a table
  * 3.emit a table
  */
class WCSql {

  //case class
  case class WordCount(word: String, count: Int)

  def main(args: Array[String]): Unit = {
    //get running environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tabEnv = TableEnvironment.getTableEnvironment(env)

    val source = env.fromElements(new WC("Hello", 1), new WC("Hello", 1),
      new WC("Hello", 1), new WC("Hello", 1), new WC("Hello", 1),
      new WC("Hello", 1), new WC("world", 1), new WC("world", 1),
      new WC("world", 1), new WC("world", 1), new WC("world", 1))

    //convert source to table
    val table:Table = source.asInstanceOf[Table]

    /**
      * three ways to register a table
      * (1)tableEnv.registerTable("table1", ...)           // or
      * (2)tableEnv.registerTableSource("table2", ...)     // or
      * (3)tableEnv.registerExternalCatalog("extCat", ...)
      */
    tabEnv.registerTable("WordCount",table)

    //register a table sink to store data
    //tabEnv.registerTableSink("outputSink",)

    //query a table
    val queryResult = tabEnv.scan("WordCount").select("select count(*) from WordCount where word = 'hello'")

    //add result to table sink,emit query result
    queryResult.insertInto("outputSink")

    env.execute("table register, query and emit")
  }
}
