package com.yss.flink.datastream.lowLevelFunction

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

class ProcessFunctionDemo {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    var input = ""
    val flag = params.has("input")
    if(flag){
      input = params.get("input")
    }else{
      println("error:the app run with wrong parameters")
      println("please use this pattern: --input <pathtofile> to run the app")
      System.exit(0)
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceStream = env.readTextFile(input)
    sourceStream
      .flatMap(_.split(" "))
      .map((_,1l))
      .keyBy(0)
      .process(new MyProcessFunction())

  }

}
