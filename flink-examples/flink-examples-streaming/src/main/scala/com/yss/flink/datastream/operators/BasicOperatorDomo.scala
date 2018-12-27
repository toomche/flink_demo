package com.yss.flink.datastream.operators

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

class BasicOperatorDomo {

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val flag = params.has("output")
    var output = ""
    if (flag){
      output = params.get("output")
    }else{
      println("error:need output for result, please run program use --output <pathtostore>")
      System.exit(0)
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = env.fromElements("this is the source data, it is a sentence", "this is another sentence")

    val result = source
//      .flatMap {
//        _.split(" ")
//      }
      .filter(_.equals("the"))
      .map((_, 1l))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .rescale()
      .writeAsText(output)

    env.execute("operators demo")
  }

}
