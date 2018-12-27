package com.yss.flink.datastream.windowing

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

class MultiSourceWindow {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.fromElements("this is a long sentence")

    val stream2 = env.fromElements("this is another long sentence")

    val kvStream1 = stream1
      .flatMap(_.split(" "))
      .filter(!_.contains("error"))
      .map((_,1l))
      .keyBy(0)

    val kvStream2 = stream2
      .flatMap(_.split(" "))
      .filter(!_.contains("error"))
      .map((_,1l))
      .keyBy(0)

    val result1 = kvStream1.join(kvStream2)

    val result2 = kvStream1
      .union(kvStream2)
      .timeWindowAll(Time.seconds(5))

    println(result1)

    println(result2)

    env.execute("join test")
  }
}
