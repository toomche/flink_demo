package com.yss.flink.datastream.socket

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount {
  def main(args: Array[String]): Unit = {
    var host = "localhost"
    var port = 10000
    if (args.length != 0) {
      val params = ParameterTool.fromArgs(args)
      host = if (params.get("hostname") != null) params.get("hostname") else "localhost"
      port = if (params.getInt("port") != null) params.getInt("port") else 10000
    }

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
    env.socketTextStream(host, port, '\n')
      .flatMap(_.split(","))
      .map((_, 1l))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("com.yss.flink.dataStream.SocketWindowWordCount")

  }
}