package com.yss.flink.datastream.windowing

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SessionWindow {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val output = params.has("output")
    val input = List(
      ("a", 1L, 1),
      ("b", 1L, 1),
      ("b", 3L, 1),
      ("b", 5L, 1),
      ("c", 6L, 1),
      // We expect to detect the session "a" earlier than this point (the old
      // functionality can only detect here when the next starts)
      ("a", 10L, 1),
      // We expect to detect session "b" and "c" at this point as well
      ("c", 11L, 1)
    )

    val source = env.addSource(new SourceFunction[String,Long, Int]() {
      override def run(sourceCtx: SourceFunction.SourceContext[String]): Unit = {
        input.foreach(value => {
          sourceCtx.collectWithTimestamp(value._1, value._2)
          sourceCtx.emitWatermark(new Watermark(value._2-1))
        })
        sourceCtx.emitWatermark(new Watermark(Long.MaxValue))
      }

      override def cancel(): Unit = {}
    })

    val result = source
      .keyBy(0)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(100l)))
      .sum(2)

    if (output){
      result.writeAsText(params.get("output"))
    }else{
      print("Printing result to log. Use --output to specify output path when start.")
      result.print()
    }
  }
}
