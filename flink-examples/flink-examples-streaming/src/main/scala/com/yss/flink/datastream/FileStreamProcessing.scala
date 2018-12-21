package com.yss.flink.datastream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

class FileStreamProcessing {
  def main(args: Array[String]): Unit = {
    var sourcePath = "hdfs://henghe-125:8020/flink/data/source.txt"
    var sinkPath = "hdfs://henghe-125:8020/flink/output/result.txt"
    val params = ParameterTool.fromArgs(args)
    sourcePath = if (params.has("sourcePath")) params.get("sourcePath") else sourcePath
    sinkPath  = if (params.has("sinkPath")) params.get("sinkPath") else sinkPath
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)


  }
}
