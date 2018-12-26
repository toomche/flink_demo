package com.yss.flink.dataSet.hdfsSource

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

object FileSourceBatchAnalyses {
  def main(args: Array[String]): Unit = {

    var sourcePath = "hdfs://henghe-125:8020/flink/data/source.txt"

    var sinkPath = "hdfs://henghe-125:8020/flink/output/result.txt"

    val params = ParameterTool.fromArgs(args)
    sourcePath = if (params.has("sourcePath")) params.get("sourcePath") else sourcePath
    sinkPath = if (params.has("sinkPath")) params.get("sinkPath") else sinkPath

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataset = env.readTextFile(sourcePath)

    val result = dataset
      .flatMap(_.split(" "))
      .filter(_.contains("error"))
      .map((_, 1l))
      .groupBy(0)
      .sum(1)

    result.writeAsText(sinkPath)

    env.execute("read file from hdfs")

  }
}
