package com.yss.flink.dataSet.wordCount

import com.yss.flink.dataSet.wordCount.util.WordCountData
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.StringUtils

object DataSetWordCount {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val source = if (params.has("inputPath"))
        env.readTextFile(params.get("inputPath"))
      else
        env.fromCollection(WordCountData.WORDS)
    val outputPath = if(params.has("outputPath")) params.get("outputPath") else "print"

    val result = source
      .flatMap(_.split(" "))
      .filter(_.equals("error"))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    if (StringUtils.isNullOrWhitespaceOnly(outputPath)){
      result.print()
    }else{
      result.writeAsText(outputPath)
    }

    env.execute("dataSet word count")
  }
}
