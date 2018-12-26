package com.yss.flink.dataSet.Pi

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * 大数定理的应用
  * 直径为2的圆和边长为2的正方形嵌套，随机打点，点落在圆内的概率就是Pi
  */
object FlinkPi {
  def main(args: Array[String]): Unit = {

    val num = if (args.length > 0) args(0).toLong else 100000l

    val env = ExecutionEnvironment.getExecutionEnvironment

    val count = env
      .generateSequence(1, num)
      .map { sample =>
      {
        val x = Math.random()
        val y = Math.random()
        val z = x * x + y * y
        if (z > 1) 0l else 1l
        }
      }
      .reduce(_+_)

    //以上数据计算是基于四分之一个图形
    val pi = count.map(_*4/num)

    printf("Pi is roughly : %s",pi)

  }
}
