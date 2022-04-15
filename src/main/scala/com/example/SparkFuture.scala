package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object SparkFuture {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    implicit val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("hive.exec.dynamic.partition","true")
      .enableHiveSupport()
      .getOrCreate()

    val lh = new LocalHive()
    lh.createTables()

    val f1 = Future{lh.appendToStage(1)}
    val f2 = Future{lh.appendToStage(2)}

    val res1 = Await.result(f1,Duration.Inf)
    val res2 = Await.result(f2,Duration.Inf)

    val resArr = Array(res1,res2)
    resArr.foreach{

      case(id,res) =>
        lh.insertToFinal(id)
        println(s"Write to Student done for ${id} and ${res}")

    }

    lh.showTable()
  }
}
