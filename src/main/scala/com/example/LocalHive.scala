package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

class LocalHive {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)


  def createTables()(implicit spark: SparkSession) = {
    spark.sql("drop table IF EXISTS my_db.student_stage1")
    spark.sql("drop table IF EXISTS my_db.student_stage2")
    spark.sql("create database if not exists my_db")
    spark.sql("create table if not exists my_db.student_stage1(name string, marks int, id int) stored as orc")
    spark.sql("create table if not exists my_db.student_stage2(name string, marks int, id int) stored as orc")
    spark.sql("create table if not exists my_db.student(name string, marks int) partitioned by(id int) stored as orc")
  }

  def appendToStage(studId: Int)(implicit  spark:SparkSession) = {

    import spark.implicits._
    val df = Seq(("ABC",100,studId),
      ("ABC",90,studId),
      ("ABC",80,studId)).toDF("name","marks","id")

    Try{
      df.write.format("orc")
        .mode(SaveMode.Append)
        .option("encoding","utf-8")
        .option("path",s"C:\\Users\\sumit\\IdeaProjects\\ScalaSparkFuture\\spark-warehouse\\my_db.db\\my_db.student_stage${studId}")
        .insertInto(s"my_db.student_stage${studId}")

    } match{
      case Success(value) =>
      println(s"Append to Hive was Success: student${studId}")
        (studId,0)
      case Failure(exception) =>
        println(s"Append to Hive was Failure: student${studId}")
        (studId,1)
    }

  }

  def showTable()(implicit  spark:SparkSession) = {
    println(s"Print for Student")
    spark.read.table(s"my_db.student").show()
  }

  def insertToFinal(id:Int)(implicit  spark:SparkSession) = {
    val df = spark.read.table(s"my_db.student_stage${id}")
    df.createOrReplaceTempView(s"student_stage${id}_tmp")
    val query = s"INSERT OVERWRITE TABLE my_db.student PARTITION (id) SELECT * FROM student_stage${id}_tmp"

    spark.sql(query)

  }



}
