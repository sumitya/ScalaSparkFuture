package com.example

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

class ScalaFuture {

  def Fut() = {
    val vanilaDo1 = Await.result(donutStock("Vanila Donut"),Duration.Inf)
    val vanilaDo2 = Await.result(donutStock("Vanila Donut"),Duration.Inf)

    val resArr = Array(vanilaDo1,vanilaDo2)
    resArr.map{
      res =>

        println(s"running for ${res}")

    }
    println(s"Stock of Vanila Donut ${vanilaDo1}")

  }

  def donutStock(donut:String): Future[Int] = Future{

    println("checking donut stock")
    10
  }

}
