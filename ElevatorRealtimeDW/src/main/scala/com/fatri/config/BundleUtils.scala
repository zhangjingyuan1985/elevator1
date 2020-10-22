package com.fatri.config

import java.util.ResourceBundle

import org.slf4j.LoggerFactory

/**
 *
 */
object BundleUtils {

  val bundle = ResourceBundle.getBundle("flink")

  def getStringByKey(key: String): String ={
    bundle.getString(key)
  }

  def getIntByKey(key: String): Int={
    try{
      getStringByKey(key).toInt
    } catch {
      case e: Exception=>
        println("get value error, value is not Int type, default 0!", e.getMessage)
        0
    }
  }

  def getBooleanByKey(key: String):Boolean ={
    try{
      getStringByKey(key).toBoolean
    } catch {
      case e: Exception=>
        println("get value error, value is not Boolean type, default false!", e.getMessage)
        false
    }
  }

  private val LOG = LoggerFactory.getLogger(BundleUtils.getClass)

  def main(args: Array[String]): Unit = {
    val redisHost = BundleUtils.getStringByKey("zookeeper.host")
    println(redisHost)
    println(getIntByKey("redis.port"))
    println(getBooleanByKey("redis.database"))

    LOG.debug("debug")

  }

}
