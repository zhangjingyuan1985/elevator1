package com.fatri.utils

import com.fatri.config.BundleUtils
import net.sf.json.JSONObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object SinkUtils {

  def getRedisSink(redisCommand: RedisCommand, additionalKey: String = "",
                   config: FlinkJedisPoolConfig = null): RedisSink[(String, String)] = {
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setDatabase(BundleUtils.getIntByKey("redis.database"))
      .setHost(BundleUtils.getStringByKey("redis.host"))
      .setPort(BundleUtils.getIntByKey("redis.port"))
      .build()
    new RedisSink[(String, String)](config, new RedisMapper[(String, String)] {
      override def getCommandDescription: RedisCommandDescription =
        new RedisCommandDescription(redisCommand, additionalKey)

      override def getKeyFromData(t: (String, String)): String = t._1

      override def getValueFromData(t: (String, String)): String = t._2
    })
  }

}
