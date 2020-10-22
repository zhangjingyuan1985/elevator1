package com.fatri.utils

import java.util
import java.util.Properties

import com.fatri.config.BundleUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

object SourceUtils {

  def getKafkaSource(props:Properties = null, topics:util.ArrayList[String]): FlinkKafkaConsumer[String] ={
    val kafkaHost = BundleUtils.getStringByKey("kafka.host")
    val props = new Properties()
    props.setProperty("auto.offset.reset", BundleUtils.getStringByKey("kafka.auto.offset"))
    props.setProperty("bootstrap.servers", kafkaHost)
    props.setProperty("group.id", BundleUtils.getStringByKey("kafka.group.id"))
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[StringDeserializer])
    new FlinkKafkaConsumer[String](topics, new SimpleStringSchema, props)
  }

}
