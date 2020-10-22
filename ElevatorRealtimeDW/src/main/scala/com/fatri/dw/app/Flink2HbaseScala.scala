//package com.fatri.dw.app
//
//import java.util
//
//import com.fatri.config.BundleUtils
//import com.fatri.utils.{HBaseUtils, SourceUtils}
//import net.sf.json.JSONObject
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//
///**
// *
// */
//object Flink2HbaseScala {
//
//  def main(args: Array[String]): Unit = {
//
//    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    streamEnv.enableCheckpointing(1000) // 非常关键，一定要设置启动检查点！！
//    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    import org.apache.flink.streaming.api.scala._
//
//    val topicArr = BundleUtils.getStringByKey("kafka.topics").split(",")
//    val topics: util.ArrayList[String] = new util.ArrayList[String]
//    for (topic <- topicArr) {
//      topics.add(topic)
//    }
//    val result = streamEnv.addSource(SourceUtils.getKafkaSource(topics=topics))
//    result.print("result")
//
//    result.addSink(new HBaseSink())
//
//    streamEnv.execute()
//  }
//
//}
//
//class HBaseSink(tableName: String = "",
//                columnFamily: String = "info") extends RichSinkFunction[String] {
//
//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
//  }
//
//  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
//    val jsonObj = JSONObject.fromObject(value)
//    val table = jsonObj.getString("table")
//    val detectTable = BundleUtils.getStringByKey("mysql.detect.table")
//    if (detectTable.contains(table)) {
//      HBaseUtils.createTable("ods1", table, columnFamily)
//      val database = jsonObj.getString("database")
//      val operateType = jsonObj.getString("type")
//      val id = jsonObj.getJSONArray("data").getJSONObject(0).getString("GUID")
//      val hostName = jsonObj.getJSONArray("data").getJSONObject(0).getString("HOSTNAME")
//      val columns = Array[String]("database", "operateType", "hostName")
//      val values = Array[String](database, operateType, hostName)
////      HBaseUtils.insert(table, id, columnFamily, columns, values)
//    }
//
//  }
//
//  override def close(): Unit = {
//    super.close()
//  }
//}
