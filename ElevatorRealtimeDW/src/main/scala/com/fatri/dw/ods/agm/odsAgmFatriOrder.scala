package com.fatri.dw.ods.agm

import java.util

import com.fatri.utils.{HBaseUtils, SinkUtils, SourceUtils}
import net.sf.json.JSONObject
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Description //TODO 
 * @Author Happiless.zhang
 * @Date
 * @Version
 */
object odsAgmFatriOrder {

  def main(args: Array[String]): Unit = {

    val mysqlTableName = "fatri_order"
    val namespace = "ods"
    val tableName = "S002_AGM_FATRI_ORDER"
    val columnFamily = "info"
    val consumerTopic = "example,example4"
    val topic = "ODS-S002-AGM-FATRI-ORDER"
    val rowKeyField = "OrderId"
    val jobName = "ODS-S002-AGM-FATRI-ORDER"
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val topics = new util.ArrayList[String]()
    for (t <- consumerTopic.split(",")) {
      topics.add(t)
    }
    val result = streamEnv.addSource(SourceUtils.getKafkaSource(topics=topics))
    val odsDatasTream: DataStream[String] = result.filter(value => {
      try {
        val jsonObj = JSONObject.fromObject(value)
        val table = jsonObj.getString("table")
        mysqlTableName.toLowerCase.equals(table.toLowerCase)
      } catch {
        case e:Exception=>
          println(e)
          false
      }
    })
    odsDatasTream.print("odsDatasTream")
    val sink = odsDatasTream.map(value => {
      var jsonObj: JSONObject = null
      try {
        jsonObj = JSONObject.fromObject(value)
      } catch {
        case e: Throwable =>
          println(e)
      }
      HBaseUtils.createTable(namespace, tableName, columnFamily)
      val data = jsonObj.getJSONArray("data").getJSONObject(0)
      val rowKey = data.getString(rowKeyField)
      val col2ValueMap: util.HashMap[String, String] = jsonToMap(data)
      new Thread(new Runnable {
        override def run(): Unit = {
          HBaseUtils.insert(namespace, tableName, rowKey, columnFamily, col2ValueMap)
        }
      }).start()
      val obj = new JSONObject()
      val it = col2ValueMap.keySet().iterator()
      while (it.hasNext) {
        val key = it.next()
        obj.put(key, col2ValueMap.get(key))
      }
      obj.toString()
    })
    sink.addSink(SinkUtils.getKafkaSink(topic))
    streamEnv.execute(jobName)
  }

  def jsonToMap(jsonObj: JSONObject): util.HashMap[String, String] = {
    val jsonKey = jsonObj.keySet()
    val iter = jsonKey.iterator()
    val map: util.HashMap[String, String] = new util.HashMap[String, String]()
    while (iter.hasNext) {
      val key = iter.next()
      val value = jsonObj.get(key).toString
      map.put(key.toString.toLowerCase, value)
      println("===key====：" + key + "===value===：" + value)
    }
    map
  }

}
