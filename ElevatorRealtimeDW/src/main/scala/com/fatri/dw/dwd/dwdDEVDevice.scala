package com.fatri.dw.dwd

import java.util
import java.util.concurrent.{ExecutorService, Executors}

import com.fatri.utils.{HBaseUtils, SinkUtils, SourceUtils}
import net.sf.json.JSONObject
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

/**
 * @Description //TODO 
 * @Author Happiless.zhang
 * @Date
 * @Version
 */
object dwdDEVDevice {

  def main(args: Array[String]): Unit = {

    val sourceNamespace = "ods"
    val targetNamespace = "dwd"
    val tableName = "DEV_DEVICE"
    val deviceTableName = "S002_DEV_FATRI_DEVICE"
    val elevatorTableName = "S002_DEV_FATRI_ELEVATOR"
    val sourceColumnFamily = "info"
    val targetColumnFamily = Array[String](deviceTableName, elevatorTableName)
    val consumerTopic = "ODS-S002-DEV-FATRI-DEVICE"
    val producerTopic = "DWD-DEV-DEVICE"
    val joinRowKeyField = "DeviceId"
    val jobName = "DWD-DEV-DEVICE"

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val topics = new util.ArrayList[String]()
    topics.add(consumerTopic)

    val result = streamEnv.addSource(SourceUtils.getKafkaSource(topics=topics))

    val filterDataSteam = result.filter(value => {
      try {
        val jsonObj = JSONObject.fromObject(value)
        null != jsonObj && jsonObj.size() > 0
      } catch {
        case e: Exception =>
          false
      }
    })

    val sink = filterDataSteam.map(value => {
      var jsonObj: JSONObject = null
      try {
        jsonObj = JSONObject.fromObject(value)
      } catch {
        case e: Exception =>
          println(e)
      }
      HBaseUtils.createTable(targetNamespace, tableName, targetColumnFamily)
      val rowKey = jsonObj.getString(joinRowKeyField.toLowerCase)
      val results: util.List[Result] = HBaseUtils.getRows(sourceNamespace, elevatorTableName, rowKey)
      val col2ValueMap: util.HashMap[String, String] = jsonToMap(jsonObj)
      import scala.collection.JavaConverters._
      val threadPool:ExecutorService=Executors.newFixedThreadPool(5)
      val targetObj = new JSONObject()
      for (result <- results.asScala.toList) {
        val familyMap = result.getFamilyMap(sourceColumnFamily.getBytes())
        val secondeCol2ValueMap: util.HashMap[String, String] = new util.HashMap[String, String]()
        for (entry <- familyMap.entrySet().asScala.toSet[util.Map.Entry[Array[Byte], Array[Byte]]]) {
          val key = Bytes.toString(entry.getKey)
          val value = Bytes.toString(entry.getValue)
          println(key, value)
          secondeCol2ValueMap.put(key, value)
        }

        try {
          threadPool.execute(new Runnable {
            override def run(): Unit = {
              HBaseUtils.insert(targetNamespace, tableName, rowKey, targetColumnFamily,
                Array[util.Map[String, String]](col2ValueMap, secondeCol2ValueMap))
            }
          })
        }finally {
          threadPool.shutdown()
        }

        val deviceObj = new JSONObject()
        val deviceIt = col2ValueMap.keySet().iterator()
        while (deviceIt.hasNext) {
          val key = deviceIt.next()
          deviceObj.put(key, col2ValueMap.get(key))
        }
        targetObj.put(deviceTableName, deviceObj)

        val elevatorObj = new JSONObject()
        val it = secondeCol2ValueMap.keySet().iterator()
        while (it.hasNext) {
          val key = it.next()
          elevatorObj.put(key, secondeCol2ValueMap.get(key))
        }
        targetObj.put(elevatorTableName, elevatorObj)
      }
      targetObj.toString()
    })

    sink.print()
    sink.addSink(SinkUtils.getKafkaSink(producerTopic))
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
