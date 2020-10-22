package com.fatri.dw.app.elevator

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.fatri.utils.{HBaseUtils, SourceUtils}
import net.sf.json.JSONObject
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

/**
 * @Description //TODO 
 * @Author Happiless.zhang
 * @Date
 * @Version
 */
object elevatorTTLMap {

  def main(args: Array[String]): Unit = {
    val sourceNamespace = "dwd"
    val targetNamespace = "elevator"
    val targetTableName = "ELEVATOR_TTL_MAP"
    val deviceTableName = "S002_DEV_FATRI_DEVICE"
    val elevatorTableName = "S002_DEV_FATRI_ELEVATOR"
    val orderTableName = "AGM_ORDER"
    val deviceField = "DeviceId"
    val lastDtm = "LastDtm"
    val elevatorField = "ElevatorId"
    val longitude = "Longitude"
    val latitude = "Latitude"
    val orderId = "OrderId"
    val processStatus = "ProcessStatus"
    val statusField = "Status"
    val sourceColumnFamily = Array[String](deviceTableName, elevatorTableName)
    val targetColumnFamily = "info"
    val consumerTopic = "DWD-DEV-DEVICE"
    val joinRowKeyField = "DeviceId"
    val jobName = "ELEVATOR_TTL_MAP"

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val topics = new util.ArrayList[String]()
    topics.add(consumerTopic)

    val result = streamEnv.addSource(SourceUtils.getKafkaSource(topics = topics))
    val filterDataSteam = result.filter(value => {
      try {
        val jsonObj = JSONObject.fromObject(value)
        null != jsonObj && jsonObj.size() > 0
      } catch {
        case e: Exception =>
          false
      }
    })
    filterDataSteam.map(value => {
      println(value)
      if (StringUtils.isNotBlank(value)) {
        try {
          val jsonObj = JSONObject.fromObject(value)
          if (jsonObj != null && jsonObj.size() > 0) {
            HBaseUtils.createTable(targetNamespace, targetTableName, targetColumnFamily)

            val deviceJson = jsonObj.getJSONObject(deviceTableName)
            val elevatorJson = jsonObj.getJSONObject(elevatorTableName)

            val col2ValueMap = new util.HashMap[String, String]()
            col2ValueMap.put(deviceField.toLowerCase, deviceJson.getString(deviceField.toLowerCase))
            val dateStr = deviceJson.getString(lastDtm.toLowerCase)
            val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr).getTime
            val now = new Date().getTime
            var status = "在线"
            if (now - time > 7 * 60 * 1000) {
              status = "离线"
            }
            col2ValueMap.put(statusField.toLowerCase, status)
            col2ValueMap.put(deviceField.toLowerCase, deviceJson.getString(deviceField.toLowerCase))
            col2ValueMap.put(elevatorField.toLowerCase, elevatorJson.getString(elevatorField.toLowerCase))
            col2ValueMap.put(longitude.toLowerCase, elevatorJson.getString(longitude.toLowerCase))
            col2ValueMap.put(latitude.toLowerCase, elevatorJson.getString(latitude.toLowerCase))


            val rowKey = elevatorJson.getString(joinRowKeyField.toLowerCase)
            val faultResults: util.List[Result] = HBaseUtils.getRows(sourceNamespace, orderTableName, rowKey)

            import scala.collection.JavaConverters._
            for (result <- faultResults.asScala.toList) {
              val orderIdValue = Bytes.toString(
                result.getValue(Bytes.toBytes(sourceColumnFamily(0)), Bytes.toBytes(orderId)))
              col2ValueMap.put(orderId.toLowerCase, orderIdValue)
              val processStatusValue = Bytes.toString(
                result.getValue(Bytes.toBytes(sourceColumnFamily(0)), Bytes.toBytes(processStatus)))
              col2ValueMap.put(processStatus.toLowerCase, processStatusValue)
              HBaseUtils.insert(targetNamespace, targetTableName, rowKey, targetColumnFamily, col2ValueMap)
            }
          }
        }
        catch {
          case e: Exception =>
            println(e)
        }
      }
    })

    streamEnv.execute(jobName)
  }

}
