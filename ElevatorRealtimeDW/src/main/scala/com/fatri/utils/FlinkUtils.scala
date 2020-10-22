package com.fatri.utils

import java.util

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * @Description //TODO 
 * @Author Happiless.zhang
 * @Date
 * @Version
 */
object FlinkUtils {

  def SqlTemplate(sql: String, tableName: util.ArrayList[String], dataSource: DataStream[Row]) = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv,settings)

  }

}
