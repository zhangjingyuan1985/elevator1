//package com.fatri.dw.app
//
//import java.util
//
//import com.fatri.utils.{SinkUtils, SourceUtils}
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand
//
//object Flink2Redis {
//
//  def main(args: Array[String]): Unit = {
//
//    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    streamEnv.setParallelism(1)
//    import org.apache.flink.streaming.api.scala._
//    val topics: util.ArrayList[String] = new util.ArrayList[String]
//    topics.add("wc")
//    val stream = streamEnv.addSource(SourceUtils.getKafkaSource(topics = topics))
//
//    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
//      .map((_, 1))
//      .keyBy(0)
//      .sum(1)
//    stream.print("result")
//
//    result.map(one => (one._1, one._2.toString)).addSink(SinkUtils.getRedisSink(RedisCommand.HSET, "wc"))
//
//    streamEnv.execute()
//
//  }
//
//}
