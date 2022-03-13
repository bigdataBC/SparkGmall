package cn.bigdatabc.realtime.app

import cn.bigdatabc.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author liufengting
 * @date 2022/3/12
 */
object Dauapp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
        val streamingContext = new StreamingContext(sparkConf, Seconds(5))

        /**
          * 消费Kafka数据基本实现
          */
        var topic:String = "gmall_start_0523"
        var groupId:String = "gmall_dau_0523"
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext, groupId)
        kafkaDStream.map(_.value()).print(10)

        streamingContext.start()
        streamingContext.awaitTermination()
    }
}
