package cn.bigdatabc.realtime.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

/**
 * 读取kafka的工具类
 *
 * @author liufengting
 * @date 2022/3/11
 */
object MyKafkaUtil {
    private val prop: Properties = MyPropertiesUtil.load("config.properties")
    private val broker_list: String = prop.getProperty("kafka.broker.list")

    var kafkaParam = collection.mutable.Map(
        "bootstrap.servers" -> broker_list,//用于初始化链接到集群的地址
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        //用于标识这个消费者属于哪个消费团体
        "group.id" -> "gmall0523_group",
        //latest自动重置偏移量为最新的偏移量
        "auto.offset.reset" -> "latest",
//        "auto.offset.reset" -> "earliest",
        //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
        //如果是false，会需要手动维护kafka偏移量
        "enable.auto.commit" -> (false: java.lang.Boolean)
//        "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    // 创建DStream，返回接收到的输入数据   使用默认的消费者组
    def getKafkaStream(topic: String, ssc: StreamingContext) = {
        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
        )
        dStream
    }

    //在对Kafka数据进行消费的时候，指定消费者组
    def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String) = {
        kafkaParam("group.id") = groupId
        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
        )
        dStream
    }

    //从指定的偏移量位置读取数据
    def getKafkaStream(topic: String, ssc: StreamingContext, offsets: Map[TopicPartition, Long], groupId:String)  = {
        kafkaParam("group.id") = groupId
        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets)
        )
        dStream
    }
}
