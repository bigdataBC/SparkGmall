package cn.bigdatabc.realtime.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Author: Felix
  * Date: 2020/10/26
  * Desc:  向Kafka主题中发送数据
  */
object MyKafkaSink {
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")
  var kafkaProducer: KafkaProducer[String, String] = null

  def createKafkaProducer: KafkaProducer[String, String] = {
    val properties = new Properties
    properties.put("bootstrap.servers", "node1:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    properties.put("enable.idempotence",(true: java.lang.Boolean))
    var producer: KafkaProducer[String, String] = null
    try
      producer = new KafkaProducer[String, String](properties)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer
  }

  def send(topic: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }

  def send(topic: String,key:String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic,key, msg))
  }

  def closeKafkaProducer() = {
    try
      kafkaProducer.close()
    catch {
      case e: Exception =>
        println("暂时没数据，生产者进程已关闭")
    }
  }

  def main(args: Array[String]): Unit = {
      send("ods_order_info","ss")
      closeKafkaProducer
  }


}
