package cn.bigdatabc.realtime.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

/**
 * 向Kafka主题中发送数据
 *
 * @author liufengting
 * @date 2022/3/11
 */
object MyKafkaSink {
    private val properties: Properties = MyPropertiesUtil.load("config.properties")
    val broker_list = properties.getProperty("kafka.broker.list")
    var kafkaProducer: KafkaProducer[String, String] = null

    def createKafkaProducer = {
        val prop = new Properties()
        properties.put("bootstrap.servers", broker_list)
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("enable.idempotence",(true: java.lang.Boolean))

        var producer: KafkaProducer[String, String] = null
        try {
            producer = new KafkaProducer[String, String](prop)
        } catch {
            case e: Exception =>
                e.printStackTrace()
        }
        producer
    }

    def send(topic: String, msg: String) = {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer
        }
        kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
    }

    def send(topic: String, key: String, msg:String) = {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer
        }
        kafkaProducer.send(new ProducerRecord[String, String](topic, key, msg))
    }
}
