package cn.bigdatabc.realtime.util

import java.nio.charset.StandardCharsets
import java.io.InputStreamReader
import java.util.Properties

/**
 * @author liufengting
 * @date 2022/3/10
 */
object MyPropertiesUtil {

    def main(args: Array[String]): Unit = {
        val prop: Properties = MyPropertiesUtil.load("config.properties")
        println(prop.getProperty("kafka.broker.list"))
    }

    def load(propertiesName: String): Properties = {
        val prop: Properties = new Properties()
        prop.load(new InputStreamReader(
            Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
            StandardCharsets.UTF_8))
        prop
    }
}
