package com.thoughtworks.la.ingestion.kafka

import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord}
import scala.collection.JavaConversions._

object Consumer {
  def consumeMessage() {
     val props = new java.util.HashMap[String, Object]()
     props.put("bootstrap.servers", "kafka:9092")
     props.put("group.id", "test")
     props.put("enable.auto.commit", "true")
     props.put("auto.commit.interval.ms", "1000")
     props.put("session.timeout.ms", "30000")
     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
     val consumer = new KafkaConsumer[String, String](props)
     consumer.subscribe(List("my-topic"))
     while (true) {
       val records = consumer.poll(100)
       for {
         record <- records
       } {
         println("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value())
       }
     }
  }

  def main(args: Array[String]): Unit = {
    consumeMessage()
  }
}
