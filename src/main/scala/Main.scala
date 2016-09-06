package com.thoughtworks.la

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord}
import scala.collection.JavaConversions._

object Main {
  def createMessage(key: String, value: String) {
    val props = new java.util.HashMap[String, Object]()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("acks", "all")
    props.put("retries", new Integer(0))
    props.put("batch.size", new Integer(16384))
    props.put("linger.ms", new Integer(1))
    props.put("buffer.memory", new Integer(33554432))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    producer.send(new ProducerRecord[String, String]("my-topic", key, value))
    producer.close()
  }

  def consumeMessage() {
     val props = new java.util.HashMap[String, Object]()
     props.put("bootstrap.servers", "localhost:9092")
     props.put("group.id", "test")
     props.put("enable.auto.commit", "true")
     props.put("auto.commit.interval.ms", "1000")
     props.put("session.timeout.ms", "30000")
     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
     val consumer = new KafkaConsumer[String, String](props)
     consumer.subscribe(List("foo", "bar"))
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
    createMessage("haha", "value value")
  }
}
