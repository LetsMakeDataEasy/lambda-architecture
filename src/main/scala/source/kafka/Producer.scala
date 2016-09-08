package com.thoughtworks.la.source.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.JavaConversions._

object Producer {
  def createMessage[K, V](key: K, value: V, ks: String, vs: String) {
    val props = new java.util.HashMap[String, Object]()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("acks", "all")
    props.put("retries", new Integer(0))
    props.put("batch.size", new Integer(16384))
    props.put("linger.ms", new Integer(1))
    props.put("buffer.memory", new Integer(33554432))
    props.put("key.serializer", ks)
    props.put("value.serializer", vs)

    val producer = new KafkaProducer[K, V](props)
    producer.send(new ProducerRecord[K, V]("my-topic", key, value))
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    val strSerializer = "org.apache.kafka.common.serialization.StringSerializer"
    createMessage("haha", "value value", strSerializer, strSerializer)
  }
}
