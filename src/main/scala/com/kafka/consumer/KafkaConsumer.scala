package com.kafka.consumer

import java.util.Properties

import org.slf4j.LoggerFactory

import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.consumer.ConsumerTimeoutException
import kafka.consumer.Whitelist
import kafka.serializer.DefaultDecoder

case class KafkaConsumer(topic: String, groupId: String, zookeeperConnect: String) {

  private val props = new Properties()

  val logger = LoggerFactory.getLogger(this.getClass)

  props.put("group.id", groupId)
  props.put("zookeeper.connect", zookeeperConnect)
  props.put("auto.offset.reset", "smallest")
  props.put("consumer.timeout.ms", "500")
  props.put("auto.commit.interval.ms", "500")

  private val config = new ConsumerConfig(props)

  private val connector = Consumer.create(config)

  private val filterSpec = new Whitelist(topic)

  private val streams = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder())(0)

  def read() =
    try {
      if (hasNext) {
        //logger.info("Getting message from queue.............")
        val message = iterator.next().message()
        Some(new String(message))
      } else {
        None
      }
    } catch {
      case ex: Throwable =>
        logger.error("Error processing message, skipping this message: ", ex)
        None
    }

  lazy val iterator = streams.iterator()

  private def hasNext(): Boolean =
    try (iterator.hasNext()) catch {
      case timeOutEx: ConsumerTimeoutException =>
        false
      case ex: Throwable =>
        logger.warn("Getting error when reading message ", ex)
        false
    }

  def close(): Unit = connector.shutdown()

}
