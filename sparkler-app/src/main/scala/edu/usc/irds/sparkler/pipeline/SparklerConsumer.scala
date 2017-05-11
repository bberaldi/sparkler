package edu.usc.irds.sparkler.pipeline

import java.util.concurrent.{ExecutorService, Executors}
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.KafkaException
import scala.collection.JavaConversions._

/**
  * Created by beraldi on 10/05/17.
  */
// scalastyle:off
class SparklerConsumer {

  val TOPIC="csv"

  val  props = new Properties()
  props.put("bootstrap.servers", "192.168.1.119:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = null

  def putProperty(key: String, value: String): Unit ={
    props.put(key, value)
  }

  def shutdown() = {
    if (consumer != null)
      consumer.close();
    if (executor != null)
      executor.shutdown();
  }

  def run() = {
    consumer.subscribe(Collections.singletonList(TOPIC))

    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000)

          for (record <- records) {
            println("Received message: (" + record.value() + ")")
            //sjob-1494519073538
            //bin/sparkler.sh inject -id sparkler-job-1465352569649 -su http://www.bbc.com/news -su http://espn.go.com/
            //bin/sparkler.sh crawl -id sparkler-job-1465352569649  -m local[*] -i 1
            //new Crawler().run(args)
          }
        }
      }
    })
  }


  }
