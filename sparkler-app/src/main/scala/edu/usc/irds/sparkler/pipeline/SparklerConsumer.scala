package edu.usc.irds.sparkler.pipeline

import java.util.concurrent.{ExecutorService, Executors}
import java.util.{Collections, Properties}

import edu.usc.irds.sparkler.service.Injector
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.KafkaException

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

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
  val injector = new Injector()
  val crawler = new Crawler()

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
//            //sjob-1494519073538
//            //bin/sparkler.sh inject -id sjob-1494519073538 -su http://www.bbc.com/news -su http://espn.go.com/
//            var injectorArgs = ArrayBuffer[String]()
//            injectorArgs += "-id"
//            injectorArgs += "sjob-1494519073538"
//            injectorArgs += "-su"
//            injectorArgs += record.value()
//            injector.run(injectorArgs.toArray)
//            println(s">>jobId = ${injector.jobId}")
//
//            //bin/sparkler.sh crawl -id sparkler-job-1465352569649  -m local[*] -i 1
//            var crawlerArgs = ArrayBuffer[String]()
//            crawlerArgs += "-id"
//            crawlerArgs += "sjob-1494519073538"
//            crawlerArgs += "-m"
//            crawlerArgs += "local[*]"
//            crawlerArgs += "-i"
//            crawlerArgs += "1"
//            crawler.run(crawlerArgs.toArray)
          }
        }
      }
    })
  }


  }
