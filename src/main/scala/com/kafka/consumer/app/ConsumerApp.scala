package com.kafka.consumer.app

import com.kafka.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.sql.hive._
import org.json4s._
import org.json4s.native.JsonMethods._
import org.ahocorasick.trie._
import scala.collection.JavaConverters._

object ConsumerAreadpp extends App {

  val logger = LoggerFactory.getLogger(this.getClass)
    
  // Define which topics to read from
  val topic = "topic_twitter"
  val groupId = "group-1"
  val consumer = KafkaConsumer(topic, groupId, "localhost:2181")
  
  //Create SparkContext
  val sparkContext = new SparkContext("local[2]", "KafkaConsumer")
  
  //Create HiveContext  
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
    
  hiveContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS twitter_data (tweetId BIGINT, tweetText STRING, userName STRING, tweetTimeStamp STRING, userLang STRING, posCount INT, neuCount INT, negCount INT)")
  hiveContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS demo (foo STRING)")
       
  //aho-corasick matching logic for machine learning
  val sentiment_positive_file  = sparkContext.textFile("/home/nihal/bigdata/dataset/sentiment_positive.txt")
  val sentiment_neutral_file   = sparkContext.textFile("/home/nihal/bigdata/dataset/sentiment_neutral.txt")
  val sentiment_negative_file  = sparkContext.textFile("/home/nihal/bigdata/dataset/sentiment_negative.txt")
  
  val triePos = new Trie().removeOverlaps.onlyWholeWords.caseInsensitive
  val trieNeu = new Trie().removeOverlaps.onlyWholeWords.caseInsensitive
  val trieNeg = new Trie().removeOverlaps.onlyWholeWords.caseInsensitive
  
  val positive_phrases = sentiment_positive_file.foreach { linesData => 
    val line = linesData.split("\n").distinct.mkString
    triePos.addKeyword(line)
    //logger.info("---->>Start Data<<<-----")
    //logger.info(line)
  }
  
  val neutral_phrases = sentiment_neutral_file.foreach { linesData => 
    val line = linesData.split("\n").distinct.mkString
    trieNeu.addKeyword(line)
    //logger.info("---->>Start Data<<<-----")
    //logger.info(line)
  }
  
  val negative_phrases = sentiment_negative_file.foreach { linesData => 
    val line = linesData.split("\n").distinct.mkString
    trieNeg.addKeyword(line)
    //logger.info("---->>Start Data<<<-----")
    //logger.info(line)
  }
  
  while (true) {
      
    consumer.read() match {
      case Some(message) =>
        
        logger.info("--JSON Message from kafka-->".concat(message))
        try{
          val msg = parse(message) 
          val tweetID        = compact(render(msg.children(0)))
          val tweetText      = compact(render(msg.children(1))).toLowerCase()
          val userName       = compact(render(msg.children(2)))
          val tweetTimeStamp = compact(render(msg.children(3)))
          val userLang       = compact(render(msg.children(4)))
          val tokensPos      = triePos.tokenize(tweetText)
          val tokensNeu      = trieNeu.tokenize(tweetText)
          val tokensNeg      = trieNeg.tokenize(tweetText)
          
          val tokenScalaCollPos = tokensPos.asScala
          val tokenScalaCollNeu = tokensNeu.asScala
          val tokenScalaCollNeg = tokensNeg.asScala
          
          var posCount = 0
          var neuCount = 0
          var negCount = 0
          
          for (tokenPos <- tokenScalaCollPos) {
            if (tokenPos.isMatch) {posCount = posCount + 1 } 
          }  
          
          for (tokenNeu <- tokenScalaCollNeu) {
            if (tokenNeu.isMatch) {neuCount = neuCount + 1 } 
          } 
          
          for (tokenNeg <- tokenScalaCollNeg) {
            if (tokenNeg.isMatch) {negCount = negCount + 1 } 
          }
          
          if ((negCount + posCount) == 0 && neuCount == 0 ) { neuCount = neuCount + 1}
          
          
          val hiveSql = "INSERT INTO TABLE twitter_data SELECT STACK( 1," + 
                           tweetID        +","  + 
                           tweetText      +"," + 
                           userName       +"," +
                           tweetTimeStamp +","  +
                           posCount       +","  +
                           neuCount       +","  +
                           negCount       +"," +
                           userLang + ") FROM demo limit 1"
          
          logger.info("HiveSql------>".concat(hiveSql))
          hiveContext.sql(hiveSql)
        
        } catch {
           case e: Exception => logger.error((e.getMessage).concat(message))
        }
        // wait for 30 milli-second
        Thread.sleep(500)
      case None =>
        logger.info("Queue is empty.......................  ")
        // wait for 2 second
        Thread.sleep(2 * 1000)
    }
  }

}