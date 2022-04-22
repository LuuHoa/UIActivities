package com.worldpay.fraudsightBDA

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FraudsightToBDA {

  def main(args: Array[String]): Unit = {

    val props: Properties = new Properties()
    val jobPropPath = new File(args(0))
    val sslPropPath = new File(args(1))
    val jobProp = new FileInputStream(jobPropPath)
    val sslProp = new FileInputStream(sslPropPath)
    props.load(jobProp)
    props.load(sslProp)
    val Card_Sha = udf(Utils.get_Card_Sha _)
    val getEventDate = udf(Utils.get_event_date _)
    val getTopicDate = udf(Utils.get_topic_date _)

    val timeStampFormatting = udf(Utils.timeStamp_formatting _)


    val sparkConf = new SparkConf().setAppName(props.getProperty("AppName"))
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    sparkConf.set("spark.scheduler.mode", "FAIR")
    sparkConf.set("spark.streaming.kafka.consumer.cache.enabled", "false")
    val ssc = new StreamingContext(sparkConf, Seconds(props.getProperty("BATCH_INVL").toInt))


    val basePath = s"${props.getProperty("basePath")}"
    val extractDate = s"${props.getProperty("extractDate")}"
    val fileName = s"${props.getProperty("fileName")}"
    val topicsList = s"${props.getProperty("topics")}".split(",")
    val kafkaParams = Map(
      "ssl.client.auth" -> s"${props.getProperty("ssl_auth")}",
      "security.protocol" -> s"${props.getProperty("security.protocol")}",
      "sasl.kerberos.service.name" -> s"${props.getProperty("sasl_service_name")}",
      "ssl.truststore.location" -> s"${props.getProperty("ssl.truststore.location")}",
      "ssl.truststore.password" -> s"${props.getProperty("ssl.truststore.password")}",
      "ssl.keystore.location" -> s"${props.getProperty("ssl.keystore.location")}",
      "ssl.keystore.password" -> s"${props.getProperty("ssl.keystore.password")}",
      "bootstrap.servers" -> s"${props.getProperty("bootstrap.servers")}",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> s"${props.getProperty("group_id")}",
      "session.timeout.ms" -> s"${props.getProperty("session_timeout_ms")}",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    for (topic <- topicsList) {
      val kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent, //Distribute partitions evenly across available executors
        Subscribe[String, String](Array(topic), kafkaParams)
      )
      kafkaStream.foreachRDD {
        consumerRecordRdd =>
          val offsetRanges = consumerRecordRdd.asInstanceOf[HasOffsetRanges].offsetRanges
          if (!consumerRecordRdd.isEmpty()) {
            val sqlContext = SQLContext.getOrCreate(consumerRecordRdd.sparkContext)

            val schemaString = "json_data offset topic_time"

            val fields = schemaString
              .split(" ")
              .map(fieldName => StructField(fieldName, StringType, nullable = true))

            val parquetCustomSchema = StructType(fields)

            val topicValueDF =
              sqlContext.createDataFrame(
                consumerRecordRdd.map(
                  cRecord => (
                    cRecord.value().toString,
                    cRecord.offset().toString,
                    cRecord.timestamp().toString
                  )
                ).map(transferedCRecord => Row(transferedCRecord._1, transferedCRecord._2, transferedCRecord._3)),
                parquetCustomSchema
              ).persist()

            val AsyncFilteredDF = topicValueDF
              .filter(!col("json_data")
                .contains(props.getProperty("TagName")) && !col("json_data")
                .contains(props.getProperty("ChargeTagName")))
              .persist()

            val uiActivityoutSchema = new StructType()
              .add("eventType", StringType)

            val uiActivityoutDF =
              topicValueDF
                .withColumn("eventTypecol", from_json(col("json_data"), uiActivityoutSchema)).persist()
            //            uiActivityoutDF.show()

            val closed_DF = uiActivityoutDF.filter(col("eventTypecol.eventType").contains(props.getProperty("ClosedType")))
            //            closed_DF.show()
            val UIActivityout_closed_JsonDf: DataFrame = UIActivityout_closed.UIActivityout_closed(Card_Sha, timeStampFormatting, getEventDate, getTopicDate, closed_DF)

            UIActivityout_closed_JsonDf.show()
            //
            val default_JsonDf = uiActivityoutDF
              .filter(!col("eventTypecol.eventType").contains(props.getProperty("ReviewCaseType"))
                && !col("eventTypecol.eventType").contains(props.getProperty("ClosedType"))
                && !col("eventTypecol.eventType").contains(props.getProperty("ReviewType")))


            val UIActivityout_default_JsonDf: DataFrame = UIActivityout_default.UIActivityout_default(Card_Sha, timeStampFormatting, getEventDate, getTopicDate, default_JsonDf)
            val review_cases_DF = uiActivityoutDF.filter(col("eventTypecol.eventType").contains(props.getProperty("ReviewCaseType")))
            val UIActivityout_review_cases_JsonDf: DataFrame = UIActivityout_review_cases.UIActivityout_review_cases(Card_Sha, timeStampFormatting, getEventDate, getTopicDate, review_cases_DF)

            val review_DF = uiActivityoutDF.filter(col("eventTypecol.eventType").contains(props.getProperty("ReviewType")))
            review_DF.show()
            val UIActivityout_review_JsonDf: DataFrame = UIActivityout_review.UIActivityout_review(Card_Sha, timeStampFormatting, getEventDate, getTopicDate, review_DF)

            UIActivityout_review_JsonDf.coalesce(1)
              .write
              .mode(SaveMode.Append).partitionBy("topic_date")
              .parquet(basePath + topic + s"${props.getProperty("ReviewPath")}")

            UIActivityout_closed_JsonDf.coalesce(1)
              .write
              .mode(SaveMode.Append).partitionBy("topic_date")
              .parquet(basePath + topic +s"${props.getProperty("ClosedPath")}")

            UIActivityout_review_cases_JsonDf.coalesce(1)
              .write
              .mode(SaveMode.Append).partitionBy("topic_date")
              .parquet(basePath + topic + s"${props.getProperty("ReviewCasePath")}")

            UIActivityout_default_JsonDf
              .write
              .mode(SaveMode.Append).partitionBy("topic_date")
              .parquet(basePath + topic + s"${props.getProperty("DefaultPath")}")


            topicValueDF.coalesce(1)
              .withColumn("topic_date", getTopicDate(col("topic_time")))
              .withColumn("topic_time1", timeStampFormatting(col("topic_time")))
              .select(col("json_data"), col("offset").alias("topic_offset")
                , col("topic_time1").alias("topic_time"), col("topic_date"))
              .coalesce(1)
              .write
              .mode(SaveMode.Append).partitionBy("topic_date")
              .parquet(basePath + topic + "/topicValue")

          }
          kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
