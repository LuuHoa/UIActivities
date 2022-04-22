package com.worldpay.fraudsightBDA

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Dataset, Row}

object UIActivityout_closed {

  def UIActivityout_closed(Card_Sha: UserDefinedFunction, timeStampFormatting: UserDefinedFunction, getEventDate: UserDefinedFunction, getTopicDate: UserDefinedFunction, uiavty_closed_filteredDf: Dataset[Row]) = {
    val uiavty_closed_schema = new StructType()

      .add("tid", new StructType()
        .add("id", StringType)
        .add("type", StringType)
        .add("tenant", StringType)
      )
      .add("timestamp", StringType)
      .add("eventType", StringType)

    val uiavty_closed_jsonDf =
      uiavty_closed_filteredDf
        .withColumn("jsonData", from_json(col("json_data"), uiavty_closed_schema))
        .withColumn("tid_id_sha", Card_Sha(col("jsonData.tid.id")))
        .withColumn("time_stamp", timeStampFormatting(col("jsonData.timestamp")))
        .withColumn("topic_time1", timeStampFormatting(col("topic_time")))
        .withColumn("topic_date", getTopicDate(col("topic_time")))

    val uiavty_closed_selectedJsonDf = uiavty_closed_jsonDf.select(

      col("jsonData.tid.id").alias("tid_id"),
      col("tid_id_sha"),
      col("jsonData.tid.type").alias("tid_type"),
      col("jsonData.tid.tenant").alias("tid_tenant"),
      col("time_stamp"),
      col("jsonData.eventType").alias("event_type"),
      col("offset").alias("topic_offset"),
      col("topic_time1").alias("topic_time"),
      col("topic_date")
    )

    uiavty_closed_selectedJsonDf
  }

}
