package com.worldpay.fraudsightBDA

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Dataset, Row}

object UIActivityout_review {

  def UIActivityout_review(Card_Sha: UserDefinedFunction, timeStampFormatting: UserDefinedFunction, getEventDate: UserDefinedFunction, getTopicDate: UserDefinedFunction, EventsInFilteredDF: Dataset[Row]) = {

    val syncOutSchema = new StructType()
      .add("financialValue", StringType)
      .add("durationInSeconds", StringType)
      .add("originalEventTypes", StringType)
      .add("rules", StringType)
      .add("eventType", StringType)
      .add("modelOutputs", StringType)
      .add("tags", StringType)
      .add("riskReasons", StringType)
      .add("comment", StringType)
      .add("reviewStatus", StringType)
      .add("incidentListId", StringType)
      .add("user", StringType)
      .add("entity", new StructType()
        .add("id", StringType)
        .add("type", StringType)
        .add("tenant", StringType))
      .add("timestamp", StringType)


    val syncOutParsedJsonDf =
      EventsInFilteredDF
        .withColumn("jsonData", from_json(col("json_data"), syncOutSchema))
        .withColumn("entity_id_sha", Card_Sha(col("jsonData.entity.id")))
        .withColumn("time_stamp", timeStampFormatting(col("jsonData.timestamp")))
        .withColumn("topic_time1", timeStampFormatting(col("topic_time")))

    val syncOutSelectedJsonDf = syncOutParsedJsonDf.withColumn("topic_date", getTopicDate(col("topic_time")))
      .select(
        col("jsonData.financialValue").alias("financial_value"),
        col("jsonData.durationInSeconds").alias("duration_in_seconds"),
        col("jsonData.originalEventTypes").alias("original_event_types"),
        col("jsonData.rules"),
        col("jsonData.eventType").alias("event_type"),
        col("jsonData.modelOutputs").alias("model_outputs"),
        col("jsonData.tags"),
        col("jsonData.riskReasons").alias("risk_reasons"),
        col("jsonData.comment").alias("comments"),
        col("jsonData.reviewStatus").alias("review_status"),
        col("jsonData.incidentListId").alias("incident_list_id"),
        col("jsonData.user"),
        col("jsonData.entity.id").alias("entity_id"),
        col("entity_id_sha"),
        col("jsonData.entity.type").alias("entity_type"),
        col("jsonData.entity.tenant").alias("entity_tenant"),
        col("time_stamp"),
        col("offset").alias("topic_offset"),
        col("topic_time1").alias("topic_time"),
        col("topic_date")
      )
    syncOutSelectedJsonDf
  }

}
