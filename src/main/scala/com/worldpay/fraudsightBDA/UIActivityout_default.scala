package com.worldpay.fraudsightBDA

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Dataset, Row}

object UIActivityout_default {

  def UIActivityout_default(Card_Sha: UserDefinedFunction, timeStampFormatting: UserDefinedFunction, getEventDate: UserDefinedFunction, getTopicDate: UserDefinedFunction, uiavty_default_filteredDf: Dataset[Row]) = {
    val uiavty_default_schema = new StructType()
      .add("incidentListId", StringType)
      .add("eventType", StringType)
      .add("timestamp", StringType)
      .add("parentACGIdentifier", StringType)
      .add("tenantIdentifier", StringType)
      .add("username", StringType)
      .add("acgAlias", StringType)
      .add("sandboxId", StringType)
      .add("tenant", StringType)

    val uiavty_default_jsonDf =
      uiavty_default_filteredDf
        .withColumn("jsonData", from_json(col("json_data"), uiavty_default_schema))
//        .withColumn("tid_id_sha", Card_Sha(col("jsonData.tid.id")))
        .withColumn("time_stamp", timeStampFormatting(col("jsonData.timestamp")))
        .withColumn("topic_time1", timeStampFormatting(col("topic_time")))
        .withColumn("topic_date", getTopicDate(col("topic_time")))

    val uiavty_default_selectedJsonDf = uiavty_default_jsonDf.select(
      col("jsonData.incidentListId").alias("incident_list_id"),
      col("jsonData.eventType").alias("event_type"),
      col("time_stamp"),
      col("jsonData.parentACGIdentifier").alias("parent_ACG_identifier"),
      col("jsonData.tenantIdentifier").alias("tenant_identifier"),
      col("jsonData.username").alias("user_name"),
      col("jsonData.acgAlias").alias("acg_alias"),
      col("jsonData.sandboxId").alias("sand_box_id"),
      col("jsonData.tenant"),
      col("offset").alias("topic_offset"),
      col("topic_time1").alias("topic_time"),
      col("topic_date")
    )

    uiavty_default_selectedJsonDf
  }

}
