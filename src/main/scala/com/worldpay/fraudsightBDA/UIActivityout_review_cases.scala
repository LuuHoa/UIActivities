package com.worldpay.fraudsightBDA

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}
import org.apache.spark.sql.{Dataset, Row}

object UIActivityout_review_cases {

  def UIActivityout_review_cases(Card_Sha: UserDefinedFunction, timeStampFormatting: UserDefinedFunction, getEventDate: UserDefinedFunction, getTopicDate: UserDefinedFunction, uiavty_review_items_filteredDf: Dataset[Row]) = {
    val uiavty_review_items_schema = new StructType()
//      .add("eventId", StringType)
//      .add("transactionId", StringType)
      .add("severity", StringType)
      .add("financialValue", StringType)
      .add("durationInSeconds", StringType)
      .add("rules", StringType)
      .add("eventType", StringType)
      .add("modelOutputs", StringType)
      .add("tags", StringType)
      .add("score", StringType)
      .add("riskReasons", StringType)
      .add("comment", StringType)
      .add("reviewStatus", StringType)
      .add("originatingEvent", new StructType()
        .add("eventId", StringType)
        .add("schemaVersion", StringType)
        .add("bin", StringType)
        .add("cardType", new StructType()
          .add("product", StringType)
          .add("reloadable", StringType)
          .add("fundingSource", StringType)
        )
        .add("merchant", new StructType()
          .add("merchantId", StringType)
          .add("city", StringType)
          .add("countryCode", StringType)
          .add("street", StringType)
          .add("postalCode", StringType)
          .add("name", StringType)
          .add("state", StringType)
        )
        .add("networkDecline", StringType)
        .add("eventType", StringType)
        .add("mcc", StringType)
        .add("accountNumber", StringType)
        .add("transactionId", StringType)
        .add("expDate", StringType)
        .add("mop", StringType)
        .add("transactionType", StringType)
        .add("tenantHierarchy", new StructType()
          .add("pgId", StringType)
          .add("orgId", StringType)
        )
        .add("issuerCountryId", StringType)
        .add("processedTransactionId", StringType)
        .add("eventTime", StringType)
        .add("tenantId", StringType)
        .add("internalAmount", new StructType()
          .add("baseValue", DecimalType(12, 8))
          .add("currency", StringType)
          .add("value", DecimalType(12, 8))
          .add("baseCurrency", StringType)
        )
        .add("_metadata", new StructType()
          .add("execution", new StructType()
            .add("expectResponse", StringType)
          )
          .add("tenantGroupId", StringType)
          .add("systemEventId", StringType)
          .add("receivedTime", StringType)
        )
      )
      .add("incidentListId", StringType)
      .add("originalEventType", StringType)
      .add("user", StringType)
      .add("entity", new StructType()
        .add("id", StringType)
        .add("type", StringType)
        .add("tenant", StringType))
      .add("timestamp", StringType)

    val uiavty_review_items_jsonDf =
      uiavty_review_items_filteredDf
        .withColumn("jsonData", from_json(col("json_data"), uiavty_review_items_schema))
        .withColumn("entity_id_sha", Card_Sha(col("jsonData.entity.id")))
        .withColumn("oe_metadata_received_time", timeStampFormatting(col("jsonData.originatingEvent._metadata.receivedTime")))
        .withColumn("oe_event_time", timeStampFormatting(col("jsonData.originatingEvent.eventTime")))
        .withColumn("time_stamp", timeStampFormatting(col("jsonData.timestamp")))
        .withColumn("topic_time1", timeStampFormatting(col("topic_time")))
        .withColumn("topic_date", getTopicDate(col("topic_time")))

    val uiavty_review_items_selectedJsonDf = uiavty_review_items_jsonDf.select(
//      col("jsonData.originatingEvent.eventId").alias("event_id"),
//      col("jsonData.originatingEvent.transactionId").alias("transaction_id"),
      col("jsonData.severity"),
      col("jsonData.financialValue").alias("financial_value"),
      col("jsonData.durationInSeconds").alias("duration_in_seconds"),
      col("jsonData.rules"),
      col("jsonData.eventType").alias("event_type"),
      col("jsonData.modelOutputs").alias("model_outputs"),
      col("jsonData.tags"),
      col("jsonData.score"),
      col("jsonData.riskReasons"),
      col("jsonData.comment").alias("comments"),
      col("jsonData.reviewStatus").alias("review_status"),
      col("jsonData.originatingEvent.eventId").alias("oe_event_id"),
      col("jsonData.originatingEvent.schemaVersion").alias("oe_schema_version"),
      col("jsonData.originatingEvent.bin").alias("oe_bin"),
      col("jsonData.originatingEvent.cardType.product").alias("oe_card_type_product"),
      col("jsonData.originatingEvent.cardType.reloadable").alias("oe_card_type_reloadable"),
      col("jsonData.originatingEvent.cardType.fundingSource").alias("oe_card_type_funding_source"),
      col("jsonData.originatingEvent.merchant.merchantId").alias("oe_merchant_merchant_id"),
      col("jsonData.originatingEvent.merchant.city").alias("oe_merchant_city"),
      col("jsonData.originatingEvent.merchant.countryCode").alias("oe_merchant_country_code"),
      col("jsonData.originatingEvent.merchant.street").alias("oe_merchant_street"),
      col("jsonData.originatingEvent.merchant.postalCode").alias("oe_merchant_postal_code"),
      col("jsonData.originatingEvent.merchant.name").alias("oe_merchant_name"),
      col("jsonData.originatingEvent.merchant.state").alias("oe_merchant_state"),
      col("jsonData.originatingEvent.networkDecline").alias("oe_network_decline"),
      col("jsonData.originatingEvent.eventType").alias("oe_event_type"),
      col("jsonData.originatingEvent.mcc").alias("oe_mcc"),
      col("jsonData.originatingEvent.accountNumber").alias("oe_account_number"),
      col("jsonData.originatingEvent.transactionId").alias("oe_transaction_id"),
      col("jsonData.originatingEvent.expDate").alias("oe_exp_date"),
      col("jsonData.originatingEvent.mop").alias("oe_mop"),
      col("jsonData.originatingEvent.transactionType").alias("oe_transaction_type"),
      col("jsonData.originatingEvent.tenantHierarchy.pgId").alias("oe_tenant_hierarchy_pgId"),
      col("jsonData.originatingEvent.tenantHierarchy.orgId").alias("oe_tenant_hierarchy_orgId"),
      col("jsonData.originatingEvent.issuerCountryId").alias("oe_issuer_country_id"),
      col("jsonData.originatingEvent.processedTransactionId").alias("oe_processed_transaction_id"),
      col("oe_event_time"),
      col("jsonData.originatingEvent.tenantId").alias("oe_tenant_id"),
      col("jsonData.originatingEvent.internalAmount.baseValue").alias("oe_internal_amount_base_value"),
      col("jsonData.originatingEvent.internalAmount.currency").alias("oe_internal_amount_currency"),
      col("jsonData.originatingEvent.internalAmount.value").alias("oe_internal_amount_value"),
      col("jsonData.originatingEvent.internalAmount.baseCurrency").alias("oe_internal_amount_base_currency"),
      col("jsonData.originatingEvent._metadata.execution.expectResponse").alias("oe_metadata_execution_expect_response"),
      col("jsonData.originatingEvent._metadata.tenantGroupId").alias("oe_metadata_tenant_group_id"),
      col("jsonData.originatingEvent._metadata.systemEventId").alias("oe_metadata_system_event_id"),
      col("oe_metadata_received_time"),
      col("jsonData.incidentListId").alias("incident_list_id"),
      col("jsonData.originalEventType").alias("original_event_type"),
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

    uiavty_review_items_selectedJsonDf
  }

}
