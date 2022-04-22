package com.worldpay.fraudsightBDA

import java.math.BigInteger
import java.security.MessageDigest
import java.time.Instant

object Utils {
  // This function receives Credit card returns SHA value
  def get_Card_Sha(card_parm_func: String): String = {
    if (card_parm_func == null) {
      return card_parm_func
    }
    else {
      val card_parm = card_parm_func.trim
      val card_parm_spaces = card_parm + "                "
      val card_bin = card_parm_spaces.substring(0, 6).trim
      val card_salt = if (card_parm.trim.size < 2) "00000" else card_parm.concat(card_parm_spaces.substring(2, 12)).trim
      val hash_val = String.format("%064x", new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(card_salt.getBytes("UTF-8"))))
      val hash_val_zerotrim = hash_val.replaceFirst("^[0]+", "")
      val final_hash = card_bin.concat(hash_val_zerotrim)
      return final_hash
    } // end of else
  } // End of Function


  // This function receives Event Time returns Event Date as string value
  def get_event_date(time_stamp1: String): String = {
    val time_stamp = Option(time_stamp1).getOrElse(return " ")
    if (time_stamp == null && time_stamp.isEmpty) {
      val a = " "
      a
    } else {
      val b = time_stamp.substring(0, 10)
      b
    }
  } // End of Function

  // This function receives Topic Time in milliseconds returns Topic Date as string value
  def get_topic_date(time_stamp1: String): String = {

    val time_stamp = Option(time_stamp1).getOrElse(return " ")
    if (time_stamp == null && time_stamp.isEmpty) {
      val a = " "
      a
    } else if (time_stamp1.contains("T")) {
      val b = time_stamp1.split("T")
      b(0)
    } else if (time_stamp1.contains("-") || time_stamp1.contains(":")) {
      val b = time_stamp1.substring(0, 10)
      b
    } else {
      val instant = Instant.ofEpochMilli(time_stamp1.toLong).toString
      val b = instant.substring(0, 10)
      b
    }
  } // End of Function

  // This function receives Time Stamp returns Standard time stamp as string value
  def timeStamp_formatting(a: String): String = {
    val time_stamp = Option(a).getOrElse(return null)
    if (time_stamp == null && time_stamp.isEmpty) {
      val time = null
      time
    } else {
      if (a.contains("T")) {
        val b = a.split("T")
        val c = b(0) + " " + b(1).split("Z")(0)
        c
      }
      else if (a.contains("-") || a.contains(":")) {
        a
      } else {
        val instant = Instant.ofEpochMilli(a.toLong).toString
        val b = instant.substring(0, 10) + " " + instant.substring(11)
        b.split("Z")(0)
      }
    }
  } // End of Function


} // End of Object
