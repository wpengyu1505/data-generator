package io.wpengyu.synthetic.equity

import java.sql.Timestamp
import java.math.BigDecimal

import com.google.gson.Gson

case class Quote (
  trade_dt:String,
  event_type:String,
  symbol:String,
  event_tm:Timestamp,
  event_seq_nb:Int,
  exchange:String,
  bid_pr:BigDecimal,
  bid_size:BigDecimal,
  ask_pr:BigDecimal,
  ask_size:BigDecimal
) {
  def toCsv(): String = {
    val outputRecord = new StringBuilder()
    outputRecord.append(trade_dt).append(",")
    outputRecord.append(event_type).append(",")
    outputRecord.append(symbol).append(",")
    outputRecord.append(event_tm).append(",")
    outputRecord.append(event_seq_nb).append(",")
    outputRecord.append(exchange).append(",")
    outputRecord.append(bid_pr).append(",")
    outputRecord.append(bid_size).append(",")
    outputRecord.append(ask_pr).append(",")
    outputRecord.append(ask_size)

    outputRecord.toString()
  }

  def toJson(gson: Gson): String = {
    gson.toJson(this)
  }
}