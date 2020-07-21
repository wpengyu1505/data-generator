package io.wpengyu.synthetic.equity

import java.math.BigDecimal
import java.sql.Timestamp

import com.google.gson.Gson

case class Trade(
  trade_dt:String,
  symbol:String,
  execution_id:String,
  event_tm:Timestamp,
  event_seq_nb:Int,
  exchange:String,
  price:BigDecimal,
  size:BigDecimal
) {
  def toCsv(): String = {
    val outputRecord = new StringBuilder()
    outputRecord.append(trade_dt).append(",")
    outputRecord.append(symbol).append(",")
    outputRecord.append(event_tm).append(",")
    outputRecord.append(event_seq_nb).append(",")
    outputRecord.append(exchange).append(",")
    outputRecord.append(price).append(",")
    outputRecord.append(size)

    outputRecord.toString()
  }

  def toJson(gson: Gson): String = {
    gson.toJson(this)
  }
}