package io.wpengyu.synthetic.equity

import java.sql.Timestamp
import java.math.BigDecimal

case class Quote (
  trade_dt:String,
  symbol:String,
  event_tm:Timestamp,
  event_seq_nb:Int,
  exchange:String,
  bid_pr:BigDecimal,
  bid_size:BigDecimal,
  ask_pr:BigDecimal,
  ask_size:BigDecimal
)