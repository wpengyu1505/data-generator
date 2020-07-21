package io.wpengyu.synthetic.equity

import java.math.BigDecimal
import java.sql.Timestamp

case class Trade(
  trade_dt:String,
  symbol:String,
  execution_id:String,
  event_tm:Timestamp,
  event_seq_nb:Int,
  exchange:String,
  price:BigDecimal,
  size:BigDecimal
)