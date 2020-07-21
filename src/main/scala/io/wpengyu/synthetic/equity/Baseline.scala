package io.wpengyu.synthetic.equity

import java.math.BigDecimal

case class Baseline (
  symbol:String,
  exchange:String,
  volume:Integer,
  source_pr:BigDecimal
)