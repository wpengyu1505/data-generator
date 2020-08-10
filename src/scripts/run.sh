date=$1
seed_location=$2
exchange=$3
unit_volume=$4
file_format=$5

spark-submit --class io.wpengyu.synthetic.equity.MarketGenerator \
    --conf spark.yarn.executor.memoryOverhead=8192 \
    --master local \
    --num-executors 10 \
    --executor-memory 3G \
    --driver-memory 3G \
    data-generator-1.0.0.jar \
    $date \
    $seed_location \
    data/$file_format/$date/$exchange \
    $exchange \
    $unit_volume \
    $file_format

