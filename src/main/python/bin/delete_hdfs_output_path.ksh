
JOBNAME="delete_hdfs_output_paths.ksh"

date=$(date '+%Y-%m-%d_%H:%M:%S')

LOGFILE="/home/praneethchavva06/HeallthAnalytics/src/main/python/logs/${JOBNAME}_${date}.log"

{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

CITY_PATH=HeallthAnalytics/output/dimension_city
hdfs dfs -test -d $CITY_PATH
status=$?
if [ $status == 0 ]
  then
  echo "The HDFS output directory $CITY_PATH is available. Proceed to delete."
  hdfs dfs -rm -r -f $CITY_PATH
  echo "The HDFS Output directory $CITY_PATH is deleted before extraction."
fi

FACT_PATH=HeallthAnalytics/output/presc
hdfs dfs -test -d $FACT_PATH
status=$?
if [ $status == 0 ]
  then
  echo "The HDFS output directory $FACT_PATH is available. Proceed to delete."
  hdfs dfs -rm -r -f $FACT_PATH
  echo "The HDFS Output directory $FACT_PATH is deleted before extraction."
fi

echo "${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
