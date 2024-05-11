JOBNAME="copy_files_hdfs_to_local.ksh"

date=$(date '+%Y-%m-%d_%H:%M:%S')

LOGFILE="/home/praneethchavva06/HeallthAnalytics/src/main/python/logs/${JOBNAME}_${date}.log"

{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"
LOCAL_OUTPUT_PATH="/home/praneethchavva06/HeallthAnalytics/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc

HDFS_OUTPUT_PATH=HeallthAnalytics/output
HDFS_CITY_DIR=${HDFS_OUTPUT_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_OUTPUT_PATH}/presc

### Copy the City  and Fact file from HDFS to Local
hdfs dfs -get -f ${HDFS_CITY_DIR}/* ${LOCAL_CITY_DIR}/
hdfs dfs -get -f ${HDFS_FACT_DIR}/* ${LOCAL_FACT_DIR}/
echo "${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
