
JOBNAME="copy_files_local_to_hdfs.ksh"

date=$(date '+%Y-%m-%d_%H:%M:%S')

LOGFILE="/home/praneethchavva06/HeallthAnalytics/src/main/python/logs/copy_files_local_to_hdfs_${date}.log"

###########################################################################
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"
LOCAL_STAGING_PATH="/home/praneethchavva06/HeallthAnalytics/src/main/python/staging"
LOCAL_CITY_DIR=${LOCAL_STAGING_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_STAGING_PATH}/fact

HDFS_STAGING_PATH=HeallthAnalytics/staging
HDFS_CITY_DIR=${HDFS_STAGING_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_STAGING_PATH}/fact

### Copy the City  and Fact file to HDFS
hdfs dfs -put -f ${LOCAL_CITY_DIR}/* ${HDFS_CITY_DIR}/
hdfs dfs -put -f ${LOCAL_FACT_DIR}/* ${HDFS_FACT_DIR}/
echo "${JOBNAME} is Completed...: $(date)"
} > ${LOGFILE} 2>&1  # <--- End of program and end of log.

