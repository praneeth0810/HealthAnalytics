
JOBNAME="copy_files_to_s3.ksh"

date=$(date '+%Y-%m-%d_%H:%M:%S')
bucket_subdir_name=$(date '+%Y-%m-%d-%H-%M-%S')

LOGFILE="/home/praneethchavva06/HeallthAnalytics/src/main/python/logs/${JOBNAME}_${date}.log"
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

LOCAL_OUTPUT_PATH="/home/praneethchavva06/HeallthAnalytics/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc

### Push City and Fact files to s3.
for file in ${LOCAL_CITY_DIR}/*.*
do
  aws s3 --profile myprofile cp ${file} "s3://healthanalytics08/dimension_city/$bucket_subdir_name/"
  echo "City File ${file} is pushed to s3."
done


for file in ${LOCAL_FACT_DIR}/*.*
do
  aws s3 --profile myprofile cp ${file} "s3://healthanalytics08/presc/$bucket_subdir_name/"
  echo "Presc File ${file} is pushed to s3."
done

echo "The ${JOBNAME} is complete...: {$date}"

} > ${LOGFILE} 2>&1 #<--- End of program and log.
