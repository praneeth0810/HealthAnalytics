PROJ_FOLDER="/home/${USER}/HeallthAnalytics/src/main/python"

printf "\nCalling copy_files_local_to_hdfs.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ... \n"
${PROJ_FOLDER}/bin/copy_files_local_to_hdfs.ksh
printf "Executing copy_files_local_to_hdfs.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

printf "Calling delete_hdfs_output_paths.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ... \n"
${PROJ_FOLDER}/bin/delete_hdfs_output_path.ksh
printf "Executing delete_hdfs_output_paths.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

printf "Calling run_presc_pipeline.py at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
spark3-submit --master yarn --num-executors 28 --jars ${PROJ_FOLDER}/lib/postgresql-42.3.5.jar run_presc_pipeline.py
printf "Executing run_presc_pipeline.py is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

printf "Calling copy_files_hdfs_to_local.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
${PROJ_FOLDER}/bin/copy_files_hdfs_to_local.ksh
printf "Executing copy_files_hdfs_to_local.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

printf "Calling copy_files_to_s3.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
${PROJ_FOLDER}/bin/copy_files_to_s3.ksh
printf "Executing copy_files_to_s3.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"
