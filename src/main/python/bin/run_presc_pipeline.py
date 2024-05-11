import logging
import logging.config
import os
import sys
from subprocess import Popen, PIPE

import get_all_variables as gav
from create_objects import get_spark_object
from presc_run_data_ingest import load_files
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report, top_5_Prescribers
from presc_run_data_extraction import extract_files
from presc_run_data_persist import data_persist_postgre
logging.config.fileConfig(fname='../util/logging_to_file.conf')


def main():

        try:
                logging.info("main() is started...\n\n")

                #Creating spark object
                spark = get_spark_object(gav.envn,gav.appName)

                #validating spark object
                get_curr_date(spark)

                #Intitating run_presc_data_ingest
                # Load the City File
                global file_format
                global header
                global inferSchema
                file_format = 'parquet' or 'csv'
                header = 'NA' or gav.header
                inferSchema='NA' or gav.inferSchema
                file_dir="HeallthAnalytics/staging/dimension_city"
                proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
                (out, err) = proc.communicate()
                if 'parquet' in out.decode():
                    file_format = 'parquet'
                    header='NA'
                    inferSchema='NA'
                elif 'csv' in out.decode():
                    file_format = 'csv'
                    header=gav.header
                    inferSchema=gav.inferSchema

                df_city = load_files(spark = spark, file_dir = file_dir, file_format =file_format , header =header, inferSchema = inferSchema)

                # Load the Prescriber Fact File
                file_dir="HeallthAnalytics/staging/fact"
                proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
                (out, err) = proc.communicate()
                if 'parquet' in out.decode():
                    file_format = 'parquet'
                    header='NA'
                    inferSchema='NA'
                elif 'csv' in out.decode():
                    file_format = 'csv'
                    header=gav.header
                    inferSchema=gav.inferSchema

                df_fact = load_files(spark = spark, file_dir = file_dir, file_format =file_format , header =header, inferSchema = inferSchema)

                # Valdiate run_data_ingest script for City Dimension dataframe
                df_count(df_city, "df_city")
                df_top10_rec(df_city, "df_city")

                df_count(df_fact,"df_fact")
                df_top10_rec(df_fact,"df_fact")

                # CHECK THE SCHEMA FOR DATAFRAMES
                df_print_schema(df_city, "df_city")
                df_print_schema(df_fact, "df_fact")


                #INTIATE PREPROCESSING OF DATA -
                #Data cleaning for data_city- take only necessary columns and converting all the city name fields to UPPER CASE

                df_city_sel,df_fact_sel = perform_data_clean(df_city,df_fact)
                #Validate for df_city after and df_fact
                df_top10_rec(df_city_sel, "df_city_sel")
                df_top10_rec(df_fact_sel, "df_fact_sel")

                # CHECK THE SCHEMA FOR DATAFRAMES
                df_print_schema(df_city_sel, "df_city_sel")
                df_print_schema(df_fact_sel, "df_fact_sel ")

                #TRANSFORMATION OF DATA AND GENERATING THE FINAL REPORTS
                df_city_final = city_report(df_city_sel,df_fact_sel)
                df_top10_rec(df_city_final, "df_city_final")
                df_print_schema(df_city_final, "df_city_final ")

                df_presc_final = top_5_Prescribers(df_fact_sel)
                df_top10_rec(df_presc_final, "df_presc_final")
                df_print_schema(df_presc_final, "df_presc_final ")


                #DATA EXTRACTION SCRIPT
                CITY_PATH = gav.output_city
                FACT_PATH = gav.output_fact
                extract_files(df_city_final,'json',CITY_PATH,1,False,'bzip2')
                extract_files(df_presc_final, 'orc', FACT_PATH, 2, False, 'snappy')

                ### PERSIST DATA AT Postgres
                data_persist_postgre(spark=spark, df=df_city_final, dfName='df_city_final', url="jdbc:postgresql://localhost:6432/prescpipeline", driver="org.postgresql.Driver", dbtable='df_city_final', mode="append", user=gav.user, password=gav.password)


                data_persist_postgre(spark=spark, df=df_presc_final, dfName='df_presc_final', url="jdbc:postgresql://localhost:6432/prescpipeline", driver="org.postgresql.Driver", dbtable='df_presc_final', mode="append", user=gav.user, password=gav.password)


                ### End of Application Part 1
                logging.info("presc_run_pipeline.py is Completed.\n\n")


        except Exception as exp:
                logging.error("Error Occured in the main() method. Please check the Stack Trace to the respective module and fix it."
                              + str(exp),exc_info=True)
                sys.exit(1)

if __name__=="__main__":
        logging.info("run_presc_pipeline started\n\n")
        main()
