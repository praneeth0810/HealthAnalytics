import logging
import logging.config
import pandas as pd
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def get_curr_date(spark):
    try:
        opDF = spark.sql(""" select current_date """)
        logger.info("Validating the Spark object by printing current date - " + str(opDF.collect()))
    except NameError as exp:
        logger.error("NameError in the method - spark_curr_date(). Please check the stack trace. " + str(exp),exc_info=True)
        raise
    except Exception as exp:
        logger.error("Error in the method - spark_curr_date(). Please check the stack trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Spark object is validated and ready to proceed \n\n")

def df_count(df,dfName):
    try:
        logger.info(f"The DataFrame validation by count df_count() is started for DataFrame {dfName}...\n\n")
        df_count=df.count()
        logger.info(f"The DataFrame count is {df_count}.\n\n")
    except Exception as exp:
        logger.error("Error in the method - df_count(). Please check the stack trace"+ str(exp))
        raise
    else:
        logger.info(f"The Dataframe validation by count df_count() is completed.\n\n")

def df_top10_rec(df,dfName):
    try:
        logger.info(f"The DataFrame validation by count df_top10_rec() is started for DataFrame {dfName}...\n\n")
        logger.info(f"The DataFrame top 10 records are:\n\n")
        df_top10=df.limit(10).toPandas()
        logger.info('\n \t' + df_top10.to_string(index=False))

    except Exception as exp:
        logger.error("Error in the method - df_top10_rec(). Please check the stack trace"+ str(exp))
        raise
    else:
        logger.info(f"The Dataframe validation by count df_top10_rec() is completed.\n\n")

def df_print_schema(df,dfName):
    try:
        logger.info(f"The data frame scheme validation for Dataframe {dfName}\n\n")
        sch=df.schema.fields
        logger.info(f"The schema for Dataframe {dfName} is \n\n")
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error in the method - df_print_schema(). Please check the stack trace"+ str(exp))
        raise
    else:
        logger.info("The Dataframe schema validation is completed \n\n")
