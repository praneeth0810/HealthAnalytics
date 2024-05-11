import datetime as date
from pyspark.sql.functions import lit
import logging
import logging.config
logger = logging.getLogger(__name__)

def data_persist_postgre(spark, df, dfName, url, driver, dbtable, mode, user, password):
    try:
        logger.info(f"Data Persist Postgre Script  - data_persist_rdbms() is started for saving dataframe "+ dfName + " into Postgre Table...\n\n")
        df.write.format("jdbc")\
                .option("url", url) \
                .option("driver", driver) \
                .option("dbtable", dbtable) \
                .mode(mode) \
                .option("user", user) \
                .option("password", password) \
                .save()
    except Exception as exp:
        logger.error("Error in the method - data_persist_postgre(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Data Persist Postgre- data_persist_postgre() is completed for saving dataframe "+ dfName +" into Postgre Table...\n\n")

