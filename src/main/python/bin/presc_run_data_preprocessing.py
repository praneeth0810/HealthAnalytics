import logging
import logging.config
from pyspark.sql.functions import upper, lit, regexp_extract, col, concat_ws, count, isnan, isnull, when, avg, round, \
    coalesce
from pyspark.sql.window import Window

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def perform_data_clean(df1, df2):
    # Cleaning df_city dataframe

    ''' Select required column and convert city fields to upper case '''
    try:
        logger.info(f"Data cleaning started for City file....\n\n")

        df_city_sel = df1.select(upper(df1.city).alias("city"),
                                 df1.state_id,
                                 upper(df1.state_name).alias("state_name"),
                                 upper(df1.county_name).alias("county_name"),
                                 df1.population,
                                 df1.zips)
        logger.info(f"Data cleaning started for Prescriptions file....\n\n")
        # Cleaning df_fact dataframe
        ''' Data Cleaning 
        1. Select only required columns
        2. Rename the columns
        3. Add a country field USA
        4. Clean years_of_exp field
        5. Convert the years_of_exp datatype from string to number
        6. Combine First Name and Last name
        7. Check and clean all the null/Nan values
        8. Impute trx_cnt where it is null as avg of trx_cnt for the prescriber
        '''

        df_fact_sel = df2.select(df2.npi.alias("presc_id"),
                                 df2.nppes_provider_last_org_name.alias("presc_lname"),
                                 df2.nppes_provider_first_name.alias("presc_fname"),
                                 df2.nppes_provider_city.alias("presc_city"),
                                 df2.nppes_provider_state.alias("presc_state"),
                                 df2.specialty_description.alias("presc_spclt"),
                                 df2.years_of_exp,
                                 df2.drug_name,
                                 df2.total_claim_count.alias("trx_cnt"),
                                 df2.total_day_supply,
                                 df2.total_drug_cost)

        df_fact_sel = df_fact_sel.withColumn("country_name", lit("USA"))


        pattern = '\d+'
        idx = 0
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", regexp_extract(col("years_of_exp"), pattern, idx))

        df_fact_sel = df_fact_sel.withColumn("years_of_exp", col("years_of_exp").cast("int"))

        df_fact_sel = df_fact_sel.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
        df_fact_sel = df_fact_sel.drop("presc_fname", "presc_lname")

        #DELETE RECORDS WHERE PRESC_ID AND DRUG NAME IS NULL
        df_fact_sel = df_fact_sel.dropna(subset=["presc_id","drug_name"])

        logger.info(f"Imputing values")
        #IMPUTING VALUES
        spec = Window.partitionBy("presc_id")
        df_fact_sel=df_fact_sel.withColumn('trx_cnt',coalesce("trx_cnt",round(avg("trx_cnt").over(spec))))
        df_fact_sel=df_fact_sel.withColumn("trx_cnt",col("trx_cnt").cast('integer'))

    except Exception as exp:
        logger.error("Error in the method - perform_data_clean(). Please check the Stack Trace. " + str(exp),
                     exc_info=True)
        raise
    else:
        logger.info("Data cleaning is done.\n\n")
    return df_city_sel, df_fact_sel
