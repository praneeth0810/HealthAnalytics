# This is a user defined function used in data transformation.
#Specifically using to split zip codes for data_city which are in form of list.

from pyspark.sql.functions import  udf
from pyspark.sql.types import IntegerType

@udf(returnType= IntegerType())
def column_split_cnt(column):
    return len(column.split(' '))
