import findspark
findspark.init()

from pyspark.sql import SparkSession

from pyspark.sql.types import (StructField,StringType,IntegerType,StructType,DateType)

spark=SparkSession.builder.appName('miss').getOrCreate()

df=spark.read.option("inferschema", "true").csv('C:/BigData/employee.csv',header=True)

df.show()

df.na.drop(thresh=2).show() ##drop null values with min 2  null values

df.na.drop(how='all') #drop all null values
df.na.drop(how='any') #drop fields with any null values
df.na.drop(subset='Age').show() # drop a specific column values with null values

df.na.fill('N/A').show()  ## fill missing values

## Problem : Replace the NULL values in salary field with mean value
from pyspark.sql.functions import mean

mean_sal=df.select(mean('salary')).collect() #collect the mean of salary field
mean_val=mean_sal[0][0] # index to first row, then the value of the dict


df.na.fill(mean_val,subset='Salary').show() # specify the column to be filled






