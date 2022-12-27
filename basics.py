import findspark
findspark.init()
#Findspark can add a startup file to the current IPython
# profile so that the environment vaiables will be properly set and pyspark will be imported upon IPython startup.
from pyspark.sql import SparkSession # import spark session
from pyspark.sql.types import (StructField,StringType,IntegerType,StructType)
spark= SparkSession.builder.appName('Basics').getOrCreate() #start the spark session

df=spark.read.option("inferschema", "true").csv('C:/BigData/people.csv',header=True)

df.show()
df.printSchema()
df.columns
df.describe().show() ##statistical summary of numeric columns



new_schema= [StructField('Name',StringType(),True),
             StructField('Age',IntegerType(),True),
             StructField('CTC',IntegerType(),True)] ##define the schema structure of your choice

final_struc=StructType(fields=new_schema)

df=spark.read.option("inferschema", "true").csv('C:/BigData/people.csv',header=True,schema=final_struc)
df.printSchema()

df.select(['Name','Age']).show() ##show column data of dataframe

df.withColumn('NewAge',df['Age']*2).show() #add a column in df

df.withColumnRenamed('Age','age').show()

df.createOrReplaceTempView('People') # Create a table from df

results=spark.sql("Select * from People where age>20")
results.show()



