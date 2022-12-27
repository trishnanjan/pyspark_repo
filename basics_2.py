import findspark
findspark.init()

from pyspark.sql import SparkSession

spark2=SparkSession.builder.appName('Ops').getOrCreate()

df=spark2.read.option("inferschema", "true").csv('C:/BigData/weather.csv',header=True)

df.show(50,truncate=False)

result=df.filter((df['MaxTemp']>35) & (df['MinTemp']<16)).collect()
row=result[0]
print(row.asDict())


