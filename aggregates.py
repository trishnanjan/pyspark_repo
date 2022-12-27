import findspark
findspark.init()

from pyspark.sql import SparkSession

spark3=SparkSession.builder.appName('Aggs').getOrCreate()

df=spark3.read.option("inferschema", "true").csv('C:/BigData/appl_stock.csv',header=True)


#df.show()
#df.printSchema()

#df1=df.groupby("Productline").mean("sales").show()  ##Using groupBy
#df2=df1=df.groupby("Productline").sum("quantityordered").show()

#df.agg({'Sales':'Max'}).show()


from pyspark.sql.functions import avg,countDistinct,stddev

from pyspark.sql.functions import  format_number

#sales_std=df.select(avg('Sales').alias('Avg Sales'))
#sales_std.select(format_number('Avg Sales',2).alias('FInal Sales')).show()

#df.orderBy(df['Sales'].desc()).show()
#df.orderBy(df['Sales'].asc()).show()

from pyspark.sql.functions import (date_format,dayofmonth,dayofyear,hour,month,year,weekofyear,format_number)


df.printSchema()
df.select(year(df['Date'])).show()

newdf=df.withColumn('Year',year(df['Date']))
result=newdf.groupby('Year').mean().select(['Year','avg(Close)'])
final=result.withColumnRenamed('avg(Close)','Average Closing Price')
final.select(['Year',format_number('Average Closing Price',2).alias('Average Closing Price')]).show()
