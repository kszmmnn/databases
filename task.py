from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, date
import os
import sys
import time


#as I am saving small files I am using option coalesce(1) while saving

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


spark = SparkSession.builder.master("spark://master:7077").appName("Project").getOrCreate()
print("spark session created")

dfTrips = spark.read.parquet("hdfs://master:9000/user/hadoop/intask/")
dfZones = spark.read.csv("hdfs://master:9000/user/hadoop/intaskcsv/taxi+_zone_lookup.csv", header=True)

# for filtering unwanted dates
dfTrips=dfTrips.filter(dfTrips.tpep_dropoff_datetime <= lit("2022-07-01"))      
dfTrips=dfTrips.filter(dfTrips.tpep_dropoff_datetime >= lit("2022-01-01"))



rddTrips = dfTrips.rdd
rddZones = dfTrips.rdd

dfZones.createOrReplaceTempView("dfzones")
dfTrips.createOrReplaceTempView("dftrips")

# Q1 To find the route with the biggest tip in March and arrival point "Battery Park"
start = time.time()
endingZones = spark.sql("select * from dfzones where Zone like 'Battery Park'")
endingZones = endingZones.withColumn("LocationID", endingZones.LocationID.cast('long'))
endingZones.createOrReplaceTempView("endingZones")

tipsMarch = spark.sql("select * from dftrips where month(tpep_dropoff_datetime) == 3")
tipsMarch.createOrReplaceTempView("tipsMarch")

maxTipsMarch = spark.sql("select tipsMarch.* from tipsMarch inner join endingZones on \
                             endingZones.LocationID == tipsMarch.DOLocationID order by tipsMarch.tip_amount")
maxTipsMarch.createOrReplaceTempView("maxTipsMarch")

result = spark.sql("select * from maxTipsMarch order by tip_amount desc").limit(1)
result.coalesce(1).write.save("hdfs://master:9000/user/hadoop/out/q1", format="csv", mode="overwrite", header=True)
end = time.time()
result.show()
print(f'Time for Q 1: {end-start} seconds.')


# Q2 To find, for each month, the route with the highest amount in tolls. Ignore zero amounts.

# Here I suppose, that I need to find 6 highest amounts, zeroes (and negatives) excluded.
start = time.time()
noZeros = dfTrips.filter(col("tolls_amount") > 0)
maxes = noZeros.groupBy(month("tpep_dropoff_datetime").alias("month")).agg(max("tolls_amount").alias("max_toll"))
maxes.orderBy(asc("month"))
maxes.createOrReplaceTempView("maxes")


result = spark.sql("select * from dftrips inner join maxes where dftrips.tolls_amount == maxes.max_toll and"+
    " month(dftrips.tpep_dropoff_datetime) == maxes.month")
result.coalesce(1).write.save("hdfs://master:9000/user/hadoop/out/q2", format="csv", mode="overwrite", header=True)
end = time.time()
result.show()
print(f'Time for Q 2: {end-start} seconds.')

# Q3 To find, per 15 days, the average distance and cost for all routes with a point of departure
#           different from the point of arrival.

# Per 15 days, meaning whichever 15 day so I will consider first 15 days (dropoffs) of January 2022

def toCSVLine(data):
  return ','.join(str(d) for d in data)


start1 = time.time()

#sql
days = dfTrips.filter(dfTrips.tpep_dropoff_datetime < lit("2022-01-16"))
days = days.filter(days.PULocationID != days.DOLocationID)
days.createOrReplaceTempView("days")
spark.sql("select avg(trip_distance), avg(total_amount) from days").show()
days.coalesce(1).write.save("hdfs://master:9000/user/hadoop/out/q3sql", format="csv", mode="overwrite")

end1 = time.time()
start2 = time.time()

#rdd
trips = rddTrips.filter(lambda x: (int(x.tpep_dropoff_datetime.timestamp()) < int(datetime(2022, 1, 16).timestamp())) and 
                            (x.PULocationID != x.DOLocationID)).map(lambda x: (x.trip_distance, x.total_amount))
tripdistance = trips.map(lambda x: x[0]).mean()
totalamount = trips.map(lambda x: x[1]).mean()
lines = rddTrips.map(toCSVLine)
lines.saveAsTextFile('hdfs://master:9000/user/hadoop/out/q3rdd')
print( "Average distance:", tripdistance, "Average cost:", totalamount)

end2 = time.time()
days.show()
print(lines)
print(f'Time for Q 3 SQL: {end1-start1} seconds.')
print(f'Time for Q 3 RDD: {end2-start2} seconds.')

# Q4 To find the three biggest (top 3) peak hours per day of the week, meaning the hours (eg,
#           7-8am, 3-4pm, etc) of the day with the highest number of passengers in a taxi race. The
#           calculation concerns all months.

# I understand the task this way: I am supposed to find top 3 peak hours per day, meaning 3*7 values in total. Each day of 
# week will be considered in group: all Mondays together, all Tuesdays etc. It is a sum of passengers in an hour, 
# but each day is summed separately.

start = time.time()
maxes = dfTrips.select(date_trunc("Hour","tpep_dropoff_datetime").alias("date"), "passenger_count")
maxes = maxes.groupBy("date").agg(sum("passenger_count").alias("passenger_sum"))
maxes = maxes.groupBy(dayofweek("date").alias("day"), hour("date").alias("hour")).agg(max("passenger_sum").alias("max_sum"))
window = Window.partitionBy(maxes['day']).orderBy(maxes['max_sum'].desc())
result = maxes.select('day', 'hour', 'max_sum', rank().over(window).alias('rank')).filter(col('rank') <= 3)
result.coalesce(1).write.save("hdfs://master:9000/user/hadoop/out/q4", format="csv", mode="overwrite", header=True)

end = time.time()
result.show(21)
print(f'Time for Q 4: {end-start} seconds.')

# Q5 Find the top five (top 5) days per month in which the races had the highest tip percentage.
#            For example, if the ride cost $10 (fare_amount) and the tip was $5, the percentage is 50%.
start = time.time()
maxes = dfTrips.select("tpep_dropoff_datetime", date_trunc("Month","tpep_dropoff_datetime")\
        .alias("helping_date"), "tip_amount", "fare_amount")\
        .withColumn('tip_percentage', dfTrips.tip_amount/dfTrips.fare_amount*100)
window = Window.partitionBy(maxes['helping_date']).orderBy(maxes['tip_percentage'].desc())
result = maxes.select('tpep_dropoff_datetime', 'tip_amount', 'fare_amount', 'tip_percentage',  rank().over(window).alias('rank'))\
        .filter(col('rank') <= 5).limit(35)
result.coalesce(1).write.save("hdfs://master:9000/user/hadoop/out/q5", format="csv", mode="overwrite", header=True)

end = time.time()
result.show(35)
print(f'Time for Q 5: {end-start} seconds.')
