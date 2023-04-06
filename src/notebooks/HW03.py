# Databricks notebook source
#Load dataset
df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header=True)
df_drivers = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True)
df_pitstops = spark.read.csv('s3://columbia-gr5069-main/raw/pit_stops.csv', header=True)
df_results = spark.read.csv('s3://columbia-gr5069-main/raw/results.csv', header=True)
df_status = spark.read.csv('s3://columbia-gr5069-main/raw/status.csv', header=True)
df_races = spark.read.csv('s3://columbia-gr5069-main/raw/races.csv', header=True)

# COMMAND ----------

display(df_pitstops)

# COMMAND ----------

# MAGIC %md [10 pts] What was the average time each driver spent at the pit stop for each race?

# COMMAND ----------

from pyspark.sql.functions import avg


# group by driver and race, and calculate the average of the duration column
avg_time = df_pitstops.groupBy('raceID','driverID').agg(avg('duration'))

# show the results
display(avg_time)

# COMMAND ----------

# MAGIC %md [20 pts] Rank the average time spent at the pit stop in order of who won each race

# COMMAND ----------

display(df_results)

# COMMAND ----------

# join the two data frames on raceId
df_pitstops_driver = df_results.join(avg_time, on =['driverID'])
# filter the winner only and sort by raceId and average pit stop duration
df_pitstops_rank = df_pitstops_driver.filter(df_pitstops_driver.rank ==1).sort(["raceID", "avg(duration)"]).select(['driverID',"raceID","avg(duration)"])
display(df_pitstops_rank)

# COMMAND ----------

# MAGIC %md [20 pts] Insert the missing code (e.g: ALO for Alonso) for drivers based on the 'drivers' dataset

# COMMAND ----------

from pyspark.sql.functions import substring, upper


# create a new column with the missing code for each driver
df_drivers = df_drivers.withColumn('code', upper(substring(df_drivers.surname, 1, 3)))

# show the results
display(df_drivers)

# COMMAND ----------

# MAGIC %md [20 pts] Who is the youngest and oldest driver for each race? Create a new column called “Age”

# COMMAND ----------

from pyspark.sql.functions import datediff
from pyspark.sql.functions import current_date
from pyspark.sql.types import IntegerType

df_drivers = df_drivers.withColumn('age', datediff(current_date(),df_drivers.dob)/365)
df_drivers = df_drivers.withColumn('age', df_drivers['age'].cast(IntegerType()))
display(df_drivers)

# COMMAND ----------

df_drivers_results = df_drivers.select(["driverID","age"]).join(df_results.select(["raceID", "driverID", "position"]), on = ["driverID"] )
display(df_drivers_results.sort(["raceID","age"]))


# COMMAND ----------

from pyspark.sql.functions import min

df_min_age = df_drivers_results.groupBy('raceId').agg(min('age').alias('age'))

df_youngest_driver = df_min_age.join(df_drivers_results, ['raceId', 'age'])

display(df_youngest_driver.sort('raceID'))

# COMMAND ----------

from pyspark.sql.functions import max

df_max_age = df_drivers_results.groupBy('raceId').agg(max('age').alias('age'))

df_oldest_driver = df_max_age.join(df_drivers_results, ['raceId', 'age'])

display(df_oldest_driver.sort('raceID'))

# COMMAND ----------

# MAGIC %md [20 pts] For a given race, which driver has the most wins and losses?

# COMMAND ----------

from pyspark.sql.functions import count, when


# filter by the desired race; since the question didn't specify, I use raceID = 1 as example
race_Id = 1  
df_drivers_results_raceID = df_drivers_results.filter(df_drivers_results["raceId"] == race_Id)

# count the number of wins and losses for each driver
df_wins_losses = df_drivers_results_raceID.groupby(['driverId']).agg(
    count(when(df_drivers_results_raceID.position == 1, True)).alias('wins'),
    count(when(df_drivers_results_raceID.position > 1, True)).alias('losses')
)

# order by wins and show the result
display(df_wins_losses.sort(df_wins_losses.wins.desc(),df_wins_losses.losses.desc()))

# COMMAND ----------

# MAGIC %md [10 pts] Continue exploring the data by answering your own question.

# COMMAND ----------

# Question: Rank the driver with the most wins in his/her life.

# COMMAND ----------

# count the number of wins for each driver
df_drivers_results_wins = df_drivers_results.groupby('driverId').agg(
    count(when(df_drivers_results.position == 1, True)).alias('wins')
)

# order by the number of wins
df_drivers_results_rank = df_drivers_results_wins.sort(df_drivers_results_wins.wins.desc())

# show the results
display(df_drivers_results_rank)
