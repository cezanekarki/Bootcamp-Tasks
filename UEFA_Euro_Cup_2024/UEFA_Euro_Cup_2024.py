# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, row_number
from pyspark.sql import functions as F 
from pyspark.sql.window import Window
import random

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("EuroCup2024") \
    .getOrCreate()

# European team names
european_teams = [
    ("Germany", "A"), ("Scotland", "A"), ("Hungary", "A"), ("Switzerland", "A"),
    ("Spain", "B"), ("Croatia", "B"), ("Italy", "B"), ("Albania", "B"),
    ("Slovenia", "C"), ("Denmark", "C"), ("Serbia", "C"), ("England", "C"), 
    ("Finland", "D"), ("Nether lands", "D"), ("Austria", "D"),("France", "D"),
    ("Belgium", "E"),("Slovakia", "E"), ("Romania", "E"),("Iceland", "E"),
    ("Türkiye", "F"), ("Greece", "F"), ("Portugal", "F"), ("Czech", "F")
]
# Create DataFrame for Group Stage
group_stage_df = spark.createDataFrame(european_teams, ["teamname", "group"])

# Show the Group Stage table
print("Table 1: Group Stage")
group_stage_df.show()


# COMMAND ----------

# This table is made according to official 'knockout phase' rules of the tournament. So Directly copied from Wikipedia.
values = [
    ('ABCD', 'A', 'D', 'B', 'C'),
    ( 'ABCE', 'A', 'E', 'B', 'C'),
    ( 'ABCF', 'A', 'F', 'B', 'C'),
    ( 'ABDE', 'D', 'E', 'A', 'B'),
    ( 'ABDF', 'D', 'F', 'A', 'B'),
    ( 'ABEF', 'E', 'F', 'B', 'A'),
    ( 'ACDE', 'E', 'D', 'C', 'A'),
    ( 'ACDF', 'F', 'D', 'C', 'A'),
    ( 'ACEF', 'E', 'F', 'C', 'A'),
    ( 'ADEF', 'E', 'F', 'D', 'A'),
    ( 'BCDE', 'E', 'D', 'B', 'C'),
    ( 'BCDF', 'F', 'D', 'C', 'B'),
    ( 'BCEF', 'F', 'E', 'C', 'B'),
    ( 'BDEF', 'F', 'E', 'D', 'B'),
    ( 'CDEF', 'F', 'E', 'D', 'C')
]

df_knockout_phase = spark.createDataFrame(values, ["Third_placed_qualified_teams", "B", "C", "E", "F"])
df_knockout_phase.show()


# COMMAND ----------

# Registering DataFrame as temporary view
group_stage_df.createOrReplaceTempView("group_of_24")

# Executing the SQL query
group_of_16 = spark.sql("""
WITH cte4 AS (
WITH cte3 AS (
WITH cte2 AS (
WITH cte1 AS (
WITH cte AS (
SELECT
  a.teamname AS team1,
  a.group AS group1,
  1 AS mp ,
  b.teamname AS team2,
  b.group AS group2,
  FLOOR(RAND() * 5) AS goals1,
  FLOOR(RAND() * 5) AS goals2,
  CASE 
    WHEN goals1 > goals2 THEN 3
    WHEN goals1 == goals2 THEN 1
    ELSE 0
    END as point1,
  CASE
    WHEN goals2 > goals1 THEN 3
    WHEN goals2 == goals1 THEN 1
    ELSE 0
    END as point2 ,
  CASE
    WHEN point1 == 3 THEN 1
    ELSE 0
    END as w1,
  CASE
    WHEN point2 == 3 THEN 1
    ELSE 0
    END as w2, 
 CASE
    WHEN point1 == 1 THEN 1
    ELSE 0
    END as d1,
  CASE
    WHEN point2 == 1 THEN 1
    ELSE 0
    END as d2,  
  CASE
    WHEN point1 == 0 THEN 1
    ELSE 0
    END as l1,
  CASE
    WHEN point2 == 0 THEN 1
    ELSE 0
    END as l2
FROM
  group_of_24 a
CROSS JOIN
  group_of_24 b
ON
  a.group = b.group AND a.teamname < b.teamname
)
select team1 as team,group1 as groups ,mp,goals1 as gf, goals2 as ga,point1 as pts ,w1 as w,d1 as d,l1 as l from cte 
union all
select team2 as team,group2 as groups,mp,goals2 as gf, goals1 as ga,point2 as pts ,w2 as w,d2 as d,l2 as l from cte 
)
select team, groups , sum(w) as w, sum(d) as d, sum(l) as l, sum(mp) as mp,sum(gf) as gf,sum(ga) as ga,sum(gf -ga) as gd,sum(pts) as pts 
from cte1 
group by team,groups
order by groups, pts desc, gd desc, gf desc
)
select *,
row_number() over(partition by groups order by pts desc, gd desc, gf desc) as rank
from cte2
)

select * 
from cte3
where rank <= 3
order by rank,pts desc,gd desc, gf desc
limit 16
)
select * from cte4""")

# Assume df is your DataFrame
group_of_16 = group_of_16.withColumnRenamed("team", "Team") \
       .withColumnRenamed("groups", "Groups") \
       .withColumnRenamed("w", "Wins") \
       .withColumnRenamed("d", "Draws") \
       .withColumnRenamed("l", "Losses") \
       .withColumnRenamed("mp", "MatchesPlayed") \
       .withColumnRenamed("gf", "GoalsFor") \
       .withColumnRenamed("ga", "GoalsAgainst") \
       .withColumnRenamed("gd", "GoalDifference") \
       .withColumnRenamed("pts", "Points")

group_of_16.display()

# COMMAND ----------

group_of_16.createOrReplaceTempView("group_of_16")
df_knockout_phase.createOrReplaceTempView("df_knockout_phase")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH combo AS (
# MAGIC WITH group_of_16_ordered AS (
# MAGIC   SELECT * FROM group_of_16
# MAGIC   WHERE rank = 3
# MAGIC   ORDER BY groups
# MAGIC   limit 4
# MAGIC )
# MAGIC SELECT  concat_ws('',array_agg(groups) )AS pattern
# MAGIC FROM group_of_16_ordered
# MAGIC )
# MAGIC select * from
# MAGIC combo
# MAGIC left join df_knockout_phase
# MAGIC on pattern = Third_placed_qualified_teams

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace table group_of_16_brackets as(
# MAGIC WITH combination AS (
# MAGIC WITH pattern AS (
# MAGIC WITH group_of_16_ordered AS (
# MAGIC   SELECT * FROM group_of_16
# MAGIC   WHERE rank = 3
# MAGIC   ORDER BY groups
# MAGIC   limit 4
# MAGIC )
# MAGIC SELECT  concat_ws('',array_agg( groups) )AS pattern
# MAGIC FROM group_of_16_ordered 
# MAGIC )
# MAGIC select * from
# MAGIC pattern
# MAGIC left join df_knockout_phase
# MAGIC on pattern = Third_placed_qualified_teams
# MAGIC )
# MAGIC select team, groups,rank, 1 as row_num from group_of_16 where rank = 1 and groups ='B'
# MAGIC union all
# MAGIC select team, groups,rank , 2 as row_num from group_of_16 where rank = 3 and groups =  (select B from combination)
# MAGIC union all
# MAGIC select team,groups,rank, 3 as row_num from group_of_16 where rank = 1 and groups ='A'
# MAGIC union all
# MAGIC select team,groups,rank, 4 as row_num from group_of_16 where rank = 2 and groups ='C'
# MAGIC union all
# MAGIC select team,groups,rank, 5 as row_num from group_of_16 where rank = 1 and groups ='F'
# MAGIC union all
# MAGIC select team,groups,rank, 6 as row_num from group_of_16 where rank = 3 and groups =(select E from combination)
# MAGIC union all
# MAGIC select team,groups,rank, 7 as row_num from group_of_16 where rank = 2 and groups ='D'
# MAGIC union all
# MAGIC select team,groups,rank, 8 as row_num from group_of_16 where rank = 2 and groups ='E'
# MAGIC union all
# MAGIC select team,groups,rank, 9 as row_num from group_of_16 where rank = 1 and groups ='E'
# MAGIC union all
# MAGIC select team,groups,rank, 10 as row_num from group_of_16 where rank = 3 and groups =(select F from combination)
# MAGIC union all
# MAGIC select team,groups,rank, 11 as row_num from group_of_16 where rank = 1 and groups ='D'
# MAGIC union all
# MAGIC select team,groups,rank, 12 as row_num from group_of_16 where rank = 2 and groups ='F'
# MAGIC union all
# MAGIC select team,groups,rank, 13 as row_num from group_of_16 where rank = 1 and groups ='C'
# MAGIC union  all
# MAGIC select team,groups,rank, 14 as row_num from group_of_16 where rank = 3 and groups =(select C from combination)
# MAGIC union all
# MAGIC select team,groups,rank, 15 as row_num from group_of_16 where rank = 2 and groups ='A'
# MAGIC union all
# MAGIC select team,groups,rank, 16 as row_num from group_of_16 where rank = 2 and groups ='B'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from group_of_16_brackets
# MAGIC order by row_num

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH group_of_16_ordered AS (
# MAGIC   SELECT * FROM group_of_16
# MAGIC   WHERE rank = 3
# MAGIC   ORDER BY groups
# MAGIC   limit 4
# MAGIC ) 
# MAGIC select * FROM group_of_16_ordered

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH combination AS (
# MAGIC WITH pattern AS (
# MAGIC WITH group_of_16_ordered AS (
# MAGIC   SELECT * FROM group_of_16
# MAGIC   WHERE rank = 3
# MAGIC   ORDER BY groups
# MAGIC   limit 4
# MAGIC )
# MAGIC SELECT  concat_ws('',array_agg( groups) )AS pattern
# MAGIC FROM group_of_16_ordered 
# MAGIC )
# MAGIC select * from
# MAGIC pattern
# MAGIC left join df_knockout_phase
# MAGIC on pattern = Third_placed_qualified_teams)
# MAGIC select * from combination

# COMMAND ----------

# DBTITLE 1,Group 16 Matches
group16 = spark.sql("""SELECT
    t1.team AS team1,
    t1.groups AS group1,
    t1.rank AS rank1,
    t1.row_num AS row1,
    t2.team AS team2,
    t2.groups AS group2,
    t2.rank AS rank2,
    t2.row_num AS row2,
    CASE WHEN FLOOR(RAND() * 10) > 5 then team1 else team2 end as won
FROM
    group_of_16_brackets t1
JOIN
    group_of_16_brackets t2
ON
    t1.row_num % 2 = 1 AND t2.row_num % 2 = 0 AND t1.row_num + 1 = t2.row_num
""")

group16.show()

# COMMAND ----------

group16_winner = group16.select("won")
group16_winner.show()

window_spec = Window.orderBy(F.monotonically_increasing_id())
group16_winner = group16_winner.withColumn("teamId", F.row_number().over(window_spec))

# Create a DataFrame with odd teamId teams as team1 and even teamId teams as team2
team1_df = group16_winner.filter((F.col("teamId") % 2) != 0).withColumnRenamed("teamId", "team1_id").withColumnRenamed("won", "team1")
team1_df = team1_df.withColumn("teamId", F.row_number().over(window_spec)).drop('team1_id')
team2_df = group16_winner.filter((F.col("teamId") % 2) == 0).withColumnRenamed("teamId", "team2_id").withColumnRenamed("won", "team2")
team2_df = team2_df.withColumn("teamId", F.row_number().over(window_spec)).drop('team2_id')
team1_df.show()
team2_df.show()

# Join team1 and team2 DataFrames to get the final DataFrame
quarterfinals_df = team1_df.join(team2_df, ["teamId"], "inner")
# Add a column for winner (randomly chosen between team1 and team2)
quarterfinals_df = quarterfinals_df.withColumn("winner", F.when(F.rand() > 0.5, quarterfinals_df["team1"]).otherwise(quarterfinals_df["team2"]))

# Select necessary columns
quarterfinals_df = quarterfinals_df.select("team1", "team2", "winner")

quarterfinals_df.show()

# COMMAND ----------

# DBTITLE 1,Semifinal
quarterfinals_df = quarterfinals_df.select("winner")
quarterfinals_df.show()

window_spec = Window.orderBy(F.monotonically_increasing_id())
quarterfinals_df = quarterfinals_df.withColumn("teamId", F.row_number().over(window_spec))

# Create a DataFrame with odd teamId teams as team1 and even teamId teams as team2
team1_df = quarterfinals_df.filter((F.col("teamId") % 2) != 0).withColumnRenamed("teamId", "team1_id").withColumnRenamed("winner", "team1")
team1_df = team1_df.withColumn("teamId", F.row_number().over(window_spec)).drop('team1_id')
team2_df = quarterfinals_df.filter((F.col("teamId") % 2) == 0).withColumnRenamed("teamId", "team2_id").withColumnRenamed("winner", "team2")
team2_df = team2_df.withColumn("teamId", F.row_number().over(window_spec)).drop('team2_id')
team1_df.show()
team2_df.show()

# Join team1 and team2 DataFrames to get the final DataFrame
semifinals_df = team1_df.join(team2_df, ["teamId"], "inner")
# Add a column for winner (randomly chosen between team1 and team2)
semifinals_df = semifinals_df.withColumn("winner", F.when(F.rand() > 0.5, semifinals_df["team1"]).otherwise(semifinals_df["team2"]))

# Select necessary columns
semifinals_df = semifinals_df.select("team1", "team2", "winner")

semifinals_df.show()

# COMMAND ----------

# DBTITLE 1,Final
semifinals_df = semifinals_df.withColumn("random_number", F.rand())
random_winner = semifinals_df.select(F.when(F.col("random_number") > 0.5, semifinals_df["winner"]).otherwise(semifinals_df["winner"])).first()[0]

print("Winner of the Tournament:", random_winner)
