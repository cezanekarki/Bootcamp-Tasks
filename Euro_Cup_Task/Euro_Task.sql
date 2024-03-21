-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType,StructField,StringType,IntegerType
-- MAGIC from pyspark.sql import functions as f
-- MAGIC from pyspark.sql.window import Window

-- COMMAND ----------


DROP TABLE IF EXISTS total_teams;

CREATE or replace TABLE total_teams (
  team_name VARCHAR(255),
  group VARCHAR(255)
);
    

-- COMMAND ----------

insert into total_teams(team_name,group)
values( 'Greece','A'),('Austria','A'),('Italy','A') ,('Hungary','A'),
('Croatia','B'),('Scotland','B'),('Albania','B'),('Spain','B'),
('Denmark','C'),('England','C'),('Slovenia','C'),('Serbia','C'),
('France','D'),('Netherlands','D'),('Switzerland','D'),('Finland','D'),
('Romania','E'),('Slovakia','E'),('Belgium','E'),('Iceland','E'),
('Turkiye','F'),('Czech Republic','F'),('Portugal','F'),('Germany','F')

-- COMMAND ----------

create or replace table round_16 as (
WITH cte4 AS (
WITH cte3 AS (
WITH cte2 AS (
WITH cte1 AS (
WITH cte AS (
SELECT
  a.team_name AS team1,
  a.group AS group1,
  1 AS mp ,
  b.team_name AS team2,
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
  CASE WHEN point1 == 3 THEN 1
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
  total_teams a
CROSS JOIN
  total_teams b
ON a.group = b.group AND a.team_name < b.team_name
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
select * from cte4
)

-- COMMAND ----------

select * from round_16

-- COMMAND ----------

DROP TABLE IF EXISTS fixtures;
CREATE or replace TABLE fixtures (
    AllGroups VARCHAR(8),
    B VARCHAR(2),
    C VARCHAR(2),
    E VARCHAR(2),
    F VARCHAR(2)
);

INSERT INTO fixtures (AllGroups, B, C, E, F) VALUES
('ABCD', 'A', 'D', 'B', 'C'),
('ABCE', 'A', 'E', 'B', 'C'),
('ABCF', 'A', 'F', 'B', 'C'),
('ABDE', 'D', 'E', 'A', 'B'),
('ABDF', 'D', 'F', 'A', 'B'),
('ABEF', 'E', 'F', 'B', 'A'),
('ACDE', 'E', 'D', 'C', 'A'),
('ACDF', 'F', 'D', 'C', 'A'),
('ACEF', 'E', 'F', 'C', 'A'),
('ADEF', 'E', 'F', 'D', 'A'),
('BCDE', 'E', 'D', 'B', 'C'),
('BCDF', 'F', 'D', 'C', 'B'),
('BCEF', 'F', 'E', 'C', 'B'),
('BDEF', 'F', 'E', 'D', 'B'),
('CDEF', 'F', 'E', 'D', 'C');

-- COMMAND ----------

select * from fixtures

-- COMMAND ----------

WITH pattern AS (
WITH round_16_ordered AS (
  SELECT * FROM round_16
  WHERE rank = 3
  ORDER BY groups
)
SELECT  concat_ws('',array_agg( groups) )AS pattern
FROM round_16_ordered 
)
select * from
pattern
left join fixtures
on pattern = AllGroups

-- COMMAND ----------

CREATE or replace table round_16_brackets as(
WITH third_matches AS (
WITH pattern AS (
WITH round_16_ordered AS (
  SELECT * FROM round_16
  WHERE rank = 3
  ORDER BY groups
)
SELECT  concat_ws('',array_agg( groups) )AS pattern
FROM round_16_ordered 
)
select * from
pattern
left join fixtures
on pattern = AllGroups
)
select team,groups,rank, 1 as row_num from round_16 where rank = 1 and groups ='B'
union all
select team,groups,rank , 2 as row_num from round_16 where rank = 3 and groups =(select b from third_matches)
union all
select team,groups,rank, 3 as row_num from round_16 where rank = 1 and groups ='A'
union all
select team,groups,rank, 4 as row_num from round_16 where rank = 2 and groups ='C'
union all
select team,groups,rank, 5 as row_num from round_16 where rank = 1 and groups ='F'
union all
select team,groups,rank, 6 as row_num from round_16 where rank = 3 and groups =(select f from third_matches)
union all
select team,groups,rank, 7 as row_num from round_16 where rank = 2 and groups ='D'
union all
select team,groups,rank, 8 as row_num from round_16 where rank = 2 and groups ='E'
union all
select team,groups,rank, 9 as row_num from round_16 where rank = 1 and groups ='E'
union all
select team,groups,rank, 10 as row_num from round_16 where rank = 3 and groups =(select e from third_matches)
union all
select team,groups,rank, 11 as row_num from round_16 where rank = 1 and groups ='D'
union all
select team,groups,rank, 12 as row_num from round_16 where rank = 2 and groups ='F'
union all
select team,groups,rank, 13 as row_num from round_16 where rank = 1 and groups ='C'
union  all
select team,groups,rank, 14 as row_num from round_16 where rank = 3 and groups =(select c from third_matches)
union all
select team,groups,rank, 15 as row_num from round_16 where rank = 2 and groups ='A'
union all
select team,groups,rank, 16 as row_num from round_16 where rank = 2 and groups ='B'
)

-- COMMAND ----------

select * from round_16_brackets
order by row_num

-- COMMAND ----------

create or replace table round_16_matches as(
select a.team as team1,a.groups as group1,a.rank as rank1,a.row_num as row1,
b.team as team2,b.groups as group2 ,b.rank as rank2, b.row_num as row2,
case 
  when FLOOR(RAND() * 10) > 5 then 1
  else 2
  end as won
from round_16_brackets a 
inner join round_16_brackets b 
on a.row_num=(b.row_num-1)
where a.row_num %2 = 1 and b.row_num %2=0
order by a.row_num
)

-- COMMAND ----------

select * from round_16_matches

-- COMMAND ----------

select
case
  when won = 1 then team1 
  else team2 
  end as team,
row_number() over (order by 1) as row_num
from round_16_matches

-- COMMAND ----------

create or replace table quarter_finalists as(
with quarter_finalists as (
select
case
  when won = 1 then team1 
  else team2 
  end as team,
row_number() over (order by 1) as row_num
from round_16_matches
)
select a.team as team1,a.row_num as row1,
b.team as team2, b.row_num as row2,
case 
  when FLOOR(RAND() * 10) > 5 then 1
  else 2
  end as won
from quarter_finalists a 
inner join quarter_finalists b 
on a.row_num=(b.row_num-1)
where a.row_num %2 = 1 and b.row_num %2=0
order by a.row_num
)

-- COMMAND ----------

select 
case
  when won = 1 then team1
  else team2
  end as team,
row_number() over (order by 1) as row_num
from quarter_finalists

-- COMMAND ----------

create or replace table semi_finalists as(
with semi_finalists as (
select
case
  when won = 1 then team1 
  else team2 
  end as team,
row_number() over (order by 1) as row_num
from quarter_finalists
)
select a.team as team1,a.row_num as row1,
b.team as team2, b.row_num as row2,
case 
  when FLOOR(RAND() * 10) > 5 then 1
  else 2
  end as won
from semi_finalists a 
inner join semi_finalists b 
on a.row_num=(b.row_num-1)
where a.row_num %2 = 1 and b.row_num %2=0
order by a.row_num
)

-- COMMAND ----------

select 
case
  when won = 1 then team1
  else team2
  end as team,
row_number() over (order by 1) as row_num
from semi_finalists
     

-- COMMAND ----------

with finalists as (
  select 
case
  when won = 1 then team1
  else team2
  end as team,
row_number() over (order by 1) as row_num
from semi_finalists
)
select team from finalists
where row_num = case when floor(rand()*10)>5 then 1 else 2 end
     

-- COMMAND ----------


