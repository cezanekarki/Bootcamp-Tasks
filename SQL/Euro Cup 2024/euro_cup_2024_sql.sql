--drop table #euro_cup_2024;
--drop table matches
--Temporary table for storing group and team data of euro 2024
go
CREATE  TABLE #euro_cup_2024 (
  team VARCHAR(255) PRIMARY KEY,
  group_name VARCHAR(2) NOT NULL
);
 
go
INSERT INTO #euro_cup_2024 (team, group_name)
VALUES
  ('Germany', 'A'),
  ('Scotland', 'A'),
  ('Hungary', 'A'),
  ('Switzerland', 'A'),
  ('Spain', 'B'),
  ('Croatia', 'B'),
  ('Italy', 'B'),
  ('Albania', 'B'),
  ('Slovenia', 'C'),
  ('Denmark', 'C'),
  ('Serbia', 'C'),
  ('England', 'C'),
  ('Netherlands', 'D'),
  ('Austria', 'D'),
  ('France', 'D'),
  ('Malta', 'D'),
  ('Belgium', 'E'), 
  ('Slovakia', 'E'),
  ('Romania', 'E'),
  ('Greece', 'E'),
  ('Turkey', 'F'),
  ('Portugal', 'F'),
  ('Czechia', 'F'),
  ('Finland', 'F');

--creating table matches for storing team, goal and stage information
 go
 CREATE  TABLE matches (
 group_name varchar(255),
 team1 VARCHAR(255),
 goal1  int,
 team2 varchar(255),
 goal2 int,
 stage varchar(255)
);

go
INSERT INTO matches (group_name, team1, goal1, team2, goal2, stage)
SELECT
    t1.group_name,
    t1.team AS team1,
    NULL AS goal1,
    t2.team AS team2,
    NULL AS goal2,
    'GroupStage' AS stage
FROM
    #euro_cup_2024 t1
CROSS JOIN
    #euro_cup_2024 t2
WHERE
    t1.group_name = t2.group_name 
    AND t1.team < t2.team -- Avoid duplicate pairs (A vs B is the same as B vs A)
ORDER BY
    group_name, team1, team2;

EXEC UpdateMatchResults @stage = 'GroupStage';
exec Standings @StageName = 'GroupStage'
exec Winners @ViewName = 'group_stage_top2_winners', @RowNumValues = '1,2', @TableName = 'GroupStage'

go
IF OBJECT_ID('tempdb..#round16_combination', 'U') IS NOT NULL
    DROP TABLE #round16_combination;
create  table #round16_combination (
	third_place_combination varchar(255) primary key,
	B1 varchar(20) not null,
	C1 varchar(20) not null,
	E1 varchar(20) not null,
	F1 varchar(20) not null
);

go
insert into #round16_combination
values ('ABCD', 'A3', 'D3', 'B3', 'C3'),
	   ('ABCE', 'A3', 'E3', 'B3', 'C3'),
	   ('ABCF', 'A3', 'F3', 'B3', 'C3'),
	   ('ABDE', 'D3', 'E3', 'A3', 'B3'),
	   ('ABDF', 'D3', 'F3', 'A3', 'B3'),
	   ('ABEF', 'E3', 'F3', 'B3', 'A3'),
	   ('ACDE', 'E3', 'D3', 'C3', 'A3'),
	   ('ACDF', 'F3', 'D3', 'C3', 'A3'),
	   ('ACEF', 'E3', 'F3', 'C3', 'A3'),
	   ('ADEF', 'E3', 'F3', 'D3', 'A3'),
	   ('BCDE', 'E3', 'D3', 'B3', 'C3'),
	   ('BCDF', 'F3', 'D3', 'C3', 'B3'),
	   ('BCEF', 'F3', 'E3', 'C3', 'B3'),
	   ('BDEF', 'F3', 'E3', 'D3', 'B3'),
	   ('CDEF', 'F3', 'E3', 'D3', 'C3');

go
IF OBJECT_ID('tempdb..#third_placement', 'U') IS NOT NULL
    DROP TABLE #third_placement;
create table #third_placement(
B1 varchar(2),
C1 varchar(2),
E1 varchar(2),
F1 varchar(2)
)

go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'third_place_combination')
    DROP VIEW third_place_combination;
GO
create view third_place_combination as
SELECT STRING_AGG(value, '') WITHIN GROUP (ORDER BY value) AS third_place_combination
FROM (
    SELECT TOP 4 group_name AS value
    FROM group_stage_third_placement_view
    ORDER BY PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC
) AS top_groups;

go
insert into #third_placement (B1,C1,E1,F1)
select B1,C1,E1,F1 from #round16_combination a inner join  third_place_combination b on a.third_place_combination = b.third_place_combination;

exec Round16Staging
exec UpdateMatchResults @stage = 'Round16Stage';
exec Standings @StageName = 'Round16Stage'
exec Winners @ViewName = 'round16_winner', @RowNumValues = '1', @TableName = 'Round16Stage'

exec QuarterFinalStaging
exec UpdateMatchResults @stage = 'QuarterFinalStage';
exec Standings @StageName = 'QuarterFinalStage'
exec Winners @ViewName = 'quarter_final_winner', @RowNumValues = '1', @TableName = 'QuarterFinalStage'

exec SemiFinalStaging
exec UpdateMatchResults @stage = 'SemiFinalStage';
exec Standings @StageName = 'SemiFinalStage'
exec Winners @ViewName = 'semi_final_winner', @RowNumValues = '1', @TableName = 'SemiFinalStage'

exec FinalStaging
exec UpdateMatchResults @stage = 'FinalStage';
exec Standings @StageName = 'FinalStage'
exec Winners @ViewName = 'final_winner', @RowNumValues = '1', @TableName = 'FinalStage'
