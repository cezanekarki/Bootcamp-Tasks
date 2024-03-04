Go
IF OBJECT_ID('tempdb..#euro_teams', 'U') IS NOT NULL
    DROP TABLE #euro_teams;

CREATE TABLE #euro_teams (
    team VARCHAR(255) PRIMARY KEY,
    group_name VARCHAR(2) NOT NULL
);

Go
INSERT INTO #euro_teams (team, group_name)
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
  Go
  --creating table matches
IF OBJECT_ID('dbo.matches', 'U') IS NOT NULL
    DROP TABLE dbo.matches;

CREATE TABLE dbo.matches (
    id VARCHAR(20) PRIMARY KEY,
    group_name VARCHAR(255),
    team1 VARCHAR(255),
    gf1 INT,
    team2 VARCHAR(255),
    gf2 INT,
    stage VARCHAR(255),
    team1_group VARCHAR(255),
    team2_group VARCHAR(255)
);
GO
 
-- Insert data into matches table with generated IDs
INSERT INTO matches (id, group_name, team1, gf1, team2, gf2, stage, team1_group, team2_group)
SELECT
    'M' + CAST(ROW_NUMBER() OVER (ORDER BY t1.group_name, t1.team, t2.team) AS VARCHAR(10)) AS id,
    t1.group_name,
    t1.team AS team1,
    NULL AS gf1,
    t2.team AS team2,
    NULL AS gf2,
    'GroupStage' AS stage,
    t1.group_name AS team1_group,
    t2.group_name AS team2_group
FROM
    euro_teams t1
CROSS JOIN
    euro_teams t2
WHERE
    t1.group_name = t2.group_name
    AND t1.team < t2.team
ORDER BY
    group_name, team1, team2;


--select * from matches where  stage='GroupStage'


-----work for the group stage------
-- generate random goals for the matches
Go
CREATE OR ALTER PROCEDURE UpdateGoalsMatches
    @stage VARCHAR(255) -- Add a new parameter for the stage
AS
BEGIN
    DECLARE @counter INT = 1;
    BEGIN
        UPDATE matches
        SET
            gf1 = CAST((RAND(CHECKSUM(NEWID())) * 6) AS INT),
            gf2 = CAST((RAND(CHECKSUM(NEWID())) * 6) AS INT)
        WHERE
            stage = @stage; -- Use the stage parameter as a condition
        IF @stage <> 'GroupStage'
        BEGIN
            UPDATE matches
            SET
                gf2 = gf2 + 1
            WHERE
                stage = @stage
                AND gf1 = gf2;
        END;
    END;
END;

Go 
EXEC UpdateGoalsMatches @stage = 'GroupStage';
Go


--group stage view---
Go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'group_stage_view')
    DROP VIEW group_stage_view;
GO
CREATE VIEW group_stage_view AS
SELECT
    group_name,
    team,
    CONVERT(VARCHAR(10), SUM(CONVERT(INT, MP))) AS MP,
    CONVERT(VARCHAR(10), SUM(CONVERT(INT, W))) AS W,
    CONVERT(VARCHAR(10), SUM(CONVERT(INT, D))) AS D,
    CONVERT(VARCHAR(10), SUM(CONVERT(INT, L))) AS L,
    CONVERT(VARCHAR(10), SUM(CONVERT(INT, GF))) AS GF,
    CONVERT(VARCHAR(10), SUM(CONVERT(INT, GA))) AS GA,
    CONVERT(VARCHAR(10), SUM(CONVERT(INT, GD))) AS GD,
    CONVERT(VARCHAR(10), SUM(CONVERT(INT, PTS))) AS PTS
FROM (
    SELECT
        group_name,
        team1 AS team,
        CONVERT(VARCHAR(10), COUNT(*)) AS MP,
        CONVERT(VARCHAR(10), SUM(CASE WHEN gf1 > gf2 THEN 1 ELSE 0 END)) AS W,
        CONVERT(VARCHAR(10), SUM(CASE WHEN gf1 = gf2 THEN 1 ELSE 0 END)) AS D,
        CONVERT(VARCHAR(10), SUM(CASE WHEN gf1 < gf2 THEN 1 ELSE 0 END)) AS L,
        CONVERT(VARCHAR(10), SUM(CONVERT(INT, gf1))) AS GF,
        CONVERT(VARCHAR(10), SUM(CONVERT(INT, gf2))) AS GA,
        CONVERT(VARCHAR(10), SUM(CONVERT(INT, gf1) - CONVERT(INT, gf2))) AS GD,
        CONVERT(VARCHAR(10), SUM(CASE WHEN gf1 > gf2 THEN 3 WHEN gf1 = gf2 THEN 1 ELSE 0 END)) AS PTS
    FROM
        matches
    GROUP BY
        group_name, team1
    UNION ALL
    SELECT
        group_name,
        team2 AS team,
        CONVERT(VARCHAR(10), COUNT(*)) AS MP,
        CONVERT(VARCHAR(10), SUM(CASE WHEN gf2 > gf1 THEN 1 ELSE 0 END)) AS W,
        CONVERT(VARCHAR(10), SUM(CASE WHEN gf2 = gf1 THEN 1 ELSE 0 END)) AS D,
        CONVERT(VARCHAR(10), SUM(CASE WHEN gf2 < gf1 THEN 1 ELSE 0 END)) AS L,
        CONVERT(VARCHAR(10), SUM(CONVERT(INT, gf2))) AS GF,
        CONVERT(VARCHAR(10), SUM(CONVERT(INT, gf1))) AS GA,
        CONVERT(VARCHAR(10), SUM(CONVERT(INT, gf2) - CONVERT(INT, gf1))) AS GD,
        CONVERT(VARCHAR(10), SUM(CASE WHEN gf2 > gf1 THEN 3 WHEN gf2 = gf1 THEN 1 ELSE 0 END)) AS PTS
    FROM
        matches
    GROUP BY
        group_name, team2
) AS TeamStats
GROUP BY
    group_name, team;
Go

--SELECT * FROM group_stage_view ORDER BY group_name, PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC;

------round of 16 preparation -----
--creating the third place view
Go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'group_stage_third_placement_view')
    DROP VIEW group_stage_third_placement_view;
GO
create view  group_stage_third_placement_view as
SELECT
    team,group_name,MP,W,D,L,GF,GA,GD,Pts
FROM (
    SELECT
         team,group_name,MP,W,D,L,GF,GA,GD,Pts,
        ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC) AS RowNum
    FROM
        group_stage_view
) AS GroupRanking
WHERE
    RowNum = 3;
Go

--creating the top 2 winners place 
Go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'grp_stage_top2_view')
    DROP VIEW grp_stage_top2_view;
GO
create view  grp_stage_top2_view as
SELECT
     team,group_name,MP,W,D,L,GF,GA,GD,Pts
FROM (
    SELECT
         team,group_name,MP,W,D,L,GF,GA,GD,Pts,
        ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC) AS RowNum
    FROM
        group_stage_view
) AS GroupRanking
WHERE
    RowNum in (1,2);
Go

--select * from grp_stage_top2_view order by group_name,PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC;
--select * from group_stage_third_placement_view;


-- temporary table for third placement
Go
IF OBJECT_ID('tempdb..#round16_combination', 'U') IS NOT NULL
    DROP TABLE #round16_combination;
create  table #round16_combination (
	third_place_combination varchar(255) primary key,
	B1 varchar(20) not null,
	C1 varchar(20) not null,
	E1 varchar(20) not null,
	F1 varchar(20) not null
);
 GO
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

Go
IF OBJECT_ID('tempdb..#third_placement', 'U') IS NOT NULL
    DROP TABLE #third_placement;
create table #third_placement(
B1 varchar(2),
C1 varchar(2),
E1 varchar(2),
F1 varchar(2)
)

--select * from #round16_combination;

Go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'third_place_cmbn_grp_view')
    DROP VIEW third_place_cmbn_grp_view;
GO
create view third_place_cmbn_grp_view as
SELECT STRING_AGG(value, '') WITHIN GROUP (ORDER BY value) AS third_place_cmbn_grp
FROM (
    SELECT TOP 4 group_name AS value
    FROM group_stage_third_placement_view
    ORDER BY PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC
) AS top_groups;
Go
select * from  third_place_cmbn_grp_view;
--select * from #round16_combination a inner join  third_place_cmbn_grp_view b on a.third_place_combination =b.third_place_cmbn_grp;
--inserting into temp table
Go
insert into #third_placement (B1,C1,E1,F1)
select B1,C1,E1,F1 from #round16_combination a inner join  third_place_cmbn_grp_view b on a.third_place_combination =b.third_place_cmbn_grp;

--select * from  #third_placement
Go
CREATE OR ALTER PROCEDURE GetTeamAndGroup
    @ColumnName NVARCHAR(50),
    @ResultTeam NVARCHAR(100) OUTPUT,
    @ResultGroupName NVARCHAR(100) OUTPUT
AS
BEGIN
    DECLARE @SqlQuery NVARCHAR(MAX);

    -- Build the dynamic SQL query
    SET @SqlQuery = N'
        SELECT @TempTeam = team, @TempGroupName = group_name
        FROM group_stage_third_placement_view
        WHERE group_name = LEFT((SELECT ' + QUOTENAME(@ColumnName) + ' FROM #third_placement), 1);
    ';

    -- Execute the dynamic SQL query and store the result in variables
    EXEC sp_executesql @SqlQuery, N'@TempTeam NVARCHAR(100) OUTPUT, @TempGroupName NVARCHAR(100) OUTPUT', @ResultTeam OUTPUT, @ResultGroupName OUTPUT;
END;

---------------------
Go
CREATE or alter PROCEDURE RoundOf16Matches
AS
BEGIN
	declare @RunnerUpGroupA varchar(255), 
			@RunnerUpGroupB varchar(255),
			@RunnerUpGroupC varchar(255),
			@RunnerUpGroupD varchar(255),
			@RunnerUpGroupE varchar(255),
			@RunnerUpGroupF varchar(255),
			@WinnerGroupA varchar(255),
			@WinnerGroupB varchar(255),
			@WinnerGroupC varchar(255),
			@WinnerGroupD varchar(255),
			@WinnerGroupE varchar(255),
			@WinnerGroupF varchar(255);

	select top 1 @WinnerGroupA = team from grp_stage_top2_view where group_name = 'A';
	select top 1 @WinnerGroupB = team from grp_stage_top2_view where group_name = 'B';
	select top 1 @WinnerGroupC = team from grp_stage_top2_view where group_name = 'C';
	select top 1 @WinnerGroupD = team from grp_stage_top2_view where group_name = 'D';
	select top 1 @WinnerGroupE = team from grp_stage_top2_view where group_name = 'E';
	select top 1 @WinnerGroupF = team from grp_stage_top2_view where group_name = 'F';

	select top 2 @RunnerUpGroupA = team from grp_stage_top2_view where group_name = 'A';
	select top 2 @RunnerUpGroupB = team from grp_stage_top2_view where group_name = 'B';
	select top 2 @RunnerUpGroupC = team from grp_stage_top2_view where group_name = 'C';
	select top 2 @RunnerUpGroupD = team from grp_stage_top2_view where group_name = 'D';
	select top 2 @RunnerUpGroupE = team from grp_stage_top2_view where group_name = 'E';
	select top 2 @RunnerUpGroupF = team from grp_stage_top2_view where group_name = 'F';
	
	declare @OpponentTeamOfWinnerB varchar(255), @OpponentGroupOfWinnerB varchar(255); 
	EXEC GetTeamAndGroup @ColumnName = 'B1', @ResultTeam = @OpponentTeamOfWinnerB OUTPUT, @ResultGroupName = @OpponentGroupOfWinnerB OUTPUT;
	
	declare @OpponentTeamOfWinnerC varchar(255), @OpponentGroupOfWinnerC varchar(255); 
	EXEC GetTeamAndGroup @ColumnName = 'C1', @ResultTeam = @OpponentTeamOfWinnerC OUTPUT, @ResultGroupName = @OpponentGroupOfWinnerC OUTPUT;
	
	declare @OpponentTeamOfWinnerE varchar(255), @OpponentGroupOfWinnerE varchar(255); 
	EXEC GetTeamAndGroup @ColumnName = 'E1', @ResultTeam = @OpponentTeamOfWinnerE OUTPUT, @ResultGroupName = @OpponentGroupOfWinnerE OUTPUT;
	
	declare @OpponentTeamOfWinnerF varchar(255), @OpponentGroupOfWinnerF varchar(255); 
	EXEC GetTeamAndGroup @ColumnName = 'F1', @ResultTeam = @OpponentTeamOfWinnerF OUTPUT, @ResultGroupName = @OpponentGroupOfWinnerF OUTPUT;

	INSERT INTO matches (team1, team2, stage, team1_group, team2_group,id)
	values 
		   (@WinnerGroupA, @RunnerUpGroupC, 'RoundOf16', 'A', 'C','M37'),
		   (@RunnerUpGroupA, @RunnerUpGroupB, 'RoundOf16', 'A', 'B','M38'),
		   (@WinnerGroupB, @OpponentTeamOfWinnerB, 'RoundOf16', 'B', @OpponentGroupOfWinnerB,'M39'),
		   (@WinnerGroupC, @OpponentTeamOfWinnerC, 'RoundOf16', 'C', @OpponentGroupOfWinnerC,'M40'),
		   (@WinnerGroupF, @OpponentTeamOfWinnerF, 'RoundOf16', 'F', @OpponentGroupOfWinnerF,'M41'),
		   (@RunnerUpGroupD, @RunnerUpGroupE, 'RoundOf16', 'D', 'E','M42'),
		   (@WinnerGroupE, @OpponentTeamOfWinnerE, 'RoundOf16', 'E', @OpponentGroupOfWinnerE,'M43'),
		   (@WinnerGroupD, @RunnerUpGroupF, 'RoundOf16', 'D', 'F','M44')
END;

Go
execute RoundOf16Matches;
Go
--execute to update the goals for round of 16
EXEC UpdateGoalsMatches @stage = 'RoundOf16';

--get winner from above r16
Go
CREATE OR ALTER PROCEDURE GetWinnerAboveGrpStg
    @Stage VARCHAR(255),
    @MatchId VARCHAR(20),
    @WinnerTeam VARCHAR(255) OUTPUT,
    @WinnerTeamGroup VARCHAR(255) OUTPUT
AS
BEGIN
    SELECT
        @WinnerTeam = CASE
                        WHEN gf1 > gf2 THEN team1
                        WHEN gf2 > gf1 THEN team2
                        ELSE NULL
                      END,
        @WinnerTeamGroup = CASE
                             WHEN gf1 > gf2 THEN team1_group
                             WHEN gf2 > gf1 THEN team2_group
                             ELSE NULL
                           END
    FROM
        matches
    WHERE
        stage = @Stage AND id = @MatchId;
END;

---procedure for insert the winner
Go
CREATE OR ALTER PROCEDURE ExecuteGetWinnerAboveGrpStgAndInsert
    @Stage VARCHAR(255),
    @MatchId1 VARCHAR(20),
    @MatchId2 VARCHAR(20),
	@CurrentMatchId varchar(20),
	@CurrentStage varchar(20)
AS
BEGIN
    DECLARE @WinnerTeamResult1 VARCHAR(255);
    DECLARE @WinnerTeamGroupResult1 VARCHAR(255);

    -- Execute the first match
    EXEC GetWinnerAboveGrpStg @Stage = @Stage,
                            @MatchId = @MatchId1,
                            @WinnerTeam = @WinnerTeamResult1 OUTPUT,
                            @WinnerTeamGroup = @WinnerTeamGroupResult1 OUTPUT;

    DECLARE @WinnerTeamResult2 VARCHAR(255);
    DECLARE @WinnerTeamGroupResult2 VARCHAR(255);

    -- Execute the second match
    EXEC GetWinnerAboveGrpStg @Stage = @Stage,
                            @MatchId = @MatchId2,
                            @WinnerTeam = @WinnerTeamResult2 OUTPUT,
                            @WinnerTeamGroup = @WinnerTeamGroupResult2 OUTPUT;

    -- Insert into another table (you can modify this part based on your requirements)
    INSERT INTO matches (team1, team2, team1_group, team2_group,stage,id)
    VALUES (@WinnerTeamResult1,@WinnerTeamResult2, @WinnerTeamGroupResult1,@WinnerTeamGroupResult2 ,@CurrentStage , @CurrentMatchId)
END;

----quarter final
Go
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M39', @MatchId2 = 'M37',@CurrentMatchId='M45', @CurrentStage='QuarterFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M41', @MatchId2 = 'M42',@CurrentMatchId='M46', @CurrentStage='QuarterFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M43', @MatchId2 = 'M44',@CurrentMatchId='M47', @CurrentStage='QuarterFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M40', @MatchId2 = 'M38',@CurrentMatchId='M48', @CurrentStage='QuarterFinal';

EXEC UpdateGoalsMatches @stage = 'QuarterFinal';
--select * from matches where stage='QuarterFinal';


--semi final----
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'QuarterFinal', @MatchId1 = 'M45', @MatchId2 = 'M46',@CurrentMatchId='M49', @CurrentStage='SemiFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'QuarterFinal', @MatchId1 = 'M47', @MatchId2 = 'M48',@CurrentMatchId='M50', @CurrentStage='SemiFinal';

EXEC UpdateGoalsMatches @stage = 'SemiFinal';
--select * from matches where stage='SemiFinal';

--final---
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'SemiFinal', @MatchId1 = 'M49', @MatchId2 = 'M50',@CurrentMatchId='M51', @CurrentStage='Final';
EXEC UpdateGoalsMatches @stage = 'Final';
--select * from matches where stage='Final';
Go
--select * from matches;
--select * from  third_place_cmbn_grp_view;