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
  ('Nepal', 'D'),
  ('Netherlands', 'D'),
  ('Austria', 'D'),
  ('France', 'D'),
  ('Belgium', 'E'), 
  ('Slovakia', 'E'),
  ('Romania', 'E'),
  ('India', 'E'),
  ('Turkey', 'F'),
  ('China', 'F'),
  ('Portugal', 'F'),
  ('Czechia', 'F');
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
);

GO

IF OBJECT_ID('dbo.knock_matches', 'U') IS NOT NULL
    DROP TABLE dbo.knock_matches;

CREATE TABLE dbo.knock_matches (
    id VARCHAR(20) PRIMARY KEY,
    team1 VARCHAR(255),
    gf1 INT,
    team2 VARCHAR(255),
    gf2 INT,
    stage VARCHAR(255),
	team1_group varchar(255),
	team2_group varchar(255),
);
GO
 
-- Insert data into matches table with generated IDs
INSERT INTO matches (id, group_name, team1, gf1, team2, gf2, stage)
SELECT
    'M' + CAST(ROW_NUMBER() OVER (ORDER BY t1.group_name, t1.team, t2.team) AS VARCHAR(10)) AS id,
    t1.group_name,
    t1.team AS team1,
    NULL AS gf1,
    t2.team AS team2,
    NULL AS gf2,
    'GroupStage' AS stage
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

--select * from euro_teams
-----work for the group stage------
-- generate random goals for the group stage matches
Go
CREATE OR ALTER PROCEDURE UpdateGoalsMatches
    @stage VARCHAR(255) -- Add a new parameter for the stage
AS
BEGIN
    DECLARE @counter INT = 1;
    BEGIN
        -- Update the matches table with random numbers for gf1 and gf2 based on the stage
        UPDATE matches
        SET
            gf1 = CAST((RAND(CHECKSUM(NEWID())) * 6) AS INT),
            gf2 = CAST((RAND(CHECKSUM(NEWID())) * 6) AS INT)
        WHERE
            stage = @stage; -- Use the stage parameter as a condition

        -- Check if the stage is not equal to 'GroupStage', then execute the additional update
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

-- Execute the stored procedure with a specific stage parameter
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
    -- Subquery for team1
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

    -- Subquery for team2
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
--select * from matches where stage ='GroupStage'


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
--select Top 4 * from group_stage_third_placement_view order by PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC;
--select * from grp_stage_top2_view order by group_name,PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC;
--select * from group_stage_third_placement_view;



---prep for round of 16 ---
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

IF OBJECT_ID('tempdb..#third_placement', 'U') IS NOT NULL
    DROP TABLE #third_placement;
create table #third_placement(
B1 varchar(2),
C1 varchar(2),
E1 varchar(2),
F1 varchar(2)
)

--select * from  third_place_cmbn_grp_view;
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
CREATE or alter PROCEDURE RoundOf16MatchMaking
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
 
    select top 2 @RunnerUpGroupA = team from grp_stage_top2_view where group_name = 'A';
    select top 2 @RunnerUpGroupB = team from grp_stage_top2_view where group_name = 'B';
    select top 2 @RunnerUpGroupC = team from grp_stage_top2_view where group_name = 'C';
    select top 2 @RunnerUpGroupD = team from grp_stage_top2_view where group_name = 'D';
    select top 2 @RunnerUpGroupE = team from grp_stage_top2_view where group_name = 'E';
    select top 2 @RunnerUpGroupF = team from grp_stage_top2_view where group_name = 'F';
 
    select top 1 @WinnerGroupA = team from grp_stage_top2_view where group_name = 'A';
    select top 1 @WinnerGroupB = team from grp_stage_top2_view where group_name = 'B';
    select top 1 @WinnerGroupC = team from grp_stage_top2_view where group_name = 'C';
    select top 1 @WinnerGroupD = team from grp_stage_top2_view where group_name = 'D';
    select top 1 @WinnerGroupE = team from grp_stage_top2_view where group_name = 'E';
    select top 1 @WinnerGroupF = team from grp_stage_top2_view where group_name = 'F';
    
    declare @OpponentTeamOfWinnerB varchar(255), @OpponentGroupOfWinnerB varchar(255); 
    EXEC GetTeamAndGroup @ColumnName = 'B1', @ResultTeam = @OpponentTeamOfWinnerB OUTPUT, @ResultGroupName = @OpponentGroupOfWinnerB OUTPUT;
    
    declare @OpponentTeamOfWinnerC varchar(255), @OpponentGroupOfWinnerC varchar(255); 
    EXEC GetTeamAndGroup @ColumnName = 'C1', @ResultTeam = @OpponentTeamOfWinnerC OUTPUT, @ResultGroupName = @OpponentGroupOfWinnerC OUTPUT;
    
    declare @OpponentTeamOfWinnerE varchar(255), @OpponentGroupOfWinnerE varchar(255); 
    EXEC GetTeamAndGroup @ColumnName = 'E1', @ResultTeam = @OpponentTeamOfWinnerE OUTPUT, @ResultGroupName = @OpponentGroupOfWinnerE OUTPUT;
    
    declare @OpponentTeamOfWinnerF varchar(255), @OpponentGroupOfWinnerF varchar(255); 
    EXEC GetTeamAndGroup @ColumnName = 'F1', @ResultTeam = @OpponentTeamOfWinnerF OUTPUT, @ResultGroupName = @OpponentGroupOfWinnerF OUTPUT;

    INSERT INTO knock_matches (team1, team2, stage, team1_group, team2_group,id)
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
execute RoundOf16MatchMaking;
Go

--select * from knock_matches;
-- random for for knockout matches
Go
CREATE OR ALTER PROCEDURE UpdateGoalsKnockoutMatches
    @stage VARCHAR(255) -- Add a new parameter for the stage
AS
BEGIN
    DECLARE @counter INT = 1;
    BEGIN
        -- Update the matches table with random numbers for gf1 and gf2 based on the stage
        UPDATE knock_matches
        SET
            gf1 = CAST((RAND(CHECKSUM(NEWID())) * 6) AS INT),
            gf2 = CAST((RAND(CHECKSUM(NEWID())) * 6) AS INT)
        WHERE
            stage = @stage; -- Use the stage parameter as a condition
        BEGIN
            UPDATE knock_matches
            SET
                gf2 = gf2 + 1
            WHERE
                stage = @stage
                AND gf1 = gf2;
        END;
    END;
END;
Go
--execute to update the goals for round of 16
EXEC UpdateGoalsKnockoutMatches @stage = 'RoundOf16';

--select * from knock_matches
--select * from matches

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
        knock_matches
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

    --SELECT @WinnerTeamResult1 AS WinnerTeam,
    --       @WinnerTeamGroupResult1 AS WinnerTeamGroup,
    --       @Stage AS Stage,
    --       @MatchId1 AS MatchId;

    DECLARE @WinnerTeamResult2 VARCHAR(255);
    DECLARE @WinnerTeamGroupResult2 VARCHAR(255);

    -- Execute the second match
    EXEC GetWinnerAboveGrpStg @Stage = @Stage,
                            @MatchId = @MatchId2,
                            @WinnerTeam = @WinnerTeamResult2 OUTPUT,
                            @WinnerTeamGroup = @WinnerTeamGroupResult2 OUTPUT;



    -- Insert into another table (you can modify this part based on your requirements)
    INSERT INTO knock_matches (team1, team2, team1_group, team2_group,stage,id)
    VALUES (@WinnerTeamResult1,@WinnerTeamResult2, @WinnerTeamGroupResult1,@WinnerTeamGroupResult2 ,@CurrentStage , @CurrentMatchId)

END;

--select * from knock_matches;

----quarter final
Go
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M39', @MatchId2 = 'M37',@CurrentMatchId='M45', @CurrentStage='QuarterFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M41', @MatchId2 = 'M42',@CurrentMatchId='M46', @CurrentStage='QuarterFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M43', @MatchId2 = 'M44',@CurrentMatchId='M47', @CurrentStage='QuarterFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M40', @MatchId2 = 'M38',@CurrentMatchId='M48', @CurrentStage='QuarterFinal';

EXEC UpdateGoalsKnockoutMatches @stage = 'QuarterFinal';
--select * from matches where stage='QuarterFinal';


--semi final----
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'QuarterFinal', @MatchId1 = 'M45', @MatchId2 = 'M46',@CurrentMatchId='M49', @CurrentStage='SemiFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'QuarterFinal', @MatchId1 = 'M47', @MatchId2 = 'M48',@CurrentMatchId='M50', @CurrentStage='SemiFinal';

EXEC UpdateGoalsKnockoutMatches @stage = 'SemiFinal';
--select * from matches where stage='SemiFinal';


--final---
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'SemiFinal', @MatchId1 = 'M49', @MatchId2 = 'M50',@CurrentMatchId='M51', @CurrentStage='Final';
EXEC UpdateGoalsKnockoutMatches @stage = 'Final';
--select * from matches where stage='Final';
Go
--select * from matches;




--grp left view 
Go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'group_stage_left_view')
    DROP VIEW group_stage_left_view;
GO
CREATE VIEW group_stage_left_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,* FROM
    (
        SELECT '' AS 'Rank','' AS Team,'Pool One' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams WHERE team='Albania' 
        UNION ALL
        SELECT 'Group A' AS 'Rank','' AS Team,'' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams WHERE team='Albania' 
        UNION ALL
        SELECT 'Rank' ,'Team' AS Team,'PTS' AS PTS,'GP' AS GP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM euro_teams WHERE team='Albania' 
        UNION ALL
        SELECT group_name AS 'Rank',team AS Team,PTS,MP AS GP,W,L,D,GF,GA,GD FROM group_stage_view WHERE group_name = 'A'
        UNION ALL
        SELECT top 2 '' AS 'Rank','' AS Team,'' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams WHERE group_name='B'
        UNION ALL
        SELECT 'Group B' AS 'Rank','' AS Team,'' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams WHERE team='Albania' 
        UNION ALL
        SELECT  'Rank','Team' AS Team,'PTS' AS PTS,'GP' AS GP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM euro_teams WHERE team='Albania' 
        UNION ALL
        SELECT group_name AS 'Rank',team AS Team,PTS,MP AS GP,W,L,D,GF,GA,GD FROM group_stage_view WHERE group_name = 'B'
        UNION ALL
        SELECT top 2 '' AS 'Rank','' AS Team,'' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams WHERE group_name='A'
        UNION ALL
        SELECT 'Group C' AS 'Rank','' AS Team,'' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams WHERE team='Albania'
        UNION ALL
        SELECT 'Rank','Team' AS Team,'PTS' AS PTS,'GP' AS GP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM euro_teams WHERE team='Albania' 
        UNION ALL
        SELECT group_name AS 'Rank',team AS Team,PTS,MP AS GP,W,L,D,GF,GA,GD FROM group_stage_view WHERE group_name = 'C'
    ) AS SubQuery;
    Go

--select * from group_stage_left_view;

-- grp right side view 
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'group_stage_right_view')
    DROP VIEW group_stage_right_view;
GO
CREATE VIEW group_stage_right_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber, * FROM
    (
    SELECT '' AS 'Rank','' AS Team,'Pool Two' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams WHERE team='Albania' 
        UNION ALL
SELECT 'Group D' AS 'Rank','' AS Team,'' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams where team='Albania' 
UNION ALL
SELECT 'Rank' ,'Team' AS Team,'PTS' AS PTS,'GP' AS GP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM euro_teams where team='Albania' 
union all
SELECT group_name AS 'Rank',team AS Team,PTS,MP AS GP,W,L,D,GF,GA,GD FROM group_stage_view WHERE group_name = 'D'
union all
SELECT top 2 '' AS 'Rank','' AS Team,'' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams where group_name='B'
union all
SELECT 'Group E' AS 'Rank','' AS Team,'' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams where team='Albania' 
UNION ALL
SELECT  'Rank','Team' AS Team,'PTS' AS PTS,'GP' AS GP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM euro_teams where team='Albania' 
union all
SELECT group_name AS 'Rank',team AS Team,PTS,MP AS GP,W,L,D,GF,GA,GD FROM group_stage_view WHERE group_name = 'E'
union all
SELECT top 2 '' AS 'Rank','' AS Team,'' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams where group_name='A'
union all
SELECT 'Group F' AS 'Rank','' AS Team,'' AS PTS,'' AS GP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM euro_teams where team='Albania'
union all
SELECT 'Rank','Team' AS Team,'PTS' AS PTS,'GP' AS GP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM euro_teams where team='Albania' 
UNION ALL
SELECT group_name AS 'Rank',team AS Team,PTS,MP AS GP,W,L,D,GF,GA,GD FROM group_stage_view WHERE group_name = 'F'
) as subquery;
Go
--select * from group_stage_right_view;

--round of 16 left view  ---
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'r16_left_view')
    DROP VIEW r16_left_view;
GO
CREATE VIEW r16_left_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber, * FROM
    (
select  'Round Of 16' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 4  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select  ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 2   ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select  ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT team1_group AS ' ',team1 AS 'Country Name',CAST(gf1 AS VARCHAR(10)) AS 'Goals 'FROM knock_matches WHERE stage = 'RoundOf16' AND id = 'M39'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M39'
union all
SELECT Top 2   ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select  ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M41'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M41'
union all
SELECT Top 2   ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select  ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M44'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M44'
) as subquery;
Go

--select * from r16_left_view;

--r16 right view
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'r16_right_view')
    DROP VIEW r16_right_view;
GO
CREATE VIEW r16_right_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,* FROM (
select  'Round Of 16' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 4  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M38'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M38'
union all
SELECT Top 2  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT team1_group AS ' ',team1 AS 'Country Name',CAST(gf1 AS VARCHAR(10)) AS 'Goals 'FROM knock_matches WHERE stage = 'RoundOf16' AND id = 'M40'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M40'
union all
SELECT Top 2  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select ' ','Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M42'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M42'
union all
SELECT Top 2  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M43'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M43'
) as subquery;
Go

--select * from r16_right_view;

---quarter final left view
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'quarter_final_left_view')
    DROP VIEW quarter_final_left_view;
GO
CREATE VIEW quarter_final_left_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
select  'Quarter Final' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='QuarterFinal' and id='M45'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='QuarterFinal' and id='M45'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT team1_group AS ' ',team1 AS 'Country Name',CAST(gf1 AS VARCHAR(10)) AS 'Goals 'FROM knock_matches WHERE stage = 'QuarterFinal' AND id = 'M46'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='QuarterFinal' and id='M46'
union all
SELECT Top 2  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
) as subquery;
Go
--select * from quarter_final_left_view;


---quarter final right view
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'quarter_final_right_view')
    DROP VIEW quarter_final_right_view;
GO
CREATE VIEW quarter_final_right_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
select  'Quarter Final' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='QuarterFinal' and id='M47'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='QuarterFinal' and id='M47'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT team1_group AS ' ',team1 AS 'Country Name',CAST(gf1 AS VARCHAR(10)) AS 'Goals 'FROM knock_matches WHERE stage = 'QuarterFinal' AND id = 'M48'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='QuarterFinal' and id='M48'
union all
SELECT Top 2  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='RoundOf16'
) as subquery;
Go
--select * from quarter_final_right_view;

--semi final left view 
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'semi_final_left_view')
    DROP VIEW semi_final_left_view;
GO
CREATE VIEW semi_final_left_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
    select  'Semi Final' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 12  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='GroupStage'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='SemiFinal' and id='M49'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='SemiFinal' and id='M49'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='GroupStage'
) as subquery;
Go
--select * from semi_final_left_view;

--semi final right view 
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'semi_final_right_view')
    DROP VIEW semi_final_right_view;
GO
CREATE VIEW semi_final_right_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
select  'Semi Final' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 12  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='GroupStage'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='SemiFinal' and id='M50'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='SemiFinal' and id='M50'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='GroupStage'
) as subquery;
Go
--select * from semi_final_right_view;

-- final right view 
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'final_pview')
    DROP VIEW final_pview;
GO
CREATE VIEW final_pview AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
    select  'Final' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 12  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM knock_matches where stage='GroupStage'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='Final' and id='M51'
union all
select team1_group as ' ', team2 as'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from knock_matches  where stage='Final' and id='M51'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='GroupStage'
) as subquery;
Go

----select * from final_pview------;
-----final view join ------
--select * from group_stage_left_view;
--select * from r16_left_view;
--select * from quarter_final_left_view;
--select * from semi_final_left_view;
--select * from final_pview;

select 
a.Rank,a.Team,a.PTS,a.GP,a.W,a.L,a.D,a.GF,a.GA,a.GD,
' ' AS N1,
b.Blank,b.[Country Name],b.goals,
' ' as N2,
c.Blank,c.[Country Name],c.goals,
' ' as N3,
d.Blank,d.[Country Name],d.goals,
' ' as N4,
f.Blank,f.[Country Name],f.goals,
' ' as N6,
g.Blank,g.[Country Name],g.goals,
' ' as N7,
h.Blank,h.[Country Name],h.goals,
' ' as N8,
i.Blank,i.[Country Name],i.goals,
' ' as N9,
j.Rank,j.Team,j.PTS,j.GP,j.W,j.L,j.D,j.GF,j.GA,j.GD

from group_stage_left_view a
left join r16_left_view b on  a.RowNumber=b.RowNumber
left join quarter_final_left_view c on a.RowNumber=c.RowNumber
left join semi_final_left_view d on a.RowNumber=d.RowNumber
left join final_pview f on a.RowNumber=f.RowNumber
left join semi_final_right_view g on a.RowNumber=g.RowNumber
left join quarter_final_right_view h on a.RowNumber=h.RowNumber
left join r16_right_view i on  a.RowNumber=i.RowNumber
left join group_stage_right_view j on  a.RowNumber=j.RowNumber;