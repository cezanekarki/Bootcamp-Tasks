CREATE  TABLE  euro_teams (
  team VARCHAR(255) PRIMARY KEY,
  group_name VARCHAR(2) NOT NULL,
);

INSERT INTO euro_teams (team, group_name)
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

CREATE TABLE matches (
 id VARCHAR(20);
 group_name varchar(255),
 team1 VARCHAR(255),
 gf1  int,
 team2 varchar(255),
 gf2 int,
 stage varchar(255),
 team1_group VARCHAR(255),
 team2_group VARCHAR(255)
);

select * from table matches;


INSERT INTO matches (group_name, team1, gf1, team2, gf2, stage)
SELECT
    t1.group_name,
    t1.team AS team1,
    NULL AS gf1,
    t2.team AS team2,
    NULL AS gf2,
    'GroupStage' AS stage  -- Insert 'GroupStage' as the value for the stage column
FROM
    euro_teams t1
CROSS JOIN
    euro_teams t2
WHERE
    t1.group_name = t2.group_name -- Match teams within the same group
    AND t1.team < t2.team -- Avoid duplicate pairs (A vs B is the same as B vs A)
ORDER BY
    group_name, team1, team2;


	-- update the match id 
WITH CTE AS (
    SELECT 
        id,
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
    FROM matches
)

UPDATE CTE
SET id = 'M' + CAST(RowNum AS VARCHAR(2));

select * from matches  where gf1=gf2 and stage='GroupStage'

--select * from matches order by group_name;


-----work for the group stage------
-- generate random goals for the matches
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

        -- Check if gf1 is equal to gf2, and increment gf2 by 1 if they are equal
        UPDATE matches
        SET
            gf2 = gf2 + 1
        WHERE
            stage = @stage
            AND gf1 = gf2;
    END;
END;


-- Execute the stored procedure with a specific stage parameter
EXEC UpdateGoalsMatches @stage = 'GroupStage';


--group stage view---
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



SELECT * FROM group_stage_view
ORDER BY group_name, PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC;


select * from matches


------round of 16 preparation -----
--creating the third place view
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

--creating the top 2 winners place 
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

--select Top 4 * from group_stage_third_placement_view order by PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC;
--select * from grp_stage_top2_view order by group_name,PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC;



---prep for round of 16 ---

-- temporary table for third placement
CREATE  TABLE #third_placement (
 group_name varchar(255),
 team VARCHAR(255),
);
truncate table #third_placement;

insert into #third_placement (group_name,team) 
select top 4 group_name,team from group_stage_third_placement_view 
order by PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC;


--select * from #third_placement;

-- procedure for geenrating random thrid place
CREATE or alter PROCEDURE RandomThirdPlaceGrp
    @ThirdGroups NVARCHAR(100),
	@GroupName VARCHAR(255) OUTPUT,
	@Team varchar(255) OUTPUT
AS
BEGIN
    -- Select the random third-place group based on the provided groups
    IF EXISTS (SELECT 1 FROM #third_placement WHERE group_name IN (SELECT value FROM STRING_SPLIT(@ThirdGroups, ',')))
    BEGIN
        SELECT TOP 1 @GroupName = group_name
		FROM #third_placement
		WHERE group_name IN (SELECT value FROM STRING_SPLIT(@ThirdGroups, ','))
		ORDER BY NEWID();
 
		select @Team = team from group_stage_third_placement_view where group_name = @GroupName
 
        -- Remove the selected group from #third_placement
        DELETE FROM #third_placement WHERE group_name = @GroupName;
    END
END;


-----procdeure for r16 macthing and inserting ---
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

	declare @OpponentGroupOfGroupC varchar(255), @OpponentTeamOfGroupC varchar(255)
	exec RandomThirdPlaceGrp @ThirdGroups = 'D,E,F', @GroupName = @OpponentGroupOfGroupC output, @Team = @OpponentTeamOfGroupC output
	select @OpponentGroupOfGroupC, @OpponentTeamOfGroupC;

	declare @OpponentGroupOfGroupB varchar(255), @OpponentTeamOfGroupB varchar(255)
	exec RandomThirdPlaceGrp @ThirdGroups = 'A,D,E,F', @GroupName = @OpponentGroupOfGroupB output, @Team = @OpponentTeamOfGroupB output
	select @OpponentGroupOfGroupB, @OpponentTeamOfGroupB

	declare @OpponentGroupOfGroupF varchar(255), @OpponentTeamOfGroupF varchar(255)
	exec RandomThirdPlaceGrp @ThirdGroups = 'A,B,C', @GroupName = @OpponentGroupOfGroupF output, @Team = @OpponentTeamOfGroupF output
	select @OpponentGroupOfGroupF, @OpponentTeamOfGroupF
 
	declare @OpponentGroupOfGroupE varchar(255), @OpponentTeamOfGroupE varchar(255)
	exec RandomThirdPlaceGrp @ThirdGroups = 'A,B,C,D', @GroupName = @OpponentGroupOfGroupE output, @Team = @OpponentTeamOfGroupE output
	select @OpponentGroupOfGroupE, @OpponentTeamOfGroupE


	INSERT INTO matches (team1, team2, stage, team1_group, team2_group,id)
	values 
		   (@WinnerGroupA, @RunnerUpGroupC, 'RoundOf16', 'A', 'C','M37'),
		   (@RunnerUpGroupA, @RunnerUpGroupB, 'RoundOf16', 'A', 'B','M38'),
		    (@WinnerGroupB, @OpponentTeamOfGroupB, 'RoundOf16', 'B', @OpponentGroupOfGroupB,'M39'),
		   (@WinnerGroupC, @OpponentTeamOfGroupC, 'RoundOf16', 'C', @OpponentGroupOfGroupC,'M40'),
		   (@WinnerGroupF, @OpponentTeamOfGroupF, 'RoundOf16', 'F', @OpponentGroupOfGroupF,'M41'),
		   (@RunnerUpGroupD, @RunnerUpGroupE, 'RoundOf16', 'D', 'E','M42'),
		   (@WinnerGroupE, @OpponentTeamOfGroupE, 'RoundOf16', 'E', @OpponentGroupOfGroupE,'M43'),
		   (@WinnerGroupD, @RunnerUpGroupF, 'RoundOf16', 'D', 'F','M44')



END;

execute RoundOf16MatchMaking


EXEC UpdateGoalsMatches @stage = 'RoundOf16';

--execute RandomGoalCalculation @Stage = 'RoundOf16', @Iterations = 8;
select * from matches where stage='RoundOf16'

--select * from matches where stage='RoundOf16'

--select * from matches where gf1=gf2 and stage='RoundOf16'

 

--get winner from above r16
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

    SELECT @WinnerTeamResult1 AS WinnerTeam,
           @WinnerTeamGroupResult1 AS WinnerTeamGroup,
           @Stage AS Stage,
           @MatchId1 AS MatchId;

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
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M39', @MatchId2 = 'M37',@CurrentMatchId='M45', @CurrentStage='QuarterFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M41', @MatchId2 = 'M42',@CurrentMatchId='M46', @CurrentStage='QuarterFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M43', @MatchId2 = 'M44',@CurrentMatchId='M47', @CurrentStage='QuarterFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'RoundOf16', @MatchId1 = 'M40', @MatchId2 = 'M38',@CurrentMatchId='M48', @CurrentStage='QuarterFinal';

select * from matches where stage='QuarterFinal'
EXEC UpdateGoalsMatches @stage = 'QuarterFinal';


--semi final----
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'QuarterFinal', @MatchId1 = 'M45', @MatchId2 = 'M46',@CurrentMatchId='M49', @CurrentStage='SemiFinal';
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'QuarterFinal', @MatchId1 = 'M47', @MatchId2 = 'M48',@CurrentMatchId='M50', @CurrentStage='SemiFinal';

EXEC UpdateGoalsMatches @stage = 'SemiFinal';
select * from matches where stage='SemiFinal'


--final---
EXEC ExecuteGetWinnerAboveGrpStgAndInsert @Stage = 'SemiFinal', @MatchId1 = 'M49', @MatchId2 = 'M50',@CurrentMatchId='M51', @CurrentStage='Final';
EXEC UpdateGoalsMatches @stage = 'Final';
select * from matches where stage='Final'



--select * from matches