drop table euro_cup_2024;

--Temporary table for storing group and team data of euro 2024
CREATE  TABLE #euro_cup_2024 (
  team VARCHAR(255) PRIMARY KEY,
  group_name VARCHAR(2) NOT NULL
);
 
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
 CREATE  TABLE matches (
 group_name varchar(255),
 team1 VARCHAR(255),
 goal1  int,
 team2 varchar(255),
 goal2 int,
 stage varchar(255)
);

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


-- generate random goals for the matches
CREATE OR ALTER PROCEDURE UpdateMatchResults
    @stage VARCHAR(255)
AS
BEGIN
    UPDATE matches
    SET
        goal1 = CAST((RAND(CHECKSUM(NEWID())) * 6) AS INT),
        goal2 = CAST((RAND(CHECKSUM(NEWID())) * 6) AS INT)
    WHERE
        stage = @stage;

	-- Check if gf1 is equal to gf2, and increment gf2 by 1 if they are equal for every stage except group
        IF @stage <> 'GroupStage'
        BEGIN
            UPDATE matches
            SET
                goal2 = goal2 + 1
            WHERE
                stage = @stage
                AND goal1 = goal2;
        END;
END;

EXEC UpdateMatchResults @stage = 'GroupStage';

-- created standings view for each stage dynamically
CREATE or alter PROCEDURE Standings
	@StageName nvarchar(255)
AS
BEGIN
	DECLARE @SQL NVARCHAR(MAX)
			

    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = @StageName)
    BEGIN
        SET @SQL = 'DROP VIEW ' + QUOTENAME(@StageName)
        EXEC sp_executesql @SQL
    END

    -- dynamic SQL statement to create the view
    SET @SQL = '
    CREATE VIEW ' + QUOTENAME(@StageName) + '
    AS
    SELECT
			group_name,
			team,
			SUM(MP) AS MP,
			SUM(W) AS W,
			SUM(D) AS D,
			SUM(L) AS L,
			SUM(GF) AS GF,
			SUM(GA) AS GA,
			SUM(GD) AS GD,
			SUM(PTS) AS PTS
		FROM (
			SELECT
				group_name,
				team1 AS team,
				COUNT(*) AS MP,
				SUM(CASE WHEN goal1 > goal2 THEN 1 ELSE 0 END) AS W,
				SUM(CASE WHEN goal1 = goal2 THEN 1 ELSE 0 END) AS D,
				SUM(CASE WHEN goal1 < goal2 THEN 1 ELSE 0 END) AS L,
				SUM(goal1) AS GF,
				SUM(goal2) AS GA,
				SUM(goal1) - SUM(goal2) AS GD,
				SUM(CASE WHEN goal1 > goal2 THEN 3 WHEN goal1 = goal2 THEN 1 ELSE 0 END) AS PTS
			FROM
				matches WHERE stage = ''' +  @StageName + 
			'''GROUP BY
				group_name, team1

			UNION ALL

			SELECT
				group_name,
				team2 AS team,
				COUNT(*) AS MP,
				SUM(CASE WHEN goal2 > goal1 THEN 1 ELSE 0 END) AS W,
				SUM(CASE WHEN goal2 = goal1 THEN 1 ELSE 0 END) AS D,
				SUM(CASE WHEN goal2 < goal1 THEN 1 ELSE 0 END) AS L,
				SUM(goal2) AS GF,
				SUM(goal1) AS GA,
				SUM(goal2) - SUM(goal1) AS GD,
				SUM(CASE WHEN goal2 > goal1 THEN 3 WHEN goal2 = goal1 THEN 1 ELSE 0 END) AS PTS
			FROM
				matches where stage = ''' + @StageName +
			'''GROUP BY
				group_name, team2
		) AS TeamStats
		GROUP BY
			group_name, team'

    EXEC sp_executesql @SQL

END;

exec Standings @StageName = 'GroupStage'


--creating the third place view for further round16 selection
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'group_stage_third_placement_view')
    DROP VIEW group_stage_third_placement_view;
GO
create view group_stage_third_placement_view as
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
    RowNum = 3


--creating the winners view for each stage dynamically
CREATE or alter PROCEDURE Winners
	@ViewName nvarchar(255),
	@RowNumValues NVARCHAR(MAX),
	@TableName nvarchar(255)
AS
BEGIN
	DECLARE @SQL NVARCHAR(MAX)

    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = @ViewName)
    BEGIN
        SET @SQL = 'DROP VIEW ' + QUOTENAME(@ViewName)
        EXEC sp_executesql @SQL
    END

    -- dynamic SQL statement to create the view
    SET @SQL = '
    CREATE VIEW ' + QUOTENAME(@ViewName) + '
    AS
	SELECT
		team,group_name,MP,W,D,L,GF,GA,GD,Pts
	FROM (
		SELECT
			team,group_name,MP,W,D,L,GF,GA,GD,Pts,
			ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC) AS RowNum
		FROM '
			+ @TableName +
	') AS GroupRanking
	WHERE RowNum IN (' + @RowNumValues + ')'

    EXEC sp_executesql @SQL

END;

exec Winners @ViewName = 'group_stage_top2_winners', @RowNumValues = '1,2', @TableName = 'GroupStage'

-- temp table for top 4 team from third place in which table will be updated after each random team selection
CREATE  TABLE #third_placement (
 group_name varchar(255),
 team VARCHAR(255),
);

insert into #third_placement (group_name,team) 
select top 4 group_name,team from group_stage_third_placement_view 
order by PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC;

-- stored procedure to randomly select team
CREATE or alter PROCEDURE RandomFromThirdPlace
    @ThirdGroups NVARCHAR(100),
	@GroupName VARCHAR(255) OUTPUT,
	@Team varchar(255) OUTPUT
AS
BEGIN

    -- Select the random third-place group from the provided groups
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

-- procedure that performs round16 staging
CREATE or alter PROCEDURE Round16Staging
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

	select top 2 @RunnerUpGroupA = team 
	from group_stage_top2_winners where group_name = 'A';
	select top 2 @RunnerUpGroupB = team 
	from group_stage_top2_winners where group_name = 'B';
	select top 2 @RunnerUpGroupC = team 
	from group_stage_top2_winners where group_name = 'C';
	select top 2 @RunnerUpGroupD = team 
	from group_stage_top2_winners where group_name = 'D';
	select top 2 @RunnerUpGroupE = team 
	from group_stage_top2_winners where group_name = 'E';
	select top 2 @RunnerUpGroupF = team 
	from group_stage_top2_winners where group_name = 'F';

	select top 1 @WinnerGroupA = team 
	from group_stage_top2_winners where group_name = 'A';
	select top 1 @WinnerGroupB = team 
	from group_stage_top2_winners where group_name = 'B';
	select top 1 @WinnerGroupC = team 
	from group_stage_top2_winners where group_name = 'C';
	select top 1 @WinnerGroupD = team 
	from group_stage_top2_winners where group_name = 'D';
	select top 1 @WinnerGroupE = team 
	from group_stage_top2_winners where group_name = 'E';
	select top 1 @WinnerGroupF = team 
	from group_stage_top2_winners where group_name = 'F';

	-- opponent of group C
	declare @OpponentGroupOfGroupC varchar(255), @OpponentTeamOfGroupC varchar(255)
	exec RandomFromThirdPlace @ThirdGroups = 'D,E,F', @GroupName = @OpponentGroupOfGroupC output, @Team = @OpponentTeamOfGroupC output
	select @OpponentGroupOfGroupC, @OpponentTeamOfGroupC;

	-- opponent of group B
	declare @OpponentGroupOfGroupB varchar(255), @OpponentTeamOfGroupB varchar(255)
	exec RandomFromThirdPlace @ThirdGroups = 'A,D,E,F', @GroupName = @OpponentGroupOfGroupB output, @Team = @OpponentTeamOfGroupB output
	select @OpponentGroupOfGroupB, @OpponentTeamOfGroupB

	-- opponent of group F
	declare @OpponentGroupOfGroupF varchar(255), @OpponentTeamOfGroupF varchar(255)
	exec RandomFromThirdPlace @ThirdGroups = 'A,B,C', @GroupName = @OpponentGroupOfGroupF output, @Team = @OpponentTeamOfGroupF output
	select @OpponentGroupOfGroupF, @OpponentTeamOfGroupF
	
	-- opponent of group F
	declare @OpponentGroupOfGroupE varchar(255), @OpponentTeamOfGroupE varchar(255)
	exec RandomFromThirdPlace @ThirdGroups = 'A,B,C,D', @GroupName = @OpponentGroupOfGroupE output, @Team = @OpponentTeamOfGroupE output
	select @OpponentGroupOfGroupE, @OpponentTeamOfGroupE


	INSERT INTO matches (group_name, team1, team2, stage)
	values ('R1', @RunnerUpGroupA, @RunnerUpGroupB, 'Round16Stage'),
		   ('R2', @WinnerGroupA, @RunnerUpGroupC, 'Round16Stage'),
		   ('R3', @WinnerGroupC, @OpponentTeamOfGroupC, 'Round16Stage'),
		   ('R4', @WinnerGroupB, @OpponentTeamOfGroupB, 'Round16Stage'),
		   ('R5', @RunnerUpGroupD, @RunnerUpGroupE, 'Round16Stage'),
		   ('R6', @WinnerGroupF, @OpponentTeamOfGroupF, 'Round16Stage'),
		   ('R7', @WinnerGroupE, @OpponentTeamOfGroupE, 'Round16Stage'),
		   ('R8', @WinnerGroupD, @RunnerUpGroupF, 'Round16Stage')
END;

exec Round16Staging
exec UpdateMatchResults @stage = 'Round16Stage';
exec Standings @StageName = 'Round16Stage'
exec Winners @ViewName = 'round16_winner', @RowNumValues = '1', @TableName = 'Round16Stage'

-- procedure that performs quarterfinal staging
CREATE or alter PROCEDURE QuarterFinalStaging
AS
BEGIN
	declare @R16Team1 varchar(255),
			@R16Team2 varchar(255),
			@R16Team3 varchar(255),
			@R16Team4 varchar(255),
			@R16Team5 varchar(255),
			@R16Team6 varchar(255),
			@R16Team7 varchar(255),
			@R16Team8 varchar(255)

	select top 1 @R16Team1 = team 
	from round16_winner where group_name = 'R1'

	select top 1 @R16Team2 = team 
	from round16_winner where group_name = 'R2'

	select top 1 @R16Team3 = team 
	from round16_winner where group_name = 'R3'

	select top 1 @R16Team4 = team 
	from round16_winner where group_name = 'R4'

	select top 1 @R16Team5 = team 
	from round16_winner where group_name = 'R5'

	select top 1 @R16Team6 = team 
	from round16_winner where group_name = 'R6'

	select top 1 @R16Team7 = team 
	from round16_winner where group_name = 'R7'

	select top 1 @R16Team8 = team 
	from round16_winner where group_name = 'R8'

	insert into matches (group_name, team1, team2, stage)
	values ('QF1', @R16Team1, @R16Team2, 'QuarterFinalStage'),
		   ('QF2', @R16Team3, @R16Team4, 'QuarterFinalStage'),
		   ('QF3', @R16Team5, @R16Team6, 'QuarterFinalStage'),
		   ('QF4', @R16Team7, @R16Team8, 'QuarterFinalStage')
end;

exec QuarterFinalStaging
exec UpdateMatchResults @stage = 'QuarterFinalStage';
exec Standings @StageName = 'QuarterFinalStage'
exec Winners @ViewName = 'quarter_final_winner', @RowNumValues = '1', @TableName = 'QuarterFinalStage'

-- procedure that performs semifinal staging
CREATE or alter PROCEDURE SemiFinalStaging
AS
BEGIN
	declare @QFTeam1 varchar(255),
			@QFTeam2 varchar(255),
			@QFTeam3 varchar(255),
			@QFTeam4 varchar(255)

	select top 1 @QFTeam1 = team 
	from quarter_final_winner where group_name = 'QF1'

	select top 1 @QFTeam2 = team 
	from quarter_final_winner where group_name = 'QF2'

	select top 1 @QFTeam3 = team 
	from quarter_final_winner where group_name = 'QF3'

	select top 1 @QFTeam4 = team 
	from quarter_final_winner where group_name = 'QF4'

	insert into matches (group_name, team1, team2, stage)
	values ('SF1', @QFTeam1, @QFTeam2, 'SemiFinalStage'),
		   ('SF2', @QFTeam3, @QFTeam4, 'SemiFinalStage')
end;

exec SemiFinalStaging
exec UpdateMatchResults @stage = 'SemiFinalStage';
exec Standings @StageName = 'SemiFinalStage'
exec Winners @ViewName = 'semi_final_winner', @RowNumValues = '1', @TableName = 'SemiFinalStage'

-- procedure that performs final staging
CREATE or alter PROCEDURE FinalStaging
AS
BEGIN
	declare @SFTeam1 varchar(255),
			@SFTeam2 varchar(255)

	select top 1 @SFTeam1 = team 
	from semi_final_winner where group_name = 'SF1'

	select top 1 @SFTeam2 = team 
	from semi_final_winner where group_name = 'SF2'

	insert into matches (group_name, team1, team2, stage)
	values ('F', @SFTeam1, @SFTeam2, 'FinalStage')
end;

exec FinalStaging
exec UpdateMatchResults @stage = 'FinalStage';
exec Standings @StageName = 'FinalStage'
exec Winners @ViewName = 'final_winner', @RowNumValues = '1', @TableName = 'FinalStage'
