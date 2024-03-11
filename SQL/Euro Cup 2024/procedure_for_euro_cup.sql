-- generate random goals for the matches
go
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

-- created standings view for each stage dynamically
go
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

--creating the winners view for each stage dynamically
go
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

go
CREATE OR ALTER PROCEDURE Round16TeamAndGroupSelection
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

-- procedure that performs round16 staging
go
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
	EXEC Round16TeamAndGroupSelection @ColumnName = 'B1', @ResultTeam = @OpponentTeamOfGroupC OUTPUT, @ResultGroupName = @OpponentGroupOfGroupC OUTPUT;

	-- opponent of group B
	declare @OpponentGroupOfGroupB varchar(255), @OpponentTeamOfGroupB varchar(255)
	EXEC Round16TeamAndGroupSelection @ColumnName = 'C1', @ResultTeam = @OpponentTeamOfGroupB OUTPUT, @ResultGroupName = @OpponentGroupOfGroupC OUTPUT;

	-- opponent of group F
	declare @OpponentGroupOfGroupE varchar(255), @OpponentTeamOfGroupE varchar(255)
	EXEC Round16TeamAndGroupSelection @ColumnName = 'E1', @ResultTeam = @OpponentTeamOfGroupE OUTPUT, @ResultGroupName = @OpponentGroupOfGroupE OUTPUT;
	
	-- opponent of group F
	declare @OpponentGroupOfGroupF varchar(255), @OpponentTeamOfGroupF varchar(255)
	EXEC Round16TeamAndGroupSelection @ColumnName = 'F1', @ResultTeam = @OpponentTeamOfGroupF OUTPUT, @ResultGroupName = @OpponentGroupOfGroupF OUTPUT;


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

-- procedure that performs quarterfinal staging
go
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

-- procedure that performs semifinal staging
go
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

-- procedure that performs final staging
go
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
