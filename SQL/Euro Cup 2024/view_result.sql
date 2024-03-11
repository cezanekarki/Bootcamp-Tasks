--GroupStage_left_view
go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'GroupStage_left_view')
    DROP VIEW GroupStage_left_view;
GO
CREATE VIEW GroupStage_left_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
		select 'Group Staging' as 'Rank', '' AS Team, CAST('' AS VARCHAR(10)) as PTS,CAST('' AS VARCHAR(10)) AS MP,CAST('' AS VARCHAR(10)) AS W,CAST('' AS VARCHAR(10)) AS L,CAST('' AS VARCHAR(10)) AS D,CAST('' AS VARCHAR(10)) AS GF,CAST('' AS VARCHAR(10)) AS GA,CAST('' AS VARCHAR(10)) AS GD FROM GroupStage WHERE team='Albania'
		union all
        SELECT 'Group A' AS 'Rank','' AS Team, CAST('' AS VARCHAR(10)) as PTS,CAST('' AS VARCHAR(10)) AS MP,CAST('' AS VARCHAR(10)) AS W,CAST('' AS VARCHAR(10)) AS L,CAST('' AS VARCHAR(10)) AS D,CAST('' AS VARCHAR(10)) AS GF,CAST('' AS VARCHAR(10)) AS GA,CAST('' AS VARCHAR(10)) AS GD FROM GroupStage WHERE team='Albania' 
        UNION ALL
        SELECT 'Rank' ,'Team' AS Team, CAST('PTS' AS VARCHAR(10)) AS PTS,'MP' AS MP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM GroupStage WHERE team='Albania' 
        UNION ALL
        SELECT group_name AS 'Rank',team AS Team, CAST(PTS AS VARCHAR(10)) as PTS,CAST(MP AS VARCHAR(10)) AS MP,CAST(W AS VARCHAR(10)) AS W,CAST(L AS VARCHAR(10)) AS L,CAST(D AS VARCHAR(10)) AS D,CAST(GF AS VARCHAR(10)) AS GF,CAST(GA AS VARCHAR(10)) AS GA,CAST(GD AS VARCHAR(10)) AS GD FROM GroupStage WHERE group_name = 'A'
        UNION ALL
        SELECT top 2 '' AS 'Rank','' AS Team, CAST('' AS VARCHAR(10)) AS PTS,'' AS MP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM GroupStage WHERE group_name='B'
        UNION ALL
        SELECT 'Group C' AS 'Rank','' AS Team, CAST('' AS VARCHAR(10)) AS PTS,'' AS MP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM GroupStage WHERE team='Albania' 
        UNION ALL
        SELECT  'Rank','Team' AS Team, CAST('PTS' AS VARCHAR(10)) AS PTS,'MP' AS MP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM GroupStage WHERE team='Albania' 
        UNION ALL
        SELECT group_name AS 'Rank',team AS Team, CAST(PTS AS VARCHAR(10)) as PTS,CAST(MP AS VARCHAR(10)) AS MP,CAST(W AS VARCHAR(10)) AS W,CAST(L AS VARCHAR(10)) AS L,CAST(D AS VARCHAR(10)) AS D,CAST(GF AS VARCHAR(10)) AS GF,CAST(GA AS VARCHAR(10)) AS GA,CAST(GD AS VARCHAR(10)) AS GD FROM GroupStage WHERE group_name = 'B'
        UNION ALL
        SELECT top 2 '' AS 'Rank','' AS Team, CAST('' AS VARCHAR(10)) AS PTS,'' AS MP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM GroupStage WHERE group_name='A'
        UNION ALL
        SELECT 'Group E' AS 'Rank','' AS Team, CAST('' AS VARCHAR(10)) AS PTS,'' AS MP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM GroupStage WHERE team='Albania'
        UNION ALL
        SELECT 'Rank','Team' AS Team, CAST('PTS' AS VARCHAR(10)) AS PTS,'MP' AS MP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM GroupStage WHERE team='Albania' 
        UNION ALL
        SELECT group_name AS 'Rank',team AS Team, CAST(PTS AS VARCHAR(10)) as PTS,CAST(MP AS VARCHAR(10)) AS MP,CAST(W AS VARCHAR(10)) AS W,CAST(L AS VARCHAR(10)) AS L,CAST(D AS VARCHAR(10)) AS D,CAST(GF AS VARCHAR(10)) AS GF,CAST(GA AS VARCHAR(10)) AS GA,CAST(GD AS VARCHAR(10)) AS GD FROM GroupStage WHERE group_name = 'C'
    ) AS SubQuery;
 
 
-- GroupStage_right_view
go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'GroupStage_right_view')
    DROP VIEW GroupStage_right_view;
GO 
CREATE VIEW GroupStage_right_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
select 'Group Staging' as 'Rank', '' AS Team, CAST('' AS VARCHAR(10)) as PTS,CAST('' AS VARCHAR(10)) AS MP,CAST('' AS VARCHAR(10)) AS W,CAST('' AS VARCHAR(10)) AS L,CAST('' AS VARCHAR(10)) AS D,CAST('' AS VARCHAR(10)) AS GF,CAST('' AS VARCHAR(10)) AS GA,CAST('' AS VARCHAR(10)) AS GD FROM GroupStage WHERE team='Albania'
union all
SELECT 'Group B' AS 'Rank','' AS Team,'' AS PTS,'' AS MP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM GroupStage where team='Albania' 
UNION ALL
SELECT 'Rank' ,'Team' AS Team,'PTS' AS PTS,'MP' AS MP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM GroupStage where team='Albania' 
union all
SELECT group_name AS 'Rank',team AS Team, CAST(PTS AS VARCHAR(10)) as PTS,CAST(MP AS VARCHAR(10)) AS MP,CAST(W AS VARCHAR(10)) AS W,CAST(L AS VARCHAR(10)) AS L,CAST(D AS VARCHAR(10)) AS D,CAST(GF AS VARCHAR(10)) AS GF,CAST(GA AS VARCHAR(10)) AS GA,CAST(GD AS VARCHAR(10)) AS GD FROM GroupStage WHERE group_name = 'D'
union all
SELECT top 2 '' AS 'Rank','' AS Team,'' AS PTS,'' AS MP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM GroupStage where group_name='B'
union all
SELECT 'Group D' AS 'Rank','' AS Team,'' AS PTS,'' AS MP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM GroupStage where team='Albania' 
UNION ALL
SELECT  'Rank','Team' AS Team,'PTS' AS PTS,'MP' AS MP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM GroupStage where team='Albania' 
union all
        SELECT group_name AS 'Rank',team AS Team, CAST(PTS AS VARCHAR(10)) as PTS,CAST(MP AS VARCHAR(10)) AS MP,CAST(W AS VARCHAR(10)) AS W,CAST(L AS VARCHAR(10)) AS L,CAST(D AS VARCHAR(10)) AS D,CAST(GF AS VARCHAR(10)) AS GF,CAST(GA AS VARCHAR(10)) AS GA,CAST(GD AS VARCHAR(10)) AS GD FROM GroupStage WHERE group_name = 'E'
union all
SELECT top 2 '' AS 'Rank','' AS Team,'' AS PTS,'' AS MP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM GroupStage where group_name='A'
union all
SELECT 'Group F' AS 'Rank','' AS Team,'' AS PTS,'' AS MP,'' AS W,'' AS L,'' AS D,'' AS GF,'' AS GA,'' AS GD FROM GroupStage where team='Albania'
union all
SELECT 'Rank','Team' AS Team,'PTS' AS PTS,'MP' AS MP,'W' AS W,'L' AS L,'D' AS D,'GF' AS GF,'GA' AS GA,'GD' AS GD FROM GroupStage where team='Albania' 
UNION ALL
        SELECT group_name AS 'Rank',team AS Team, CAST(PTS AS VARCHAR(10)) as PTS,CAST(MP AS VARCHAR(10)) AS MP,CAST(W AS VARCHAR(10)) AS W,CAST(L AS VARCHAR(10)) AS L,CAST(D AS VARCHAR(10)) AS D,CAST(GF AS VARCHAR(10)) AS GF,CAST(GA AS VARCHAR(10)) AS GA,CAST(GD AS VARCHAR(10)) AS GD FROM GroupStage WHERE group_name = 'F'
) as subquery
 

--round of 16 left view  ---
go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'r16_left_view')
    DROP VIEW r16_left_view;
GO
CREATE VIEW r16_left_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
select top 1 'Round of 16' as 'Country Name', Cast('' as varchar(10)) as 'Goals'   FROM Round16Stage
union all
SELECT Top 4  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM Round16Stage 
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R1'
union all
select  top 2 team as'Country Name',CAST(GF AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R1'
union all
SELECT Top 2  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM Round16Stage
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R1'
union all
SELECT top 2 team AS 'Country Name',CAST(GF AS VARCHAR(10)) AS 'Goals'  FROM Round16Stage where group_name = 'R2'
union all
SELECT Top 2  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM Round16Stage
union all
select  top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R1'
union all
select  top 2 team as'Country Name',CAST(GF AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R3'
union all
SELECT Top 2  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM Round16Stage
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R1'
union all
select  top 2 team as'Country Name',CAST(GF AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R4'
) as subquery;


--round of 16 right view  ---
go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'r16_right_view')
    DROP VIEW r16_right_view;
GO
CREATE VIEW r16_right_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
select top 1 'Round of 16' as 'Country Name', Cast('' as varchar(10)) as 'Goals'   FROM Round16Stage
union all
SELECT Top 3  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM Round16Stage 
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R1'
union all
select  top 2 team as'Country Name',CAST(GF AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R5'
union all
SELECT Top 2  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM Round16Stage
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R1'
union all
SELECT top 2 team AS 'Country Name',CAST(GF AS VARCHAR(10)) AS 'Goals'  FROM Round16Stage where group_name = 'R6'
union all
SELECT Top 2  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM Round16Stage
union all
select  top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R1'
union all
select  top 2 team as'Country Name',CAST(GF AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R7'
union all
SELECT Top 2  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM Round16Stage
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R1'
union all
select  top 2 team as'Country Name',CAST(GF AS VARCHAR(10)) as 'Goals'  FROM Round16Stage where group_name = 'R8'
) as subquery;


--quarter final left view  ---
go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'qf_left_view')
    DROP VIEW qf_left_view;
GO
 
CREATE VIEW qf_left_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (

select top 1 'Quarter Final' as 'Country Name', Cast('' as varchar(10)) as 'Goals'   FROM QuarterFinalStage
union all
SELECT Top 7  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM QuarterFinalStage
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM QuarterFinalStage where group_name = 'QF1'
union all
select  top 2 team as'Country Name',CAST(GF AS VARCHAR(10)) as 'Goals'  FROM QuarterFinalStage where group_name = 'QF1'
union all
SELECT Top 2  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM QuarterFinalStage
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM QuarterFinalStage where group_name = 'QF1'
union all
SELECT top 2 team AS 'Country Name',CAST(GF AS VARCHAR(10)) AS 'Goals'  FROM QuarterFinalStage where group_name = 'QF2'
union all
SELECT Top 7  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM QuarterFinalStage 
) as subquery;
 
--quarter final right view  ---
go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'QF_right_view')
    DROP VIEW QF_right_view;
GO
CREATE VIEW QF_right_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
select top 1 'Quarter Final' as 'Country Name', Cast('' as varchar(10)) as 'Goals'   FROM QuarterFinalStage
union all
SELECT Top 7  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM QuarterFinalStage 
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM QuarterFinalStage where group_name = 'QF1'
union all
select  top 2 team as'Country Name',CAST(GF AS VARCHAR(10)) as 'Goals'  FROM QuarterFinalStage where group_name = 'QF3'
union all
SELECT Top 2  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM QuarterFinalStage
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM QuarterFinalStage where group_name = 'QF1'
union all
SELECT top 2 team AS 'Country Name',CAST(GF AS VARCHAR(10)) AS 'Goals'  FROM QuarterFinalStage where group_name = 'QF4'
union all
SELECT Top 7  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM QuarterFinalStage 
) as subquery;
 

-- semi final left view
go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'sf_left_view')
    DROP VIEW sf_left_view;
GO
CREATE VIEW sf_left_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
	
select top 1 'Semifinal' as 'Country Name', Cast('' as varchar(10)) as 'Goals'   FROM SemiFinalStage
union all
SELECT Top 12  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM GroupStage 
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM SemiFinalStage where group_name = 'SF1'
union all
select  top 2 team as'Country Name',CAST(GF AS VARCHAR(10)) as 'Goals'  FROM SemiFinalStage where group_name = 'SF1'
union all
SELECT Top 7  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM GroupStage 
) as subquery;
 

-- semi final right view
go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'SF_right_view')
    DROP VIEW SF_right_view;
GO
CREATE VIEW SF_right_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
select top 1 'Semifinal' as 'Country Name', Cast('' as varchar(10)) as 'Goals'   FROM SemiFinalStage
union all
SELECT Top 12  '' as'Country Name',Cast('' as varchar(10)) as 'Goals' FROM GroupStage 
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM SemiFinalStage where group_name = 'SF1'
union all
select  top 2 team as'Country Name',CAST(GF AS VARCHAR(10)) as 'Goals'  FROM SemiFinalStage where group_name = 'SF2'
union all
SELECT Top 7  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM GroupStage 
) as subquery;


--final view
go
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'final_view')
    DROP VIEW final_view;
GO
 
CREATE VIEW final_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
    (
select top 1 'Final' as 'Country Name', Cast('' as varchar(10)) as 'Goals'   FROM FinalStage
union all
SELECT Top 12  '' as'Country Name',Cast('' as varchar(10)) as 'Goals' FROM GroupStage 
union all
select top 1 'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals'  FROM FinalStage where group_name = 'F'
union all
select top 2 team as'Country Name',CAST(GF AS VARCHAR(10)) as 'Goals'  FROM FinalStage where group_name = 'F'
union all
SELECT Top 8  '' as'Country Name',Cast('' as varchar(10)) as 'Goals'   FROM GroupStage 
) as subquery;

--final euro cup view
go
select
a.Rank,a.Team,a.PTS,a.MP,a.W,a.L,a.D,a.GF,a.GA,a.GD,
' ' AS N1,
b.[Country Name],b.Goals,
' ' as N2,
c.[Country Name],c.Goals,
' ' as N3,
d.[Country Name],d.Goals,
' ' as N4,
f.[Country Name],f.Goals,
' ' as N6,
g.[Country Name],g.Goals,
' ' as N7,
h.[Country Name],h.Goals,
' ' as N8,
i.[Country Name],i.Goals,
' ' as N9,
j.Rank,j.Team,j.PTS,j.MP,j.W,j.L,j.D,j.GF,j.GA,j.GD
from GroupStage_left_view a
left join r16_left_view b on  a.RowNumber=b.RowNumber
left join qf_left_view c on a.RowNumber=c.RowNumber
left join sf_left_view d on a.RowNumber=d.RowNumber
left join final_view f on a.RowNumber=f.RowNumber
left join SF_right_view g on a.RowNumber=g.RowNumber
left join QF_right_view h on a.RowNumber=h.RowNumber
left join r16_right_view i on  a.RowNumber=i.RowNumber
left join GroupStage_right_view j on  a.RowNumber=j.RowNumber