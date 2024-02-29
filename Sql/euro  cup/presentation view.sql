--select * from group_stage_view ORDER BY group_name ,
--PTS DESC, GD DESC, GF DESC, GA DESC, W DESC, L DESC, D DESC;


--grp left view 
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'group_stage_left_view')
    DROP VIEW group_stage_left_view;
GO
CREATE VIEW group_stage_left_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
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

	select * from group_stage_left_view;



-- grp right side view 
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'group_stage_right_view')
    DROP VIEW group_stage_right_view;
GO
CREATE VIEW group_stage_right_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,
    * 
FROM
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


select * from group_stage_right_view;

--round of 16 left view  ---
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'r16_left_view')
    DROP VIEW r16_left_view;
GO
CREATE VIEW r16_left_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber, * FROM
    (
select  'Round Of 16' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 4  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select  ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 2   ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select  ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT team1_group AS ' ',team1 AS 'Country Name',CAST(gf1 AS VARCHAR(10)) AS 'Goals 'FROM matches WHERE stage = 'RoundOf16' AND id = 'M39'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M39'
union all
SELECT Top 2   ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select  ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M41'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M41'
union all
SELECT Top 2   ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select  ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M44'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M44'
) as subquery;


select * from r16_left_view;

--r16 right view
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'r16_right_view')
    DROP VIEW r16_right_view;
GO
CREATE VIEW r16_right_view AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber,* FROM (
select  'Round Of 16' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 4  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M38'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M38'
union all
SELECT Top 2  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT team1_group AS ' ',team1 AS 'Country Name',CAST(gf1 AS VARCHAR(10)) AS 'Goals 'FROM matches WHERE stage = 'RoundOf16' AND id = 'M40'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M40'
union all
SELECT Top 2  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select ' ','Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M42'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M42'
union all
SELECT Top 2  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M43'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M43'
) as subquery;


select * from r16_right_view;

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
select  'Quarter Final' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from matches  where stage='QuarterFinal' and id='M45'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='QuarterFinal' and id='M45'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT team1_group AS ' ',team1 AS 'Country Name',CAST(gf1 AS VARCHAR(10)) AS 'Goals 'FROM matches WHERE stage = 'QuarterFinal' AND id = 'M46'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='QuarterFinal' and id='M46'
union all
SELECT Top 2  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
) as subquery;


select * from quarter_final_left_view;


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
select  'Quarter Final' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from matches  where stage='QuarterFinal' and id='M47'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='QuarterFinal' and id='M47'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT team1_group AS ' ',team1 AS 'Country Name',CAST(gf1 AS VARCHAR(10)) AS 'Goals 'FROM matches WHERE stage = 'QuarterFinal' AND id = 'M48'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='QuarterFinal' and id='M48'
union all
SELECT Top 2  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='RoundOf16'
) as subquery;


select * from quarter_final_right_view;



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
	select  'Semi Final' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 12  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='GroupStage'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from matches  where stage='SemiFinal' and id='M49'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='SemiFinal' and id='M49'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='GroupStage'
) as subquery;


select * from semi_final_left_view;


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
select  'Semi Final' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 12  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='GroupStage'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from matches  where stage='SemiFinal' and id='M50'
union all
select team2_group as ' ',team2 as 'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='SemiFinal' and id='M50'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='GroupStage'
) as subquery;


select * from semi_final_right_view;


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
	select  'Final' as Blank,''  as'Country Name',CAST('' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
SELECT Top 12  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='GroupStage'
union all
select ' ' as Blank,'Country Name'  as'Country Name',CAST('Goals' AS VARCHAR(10)) as 'Goals' from matches  where stage='RoundOf16' and id='M37'
union all
select team1_group as ' ', team1 as'Country Name',CAST(gf1 AS VARCHAR(10)) as 'Goals' from matches  where stage='Final' and id='M51'
union all
select team1_group as ' ', team2 as'Country Name',CAST(gf2 AS VARCHAR(10)) as 'Goals' from matches  where stage='Final' and id='M51'
union all
SELECT Top 7  ' ' as Blank, '' as'Country Name',Cast('' as varchar(10)) as 'Goals'  FROM matches where stage='GroupStage'
) as subquery;


select * from final_pview;





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



