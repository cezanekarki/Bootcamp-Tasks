-- Databricks notebook source
-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #EuroSimulation SQL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/euroteaminfo', recurse=True)
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/thirdplacecombi', recurse=True)
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/qualifiersplacement', recurse=True)
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS EuroTeamInfo;
DROP TABLE IF EXISTS EuroGroupStage;
DROP TABLE IF EXISTS EuroGroupStageDetail;
DROP TABLE IF EXISTS ThirdPlaceCombi;
DROP TABLE IF EXISTS QualifiersPlacement;
DROP TABLE IF EXISTS EuroTournamentResults;

-- COMMAND ----------

CREATE OR REPLACE TABLE EuroTeamInfo(
    EuroTeamID INT,
    EuroGroupName CHAR(1),
    EuroTeamName VARCHAR(53)
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Values for Above Created Table

-- COMMAND ----------

INSERT INTO EuroTeamInfo(EuroTeamID, EuroGroupName, EuroTeamName)
VALUES
    (1, 'A', 'Germany'),
    (2, 'A', 'Scotland'),
    (3, 'A', 'Hungary'),
    (4, 'A', 'Switzerland'),
    (5, 'B', 'Spain'),
    (6, 'B', 'Croatia'),
    (7, 'B', 'Italy'),
    (8, 'B', 'Albania'),
    (9, 'C', 'Slovenia'),
    (10, 'C', 'Denmark'),
    (11, 'C', 'Serbia'),
    (12, 'C', 'England'),
    (13, 'D','Netherlands'),
    (14, 'D', 'Austria'),
    (15, 'D', 'France'),
    (16, 'D', 'Poland'),
    (17, 'E', 'Belgium'),
    (18, 'E', 'Slovakia'),
    (19, 'E', 'Romania'),
    (20, 'E', 'Ukraine'),
    (21, 'F', 'Turkey'),
    (22, 'F', 'Portugal'),
    (23, 'F', 'Czech Republic'),
    (24, 'F', 'Greece');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking Data Insert

-- COMMAND ----------

SELECT * FROM euroteaminfo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query for Generating GroupStage Matches

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Cheking those Matches

-- COMMAND ----------

CREATE OR REPLACE TABLE EuroGroupStage AS
SELECT
    concat('GS', Row_Number() over(order by 1)) as MatchID,
    t1.EuroGroupName AS Team1Group,
    t1.EuroTeamID AS Team1ID,
    t1.EuroTeamName AS Team1Name,
    t2.EuroGroupName AS Team2Group,
    t2.EuroTeamID AS Team2ID,
    t2.EuroTeamName AS Team2Name,
    ROUND(RAND() * 6) AS Team1Goals, 
    ROUND(RAND() * 6) AS Team2Goals,   
    CASE
        WHEN Team1Goals > Team2Goals THEN t1.EuroTeamName
        WHEN Team2Goals > Team1Goals THEN t2.EuroTeamName
        ELSE "Draw"
    END AS Winner
FROM
    euroteaminfo t1
CROSS JOIN
    euroteaminfo t2
WHERE
    t1.EuroGroupName = t2.EuroGroupName
    AND t1.EuroTeamID < t2.EuroTeamID;  

-- COMMAND ----------

SELECT MatchID, Team1Name, Team1Goals, Team2Goals, Team2Name, Winner FROM eurogroupstage;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Result Table Calculation Accroding to the Above Matches

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking Group Stage Table

-- COMMAND ----------

SELECT * FROM EuroGroupStage;

-- COMMAND ----------

CREATE OR REPLACE TABLE EuroGroupStageDetail AS
SELECT
    EuroGroupName,
    EuroTeamName,
    COUNT(*) AS MatchesPlayed,
    SUM(CASE WHEN Winner = EuroTeamName THEN 1 ELSE 0 END) AS MatchesWon,
    SUM(CASE WHEN Winner != EuroTeamName AND Winner != 'Draw' THEN 1 ELSE 0 END) AS MatchesLost,
    SUM(CASE WHEN Winner = 'Draw' THEN 1 ELSE 0 END) AS MatchesDrawn,
    SUM(GoalsFor) AS GF,
    SUM(GoalsAgainst) AS GA,
    SUM(GoalsFor - GoalsAgainst) AS GD,
    SUM(CASE WHEN Winner = EuroTeamName THEN 3 WHEN Winner = 'Draw' THEN 1 ELSE 0 END) AS TotalPoints,
    concat(EuroGroupName, ROW_NUMBER() OVER (PARTITION BY EuroGroupName ORDER BY SUM(CASE WHEN Winner = EuroTeamName THEN 3 WHEN Winner = 'Draw' THEN 1 ELSE 0 END) DESC, SUM(GoalsFor - GoalsAgainst) DESC)) AS Position
FROM (
    SELECT
        Team1ID AS EuroTeamID,
        Team1Name AS EuroTeamName,
        Team1Group AS EuroGroupName,
        Winner,
        Team1Goals AS GoalsFor,
        Team2Goals AS GoalsAgainst
    FROM
        eurogroupstage

    UNION ALL

    SELECT
        Team2ID AS EuroTeamID,
        Team2Name AS EuroTeamName,
        Team2Group AS EuroGroupName,
        Winner,
        Team2Goals AS GoalsFor,
        Team1Goals AS GoalsAgainst
    FROM
        eurogroupstage
) AS AllTeams
GROUP BY
    EuroTeamID, EuroTeamName, EuroGroupName
ORDER BY
    EuroGroupName, TotalPoints DESC, GD DESC, GF DESC, GA ASC, EuroTeamName;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking for all 3rd Position Teams

-- COMMAND ----------

SELECT * FROM EuroGroupStageDetail WHERE Position LIKE '%3' ORDER BY TotalPoints DESC, GD DESC, GF DESC, GA ASC, EuroTeamName;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Table for Seeding Possibilities According to 3rd Place Qualification

-- COMMAND ----------

CREATE or replace TABLE ThirdPlaceCombi(
  SId INT,
  Sequence VARCHAR(8),
  B1 CHAR(2),
  C1 CHAR(2),
  E1 CHAR(2),
  F1 CHAR(2)
);

INSERT INTO  ThirdPlaceCombi (SId, Sequence, B1, C1, E1, F1)
VALUES
    (1, 'A3B3C3D3', 'A3', 'D3', 'B3', 'C3'),
    (2, 'A3B3C3E3', 'A3', 'E3', 'B3', 'C3'),
    (3, 'A3B3C3F3', 'A3', 'F3', 'B3', 'C3'),
    (4, 'A3B3D3E3', 'D3', 'E3', 'A3', 'B3'),
    (5, 'A3B3D3F3', 'D3', 'F3', 'A3', 'B3'),
    (6, 'A3B3E3F3', 'E3', 'F3', 'B3', 'A3'),
    (7, 'A3C3D3E3', 'E3', 'D3', 'C3', 'A3'),
    (8, 'A3C3D3F3', 'F3', 'D3', 'C3', 'A3'),
    (9, 'A3C3E3F3', 'E3', 'F3', 'C3', 'A3'),
    (10, 'A3D3E3F3', 'E3', 'F3', 'D3', 'A3'),
    (11, 'B3C3D3E3', 'E3', 'D3', 'B3', 'C3'),
    (12, 'B3C3D3F3', 'F3', 'D3', 'C3', 'B3'),
    (13, 'B3C3E3F3', 'F3', 'E3', 'C3', 'B3'),
    (14, 'B3D3E3F3', 'F3', 'E3', 'D3', 'B3'),
    (15, 'C3D3E3F3', 'F3', 'E3', 'D3', 'C3');

SELECT * FROM ThirdPlaceCombi;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Table for Rounf of 16 Qualification Placement and Matches According to Above Conditions

-- COMMAND ----------

CREATE OR REPLACE TABLE QualifiersPlacement (
    MatchID VARCHAR(100),
    EuroTeam1 VARCHAR(100),
    Team1Goals INT,
    Team2Goals INT,
    EuroTeam2 VARCHAR(100),
    Winner VARCHAR(100),
    MatchStage VARCHAR(100)
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Isolating Placement Combination for this Scenario

-- COMMAND ----------

WITH RoundOfSixteenQualified AS
(
    (
        SELECT Position, EuroTeamName
        FROM EuroGroupStageDetail
        WHERE Position LIKE '%1' OR Position LIKE '%2'
    )
    UNION ALL
    (
        SELECT Position, EuroTeamName
        FROM EuroGroupStageDetail
        WHERE Position LIKE '%3'
        ORDER BY TotalPoints DESC, GD DESC, GF DESC, GA ASC, EuroTeamName
        LIMIT 4
    )
    ORDER BY Position
),
QualificationCompare AS (
  SELECT c.SId, c.B1, c.C1, c.E1, c.F1, pc.PlacementCombination 
  FROM ThirdPlaceCombi c
  JOIN (
    SELECT REGEXP_REPLACE(CONCAT_WS('', COLLECT_LIST(SUBSTRING(Position,0))), '\\s', '') AS PlacementCombination
    FROM RoundOfSixteenQualified 
    WHERE Position LIKE '%3'
  ) pc
  ON c.Sequence = pc.PlacementCombination
)
  SELECT * FROM QualificationCompare;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Calculating and Inserting Qualification Placements, Matches

-- COMMAND ----------

WITH RoundOfSixteenQualified AS
(
    (
        SELECT Position, EuroTeamName
        FROM EuroGroupStageDetail
        WHERE Position LIKE '%1' OR Position LIKE '%2'
    )
    UNION ALL
    (
        SELECT Position, EuroTeamName
        FROM EuroGroupStageDetail
        WHERE Position LIKE '%3'
        ORDER BY TotalPoints DESC, GD DESC, GF DESC, GA ASC, EuroTeamName
        LIMIT 4
    )
    ORDER BY Position
),
QualificationCompare AS (
  SELECT c.SId, c.B1, c.C1, c.E1, c.F1, pc.PlacementCombination 
  FROM ThirdPlaceCombi c
  JOIN (
    SELECT REGEXP_REPLACE(CONCAT_WS('', COLLECT_LIST(SUBSTRING(Position,0))), '\\s', '') AS PlacementCombination
    FROM RoundOfSixteenQualified 
    WHERE Position LIKE '%3'
  ) pc
  ON c.Sequence = pc.PlacementCombination
)
INSERT INTO QualifiersPlacement (MatchID, EuroTeam1, Team1Goals, Team2Goals, EuroTeam2, Winner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 
      CASE t1.Position
        WHEN 'B1' THEN 1
        WHEN 'A1' THEN 2
        WHEN 'F1' THEN 3
        WHEN 'D2' THEN 4
        WHEN 'E1' THEN 5
        WHEN 'D1' THEN 6
        WHEN 'C1' THEN 7
        WHEN 'A2' THEN 8
      END
    ) AS MatchID,
    t1.EuroTeamName AS EuroTeam1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    t2.EuroTeamName AS EuroTeam2,
    CASE
        WHEN Team1Goals > Team2Goals THEN t1.EuroTeamName
        WHEN Team2Goals > Team1Goals THEN t2.EuroTeamName
        ELSE
          CASE
            WHEN EuroTeam1 < EuroTeam2 THEN EuroTeam1
            ELSE EuroTeam2
          END
    END AS Winner,
    'R16' AS MatchStage

FROM RoundOfSixteenQualified t1
JOIN RoundOfSixteenQualified t2
ON (
    (t1.Position = 'B1' AND t2.Position = (SELECT B1 FROM QualificationCompare)) OR
    (t1.Position = 'A1' AND t2.Position = 'C2') OR
    (t1.Position = 'F1' AND t2.Position = (SELECT F1 FROM QualificationCompare)) OR
    (t1.Position = 'D2' AND t2.Position = 'E2') OR
    (t1.Position = 'E1' AND t2.Position = (SELECT E1 FROM QualificationCompare)) OR
    (t1.Position = 'D1' AND t2.Position = 'F2') OR
    (t1.Position = 'C1' AND t2.Position = (SELECT C1 FROM QualificationCompare)) OR
    (t1.Position = 'A2' AND t2.Position = 'B2')
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking Round Of 16 Results

-- COMMAND ----------

SELECT * FROM QualifiersPlacement;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Quarter Final

-- COMMAND ----------

INSERT INTO QualifiersPlacement (MatchID, EuroTeam1, Team1Goals, Team2Goals, EuroTeam2, Winner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 8 AS MatchID,
    t1.Winner AS Team1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    t2.Winner AS EuroTeam2,
    'TBD' AS Winner,
    'QF' AS MatchStage

FROM QualifiersPlacement t1
CROSS JOIN QualifiersPlacement t2
ON
  (t1.MatchID = 1 AND t2.MatchId = 2) OR
  (t1.MatchID = 3 AND t2.MatchId = 4) OR
  (t1.MatchID = 5 AND t2.MatchId = 6) OR
  (t1.MatchID = 7 AND t2.MatchId = 8);

UPDATE QualifiersPlacement
SET Winner = 
    CASE
        WHEN Team1Goals > Team2Goals THEN EuroTeam1
        WHEN Team1Goals < Team2Goals THEN EuroTeam2
        ELSE
            CASE
                WHEN EuroTeam1 < EuroTeam2 THEN EuroTeam1
                ELSE EuroTeam2
            END
    END
WHERE MatchStage = 'QF';

-- COMMAND ----------

SELECT * FROM QualifiersPlacement
where MatchStage = "QF"

-- COMMAND ----------

INSERT INTO QualifiersPlacement (MatchID, EuroTeam1, Team1Goals, Team2Goals, EuroTeam2, Winner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 12 AS MatchID,
    t1.Winner AS EuroTeam1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    t2.Winner AS EuroTeam2,
    'TBD' AS Winner,
    'SF' AS MatchStage

FROM QualifiersPlacement t1
CROSS JOIN QualifiersPlacement t2
ON
  (t1.MatchID = 9 AND t2.MatchId = 10) OR
  (t1.MatchID = 11 AND t2.MatchId = 12);

UPDATE QualifiersPlacement
SET Winner = 
    CASE
        WHEN Team1Goals > Team2Goals THEN EuroTeam1
        WHEN Team1Goals < Team2Goals THEN EuroTeam2
        ELSE
            CASE
                WHEN EuroTeam1 < EuroTeam2 THEN EuroTeam1
                ELSE EuroTeam2
            END
    END
WHERE MatchStage = 'SF';

-- COMMAND ----------

select * from QualifiersPlacement where MatchStage="SF"

-- COMMAND ----------

INSERT INTO QualifiersPlacement (MatchID, EuroTeam1, Team1Goals, Team2Goals, EuroTeam2, Winner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 14 AS MatchID,
    CASE
      WHEN
        t1.Winner!= t1.EuroTeam1 THEN t1.EuroTeam1 ELSE t1.EuroTeam2
        END AS EuroTeam1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    CASE
      WHEN
        t2.Winner!= t2.EuroTeam2 THEN t2.EuroTeam2 ELSE t2.EuroTeam1
        END AS EuroTeam2,
    'TBD' AS Winner,
    'TP' AS MatchStage
FROM QualifiersPlacement t1
CROSS JOIN QualifiersPlacement t2
ON
  (t1.MatchID = 13 AND t2.MatchId = 14);

UPDATE QualifiersPlacement
SET Winner = 
    CASE
        WHEN Team1Goals > Team2Goals THEN EuroTeam1
        WHEN Team1Goals < Team2Goals THEN EuroTeam2
        ELSE
            CASE
                WHEN EuroTeam1 < EuroTeam2 THEN EuroTeam1
                ELSE EuroTeam2
            END
    END
WHERE MatchStage = 'TP';


SELECT * FROM QualifiersPlacement where MatchStage= 'TP';

-- COMMAND ----------

INSERT INTO QualifiersPlacement (MatchID, EuroTeam1, Team1Goals, Team2Goals, EuroTeam2, Winner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 15 AS MatchID,
    t1.Winner AS EuroTeam1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    t2.Winner AS EuroTeam2,
    'TBD' AS Winner,
    'FIN' AS MatchStage
FROM QualifiersPlacement t1
CROSS JOIN QualifiersPlacement t2
ON
  (t1.MatchID = 13 AND t2.MatchId = 14);

UPDATE QualifiersPlacement
SET Winner = 
    CASE
        WHEN Team1Goals > Team2Goals THEN EuroTeam1
        WHEN Team1Goals < Team2Goals THEN EuroTeam2
        ELSE
            CASE
                WHEN EuroTeam1 < EuroTeam2 THEN EuroTeam1
                ELSE EuroTeam2
            END
    END
WHERE MatchStage = 'FIN';


-- COMMAND ----------

Select * from QualifiersPlacement where MatchStage="FIN"

-- COMMAND ----------

Select * from qualifiersplacement

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("euros_sql_visulization") \
-- MAGIC     .getOrCreate()
-- MAGIC
-- MAGIC
-- MAGIC groupstage = spark.sql("SELECT * FROM EuroGroupStageDetail")
-- MAGIC groupstage_pd = groupstage.toPandas()
-- MAGIC
-- MAGIC groupstage_pd = groupstage_pd.applymap(lambda x: str(x).split(".")[0])
-- MAGIC
-- MAGIC display(groupstage_pd)
-- MAGIC
-- MAGIC
-- MAGIC euro_vis = pd.DataFrame({
-- MAGIC     'Grp/Pos': groupstage_pd['Position'],
-- MAGIC     'Country': groupstage_pd['EuroTeamName'],
-- MAGIC     'MP': groupstage_pd['MatchesPlayed'],
-- MAGIC     'W': groupstage_pd['MatchesWon'],
-- MAGIC     'D': groupstage_pd['MatchesDrawn'],
-- MAGIC     'L': groupstage_pd['MatchesLost'],
-- MAGIC     'GF': groupstage_pd['GF'],
-- MAGIC     'GA': groupstage_pd['GA'],
-- MAGIC     'GD': groupstage_pd['GD'],
-- MAGIC     'Pts': groupstage_pd['TotalPoints']
-- MAGIC })
-- MAGIC
-- MAGIC
-- MAGIC mtgs_row_pd = pd.DataFrame({col: [''] for col in euro_vis.columns})
-- MAGIC
-- MAGIC left_gs = euro_vis[euro_vis['Grp/Pos'].str.contains('^[ABC]')]
-- MAGIC
-- MAGIC right_gs = euro_vis[euro_vis['Grp/Pos'].str.contains('^[DEF]')]
-- MAGIC
-- MAGIC left_gs.reset_index(drop=True, inplace=True)
-- MAGIC right_gs.reset_index(drop=True, inplace=True)
-- MAGIC
-- MAGIC display(left_gs)
-- MAGIC display(right_gs)
-- MAGIC
-- MAGIC
-- MAGIC left_gs_grouped = [left_gs.iloc[i:i+4] for i in range(0, len(left_gs), 4)]
-- MAGIC
-- MAGIC for i in range(len(left_gs_grouped)):
-- MAGIC     if i < len(left_gs_grouped) - 1:
-- MAGIC         left_gs_grouped[i] = pd.concat([left_gs_grouped[i], mtgs_row_pd,mtgs_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC left_gs_spaced = pd.concat(left_gs_grouped).reset_index(drop=True)
-- MAGIC
-- MAGIC lgs_new_column_name ={'Grp/Pos': 'L_Grp/Pos',
-- MAGIC                        'Country': 'L_Country',
-- MAGIC                        'MP': 'L_MP',
-- MAGIC                        'W': 'L_W',
-- MAGIC                        'D': 'L-D',
-- MAGIC                        'L': 'L_L',
-- MAGIC                        'GF': 'L-GF',
-- MAGIC                        'GA': 'L_GA',
-- MAGIC                        'GD': 'L_GD',
-- MAGIC                        'Pts': 'L_Pts'
-- MAGIC                        }
-- MAGIC left_gs_spaced = left_gs_spaced.rename(columns=lgs_new_column_name)
-- MAGIC
-- MAGIC display(left_gs_spaced)
-- MAGIC
-- MAGIC
-- MAGIC right_gs_grouped = [right_gs.iloc[i:i+4] for i in range(0, len(right_gs), 4)]
-- MAGIC
-- MAGIC for i in range(len(right_gs_grouped)):
-- MAGIC     if i < len(right_gs_grouped) - 1:
-- MAGIC         right_gs_grouped[i] = pd.concat([right_gs_grouped[i], mtgs_row_pd,mtgs_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC right_gs_spaced = pd.concat(right_gs_grouped).reset_index(drop=True)
-- MAGIC
-- MAGIC rgs_new_column_name ={'Grp/Pos': 'R_Grp/Pos',
-- MAGIC                        'Country': 'R_Country',
-- MAGIC                        'MP': 'R_MP',
-- MAGIC                        'W': 'R_W',
-- MAGIC                        'D': 'R-D',
-- MAGIC                        'L': 'R_L',
-- MAGIC                        'GF': 'R-GF',
-- MAGIC                        'GA': 'R_GA',
-- MAGIC                        'GD': 'R_GD',
-- MAGIC                        'Pts': 'R_Pts'
-- MAGIC                        }
-- MAGIC right_gs_spaced = right_gs_spaced.rename(columns=rgs_new_column_name)
-- MAGIC
-- MAGIC display(right_gs_spaced)
-- MAGIC
-- MAGIC
-- MAGIC round_16 = spark.sql("SELECT * FROM QualifiersPlacement WHERE MatchStage = 'R16'")
-- MAGIC round_16_pd = round_16.toPandas()
-- MAGIC
-- MAGIC round_16_pd = round_16_pd.applymap(lambda x: str(x).split(".")[0])
-- MAGIC
-- MAGIC display(round_16_pd)
-- MAGIC
-- MAGIC
-- MAGIC euro_r16_vis = pd.DataFrame(columns=['R16_Teams', 'R16_Score'])
-- MAGIC
-- MAGIC for index, row in round_16_pd.iterrows():
-- MAGIC     team1 = row['EuroTeam1']
-- MAGIC     team1goals = row['Team1Goals']
-- MAGIC     team2 = row['EuroTeam2']
-- MAGIC     team2goals = row['Team2Goals']
-- MAGIC     euro_r16_vis = pd.concat([euro_r16_vis, pd.DataFrame({'R16_Score': [team1goals], 'R16_Teams': [team1]})], ignore_index=True)
-- MAGIC     euro_r16_vis = pd.concat([euro_r16_vis, pd.DataFrame({'R16_Score': [team2goals], 'R16_Teams': [team2]})], ignore_index=True)
-- MAGIC
-- MAGIC display(euro_r16_vis)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC midpoint_index1 = len(euro_r16_vis) // 2
-- MAGIC
-- MAGIC left_r16 = euro_r16_vis.iloc[:midpoint_index1]
-- MAGIC right_r16 = euro_r16_vis.iloc[midpoint_index1:]
-- MAGIC
-- MAGIC left_r16.reset_index(drop=True, inplace=True)
-- MAGIC right_r16.reset_index(drop=True, inplace=True)
-- MAGIC
-- MAGIC display(left_r16)
-- MAGIC display(right_r16)
-- MAGIC
-- MAGIC
-- MAGIC r16_row_pd = pd.DataFrame({col: [''] for col in euro_r16_vis.columns})
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC left_r16_grouped = [left_r16.iloc[i:i+2] for i in range(0, len(left_r16), 2)]
-- MAGIC
-- MAGIC for i in range(len(left_r16_grouped)):
-- MAGIC     if i < len(left_r16_grouped):
-- MAGIC         left_r16_grouped[i] = pd.concat([r16_row_pd,left_r16_grouped[i], r16_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC left_r16_spaced = pd.concat(left_r16_grouped).reset_index(drop=True)
-- MAGIC
-- MAGIC lr16_new_column_name ={'R16_Teams': 'L_R16_Teams',
-- MAGIC                        'R16_Score': 'L_R16_Score'
-- MAGIC                        }
-- MAGIC left_r16_spaced = left_r16_spaced.rename(columns=lr16_new_column_name)
-- MAGIC
-- MAGIC display(left_r16_spaced)
-- MAGIC
-- MAGIC
-- MAGIC right_r16_grouped = [right_r16.iloc[i:i+2] for i in range(0, len(right_r16), 2)]
-- MAGIC
-- MAGIC for i in range(len(right_r16_grouped)):
-- MAGIC     if i < len(right_r16_grouped):
-- MAGIC         right_r16_grouped[i] = pd.concat([r16_row_pd,right_r16_grouped[i], r16_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC right_r16_spaced = pd.concat(right_r16_grouped).reset_index(drop=True)
-- MAGIC
-- MAGIC rr16_new_column_name ={'R16_Teams': 'R_R16_Teams',
-- MAGIC                        'R16_Score': 'R_R16_Score'
-- MAGIC                        }
-- MAGIC right_r16_spaced = right_r16_spaced.rename(columns=rr16_new_column_name)
-- MAGIC
-- MAGIC rr16_new_column_order = ['R_R16_Score', 'R_R16_Teams']
-- MAGIC right_r16_spaced = right_r16_spaced[rr16_new_column_order]
-- MAGIC
-- MAGIC display(right_r16_spaced)
-- MAGIC
-- MAGIC
-- MAGIC qf = spark.sql("SELECT * FROM QualifiersPlacement WHERE MatchStage = 'QF'")
-- MAGIC qf_pd = qf.toPandas()
-- MAGIC qf_pd = qf_pd.applymap(lambda x: str(x).split(".")[0])
-- MAGIC
-- MAGIC display(qf_pd)
-- MAGIC
-- MAGIC
-- MAGIC qf_vis = pd.DataFrame(columns=['QF_Teams', 'QF_Score'])
-- MAGIC
-- MAGIC for index, row in qf_pd.iterrows():
-- MAGIC     team1 = row['EuroTeam1']
-- MAGIC     team1goals = row['Team1Goals']
-- MAGIC     team2 = row['EuroTeam2']
-- MAGIC     team2goals = row['Team2Goals']
-- MAGIC     qf_vis = pd.concat([qf_vis, pd.DataFrame({'QF_Teams': [team1], 'QF_Score': [team1goals]})], ignore_index=True)
-- MAGIC     qf_vis = pd.concat([qf_vis, pd.DataFrame({'QF_Teams': [team2], 'QF_Score': [team2goals]})], ignore_index=True)
-- MAGIC
-- MAGIC display(qf_vis)
-- MAGIC
-- MAGIC
-- MAGIC midpoint_index2 = len(qf_vis) // 2
-- MAGIC
-- MAGIC left_qf = qf_vis.iloc[:midpoint_index2]
-- MAGIC right_qf = qf_vis.iloc[midpoint_index2:]
-- MAGIC
-- MAGIC left_qf.reset_index(drop=True, inplace=True)
-- MAGIC right_qf.reset_index(drop=True, inplace=True)
-- MAGIC
-- MAGIC display(left_qf)
-- MAGIC display(right_qf)
-- MAGIC
-- MAGIC
-- MAGIC qf_row_pd = pd.DataFrame({col: [''] for col in qf_vis.columns})
-- MAGIC
-- MAGIC
-- MAGIC left_qf_grouped = [left_qf.iloc[i:i+2] for i in range(0, len(left_qf), 2)]
-- MAGIC
-- MAGIC for i in range(len(left_qf_grouped)):
-- MAGIC     if i < len(left_qf_grouped):
-- MAGIC         left_qf_grouped[i] = pd.concat([qf_row_pd, qf_row_pd, qf_row_pd,qf_row_pd, left_qf_grouped[i]], ignore_index=True)
-- MAGIC
-- MAGIC left_qf_spaced = pd.concat(left_qf_grouped).reset_index(drop=True)
-- MAGIC
-- MAGIC for _ in range(4):
-- MAGIC     left_qf_spaced = pd.concat([left_qf_spaced, qf_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC lqf_new_column_name ={'QF_Teams': 'L_QF_Teams',
-- MAGIC                        'QF_Score': 'L_QF_Score'
-- MAGIC                        }
-- MAGIC left_qf_spaced = left_qf_spaced.rename(columns=lqf_new_column_name)
-- MAGIC
-- MAGIC display(left_qf_spaced)
-- MAGIC
-- MAGIC
-- MAGIC right_qf_grouped = [right_qf.iloc[i:i+2] for i in range(0, len(left_qf), 2)]
-- MAGIC
-- MAGIC for i in range(len(right_qf_grouped)):
-- MAGIC     if i < len(right_qf_grouped):
-- MAGIC         right_qf_grouped[i] = pd.concat([qf_row_pd, qf_row_pd, qf_row_pd,qf_row_pd, right_qf_grouped[i]], ignore_index=True)
-- MAGIC
-- MAGIC right_qf_spaced = pd.concat(right_qf_grouped).reset_index(drop=True)
-- MAGIC
-- MAGIC for _ in range(4):
-- MAGIC     right_qf_spaced = pd.concat([right_qf_spaced, qf_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC rqf_new_column_name ={'QF_Teams': 'R_QF_Teams',
-- MAGIC                        'QF_Score': 'R_QF_Score'
-- MAGIC                        }
-- MAGIC right_qf_spaced = right_qf_spaced.rename(columns=rqf_new_column_name)
-- MAGIC
-- MAGIC rqf_new_column_order = ['R_QF_Score', 'R_QF_Teams']
-- MAGIC right_qf_spaced = right_qf_spaced[rqf_new_column_order]
-- MAGIC
-- MAGIC display(right_qf_spaced)
-- MAGIC
-- MAGIC
-- MAGIC sf = spark.sql("SELECT * FROM QualifiersPlacement WHERE MatchStage = 'SF'")
-- MAGIC sf_pd = sf.toPandas()
-- MAGIC sf_pd = sf_pd.applymap(lambda x: str(x).split(".")[0])
-- MAGIC
-- MAGIC display(sf_pd)
-- MAGIC
-- MAGIC
-- MAGIC sf_vis = pd.DataFrame(columns=['SF_Teams', 'SF_Score'])
-- MAGIC
-- MAGIC for index, row in sf_pd.iterrows():
-- MAGIC     team1 = row['EuroTeam1']
-- MAGIC     team1goals = row['Team1Goals']
-- MAGIC     team2 = row['EuroTeam2']
-- MAGIC     team2goals = row['Team2Goals']
-- MAGIC     sf_vis = pd.concat([sf_vis, pd.DataFrame({'SF_Teams': [team1], 'SF_Score': [team1goals]})], ignore_index=True)
-- MAGIC     sf_vis = pd.concat([sf_vis, pd.DataFrame({'SF_Teams': [team2], 'SF_Score': [team2goals]})], ignore_index=True)
-- MAGIC
-- MAGIC display(sf_vis)
-- MAGIC
-- MAGIC
-- MAGIC midpoint_index2 = len(sf_vis) // 2
-- MAGIC
-- MAGIC left_sf = sf_vis.iloc[:midpoint_index2]
-- MAGIC right_sf = sf_vis.iloc[midpoint_index2:]
-- MAGIC
-- MAGIC left_sf.reset_index(drop=True, inplace=True)
-- MAGIC right_sf.reset_index(drop=True, inplace=True)
-- MAGIC
-- MAGIC display(left_sf)
-- MAGIC display(right_sf)
-- MAGIC
-- MAGIC
-- MAGIC sf_row_pd = pd.DataFrame({col: [''] for col in sf_vis.columns})
-- MAGIC
-- MAGIC
-- MAGIC left_sf_grouped = [left_sf.iloc[i:i+1] for i in range(0, len(left_sf), 1)]
-- MAGIC
-- MAGIC for i in range(len(left_sf_grouped)):
-- MAGIC     if i < len(left_sf_grouped):
-- MAGIC         left_sf_grouped[i] = pd.concat([left_sf_grouped[i], sf_row_pd, sf_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC left_sf_spaced = pd.concat(left_sf_grouped).reset_index(drop=True)
-- MAGIC
-- MAGIC for _ in range(6):
-- MAGIC     left_sf_spaced = pd.concat([sf_row_pd, left_sf_spaced], ignore_index=True)
-- MAGIC
-- MAGIC for _ in range(4):
-- MAGIC     left_sf_spaced = pd.concat([left_sf_spaced, sf_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC lsf_new_column_name ={'SF_Teams': 'L_SF_Teams',
-- MAGIC                        'SF_Score': 'L_SF_Score'
-- MAGIC                        }
-- MAGIC
-- MAGIC left_sf_spaced = left_sf_spaced.rename(columns=lsf_new_column_name)
-- MAGIC
-- MAGIC display(left_sf_spaced)
-- MAGIC
-- MAGIC
-- MAGIC right_sf_grouped = [right_sf.iloc[i:i+1] for i in range(0, len(right_sf), 1)]
-- MAGIC
-- MAGIC for i in range(len(right_sf_grouped)):
-- MAGIC     if i < len(right_sf_grouped):
-- MAGIC         right_sf_grouped[i] = pd.concat([right_sf_grouped[i], sf_row_pd, sf_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC right_sf_spaced = pd.concat(right_sf_grouped).reset_index(drop=True)
-- MAGIC
-- MAGIC for _ in range(6):
-- MAGIC     right_sf_spaced = pd.concat([sf_row_pd, right_sf_spaced], ignore_index=True)
-- MAGIC
-- MAGIC for _ in range(4):
-- MAGIC     right_sf_spaced = pd.concat([right_sf_spaced, sf_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC rsf_new_column_name ={'SF_Teams': 'R_SF_Teams',
-- MAGIC                        'SF_Score': 'R_SF_Score'
-- MAGIC                        }
-- MAGIC
-- MAGIC right_sf_spaced = right_sf_spaced.rename(columns=rsf_new_column_name)
-- MAGIC
-- MAGIC rsf_new_column_order = ['R_SF_Score', 'R_SF_Teams']
-- MAGIC right_sf_spaced = right_sf_spaced[rsf_new_column_order]
-- MAGIC
-- MAGIC display(right_sf_spaced)
-- MAGIC
-- MAGIC
-- MAGIC fin = spark.sql("SELECT * FROM QualifiersPlacement WHERE MatchStage = 'FIN'")
-- MAGIC fin_pd = fin.toPandas()
-- MAGIC fin_pd = fin_pd.applymap(lambda x: str(x).split(".")[0])
-- MAGIC
-- MAGIC display(fin_pd)
-- MAGIC
-- MAGIC
-- MAGIC fin_vis = pd.DataFrame(columns=['Fin_Teams', 'Fin_Score'])
-- MAGIC
-- MAGIC for index, row in fin_pd.iterrows():
-- MAGIC     team1 = row['EuroTeam1']
-- MAGIC     team1goals = row['Team1Goals']
-- MAGIC     team2 = row['EuroTeam2']
-- MAGIC     team2goals = row['Team2Goals']
-- MAGIC     fin_vis = pd.concat([fin_vis, pd.DataFrame({'Fin_Teams': [team1], 'Fin_Score': [team1goals]})], ignore_index=True)
-- MAGIC     fin_vis = pd.concat([fin_vis, pd.DataFrame({'Fin_Teams': [team2], 'Fin_Score': [team2goals]})], ignore_index=True)
-- MAGIC
-- MAGIC display(fin_vis)
-- MAGIC
-- MAGIC
-- MAGIC fin_row_pd = pd.DataFrame({col: [''] for col in fin_vis.columns})
-- MAGIC
-- MAGIC
-- MAGIC fin1 = fin_vis.iloc[:1]  # First value
-- MAGIC fin2 = fin_vis.iloc[1:]  # Second value
-- MAGIC
-- MAGIC for _ in range(8):
-- MAGIC     fin1 = pd.concat([fin_row_pd, fin1], ignore_index=True)
-- MAGIC     fin2 = pd.concat([fin_row_pd, fin2], ignore_index=True)
-- MAGIC
-- MAGIC for _ in range(7):
-- MAGIC     fin1 = pd.concat([fin1, fin_row_pd], ignore_index=True)
-- MAGIC     fin2 = pd.concat([fin2, fin_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC lfin_new_column_name ={'Fin_Teams': 'L_Fin_Teams',
-- MAGIC                        'Fin_Score': 'L_Fin_Score'
-- MAGIC                        }
-- MAGIC rfin_new_column_name ={'Fin_Teams': 'R_Fin_Teams',
-- MAGIC                        'Fin_Score': 'R_Fin_Score'
-- MAGIC                        }
-- MAGIC
-- MAGIC fin1 = fin1.rename(columns=lfin_new_column_name)
-- MAGIC fin2 = fin2.rename(columns=rfin_new_column_name)
-- MAGIC
-- MAGIC rfin_new_column_order = ['R_Fin_Score', 'R_Fin_Teams']
-- MAGIC fin2 = fin2[rfin_new_column_order]
-- MAGIC
-- MAGIC display(fin1)
-- MAGIC display(fin2)
-- MAGIC
-- MAGIC
-- MAGIC winner = spark.sql("SELECT Winner FROM QualifiersPlacement WHERE MatchStage = 'FIN'")
-- MAGIC winner_pd = winner.toPandas()
-- MAGIC winner_pd = winner_pd.applymap(lambda x: str(x).split(".")[0])
-- MAGIC
-- MAGIC win_row_pd = pd.DataFrame({col: [''] for col in winner_pd.columns})
-- MAGIC
-- MAGIC for _ in range(7):
-- MAGIC     winner_pd = pd.concat([win_row_pd, winner_pd], ignore_index=True)
-- MAGIC
-- MAGIC for _ in range(8):
-- MAGIC     winner_pd = pd.concat([winner_pd, win_row_pd], ignore_index=True)
-- MAGIC
-- MAGIC fin_col = {"Winner":"TournamentWinner"}
-- MAGIC winner_pd = winner_pd.rename(columns = fin_col)
-- MAGIC display(winner_pd)
-- MAGIC
-- MAGIC
-- MAGIC knockout_df = pd.concat([left_gs_spaced,
-- MAGIC                         left_r16_spaced, 
-- MAGIC                         left_qf_spaced, 
-- MAGIC                         left_sf_spaced,
-- MAGIC                         fin1,
-- MAGIC                         winner_pd,
-- MAGIC                         fin2,
-- MAGIC                         right_sf_spaced,
-- MAGIC                         right_qf_spaced,
-- MAGIC                         right_r16_spaced,
-- MAGIC                         right_gs_spaced
-- MAGIC                          ], 
-- MAGIC                         axis=1, 
-- MAGIC                         
-- MAGIC                         )
-- MAGIC
-- MAGIC display(knockout_df)
-- MAGIC
