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
