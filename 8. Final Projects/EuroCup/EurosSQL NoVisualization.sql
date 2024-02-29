-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Euros SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Removing files from dbfs

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/teaminfo", True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/thirdplacementcombinations", True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/qualificationplacement", True)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dropping Tables and Views

-- COMMAND ----------

DROP TABLE IF EXISTS TeamInfo;
DROP TABLE IF EXISTS GroupStageMatches;
DROP TABLE IF EXISTS GroupStageResults;
DROP TABLE IF EXISTS ThirdPlacementCombinations;
DROP TABLE IF EXISTS QualificationPlacement;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Table For Team Names and Groups

-- COMMAND ----------

CREATE OR REPLACE TABLE TeamInfo(
    TeamID INT,
    GroupName CHAR(1),
    TeamName VARCHAR(53)
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Values for Above Created Table

-- COMMAND ----------

INSERT INTO TeamInfo(TeamID, GroupName, TeamName)
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

SELECT * FROM TeamInfo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query for Generating GroupStage Matches

-- COMMAND ----------

CREATE OR REPLACE TABLE GroupStageMatches AS
SELECT
    Row_Number() over(order by 1) as MatchID,
    t1.GroupName AS Team1Group,
    t1.TeamID AS Team1ID,
    t1.TeamName AS Team1Name,
    t2.GroupName AS Team2Group,
    t2.TeamID AS Team2ID,
    t2.TeamName AS Team2Name,
    ROUND(RAND() * 6) AS Team1Goals,  -- Random goal value for Team1 with maximun goals 6
    ROUND(RAND() * 6) AS Team2Goals,   -- Random goal value for Team2 with maximun goals 6
    CASE
        WHEN Team1Goals > Team2Goals THEN t1.TeamName
        WHEN Team2Goals > Team1Goals THEN t2.TeamName
        ELSE "Draw"
    END AS MatchWinner
FROM
    TeamInfo t1
CROSS JOIN
    TeamInfo t2
WHERE
    t1.GroupName = t2.GroupName
    AND t1.TeamID < t2.TeamID;  -- TO avoid duplicate matches and self-matches

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Cheking those Matches

-- COMMAND ----------

SELECT MatchID, Team1Name, Team1Goals, Team2Goals, Team2Name, MatchWinner FROM GroupStageMatches;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Result Table Calculation Accroding to the Above Matches

-- COMMAND ----------

CREATE OR REPLACE TABLE GroupStageResults AS
SELECT
    GroupName,
    TeamName,
    COUNT(*) AS MatchesPlayed,
    SUM(CASE WHEN MatchWinner = TeamName THEN 1 ELSE 0 END) AS MatchesWon,
    SUM(CASE WHEN MatchWinner != TeamName AND MatchWinner != 'Draw' THEN 1 ELSE 0 END) AS MatchesLost,
    SUM(CASE WHEN MatchWinner = 'Draw' THEN 1 ELSE 0 END) AS MatchesDrawn,
    SUM(GoalsFor) AS GF,
    SUM(GoalsAgainst) AS GA,
    SUM(GoalsFor - GoalsAgainst) AS GD,
    SUM(CASE WHEN MatchWinner = TeamName THEN 3 WHEN MatchWinner = 'Draw' THEN 1 ELSE 0 END) AS TotalPoints,
    concat(GroupName, ROW_NUMBER() OVER (PARTITION BY GroupName ORDER BY SUM(CASE WHEN MatchWinner = TeamName THEN 3 WHEN MatchWinner = 'Draw' THEN 1 ELSE 0 END) DESC, SUM(GoalsFor - GoalsAgainst) DESC)) AS Position
FROM (
    SELECT
        Team1ID AS TeamID,
        Team1Name AS TeamName,
        Team1Group AS GroupName,
        MatchWinner,
        Team1Goals AS GoalsFor,
        Team2Goals AS GoalsAgainst
    FROM
        GroupStageMatches

    UNION ALL

    SELECT
        Team2ID AS TeamID,
        Team2Name AS TeamName,
        Team2Group AS GroupName,
        MatchWinner,
        Team2Goals AS GoalsFor,
        Team1Goals AS GoalsAgainst
    FROM
        GroupStageMatches
) AS AllTeams
GROUP BY
    TeamID, TeamName, GroupName
ORDER BY
    GroupName, TotalPoints DESC, GD DESC, GF DESC, GA ASC, TeamName;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking Group Stage Table

-- COMMAND ----------

SELECT * FROM GroupStageResults;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking for all 3rd Position Teams

-- COMMAND ----------

SELECT * FROM GroupStageResults WHERE Position LIKE '%3' ORDER BY TotalPoints DESC, GD DESC, GF DESC, GA ASC, TeamName;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Table for Seeding Possibilities According to 3rd Place Qualification

-- COMMAND ----------

CREATE or replace TABLE ThirdPlacementCombinations(
  SId INT,
  QualificationSequence VARCHAR(8),
  B1 CHAR(2),
  C1 CHAR(2),
  E1 CHAR(2),
  F1 CHAR(2)
);

INSERT INTO  ThirdPlacementCombinations (SId, QualificationSequence, B1, C1, E1, F1)
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

SELECT * FROM ThirdPlacementCombinations;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Table for Rounf of 16 Qualification Placement and Matches According to Above Conditions

-- COMMAND ----------

CREATE OR REPLACE TABLE QualificationPlacement (
    MatchID INT,
    Team1 VARCHAR(53),
    Team1Goals INT,
    Team2Goals INT,
    Team2 VARCHAR(53),
    MatchWinner VARCHAR(53),
    MatchStage VARCHAR(3)
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Isolating Placement Combination for this Scenario

-- COMMAND ----------

WITH RoundOfSixteenQualified AS
(
    (
        SELECT Position, TeamName
        FROM GroupStageResults
        WHERE Position LIKE '%1' OR Position LIKE '%2'
    )
    UNION ALL
    (
        SELECT Position, TeamName
        FROM GroupStageResults
        WHERE Position LIKE '%3'
        ORDER BY TotalPoints DESC, GD DESC, GF DESC, GA ASC, TeamName
        LIMIT 4
    )
    ORDER BY Position
),
QualificationCompare AS (
  SELECT c.SId, c.B1, c.C1, c.E1, c.F1, pc.PlacementCombination 
  FROM ThirdPlacementCombinations c
  JOIN (
    SELECT REGEXP_REPLACE(CONCAT_WS('', COLLECT_LIST(SUBSTRING(Position,0))), '\\s', '') AS PlacementCombination
    FROM RoundOfSixteenQualified 
    WHERE Position LIKE '%3'
  ) pc
  ON c.QualificationSequence = pc.PlacementCombination
)
  SELECT * FROM QualificationCompare;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Calculating and Inserting Qualification Placements, Matches

-- COMMAND ----------

WITH RoundOfSixteenQualified AS
(
    (
        SELECT Position, TeamName
        FROM GroupStageResults
        WHERE Position LIKE '%1' OR Position LIKE '%2'
    )
    UNION ALL
    (
        SELECT Position, TeamName
        FROM GroupStageResults
        WHERE Position LIKE '%3'
        ORDER BY TotalPoints DESC, GD DESC, GF DESC, GA ASC, TeamName
        LIMIT 4
    )
    ORDER BY Position
),
QualificationCompare AS (
  SELECT c.SId, c.B1, c.C1, c.E1, c.F1, pc.PlacementCombination 
  FROM ThirdPlacementCombinations c
  JOIN (
    SELECT REGEXP_REPLACE(CONCAT_WS('', COLLECT_LIST(SUBSTRING(Position,0))), '\\s', '') AS PlacementCombination
    FROM RoundOfSixteenQualified 
    WHERE Position LIKE '%3'
  ) pc
  ON c.QualificationSequence = pc.PlacementCombination
)
INSERT INTO QualificationPlacement (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
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
    t1.TeamName AS Team1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    t2.TeamName AS Team2,
    CASE
        WHEN Team1Goals > Team2Goals THEN t1.TeamName
        WHEN Team2Goals > Team1Goals THEN t2.TeamName
        ELSE
          CASE
            WHEN Team1 < Team2 THEN Team1
            ELSE Team2
          END
    END AS MatchWinner,
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

SELECT * FROM QualificationPlacement;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Quarter Final

-- COMMAND ----------

INSERT INTO QualificationPlacement (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 8 AS MatchID,
    t1.MatchWinner AS Team1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    t2.MatchWinner AS Team2,
    'TBD' AS MatchWinner,
    'QF' AS MatchStage

FROM QualificationPlacement t1
CROSS JOIN QualificationPlacement t2
ON
  (t1.MatchID = 1 AND t2.MatchId = 2) OR
  (t1.MatchID = 3 AND t2.MatchId = 4) OR
  (t1.MatchID = 5 AND t2.MatchId = 6) OR
  (t1.MatchID = 7 AND t2.MatchId = 8);

UPDATE QualificationPlacement
SET MatchWinner = 
    CASE
        WHEN Team1Goals > Team2Goals THEN Team1
        WHEN Team1Goals < Team2Goals THEN Team2
        ELSE
            CASE
                WHEN Team1 < Team2 THEN Team1
                ELSE Team2
            END
    END
WHERE MatchStage = 'QF';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CHecking Quarter Final Result

-- COMMAND ----------

SELECT * FROM QualificationPlacement;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Semi Final

-- COMMAND ----------

INSERT INTO QualificationPlacement (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 12 AS MatchID,
    t1.MatchWinner AS Team1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    t2.MatchWinner AS Team2,
    'TBD' AS MatchWinner,
    'SF' AS MatchStage

FROM QualificationPlacement t1
CROSS JOIN QualificationPlacement t2
ON
  (t1.MatchID = 9 AND t2.MatchId = 10) OR
  (t1.MatchID = 11 AND t2.MatchId = 12);

UPDATE QualificationPlacement
SET MatchWinner = 
    CASE
        WHEN Team1Goals > Team2Goals THEN Team1
        WHEN Team1Goals < Team2Goals THEN Team2
        ELSE
            CASE
                WHEN Team1 < Team2 THEN Team1
                ELSE Team2
            END
    END
WHERE MatchStage = 'SF';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking Semi Final

-- COMMAND ----------

SELECT * FROM QualificationPlacement;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Third Place

-- COMMAND ----------

INSERT INTO QualificationPlacement (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 14 AS MatchID,
    CASE
      WHEN
        t1.MatchWinner!= t1.Team1 THEN t1.Team1 ELSE t1.Team2
        END AS Team1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    CASE
      WHEN
        t2.MatchWinner!= t2.Team2 THEN t2.Team2 ELSE t2.Team1
        END AS Team2,
    'TBD' AS MatchWinner,
    'TP' AS MatchStage
FROM QualificationPlacement t1
CROSS JOIN QualificationPlacement t2
ON
  (t1.MatchID = 13 AND t2.MatchId = 14);

UPDATE QualificationPlacement
SET MatchWinner = 
    CASE
        WHEN Team1Goals > Team2Goals THEN Team1
        WHEN Team1Goals < Team2Goals THEN Team2
        ELSE
            CASE
                WHEN Team1 < Team2 THEN Team1
                ELSE Team2
            END
    END
WHERE MatchStage = 'TP';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking Third Place

-- COMMAND ----------

SELECT * FROM QualificationPlacement;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Final

-- COMMAND ----------

INSERT INTO QualificationPlacement (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 15 AS MatchID,
    t1.MatchWinner AS Team1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    t2.MatchWinner AS Team2,
    'TBD' AS MatchWinner,
    'FIN' AS MatchStage
FROM QualificationPlacement t1
CROSS JOIN QualificationPlacement t2
ON
  (t1.MatchID = 13 AND t2.MatchId = 14);

UPDATE QualificationPlacement
SET MatchWinner = 
    CASE
        WHEN Team1Goals > Team2Goals THEN Team1
        WHEN Team1Goals < Team2Goals THEN Team2
        ELSE
            CASE
                WHEN Team1 < Team2 THEN Team1
                ELSE Team2
            END
    END
WHERE MatchStage = 'FIN';




-- COMMAND ----------

-- MAGIC %md
-- MAGIC Final Result

-- COMMAND ----------

SELECT * FROM QualificationPlacement;
