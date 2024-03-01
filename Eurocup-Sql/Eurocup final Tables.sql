-- Databricks notebook source
-- Create Teams table
CREATE OR REPLACE TABLE TeamsEuro (
    TeamID INT,
    TeamName STRING,
    GroupName STRING
);

-- Insert data into Teams table
INSERT INTO TeamsEuro VALUES
    (1, 'France', 'A'),
    (2, 'Germany', 'A'),
    (3, 'Spain', 'A'),
    (4, 'Italy', 'A'),
    (5, 'Portugal', 'B'),
    (6, 'England', 'B'),
    (7, 'Netherlands', 'B'),
    (8, 'Belgium', 'B'),
    (9, 'Croatia', 'C'),
    (10, 'Poland', 'C'),
    (11, 'Switzerland', 'C'),
    (12, 'Turkey', 'C'),
    (13, 'Sweden', 'D'),
    (14, 'Denmark', 'D'),
    (15, 'Russia', 'D'),
    (16, 'Austria', 'D'),
    (17, 'Ukraine', 'E'),
    (18, 'Wales', 'E'),
    (19, 'Scotland', 'E'),
    (20, 'Czech Republic', 'E'),
    (21, 'Slovakia', 'F'),
    (22, 'Hungary', 'F'),
    (23, 'Finland', 'F'),
    (24, 'North Macedonia', 'F');


-- COMMAND ----------

select * from TeamsEuro 

-- COMMAND ----------

-- Create GroupStageMatches table
CREATE OR REPLACE TABLE GroupStageMatche AS
SELECT
    t1.TeamID AS Team1ID,
    t1.TeamName AS Team1Name,
    t1.GroupName AS Team1Group,
    t2.TeamID AS Team2ID,
    t2.TeamName AS Team2Name,
    t2.GroupName AS Team2Group,
    ROUND(RAND() * 5) AS Team1Goals,  -- Random goal value for Team1
    ROUND(RAND() * 5) AS Team2Goals,   -- Random goal value for Team2
    CASE
        WHEN Team1Goals > Team2Goals THEN t1.TeamName
        WHEN Team2Goals > Team1Goals THEN t2.TeamName
        ELSE "Draw"
    END AS Winner
FROM
    TeamsEuro t1
CROSS JOIN
    TeamsEuro t2
WHERE
    t1.GroupName = t2.GroupName
    AND t1.TeamID < t2.TeamID;  -- Avoid duplicate matches and self-matches


-- COMMAND ----------

select * from GroupStageMatche

-- COMMAND ----------

-- Create GroupStageSummary table with Team Rank and Tie-breaking on Goal Difference
CREATE OR REPLACE TABLE GroupStageSummary AS
WITH MatchResults AS (
    SELECT
        Team1ID AS TeamID,
        Team1Name AS TeamName,
        Team1Group AS GroupName,
        Team1Goals,
        Team2ID,
        Team2Name,
        Team2Group,
        Team2Goals,
        Winner
    FROM
        GroupStageMatche

    UNION ALL

    SELECT
        Team2ID AS TeamID,
        Team2Name AS TeamName,
        Team2Group AS GroupName,
        Team2Goals,
        Team1ID,
        Team1Name,
        Team1Group,
        Team1Goals,
        Winner
    FROM
        GroupStageMatche
)
SELECT
    TeamID,
    TeamName,
    GroupName,

    DENSE_RANK() OVER (PARTITION BY GroupName ORDER BY
        SUM(CASE WHEN Winner = TeamName THEN 3 WHEN Winner = 'Draw' THEN 1 ELSE 0 END) DESC,
        SUM(Team1Goals - Team2Goals) DESC,
        SUM(Team1Goals) DESC,
        SUM(Team2Goals) ASC,
        COUNT(*) DESC
    ) AS TeamRank,
    COUNT(*) AS MatchesPlayed,
    SUM(CASE WHEN Winner = TeamName THEN 1 ELSE 0 END) AS MatchesWon,
    SUM(CASE WHEN Winner != TeamName AND Winner != 'Draw' THEN 1 ELSE 0 END) AS MatchesLost,
    SUM(CASE WHEN Winner = 'Draw' THEN 1 ELSE 0 END) AS MatchesDrawn,
    SUM(CASE WHEN Winner = TeamName THEN 3 WHEN Winner = 'Draw' THEN 1 ELSE 0 END) AS TotalPoints,
    SUM(Team1Goals - Team2Goals) AS GoalDifference,
    SUM(Team1Goals) AS GoalScored,
    SUM(Team2Goals) AS GoalConceled
   
FROM MatchResults
GROUP BY TeamID, TeamName, GroupName
ORDER BY GroupName, TeamRank;


-- COMMAND ----------

select * from groupstagesummary

-- COMMAND ----------

-- Create Round of 16 (Ro16) table
CREATE OR REPLACE TABLE RoundOf16 AS
WITH GroupStandings AS (
    SELECT
        TeamID,
        TeamName,
        GroupName,
        TeamRank,
        TotalPoints,
        GoalDifference,
        GoalScored,
        GoalConceled,
        ROW_NUMBER() OVER (PARTITION BY GroupName ORDER BY TeamRank) AS RowInGroup
    FROM
        GroupStageSummary
)
SELECT
    TeamID,
    TeamName,
    TeamRank,   GroupName,
    CONCAT(GroupName, TeamRank) AS GroupRank,
    TotalPoints,
    GoalDifference,
    GoalScored,
    GoalConceled
FROM
    GroupStandings
WHERE
    RowInGroup <= 2
UNION ALL
SELECT
    TeamID,
    TeamName,
    TeamRank,   GroupName,
    CONCAT(GroupName, TeamRank) AS GroupRank,
    TotalPoints,
    GoalDifference,
    GoalScored,
    GoalConceled
FROM (
    SELECT
        gs.TeamID,
        gs.TeamName,
        gs.TeamRank,
        gs.TotalPoints,
        gs.GoalDifference,
        gs.GoalScored,
        gs.GoalConceled,
        ROW_NUMBER() OVER (ORDER BY gs.TotalPoints DESC, gs.GoalDifference DESC, gs.GoalScored DESC, gs.GoalConceled DESC) AS RowOverall,
        gs.GroupName  -- Include GroupName in the subquery
    FROM
        GroupStandings gs
    WHERE
        gs.RowInGroup = 3
) AS ThirdPlaceTeams
WHERE
    RowOverall <= 4
ORDER BY
    TeamID;


-- COMMAND ----------

SELECT * FROM RoundOf16


-- COMMAND ----------

SELECT
    CONCAT_WS('', COLLECT_LIST(SUBSTRING(GroupName, 1) || TeamRank)) AS FinalCombination
FROM
    RoundOf16
WHERE
    TeamRank = 3;

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



-- COMMAND ----------

-- %sql
-- WITH FirstQuery AS (
--     SELECT
--         CONCAT_WS('', COLLECT_LIST(SUBSTRING(GroupName, 1) || TeamRank)) AS DynamicValue
--     FROM
--         RoundOf16
--     WHERE
--         TeamRank = 3
-- )

-- -- Compare with the provided table
-- SELECT *
-- FROM ThirdPlacementCombinations
-- WHERE QualificationSequence = (SELECT DynamicValue FROM FirstQuery);

-- COMMAND ----------


With QualificationCompare AS (
  SELECT c.SId, c.B1, c.C1, c.E1, c.F1, pc.PlacementCombination 
  FROM ThirdPlacementCombinations c
  JOIN (
    SELECT
        CONCAT_WS('', COLLECT_LIST(SUBSTRING(GroupName, 1) || TeamRank)) AS PlacementCombination
    FROM
        RoundOf16
    WHERE
        TeamRank = 3
  ) pc
  ON c.QualificationSequence = pc.PlacementCombination
)
  SELECT * FROM QualificationCompare;

-- COMMAND ----------

CREATE OR REPLACE TABLE EliminationMatches (
    MatchID INT,
    Team1 VARCHAR(53),
    Team1Goals INT,
    Team2Goals INT,
    Team2 VARCHAR(53),
    MatchWinner VARCHAR(53),
    MatchStage VARCHAR(20)
);

-- COMMAND ----------


With QualificationCompare AS (
  SELECT c.SId, c.B1, c.C1, c.E1, c.F1, pc.PlacementCombination 
  FROM ThirdPlacementCombinations c
  JOIN (
    SELECT
        CONCAT_WS('', COLLECT_LIST(SUBSTRING(GroupName, 1) || TeamRank)) AS PlacementCombination
    FROM
        RoundOf16
    WHERE
        TeamRank = 3
  ) pc
  ON c.QualificationSequence = pc.PlacementCombination
)
  
INSERT INTO EliminationMatches (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 
      CASE 
        WHEN t1.TeamRank = 1 AND t1.GroupName = 'B' THEN 1
        WHEN t1.TeamRank = 1 AND t1.GroupName = 'A' THEN 2
        WHEN t1.TeamRank = 1 AND t1.GroupName = 'F' THEN 3
        WHEN t1.TeamRank = 2 AND t1.GroupName = 'D' THEN 4
        WHEN t1.TeamRank = 1 AND t1.GroupName = 'E' THEN 5
        WHEN t1.TeamRank = 1 AND t1.GroupName = 'D' THEN 6
        WHEN t1.TeamRank = 1 AND t1.GroupName = 'C' THEN 7
        WHEN t1.TeamRank = 2 AND t1.GroupName = 'A' THEN 8
 
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

FROM RoundOf16 t1
JOIN RoundOf16 t2
ON (
     (t1.GroupRank = 'B1' AND t2.GroupRank = (SELECT B1 FROM QualificationCompare)) OR
    (t1.GroupRank = 'A1' AND t2.GroupRank = 'C2') OR
    (t1.GroupRank = 'F1' AND t2.GroupRank = (SELECT F1 FROM QualificationCompare)) OR
    (t1.GroupRank = 'D2' AND t2.GroupRank = 'E2') OR
    (t1.GroupRank = 'E1' AND t2.GroupRank = (SELECT E1 FROM QualificationCompare)) OR
    (t1.GroupRank = 'D1' AND t2.GroupRank = 'F2') OR
    (t1.GroupRank = 'C1' AND t2.GroupRank = (SELECT C1 FROM QualificationCompare)) OR
    (t1.GroupRank = 'A2' AND t2.GroupRank = 'B2')
);


-- COMMAND ----------

select * from eliminationmatches

-- COMMAND ----------



INSERT INTO EliminationMatches (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 8 AS MatchID,
    t1.MatchWinner AS Team1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    t2.MatchWinner AS Team2,
    'TBD' AS MatchWinner,
    'QF' AS MatchStage

FROM EliminationMatches t1
CROSS JOIN EliminationMatches t2
ON
  (t1.MatchID = 1 AND t2.MatchId = 2) OR
  (t1.MatchID = 3 AND t2.MatchId = 4) OR
  (t1.MatchID = 5 AND t2.MatchId = 6) OR
  (t1.MatchID = 7 AND t2.MatchId = 8);

UPDATE EliminationMatches
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

select * from eliminationmatches

-- COMMAND ----------

INSERT INTO EliminationMatches (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 12 AS MatchID,
    t1.MatchWinner AS Team1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    t2.MatchWinner AS Team2,
    'TBD' AS MatchWinner,
    'SF' AS MatchStage

FROM EliminationMatches t1
CROSS JOIN EliminationMatches t2
ON
  (t1.MatchID = 9 AND t2.MatchId = 10) OR
  (t1.MatchID = 11 AND t2.MatchId = 12);

UPDATE EliminationMatches
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

select * from EliminationMatches

-- COMMAND ----------

INSERT INTO EliminationMatches (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 14 AS MatchID,
    t1.MatchWinner AS Team1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    t2.MatchWinner AS Team2,
    'TBD' AS MatchWinner,
    'Final' AS MatchStage

FROM EliminationMatches t1
CROSS JOIN EliminationMatches t2
ON
  (t1.MatchID = 13 AND t2.MatchId = 14);

UPDATE EliminationMatches
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
WHERE MatchStage = 'Final';


-- COMMAND ----------

select * from EliminationMatches

-- COMMAND ----------



-- COMMAND ----------

select * from EliminationMatches

-- COMMAND ----------

-- Inserting Third Place Playoff
INSERT INTO EliminationMatches (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
SELECT
    ROW_NUMBER() OVER (ORDER BY 1) + 16 AS MatchID,
    CASE
        WHEN t1.Team1Goals < t1.Team2Goals THEN t1.Team1
        ELSE t1.Team2
    END AS Team1,
    ROUND(RAND() * 6) AS Team1Goals,
    ROUND(RAND() * 6) AS Team2Goals,
    CASE
        WHEN t2.Team1Goals < t2.Team2Goals THEN t2.Team1
        ELSE t2.Team2
    END AS Team2,
    'TBD' AS MatchWinner,
    '3rd Place' AS MatchStage
FROM EliminationMatches t1
JOIN EliminationMatches t2 ON (t1.MatchID = 13 AND t2.MatchId = 14);

-- Updating Third Place Playoff Winner
UPDATE EliminationMatches
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
WHERE MatchStage = '3rd Place';


-- COMMAND ----------

select * from EliminationMatches

-- COMMAND ----------


