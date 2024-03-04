# Databricks notebook source
# MAGIC %md
# MAGIC #Euros SQL

# COMMAND ----------

# MAGIC %md
# MAGIC Removing files from dbfs

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/teaminfo", True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/thirdplacementcombinations", True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/qualificationplacement", True)


# COMMAND ----------

# MAGIC %md
# MAGIC Dropping Tables and Views

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TeamInfo;
# MAGIC DROP TABLE IF EXISTS GroupStageMatches;
# MAGIC DROP TABLE IF EXISTS GroupStageResults;
# MAGIC DROP TABLE IF EXISTS ThirdPlacementCombinations;
# MAGIC DROP TABLE IF EXISTS QualificationPlacement;

# COMMAND ----------

# MAGIC %md
# MAGIC Table For Team Names and Groups

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE TeamInfo(
# MAGIC     TeamID INT,
# MAGIC     GroupName CHAR(1),
# MAGIC     TeamName VARCHAR(53)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Values for Above Created Table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TeamInfo(TeamID, GroupName, TeamName)
# MAGIC VALUES
# MAGIC     (1, 'A', 'Germany'),
# MAGIC     (2, 'A', 'Scotland'),
# MAGIC     (3, 'A', 'Hungary'),
# MAGIC     (4, 'A', 'Switzerland'),
# MAGIC     (5, 'B', 'Spain'),
# MAGIC     (6, 'B', 'Croatia'),
# MAGIC     (7, 'B', 'Italy'),
# MAGIC     (8, 'B', 'Albania'),
# MAGIC     (9, 'C', 'Slovenia'),
# MAGIC     (10, 'C', 'Denmark'),
# MAGIC     (11, 'C', 'Serbia'),
# MAGIC     (12, 'C', 'England'),
# MAGIC     (13, 'D','Netherlands'),
# MAGIC     (14, 'D', 'Austria'),
# MAGIC     (15, 'D', 'France'),
# MAGIC     (16, 'D', 'Poland'),
# MAGIC     (17, 'E', 'Belgium'),
# MAGIC     (18, 'E', 'Slovakia'),
# MAGIC     (19, 'E', 'Romania'),
# MAGIC     (20, 'E', 'Ukraine'),
# MAGIC     (21, 'F', 'Turkey'),
# MAGIC     (22, 'F', 'Portugal'),
# MAGIC     (23, 'F', 'Czech Republic'),
# MAGIC     (24, 'F', 'Greece');

# COMMAND ----------

# MAGIC %md
# MAGIC Checking Data Insert

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TeamInfo;

# COMMAND ----------

# MAGIC %md
# MAGIC Query for Generating GroupStage Matches

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE GroupStageMatches AS
# MAGIC SELECT
# MAGIC     Row_Number() over(order by 1) as MatchID,
# MAGIC     t1.GroupName AS Team1Group,
# MAGIC     t1.TeamID AS Team1ID,
# MAGIC     t1.TeamName AS Team1Name,
# MAGIC     t2.GroupName AS Team2Group,
# MAGIC     t2.TeamID AS Team2ID,
# MAGIC     t2.TeamName AS Team2Name,
# MAGIC     ROUND(RAND() * 6) AS Team1Goals,  -- Random goal value for Team1 with maximun goals 6
# MAGIC     ROUND(RAND() * 6) AS Team2Goals,   -- Random goal value for Team2 with maximun goals 6
# MAGIC     CASE
# MAGIC         WHEN Team1Goals > Team2Goals THEN t1.TeamName
# MAGIC         WHEN Team2Goals > Team1Goals THEN t2.TeamName
# MAGIC         ELSE "Draw"
# MAGIC     END AS MatchWinner
# MAGIC FROM
# MAGIC     TeamInfo t1
# MAGIC CROSS JOIN
# MAGIC     TeamInfo t2
# MAGIC WHERE
# MAGIC     t1.GroupName = t2.GroupName
# MAGIC     AND t1.TeamID < t2.TeamID;  -- TO avoid duplicate matches and self-matches

# COMMAND ----------

# MAGIC %md
# MAGIC Cheking those Matches

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MatchID, Team1Name, Team1Goals, Team2Goals, Team2Name, MatchWinner FROM GroupStageMatches;

# COMMAND ----------

# MAGIC %md
# MAGIC Result Table Calculation Accroding to the Above Matches

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE GroupStageResults AS
# MAGIC SELECT
# MAGIC     GroupName,
# MAGIC     TeamName,
# MAGIC     COUNT(*) AS MatchesPlayed,
# MAGIC     SUM(CASE WHEN MatchWinner = TeamName THEN 1 ELSE 0 END) AS MatchesWon,
# MAGIC     SUM(CASE WHEN MatchWinner != TeamName AND MatchWinner != 'Draw' THEN 1 ELSE 0 END) AS MatchesLost,
# MAGIC     SUM(CASE WHEN MatchWinner = 'Draw' THEN 1 ELSE 0 END) AS MatchesDrawn,
# MAGIC     SUM(GoalsFor) AS GF,
# MAGIC     SUM(GoalsAgainst) AS GA,
# MAGIC     SUM(GoalsFor - GoalsAgainst) AS GD,
# MAGIC     SUM(CASE WHEN MatchWinner = TeamName THEN 3 WHEN MatchWinner = 'Draw' THEN 1 ELSE 0 END) AS TotalPoints,
# MAGIC     concat(GroupName, ROW_NUMBER() OVER (PARTITION BY GroupName ORDER BY SUM(CASE WHEN MatchWinner = TeamName THEN 3 WHEN MatchWinner = 'Draw' THEN 1 ELSE 0 END) DESC, SUM(GoalsFor - GoalsAgainst) DESC)) AS Position
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         Team1ID AS TeamID,
# MAGIC         Team1Name AS TeamName,
# MAGIC         Team1Group AS GroupName,
# MAGIC         MatchWinner,
# MAGIC         Team1Goals AS GoalsFor,
# MAGIC         Team2Goals AS GoalsAgainst
# MAGIC     FROM
# MAGIC         GroupStageMatches
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         Team2ID AS TeamID,
# MAGIC         Team2Name AS TeamName,
# MAGIC         Team2Group AS GroupName,
# MAGIC         MatchWinner,
# MAGIC         Team2Goals AS GoalsFor,
# MAGIC         Team1Goals AS GoalsAgainst
# MAGIC     FROM
# MAGIC         GroupStageMatches
# MAGIC ) AS AllTeams
# MAGIC GROUP BY
# MAGIC     TeamID, TeamName, GroupName
# MAGIC ORDER BY
# MAGIC     GroupName, TotalPoints DESC, GD DESC, GF DESC, GA ASC, TeamName;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Checking Group Stage Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GroupStageResults;

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for all 3rd Position Teams

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GroupStageResults WHERE Position LIKE '%3' ORDER BY TotalPoints DESC, GD DESC, GF DESC, GA ASC, TeamName;

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Table for Seeding Possibilities According to 3rd Place Qualification

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE ThirdPlacementCombinations(
# MAGIC   SId INT,
# MAGIC   QualificationSequence VARCHAR(8),
# MAGIC   B1 CHAR(2),
# MAGIC   C1 CHAR(2),
# MAGIC   E1 CHAR(2),
# MAGIC   F1 CHAR(2)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO  ThirdPlacementCombinations (SId, QualificationSequence, B1, C1, E1, F1)
# MAGIC VALUES
# MAGIC     (1, 'A3B3C3D3', 'A3', 'D3', 'B3', 'C3'),
# MAGIC     (2, 'A3B3C3E3', 'A3', 'E3', 'B3', 'C3'),
# MAGIC     (3, 'A3B3C3F3', 'A3', 'F3', 'B3', 'C3'),
# MAGIC     (4, 'A3B3D3E3', 'D3', 'E3', 'A3', 'B3'),
# MAGIC     (5, 'A3B3D3F3', 'D3', 'F3', 'A3', 'B3'),
# MAGIC     (6, 'A3B3E3F3', 'E3', 'F3', 'B3', 'A3'),
# MAGIC     (7, 'A3C3D3E3', 'E3', 'D3', 'C3', 'A3'),
# MAGIC     (8, 'A3C3D3F3', 'F3', 'D3', 'C3', 'A3'),
# MAGIC     (9, 'A3C3E3F3', 'E3', 'F3', 'C3', 'A3'),
# MAGIC     (10, 'A3D3E3F3', 'E3', 'F3', 'D3', 'A3'),
# MAGIC     (11, 'B3C3D3E3', 'E3', 'D3', 'B3', 'C3'),
# MAGIC     (12, 'B3C3D3F3', 'F3', 'D3', 'C3', 'B3'),
# MAGIC     (13, 'B3C3E3F3', 'F3', 'E3', 'C3', 'B3'),
# MAGIC     (14, 'B3D3E3F3', 'F3', 'E3', 'D3', 'B3'),
# MAGIC     (15, 'C3D3E3F3', 'F3', 'E3', 'D3', 'C3');
# MAGIC
# MAGIC SELECT * FROM ThirdPlacementCombinations;

# COMMAND ----------

# MAGIC %md
# MAGIC Table for Rounf of 16 Qualification Placement and Matches According to Above Conditions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE QualificationPlacement (
# MAGIC     MatchID INT,
# MAGIC     Team1 VARCHAR(53),
# MAGIC     Team1Goals INT,
# MAGIC     Team2Goals INT,
# MAGIC     Team2 VARCHAR(53),
# MAGIC     MatchWinner VARCHAR(53),
# MAGIC     MatchStage VARCHAR(3)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Isolating Placement Combination for this Scenario

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH RoundOfSixteenQualified AS
# MAGIC (
# MAGIC     (
# MAGIC         SELECT Position, TeamName
# MAGIC         FROM GroupStageResults
# MAGIC         WHERE Position LIKE '%1' OR Position LIKE '%2'
# MAGIC     )
# MAGIC     UNION ALL
# MAGIC     (
# MAGIC         SELECT Position, TeamName
# MAGIC         FROM GroupStageResults
# MAGIC         WHERE Position LIKE '%3'
# MAGIC         ORDER BY TotalPoints DESC, GD DESC, GF DESC, GA ASC, TeamName
# MAGIC         LIMIT 4
# MAGIC     )
# MAGIC     ORDER BY Position
# MAGIC ),
# MAGIC QualificationCompare AS (
# MAGIC   SELECT c.SId, c.B1, c.C1, c.E1, c.F1, pc.PlacementCombination 
# MAGIC   FROM ThirdPlacementCombinations c
# MAGIC   JOIN (
# MAGIC     SELECT REGEXP_REPLACE(CONCAT_WS('', COLLECT_LIST(SUBSTRING(Position,0))), '\\s', '') AS PlacementCombination
# MAGIC     FROM RoundOfSixteenQualified 
# MAGIC     WHERE Position LIKE '%3'
# MAGIC   ) pc
# MAGIC   ON c.QualificationSequence = pc.PlacementCombination
# MAGIC )
# MAGIC   SELECT * FROM QualificationCompare;

# COMMAND ----------

# MAGIC %md
# MAGIC Calculating and Inserting Qualification Placements, Matches

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH RoundOfSixteenQualified AS
# MAGIC (
# MAGIC     (
# MAGIC         SELECT Position, TeamName
# MAGIC         FROM GroupStageResults
# MAGIC         WHERE Position LIKE '%1' OR Position LIKE '%2'
# MAGIC     )
# MAGIC     UNION ALL
# MAGIC     (
# MAGIC         SELECT Position, TeamName
# MAGIC         FROM GroupStageResults
# MAGIC         WHERE Position LIKE '%3'
# MAGIC         ORDER BY TotalPoints DESC, GD DESC, GF DESC, GA ASC, TeamName
# MAGIC         LIMIT 4
# MAGIC     )
# MAGIC     ORDER BY Position
# MAGIC ),
# MAGIC QualificationCompare AS (
# MAGIC   SELECT c.SId, c.B1, c.C1, c.E1, c.F1, pc.PlacementCombination 
# MAGIC   FROM ThirdPlacementCombinations c
# MAGIC   JOIN (
# MAGIC     SELECT REGEXP_REPLACE(CONCAT_WS('', COLLECT_LIST(SUBSTRING(Position,0))), '\\s', '') AS PlacementCombination
# MAGIC     FROM RoundOfSixteenQualified 
# MAGIC     WHERE Position LIKE '%3'
# MAGIC   ) pc
# MAGIC   ON c.QualificationSequence = pc.PlacementCombination
# MAGIC )
# MAGIC INSERT INTO QualificationPlacement (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY 
# MAGIC       CASE t1.Position
# MAGIC         WHEN 'B1' THEN 1
# MAGIC         WHEN 'A1' THEN 2
# MAGIC         WHEN 'F1' THEN 3
# MAGIC         WHEN 'D2' THEN 4
# MAGIC         WHEN 'E1' THEN 5
# MAGIC         WHEN 'D1' THEN 6
# MAGIC         WHEN 'C1' THEN 7
# MAGIC         WHEN 'A2' THEN 8
# MAGIC       END
# MAGIC     ) AS MatchID,
# MAGIC     t1.TeamName AS Team1,
# MAGIC     ROUND(RAND() * 6) AS Team1Goals,
# MAGIC     ROUND(RAND() * 6) AS Team2Goals,
# MAGIC     t2.TeamName AS Team2,
# MAGIC     CASE
# MAGIC         WHEN Team1Goals > Team2Goals THEN t1.TeamName
# MAGIC         WHEN Team2Goals > Team1Goals THEN t2.TeamName
# MAGIC         ELSE
# MAGIC           CASE
# MAGIC             WHEN Team1 < Team2 THEN Team1
# MAGIC             ELSE Team2
# MAGIC           END
# MAGIC     END AS MatchWinner,
# MAGIC     'R16' AS MatchStage
# MAGIC
# MAGIC FROM RoundOfSixteenQualified t1
# MAGIC JOIN RoundOfSixteenQualified t2
# MAGIC ON (
# MAGIC     (t1.Position = 'B1' AND t2.Position = (SELECT B1 FROM QualificationCompare)) OR
# MAGIC     (t1.Position = 'A1' AND t2.Position = 'C2') OR
# MAGIC     (t1.Position = 'F1' AND t2.Position = (SELECT F1 FROM QualificationCompare)) OR
# MAGIC     (t1.Position = 'D2' AND t2.Position = 'E2') OR
# MAGIC     (t1.Position = 'E1' AND t2.Position = (SELECT E1 FROM QualificationCompare)) OR
# MAGIC     (t1.Position = 'D1' AND t2.Position = 'F2') OR
# MAGIC     (t1.Position = 'C1' AND t2.Position = (SELECT C1 FROM QualificationCompare)) OR
# MAGIC     (t1.Position = 'A2' AND t2.Position = 'B2')
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Checking Round Of 16 Results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM QualificationPlacement;

# COMMAND ----------

# MAGIC %md
# MAGIC Quarter Final

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO QualificationPlacement (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY 1) + 8 AS MatchID,
# MAGIC     t1.MatchWinner AS Team1,
# MAGIC     ROUND(RAND() * 6) AS Team1Goals,
# MAGIC     ROUND(RAND() * 6) AS Team2Goals,
# MAGIC     t2.MatchWinner AS Team2,
# MAGIC     'TBD' AS MatchWinner,
# MAGIC     'QF' AS MatchStage
# MAGIC
# MAGIC FROM QualificationPlacement t1
# MAGIC CROSS JOIN QualificationPlacement t2
# MAGIC ON
# MAGIC   (t1.MatchID = 1 AND t2.MatchId = 2) OR
# MAGIC   (t1.MatchID = 3 AND t2.MatchId = 4) OR
# MAGIC   (t1.MatchID = 5 AND t2.MatchId = 6) OR
# MAGIC   (t1.MatchID = 7 AND t2.MatchId = 8);
# MAGIC
# MAGIC UPDATE QualificationPlacement
# MAGIC SET MatchWinner = 
# MAGIC     CASE
# MAGIC         WHEN Team1Goals > Team2Goals THEN Team1
# MAGIC         WHEN Team1Goals < Team2Goals THEN Team2
# MAGIC         ELSE
# MAGIC             CASE
# MAGIC                 WHEN Team1 < Team2 THEN Team1
# MAGIC                 ELSE Team2
# MAGIC             END
# MAGIC     END
# MAGIC WHERE MatchStage = 'QF';

# COMMAND ----------

# MAGIC %md
# MAGIC Checking Quarter Final Result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM QualificationPlacement WHERE MatchStage = 'QF';

# COMMAND ----------

# MAGIC %md
# MAGIC Semi Final

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO QualificationPlacement (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY 1) + 12 AS MatchID,
# MAGIC     t1.MatchWinner AS Team1,
# MAGIC     ROUND(RAND() * 6) AS Team1Goals,
# MAGIC     ROUND(RAND() * 6) AS Team2Goals,
# MAGIC     t2.MatchWinner AS Team2,
# MAGIC     'TBD' AS MatchWinner,
# MAGIC     'SF' AS MatchStage
# MAGIC
# MAGIC FROM QualificationPlacement t1
# MAGIC CROSS JOIN QualificationPlacement t2
# MAGIC ON
# MAGIC   (t1.MatchID = 9 AND t2.MatchId = 10) OR
# MAGIC   (t1.MatchID = 11 AND t2.MatchId = 12);
# MAGIC
# MAGIC UPDATE QualificationPlacement
# MAGIC SET MatchWinner = 
# MAGIC     CASE
# MAGIC         WHEN Team1Goals > Team2Goals THEN Team1
# MAGIC         WHEN Team1Goals < Team2Goals THEN Team2
# MAGIC         ELSE
# MAGIC             CASE
# MAGIC                 WHEN Team1 < Team2 THEN Team1
# MAGIC                 ELSE Team2
# MAGIC             END
# MAGIC     END
# MAGIC WHERE MatchStage = 'SF';

# COMMAND ----------

# MAGIC %md
# MAGIC Checking Semi Final

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM QualificationPlacement WHERE MatchStage = 'SF';

# COMMAND ----------

# MAGIC %md
# MAGIC Third Place

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO QualificationPlacement (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY 1) + 14 AS MatchID,
# MAGIC     CASE
# MAGIC       WHEN
# MAGIC         t1.MatchWinner!= t1.Team1 THEN t1.Team1 ELSE t1.Team2
# MAGIC         END AS Team1,
# MAGIC     ROUND(RAND() * 6) AS Team1Goals,
# MAGIC     ROUND(RAND() * 6) AS Team2Goals,
# MAGIC     CASE
# MAGIC       WHEN
# MAGIC         t2.MatchWinner!= t2.Team2 THEN t2.Team2 ELSE t2.Team1
# MAGIC         END AS Team2,
# MAGIC     'TBD' AS MatchWinner,
# MAGIC     'TP' AS MatchStage
# MAGIC FROM QualificationPlacement t1
# MAGIC CROSS JOIN QualificationPlacement t2
# MAGIC ON
# MAGIC   (t1.MatchID = 13 AND t2.MatchId = 14);
# MAGIC
# MAGIC UPDATE QualificationPlacement
# MAGIC SET MatchWinner = 
# MAGIC     CASE
# MAGIC         WHEN Team1Goals > Team2Goals THEN Team1
# MAGIC         WHEN Team1Goals < Team2Goals THEN Team2
# MAGIC         ELSE
# MAGIC             CASE
# MAGIC                 WHEN Team1 < Team2 THEN Team1
# MAGIC                 ELSE Team2
# MAGIC             END
# MAGIC     END
# MAGIC WHERE MatchStage = 'TP';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Checking Third Place

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM QualificationPlacement WHERE MatchStage = 'TP';

# COMMAND ----------

# MAGIC %md
# MAGIC Final

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO QualificationPlacement (MatchID, Team1, Team1Goals, Team2Goals, Team2, MatchWinner, MatchStage)
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY 1) + 15 AS MatchID,
# MAGIC     t1.MatchWinner AS Team1,
# MAGIC     ROUND(RAND() * 6) AS Team1Goals,
# MAGIC     ROUND(RAND() * 6) AS Team2Goals,
# MAGIC     t2.MatchWinner AS Team2,
# MAGIC     'TBD' AS MatchWinner,
# MAGIC     'FIN' AS MatchStage
# MAGIC FROM QualificationPlacement t1
# MAGIC CROSS JOIN QualificationPlacement t2
# MAGIC ON
# MAGIC   (t1.MatchID = 13 AND t2.MatchId = 14);
# MAGIC
# MAGIC UPDATE QualificationPlacement
# MAGIC SET MatchWinner = 
# MAGIC     CASE
# MAGIC         WHEN Team1Goals > Team2Goals THEN Team1
# MAGIC         WHEN Team1Goals < Team2Goals THEN Team2
# MAGIC         ELSE
# MAGIC             CASE
# MAGIC                 WHEN Team1 < Team2 THEN Team1
# MAGIC                 ELSE Team2
# MAGIC             END
# MAGIC     END
# MAGIC WHERE MatchStage = 'FIN';
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Final Result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM QualificationPlacement WHERE MatchStage = 'FIN';
