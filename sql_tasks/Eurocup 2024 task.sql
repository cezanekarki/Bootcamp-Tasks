-- Databricks notebook source
-- Prepare a Euro 2024 Group Stage and Knockout Stage with random goal values with less number of tables as possible

-- COMMAND ----------

-- TeamTable
CREATE TABLE TeamTable1 (
    TeamId INT,
    CountryName STRING
);

-- MatchTable
CREATE TABLE MatchTable1 (
    MatchId INT,
    RoundId INT,
    Team1Id INT,
    Team2Id INT,
    Team1Score INT,
    Team2Score INT,
    WinnerId INT
);

CREATE TABLE GroupTable1 (
    GroupId INT,
    GroupCode STRING,
    TeamIds ARRAY<INT>
);


CREATE TABLE RoundsTable1 (
    RoundId INT,
    RoundName STRING
);

-- COMMAND ----------

-- Inserting 16 countries into TeamTable1
INSERT INTO TeamTable1 (TeamId, CountryName) VALUES
(1, 'Germany'),
(2, 'France'),
(3, 'Spain'),
(4, 'Portugal'),
(5, 'Italy'),
(6, 'Netherlands'),
(7, 'Belgium'),
(8, 'England'),
(9, 'Croatia'),
(10, 'Switzerland'),
(11, 'Poland'),
(12, 'Austria'),
(13, 'Sweden'),
(14, 'Denmark'),
(15, 'Czech Republic'),
(16, 'Slovakia');


-- COMMAND ----------

-- Inserting rounds into RoundsTable
INSERT INTO RoundsTable1 (RoundId, RoundName) VALUES
(1, 'RoundOf16'),
(2, 'QuaterFinals'),
(3, 'SemiFinals'),
(4, 'Finals');


-- COMMAND ----------


-- Inserting teams into GroupTable for each group
INSERT INTO GroupTable1 (GroupId, GroupCode, TeamIds) VALUES
(1, 'GroupA', ARRAY(1, 2, 3, 4)),    -- Group A teams
(2, 'GroupB', ARRAY(5, 6, 7, 8)),    -- Group B teams
(3, 'GroupC', ARRAY(9, 10, 11, 12)), -- Group C teams
(4, 'GroupD', ARRAY(13, 14, 15, 16)); -- Group D teams


-- COMMAND ----------

-- Inserting Round of 16 matches into MatchTable with random scores and winners
INSERT INTO MatchTable1 (MatchId, RoundId, Team1Id, Team2Id, Team1Score, Team2Score, WinnerId)
VALUES
-- Group A matches
(1, 1, 1, 2, 3, 1, 1),   -- Germany vs France (Winner: Germany)
(2, 1, 3, 4, 2, 3, 4),   -- Spain vs Portugal (Winner: Portugal)

-- Group B matches
(3, 1, 5, 6, 1, 3, 6),   -- Italy vs Netherlands (Winner: Netherlands)
(4, 1, 7, 8, 2, 1, 7),   -- Belgium vs England (Winner: Belgium)

-- Group C matches
(5, 1, 9, 10, 0, 2, 10),  -- Croatia vs Switzerland (Winner: Switzerland)
(6, 1, 11, 12, 2, 1, 11), -- Poland vs Austria (Winner: Poland)

-- Group D matches
(7, 1, 13, 14, 2, 0, 13), -- Sweden vs Denmark (Winner: Sweden)
(8, 1, 15, 16, 3, 1, 15); -- Czech Republic vs Slovakia (Winner: Czech Republic)


-- COMMAND ----------

-- Quarterfinals
INSERT INTO MatchTable1 (MatchId, RoundId, Team1Id, Team2Id, Team1Score, Team2Score, WinnerId) VALUES
(9, 2, 1, 4, 2, 1, 1),  -- Germany vs Belgium (Winner: Germany)
(10, 2, 5, 8, 1, 3, 8),  -- Netherlands vs Portugal (Winner: Netherlands)
(11, 2, 9, 10, 3, 5, 10),  -- Switzerland vs Poland (Winner: Poland)
(12, 2, 13, 15, 1, 0, 13);  -- Sweden vs Czech Republic (Winner: Sweden)


-- COMMAND ----------

INSERT INTO MatchTable1 (MatchId, RoundId, Team1Id, Team2Id, Team1Score, Team2Score, WinnerId) VALUES
(13, 3, 1, 8, 2, 1, 1),  -- Germany vs Netherlands (Winner: Germany)
(14, 3, 10, 13, 3, 4, 13);  -- Poland vs Sweden (Winner:Sweden)


-- COMMAND ----------

INSERT INTO MatchTable1 (MatchId, RoundId, Team1Id, Team2Id, Team1Score, Team2Score, WinnerId) VALUES
(15, 4, 1, 13, 2, 6, 13);  -- Germany vs Sweden (Winner: Sweden)

-- COMMAND ----------

select * from matchtable1

-- COMMAND ----------

SELECT
    M.MatchId,
    R.RoundName,
    T1.CountryName AS Team1,
    T2.CountryName AS Team2,
    M.Team1Score,
    M.Team2Score,
    COALESCE(TW.CountryName, 'Draw') AS Winner
FROM
    MatchTable1 M
JOIN RoundsTable1 R ON M.RoundId = R.RoundId
LEFT JOIN TeamTable1 T1 ON M.Team1Id = T1.TeamId
LEFT JOIN TeamTable1 T2 ON M.Team2Id = T2.TeamId
LEFT JOIN TeamTable1 TW ON M.WinnerId = TW.TeamId;


-- COMMAND ----------


