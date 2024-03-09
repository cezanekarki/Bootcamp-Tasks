-- Databricks notebook source
-- MAGIC %md
-- MAGIC EURO 2024 play-off draw
-- MAGIC
-- MAGIC Semi-finals – Path A: Poland vs Estonia, Wales vs Finland
-- MAGIC Semi-finals – Path B: Israel vs ﻿Iceland, Bosnia-Herzegovina vs Ukraine
-- MAGIC Semi-finals – Path C: Georgia vs Luxembourg, Greece vs Kazakhstan
-- MAGIC
-- MAGIC Final – Path A: Wales/Finland vs Poland/Estonia
-- MAGIC Final – Path B: Bosnia-Herzegovina/Ukraine vs Israel/﻿Iceland
-- MAGIC Final – Path C: Georgia/Luxembourg vs Greece/Kazakhstan

-- COMMAND ----------

CREATE OR REPLACE TABLE playoff_winners AS(
-- Simulate playoffdraw within Group A
WITH GroupA AS (
    SELECT 'A' as path,
        CASE WHEN RAND() > 0.5 THEN 'Poland' ELSE 'Estonia' END AS winner_A1,
        CASE WHEN RAND() > 0.5 THEN 'Wales'  ELSE 'Finland' END AS winner_A2
    LIMIT 1
),

--Simulate playoffdraw within Group B
GroupB AS (
    SELECT 'B' as path,
        CASE WHEN RAND() > 0.5 THEN 'Israel' ELSE 'Iceland' END AS winner_B1,
        CASE WHEN RAND() > 0.5 THEN 'Bosnia-Herzegovina' ELSE 'Ukraine' END AS winner_B2
    LIMIT 1
),

-- Simulate playoffdraw within Group C
GroupC AS (
    SELECT 'C' as path,
        CASE WHEN RAND() > 0.5 THEN 'Georgia' ELSE 'Luxembourg' END AS winner_C1,
        CASE WHEN RAND() > 0.5 THEN 'Kazakhstan' ELSE 'Greece' END AS winner_C2
    LIMIT 1
)

-- Output the final winners from each group with their paths
SELECT CASE WHEN RAND() > 0.5 THEN winner_A1 ELSE winner_A2 END AS winner, 'A' AS path FROM GroupA 
UNION ALL
SELECT CASE WHEN RAND() > 0.5 THEN winner_B1 ELSE winner_B2 END AS winner, 'B' AS path FROM GroupB 
UNION ALL
SELECT CASE WHEN RAND() > 0.5 THEN winner_C1 ELSE winner_C2 END AS winner, 'C' AS path FROM GroupC 
);


-- COMMAND ----------

SELECT * FROM playoff_winners ORDER BY path;

-- COMMAND ----------

CREATE OR REPLACE TABLE euro_group24(
  team_name STRING,
  group_name STRING,
  played INT,
  won INT,
  drawn INT,
  lost INT,
  goals_for INT,
  goals_against INT,
  goal_difference INT,
  points INT
);

INSERT INTO euro_group24(team_name,group_name,played,won,drawn,lost,goals_for,goals_against,goal_difference,points)
VALUES
("Germany","A",0,0,0,0,0,0,0,0),
("Scotland","A",0,0,0,0,0,0,0,0),
("Hungary","A",0,0,0,0,0,0,0,0),
("Switzerland","A",0,0,0,0,0,0,0,0),
("Spain","B",0,0,0,0,0,0,0,0),
("Croatia","B",0,0,0,0,0,0,0,0),
("Italy","B",0,0,0,0,0,0,0,0),
("Albania","B",0,0,0,0,0,0,0,0),
("Slovenia","C",0,0,0,0,0,0,0,0),
("Denmark","C",0,0,0,0,0,0,0,0),
("Serbia","C",0,0,0,0,0,0,0,0),
("England","C",0,0,0,0,0,0,0,0),
("Play-off winner A","D",0,0,0,0,0,0,0,0),
("Netherlands","D",0,0,0,0,0,0,0,0),
("Austria","D",0,0,0,0,0,0,0,0),
("France","D",0,0,0,0,0,0,0,0),
("Belgium","E",0,0,0,0,0,0,0,0),
("Slovakia","E",0,0,0,0,0,0,0,0),
("Romania","E",0,0,0,0,0,0,0,0),
("Play-off winner B","E",0,0,0,0,0,0,0,0),
("Turkey","F",0,0,0,0,0,0,0,0),
("Play-off winner C","F",0,0,0,0,0,0,0,0),
("Portugal","F",0,0,0,0,0,0,0,0),
("Czech Republic","F",0,0,0,0,0,0,0,0);

-- COMMAND ----------

SELECT * FROM euro_group24;

-- COMMAND ----------

UPDATE euro_group24
SET team_name = 
    CASE 
        WHEN team_name = 'Play-off winner A' THEN (SELECT winner FROM playoff_winners WHERE path = 'A')
        WHEN team_name = 'Play-off winner B' THEN (SELECT winner FROM playoff_winners WHERE path = 'B')
        WHEN team_name = 'Play-off winner C' THEN (SELECT winner FROM playoff_winners WHERE path = 'C')
        ELSE team_name
    END;


-- COMMAND ----------

SELECT * FROM euro_group24;

-- COMMAND ----------

-- Create a table to represent match_of_24teams
CREATE OR REPLACE TABLE match_of_24teams (
    MatchID INT,
    Team1Name VARCHAR(50),
    Team2Name VARCHAR(50),
    Team1Goals INT,
    Team2Goals INT,
    MatchOutcome VARCHAR(50)
);

-- Generate match_of_24teams between euro_group24 in the same group
INSERT INTO match_of_24teams (MatchID, Team1Name, Team2Name, Team1Goals, Team2Goals, MatchOutcome)
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY t1.team_name, t2.team_name) AS MatchID,
    t1.team_name AS Team1Name,
    t2.team_name AS Team2Name,
    CEIL(RAND() * 5) AS Team1Goals, -- Generating random goals for Team 1
    CEIL(RAND() * 5) AS Team2Goals, -- Generating random goals for Team 2
    CASE
        WHEN  Team1Goals > Team2Goals THEN t1.team_name
        WHEN  Team1Goals < Team2Goals THEN t2.team_name
        ELSE 'Draw'
    END AS MatchOutcome -- Determining match outcome randomly
FROM
    euro_group24 t1
CROSS JOIN
    euro_group24 t2
WHERE
    t1.team_name < t2.team_name
    AND t1.group_name = t2.group_name
    AND t1.team_name != t2.team_name -- Ensure teams don't play against themselves
ORDER BY
    MatchID;

-- Update euro_group24 table
UPDATE euro_group24 AS g
SET 
    played = (
        SELECT COUNT(*) 
        FROM (
            SELECT Team1Name AS team_name FROM match_of_24teams WHERE Team1Name = g.team_name
            UNION ALL
            SELECT Team2Name AS team_name FROM match_of_24teams WHERE Team2Name = g.team_name
        ) AS subquery
    ),
    goals_for = (
        SELECT SUM(goals) 
        FROM (
            SELECT Team1Goals AS goals FROM match_of_24teams WHERE Team1Name = g.team_name
            UNION ALL
            SELECT Team2Goals AS goals FROM match_of_24teams WHERE Team2Name = g.team_name
        ) AS subquery
    ),
    goals_against = (
        SELECT SUM(goals) 
        FROM (
            SELECT Team2Goals AS goals FROM match_of_24teams WHERE Team1Name = g.team_name
            UNION ALL
            SELECT Team1Goals AS goals FROM match_of_24teams WHERE Team2Name = g.team_name
        ) AS subquery
    ),
    won = (
        SELECT COUNT(winner) 
        FROM (
            SELECT MatchOutcome AS winner FROM match_of_24teams WHERE MatchOutcome = g.team_name AND Team1Name = g.team_name
            UNION ALL
            SELECT MatchOutcome AS winner FROM match_of_24teams WHERE MatchOutcome = g.team_name AND Team2Name = g.team_name
        ) AS subquery
    ),
    lost = (
        SELECT COUNT(loser) 
        FROM (
            SELECT MatchOutcome AS loser FROM match_of_24teams WHERE MatchOutcome != g.team_name AND Team1Name = g.team_name AND Matchoutcome != "Draw"
            UNION ALL
            SELECT MatchOutcome AS loser FROM match_of_24teams WHERE MatchOutcome != g.team_name AND Team2Name = g.team_name AND Matchoutcome != "Draw"
        ) AS subquery
    ),
    drawn = (
        SELECT COUNT(drawn) 
        FROM (
            SELECT MatchOutcome AS drawn FROM match_of_24teams WHERE MatchOutcome = "Draw" AND Team1Name = g.team_name 
            UNION ALL
            SELECT MatchOutcome AS drawn FROM match_of_24teams WHERE MatchOutcome = "Draw" AND Team2Name = g.team_name
        ) AS subquery
    ),
    points = (
        (SELECT COUNT(*) 
        FROM match_of_24teams  WHERE (Team1Name = g.team_name OR Team2Name = g.team_name) AND MatchOutcome = g.team_name) * 3
            +
        (SELECT COUNT(*) FROM match_of_24teams WHERE (Team1Name = g.team_name OR Team2Name = g.team_name) AND MatchOutcome = 'Draw')
    ),
    goal_difference = (
        (
            SELECT COALESCE(SUM(goals), 0)
            FROM (
                SELECT Team1Goals AS goals FROM match_of_24teams WHERE Team1Name = g.team_name
                UNION ALL
                SELECT Team2Goals AS goals FROM match_of_24teams WHERE Team2Name = g.team_name
            ) AS goals_for_subquery
        )
        - 
        (
            SELECT COALESCE(SUM(goals), 0)
            FROM (
                SELECT Team2Goals AS goals FROM match_of_24teams WHERE Team1Name = g.team_name
                UNION ALL
                SELECT Team1Goals AS goals FROM match_of_24teams WHERE Team2Name = g.team_name
            ) AS goals_against_subquery
        )
    );





-- COMMAND ----------

-- Display the updated euro_group24 table
SELECT * FROM euro_group24 ORDER BY group_name, points DESC;

-- COMMAND ----------

CREATE OR REPLACE TABLE euro_group16 AS(
WITH RankedTeams AS (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY group_name ORDER BY points DESC, goal_difference ASC) AS Rank
  FROM euro_group24
),
TopTwoTeams AS (
  SELECT * 
  FROM RankedTeams
  WHERE Rank <= 2
),
TopFourByGoalDiff AS (
  SELECT DISTINCT *
  FROM RankedTeams
  WHERE Rank >= 3
  ORDER BY points DESC, goal_difference ASC
  LIMIT 4
)
SELECT * FROM TopTwoTeams
UNION ALL
SELECT * FROM TopFourByGoalDiff
LIMIT 16
);

-- COMMAND ----------

SELECT * FROM euro_group16;

-- COMMAND ----------

CREATE OR REPLACE TABLE third_rank_combination(
  match_pair STRING,
  1B_VS STRING,
  1C_VS STRING,
  1E_VS STRING,
  1F_VS STRING
);

INSERT INTO third_rank_combination( match_pair, 1B_VS, 1C_VS, 1E_VS, 1F_VS)
VALUES
('ABCD','A','D','B','C'),
('ABCE','A','E','B','C'),
('ABCF','A','F','B','C'),
('ABDE','D','E','A','B'),
('ABDF','D','F','A','B'),
('ABEF','E','F','B','A'),
('ACDE','E','D','C','A'),
('ACDF','F','D','C','A'),
('ACEF','E','F','C','A'),
('ADEF','E','F','D','A'),
('BCDE','E','D','B','C'),
('BCDF','F','D','C','B'),
('BCEF','F','E','C','B'),
('BDEF','F','E','D','B'),
('CDEF','F','E','D','C');


SELECT * FROM third_rank_combination;

-- COMMAND ----------

SELECT concat_ws('',array_agg(group_name)) AS merged_groups
FROM (
    SELECT group_name
    FROM euro_group16
    WHERE rank >= 3
    ORDER BY group_name ASC
) AS subquery;


-- COMMAND ----------

CREATE OR REPLACE TABLE final_match_pair AS(
SELECT *
FROM third_rank_combination
WHERE match_pair = (
    SELECT CONCAT_WS('', array_agg(group_name)) AS merged_groups
    FROM (
        SELECT DISTINCT group_name
        FROM euro_group16
        WHERE rank >= 3
        ORDER BY group_name ASC
    ) AS subquery
)
);

-- COMMAND ----------

SELECT * FROM final_match_pair;

-- COMMAND ----------

CREATE OR REPLACE TABLE euro_group8 AS (
WITH TeamPairs AS (
    SELECT g1.team_name AS team_1, g2.team_name AS team_2
    FROM euro_group16 g1
    CROSS JOIN euro_group16 g2
    JOIN final_match_pair fmp ON g2.group_name = fmp.1B_VS
    WHERE g1.group_name = 'B' AND g1.Rank = 1 AND g2.Rank >= 3

    UNION ALL

    SELECT g1.team_name AS team_1, g2.team_name AS team_2
    FROM euro_group16 g1
    CROSS JOIN euro_group16 g2
    WHERE (g1.group_name = 'A' AND g1.Rank = 1) 
    AND (g2.group_name = 'C' AND g2.Rank = 2)

    UNION ALL

    SELECT g1.team_name AS team_1, g2.team_name AS team_2
    FROM euro_group16 g1
    CROSS JOIN euro_group16 g2
    JOIN final_match_pair fmp ON g2.group_name = fmp.1F_VS
    WHERE g1.group_name = 'F' AND g1.Rank = 1 AND g2.Rank >= 3

    UNION ALL

    SELECT g1.team_name AS team_1, g2.team_name AS team_2
    FROM euro_group16 g1
    CROSS JOIN euro_group16 g2
    WHERE (g1.group_name = 'D' AND g1.Rank = 2) 
    AND (g2.group_name = 'E' AND g2.Rank = 2)

    UNION ALL

    SELECT g1.team_name AS team_1, g2.team_name AS team_2
    FROM euro_group16 g1
    CROSS JOIN euro_group16 g2
    JOIN final_match_pair fmp ON g2.group_name = fmp.1E_VS
    WHERE g1.group_name = 'E' AND g1.Rank = 1 AND g2.Rank >= 3

    UNION ALL

    SELECT g1.team_name AS team_1, g2.team_name AS team_2
    FROM euro_group16 g1
    CROSS JOIN euro_group16 g2
    WHERE (g1.group_name = 'D' AND g1.Rank = 1) 
    AND (g2.group_name = 'F' AND g2.Rank = 2)

    UNION ALL

    SELECT g1.team_name AS team_1, g2.team_name AS team_2
    FROM euro_group16 g1
    CROSS JOIN euro_group16 g2
    JOIN final_match_pair fmp ON g2.group_name = fmp.1C_VS
    WHERE g1.group_name = 'C' AND g1.Rank = 1 AND g2.Rank >= 3

    UNION ALL

    SELECT g1.team_name AS team_1, g2.team_name AS team_2
    FROM euro_group16 g1
    CROSS JOIN euro_group16 g2
    WHERE (g1.group_name = 'A' AND g1.Rank = 2) 
    AND (g2.group_name = 'B' AND g2.Rank = 2)

    
),
RandomWinner AS (
    SELECT 
        CONCAT_WS(' VS ', team_1, team_2) AS match,
        CASE WHEN RANDOM() < 0.5 THEN team_1 ELSE team_2 END AS winner
    FROM TeamPairs
)
    SELECT * FROM Randomwinner
);


-- COMMAND ----------

SELECT * FROM euro_group8;

-- COMMAND ----------

CREATE OR REPLACE TABLE euro_group4 AS(
WITH Winners AS (
    SELECT 
        winner AS team_name,
        ROW_NUMBER() OVER (ORDER BY '') AS rank
    FROM 
        euro_group8
),
Matchups AS (
    SELECT 
        w1.team_name AS team1,
        w2.team_name AS team2
    FROM 
        Winners w1
    JOIN Winners w2 ON w1.rank = w2.rank - 1
    WHERE 
        w1.rank % 2 <> 0
)
SELECT 
    CONCAT_WS(' VS ', team1, team2) AS match, 
       CASE 
        WHEN RANDOM() < 0.5 THEN team1
        ELSE team2
    END AS winner
FROM 
    Matchups
);

-- COMMAND ----------

SELECT * FROM euro_group4;

-- COMMAND ----------

CREATE OR REPLACE TABLE euro_group2 AS(
WITH Winners AS (
    SELECT 
        winner AS team_name,
        ROW_NUMBER() OVER (ORDER BY '') AS rank
    FROM 
        euro_group4
),
Matchups AS (
    SELECT 
        w1.team_name AS team1,
        w2.team_name AS team2
    FROM 
        Winners w1
    JOIN Winners w2 ON w1.rank = w2.rank - 1
    WHERE 
        w1.rank % 2 <> 0
)
SELECT 
    CONCAT_WS(' VS ', team1, team2) AS match, 
       CASE 
        WHEN RANDOM() < 0.5 THEN team1
        ELSE team2
    END AS winner
FROM 
    Matchups
)

-- COMMAND ----------

SELECT * FROM euro_group2;

-- COMMAND ----------

WITH Winners AS (
    SELECT 
        winner AS team_name,
        ROW_NUMBER() OVER (ORDER BY '') AS rank
    FROM 
        euro_group2
),
Matchups AS (
    SELECT 
        w1.team_name AS team1,
        w2.team_name AS team2
    FROM 
        Winners w1
    JOIN Winners w2 ON w1.rank = w2.rank - 1
    WHERE 
        w1.rank % 2 <> 0
)
SELECT 
    CONCAT_WS(' VS ', team1, team2) AS match, 
       CASE 
        WHEN RANDOM() < 0.5 THEN team1
        ELSE team2
    END AS winner
FROM 
    Matchups


-- COMMAND ----------


