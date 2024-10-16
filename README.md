# IPL Data Analysis (2008-2017) 🏏📊

Welcome to the IPL Data Analysis project! This repository explores IPL match data from 2008 through 2017, providing a deep dive into one of the world's most popular cricket leagues. The project leverages various data analysis techniques to uncover insights and visualize trends across seasons.

## 📂 Data Overview
This project uses a dataset with detailed information about IPL matches, players, and teams from 2008 to 2017, all in CSV format. Here's a quick overview of the data tables:

### 📝 Data Dictionary

#### `Ball_By_Ball.csv`
It contains ball-by-ball details for each match, including batting and bowling teams, runs scored, extras, and dismissals.

| Column Name               | Type      | Description                                        |
|---------------------------|-----------|----------------------------------------------------|
| `match_id`                  | integer   | Unique ID for an IPL Match                         |
| `over_id`                   | integer   | Unique identifier for an over in an innings        |
| `ball_id`                   | integer   | Unique identifier for a ball in an over            |
| `innings_no`                | integer   | Innings number                                     |
| `team_batting`              | string    | Name of the team batting                           |
| `team_bowling`              | string    | Name of the team bowling                           |
| `striker_batting_position`  | integer   | Batting position of the striker                    |
| `extra_type`                | string    | Type of extra run (e.g., wide, no-ball)            |
| `runs_scored`               | integer   | Runs scored on this ball                           |
| `extra_runs`                | integer   | Extra runs conceded on this ball                   |
| ...                         | ...       | ...                                                |

#### `Match.csv`
Details about each match, including teams, venue, date, and winner information.

| Column Name | Type    | Description                |
|-------------|---------|----------------------------|
| `match_sk`    | integer | Primary key for the match  |
| `match_id`    | integer | Unique match ID           |
| `team1`       | string  | Team 1 name               |
| `team2`       | string  | Team 2 name               |
| `match_date`  | date    | Date of the match         |
| `season_year` | year    | IPL season year           |
| ...           | ...     | ...                        |

#### `Player.csv`
Information about players, including their name, date of birth, and country.

| Column Name | Type    | Description               |
|-------------|---------|---------------------------|
| `player_sk`   | integer | Primary key for the player|
| `player_id`   | integer | Unique player ID          |
| `player_name` | string  | Player's full name        |
| `dob`         | date    | Date of birth             |
| ...           | ...     | ...                       |

#### `Player_match.csv`
Player-level data for each match, covering roles, teams, and performance details.

| Column Name        | Type    | Description                        |
|--------------------|---------|------------------------------------|
| `player_match_sk`    | integer | Primary key for player-match       |
| `match_id`           | integer | Unique match ID                    |
| `player_id`          | integer | Unique player ID                   |
| `role_desc`          | string  | Role (e.g., batsman, bowler)       |
| `season_year`        | year    | IPL season year                    |
| ...                  | ...     | ...                                |

#### `Team.csv`
Details about the teams participating in the IPL seasons.

| Column Name | Type    | Description      |
|-------------|---------|------------------|
| `team_sk`     | integer | Primary key for the team |
| `team_id`     | integer | Unique team ID   |
| `team_name`   | string  | Team name        |

## 🎯 Project Goals
We aim to answer questions such as:
- Which players have the best batting averages over the years?
- How do teams perform when batting first vs. chasing?
- Who are the top bowlers based on wickets taken and economy rate?
- What are the most common dismissal types, and which bowlers excel at each?
- How does a team’s toss decision (bat/field) impact match outcomes?
- Which teams are consistently the best performers across seasons?
