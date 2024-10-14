# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType
from pyspark.sql import Window, functions as F


# COMMAND ----------

from pyspark.sql import SparkSession

#create session
spark = SparkSession.builder.appName("IPL Data Analysis").getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

ball_by_ball_df_raw = spark.read.format("csv").option("header", "true").load("s3://ipl-data-analysis-project/Ball_By_Ball.csv")
ball_by_ball_df_raw.printSchema()

# COMMAND ----------

ball_by_ball_df = ball_by_ball_df_raw \
    .withColumn("match_id", F.col("MatcH_id").cast(IntegerType())) \
    .withColumn("over_id", F.col("Over_id").cast(IntegerType())) \
    .withColumn("ball_id", F.col("Ball_id").cast(IntegerType())) \
    .withColumn("innings_no", F.col("Innings_No").cast(IntegerType())) \
    .withColumn("team_batting", F.col("Team_Batting").cast(StringType())) \
    .withColumn("team_bowling", F.col("Team_Bowling").cast(StringType())) \
    .withColumn("striker_batting_position", F.col("Striker_Batting_Position").cast(IntegerType())) \
    .withColumn("extra_type", F.col("Extra_Type").cast(StringType())) \
    .withColumn("runs_scored", F.col("Runs_Scored").cast(IntegerType())) \
    .withColumn("extra_runs", F.col("Extra_runs").cast(IntegerType())) \
    .withColumn("wides", F.col("Wides").cast(IntegerType())) \
    .withColumn("legbyes", F.col("Legbyes").cast(IntegerType())) \
    .withColumn("byes", F.col("Byes").cast(IntegerType())) \
    .withColumn("noballs", F.col("Noballs").cast(IntegerType())) \
    .withColumn("penalty", F.col("Penalty").cast(IntegerType())) \
    .withColumn("bowler_extras", F.col("Bowler_Extras").cast(IntegerType())) \
    .withColumn("out_type", F.col("Out_type").cast(StringType())) \
    .withColumn("caught", F.col("Caught").cast(BooleanType())) \
    .withColumn("bowled", F.col("Bowled").cast(BooleanType())) \
    .withColumn("run_out", F.col("Run_out").cast(BooleanType())) \
    .withColumn("lbw", F.col("LBW").cast(BooleanType())) \
    .withColumn("retired_hurt", F.col("Retired_hurt").cast(BooleanType())) \
    .withColumn("stumped", F.col("Stumped").cast(BooleanType())) \
    .withColumn("caught_and_bowled", F.col("caught_and_bowled").cast(BooleanType())) \
    .withColumn("hit_wicket", F.col("hit_wicket").cast(BooleanType())) \
    .withColumn("obstructingfeild", F.col("ObstructingFeild").cast(BooleanType())) \
    .withColumn("bowler_wicket", F.col("Bowler_Wicket").cast(BooleanType())) \
    .withColumn("match_date", F.to_date(F.col("match_date"), "M/d/yyyy")) \
    .withColumn("season", F.col("Season").cast(IntegerType())) \
    .withColumn("striker", F.col("Striker").cast(IntegerType())) \
    .withColumn("non_striker", F.col("Non_Striker").cast(IntegerType())) \
    .withColumn("bowler", F.col("Bowler").cast(IntegerType())) \
    .withColumn("player_out", F.col("Player_Out").cast(IntegerType())) \
    .withColumn("fielders", F.col("Fielders").cast(IntegerType())) \
    .withColumn("striker_match_sk", F.col("Striker_match_SK").cast(IntegerType())) \
    .withColumn("strikersk", F.col("StrikerSK").cast(IntegerType())) \
    .withColumn("nonstriker_match_sk", F.col("NonStriker_match_SK").cast(IntegerType())) \
    .withColumn("nonstriker_sk", F.col("NONStriker_SK").cast(IntegerType())) \
    .withColumn("fielder_match_sk", F.col("Fielder_match_SK").cast(IntegerType())) \
    .withColumn("fielder_sk", F.col("Fielder_SK").cast(IntegerType())) \
    .withColumn("bowler_match_sk", F.col("Bowler_match_SK").cast(IntegerType())) \
    .withColumn("bowler_sk", F.col("BOWLER_SK").cast(IntegerType())) \
    .withColumn("playerout_match_sk", F.col("PlayerOut_match_SK").cast(IntegerType())) \
    .withColumn("battingteam_sk", F.col("BattingTeam_SK").cast(IntegerType())) \
    .withColumn("bowlingteam_sk", F.col("BowlingTeam_SK").cast(IntegerType())) \
    .withColumn("keeper_catch", F.col("Keeper_Catch").cast(BooleanType())) \
    .withColumn("player_out_sk", F.col("Player_out_sk").cast(IntegerType())) \
    .withColumn("matchdatesk", F.to_date(F.col("MatchDateSK"), "yyyyMMdd"))

# COMMAND ----------

ball_by_ball_df.show(1)

# COMMAND ----------

match_df_raw = spark.read.format("csv").option("header","true").load("s3://ipl-data-analysis-project/Match.csv")
match_df_raw.printSchema()

# COMMAND ----------

match_df = match_df_raw \
    .withColumn("match_sk", F.col("Match_SK").cast(IntegerType())) \
    .withColumn("match_id", F.col("match_id").cast(IntegerType())) \
    .withColumn("match_date", F.to_date("match_date","M/d/yyyy")) \
    .withColumn("season_year", F.col("season_year").cast(IntegerType())) \
    .withColumn("win_margin", F.col("win_margin").cast(IntegerType())) \
    .withColumn("country_id", F.col("country_id").cast(IntegerType()))

# COMMAND ----------

match_df.show(1)

# COMMAND ----------

player_df_raw = spark.read.format("csv").option("header","true").option("dateFormat","M/d/yyyy").load("s3://ipl-data-analysis-project/Player.csv")
player_df_raw.printSchema()

# COMMAND ----------

player_df = player_df_raw \
    .withColumn("player_sk", F.col("player_sk").cast(IntegerType())) \
    .withColumn("player_id", F.col("player_id").cast(IntegerType())) \
    .withColumn("dob", F.to_date("dob", "M/d/yyyy"))

# COMMAND ----------

player_df.show(1)

# COMMAND ----------

player_match_df_raw = spark.read.format("csv").option("header","true").load("s3://ipl-data-analysis-project/Player_match.csv")
player_match_df_raw.printSchema()

# COMMAND ----------

player_match_df = player_match_df_raw \
    .withColumn("player_match_sk", F.col("player_match_sk").cast(IntegerType())) \
    .withColumn("playermatch_key", F.col("playermatch_key").cast(DecimalType())) \
    .withColumn("match_id", F.col("match_id").cast(IntegerType())) \
    .withColumn("player_id", F.col("player_id").cast(IntegerType())) \
    .withColumn("dob", F.to_date("dob","M/d/yyyy")) \
    .withColumn("season_year", F.col("season_year").cast(IntegerType())) \
    .withColumn("age_as_on_match", F.col("age_as_on_match").cast(IntegerType())) \
    .withColumn("is_manofthematch", F.col("is_manofthematch").cast(BooleanType())) \
    .withColumn("isplayers_team_won", F.col("isplayers_team_won").cast(BooleanType())) 


# COMMAND ----------

player_match_df.show(5)

# COMMAND ----------

team_df_raw = spark.read.format("csv").option("header","true").load("s3://ipl-data-analysis-project/Team.csv")
team_df_raw.printSchema()

# COMMAND ----------

team_df = team_df_raw \
    .withColumn("team_sk", F.col("team_sk").cast(IntegerType())) \
    .withColumn("team_id", F.col("team_id").cast(IntegerType()))

# COMMAND ----------

team_df.show(5)

# COMMAND ----------

# No of bowler extras by each team per season

joined_df = ball_by_ball_df.join(team_df, ball_by_ball_df["team_bowling"] == team_df["team_id"], "inner")
grouped_df = joined_df.groupBy("team_name", "season").sum("bowler_extras").withColumnRenamed("sum(bowler_extras)", "total_bowler_extras")
sorted_df = grouped_df.orderBy(grouped_df.season.asc(), grouped_df.total_bowler_extras.desc())
output_path = "dbfs:/FileStore/tables/bowler_extras_by_team_per_season"
sorted_df.write.csv(output_path, header=True, mode="overwrite")



# COMMAND ----------

# No of extra runs by each team per season

joined_df = ball_by_ball_df.join(team_df, ball_by_ball_df["team_batting"] == team_df["team_id"], "inner")
grouped_df = joined_df.groupBy("team_name", "season").sum("extra_runs").withColumnRenamed("sum(extra_runs)", "total_extra_runs")
sorted_df = grouped_df.orderBy(grouped_df.season.asc(), grouped_df.total_extra_runs.desc())
output_path = "dbfs:/FileStore/tables/extras_by_team_per_season"
sorted_df.write.csv(output_path, header=True, mode="overwrite")


# COMMAND ----------

# No of extras by top 5 bowlers each season

grouped_df = ball_by_ball_df.groupBy("bowler", "season").sum("bowler_extras").withColumnRenamed("sum(bowler_extras)", "total_bowler_extras")
joined_df = grouped_df.join(player_df, grouped_df["bowler"] == player_df["player_id"], "inner")
select_df = joined_df.select("player_name", "country_name", "season", "total_bowler_extras")
output_path = "dbfs:/FileStore/tables/top_bowlers_with_most_extras"
select_df.write.csv(output_path, header=True, mode="overwrite")

# COMMAND ----------

# No of sixes by top 5 batters each season

filter_sixes_df = ball_by_ball_df.filter(F.col("runs_scored") == 6)
grouped_df = filter_sixes_df.groupBy("striker", "season").count().withColumnRenamed("count", "total_sixes")
joined_df = grouped_df.join(player_df, grouped_df["striker"] == player_df["player_id"] , "inner")
select_df = joined_df.select("player_name", "country_name", "season", "total_sixes")
output_path = "dbfs:/FileStore/tables/top_batters_with_most_runs"
select_df.write.csv(output_path, header=True, mode="overwrite")


# COMMAND ----------

# Determine which players have the best batting averages over the years.

# Step 1: Calculate Total Outs by Each Player in Each Season
# Filter for deliveries that resulted in an out, group by player and season, and count the total outs.
player_outs_per_season_df = ball_by_ball_df.select("striker", "season") \
    .where(F.col("bowler_wicket") == 'true') \
    .groupBy("striker", "season") \
    .agg(F.count("striker").alias("total_outs"))

# Step 2: Calculate Total Runs Scored by Each Player in Each Season
# Group by player and season, and sum the runs scored.
player_runs_per_season_df = ball_by_ball_df.select("striker", "season", "runs_scored") \
    .groupBy("striker", "season") \
    .agg(F.sum("runs_scored").alias("total_runs"))

# Step 3: Join Runs and Outs Data for Each Player per Season
# Join the runs and outs data to have both total runs and outs for each player-season.
seasonal_runs_outs_df = player_runs_per_season_df \
    .join(player_outs_per_season_df, ["striker", "season"], "inner") \
    .select("striker", "season", "total_runs", "total_outs")

# Step 4: Define a Window Specification for Calculating Cumulative Sums per Player per Season
# Partition by player to calculate cumulative statistics within each player group, ordered by season.
player_season_window_spec = Window.partitionBy("striker").orderBy("season")

# Step 5: Calculate Cumulative Runs, Outs, and Batting Average
# Calculate cumulative totals and batting average for each player over the seasons.
cumulative_runs_outs_df = seasonal_runs_outs_df \
    .withColumn("cumulative_runs", F.sum("total_runs").over(player_season_window_spec)) \
    .withColumn("cumulative_outs", F.sum("total_outs").over(player_season_window_spec)) \
    .withColumn("cumulative_avg", F.round(F.col("cumulative_runs") / F.col("cumulative_outs"), 2))

# Step 6: Join with Player Data to Include Detailed Player Information
# Join the cumulative data with player information, including player name and country.
player_cumulative_avg_df = cumulative_runs_outs_df \
    .join(player_df, player_df["player_id"] == cumulative_runs_outs_df["striker"], "inner") \
    .select("player_name", "country_name", "season", "cumulative_avg")

output_path = "dbfs:/FileStore/tables/top_batters_with_best_cumulative_avg"
player_cumulative_avg_df.write.csv(output_path, header=True, mode="overwrite")

# COMMAND ----------

# Analyze team performance when batting first versus chasing

# Step 1: Select Relevant Columns for Analysis
# Create a DataFrame with essential columns for determining match outcomes based on toss and innings choices.
winning_teams_df = match_df \
    .select("match_id", "season_year", "toss_winner", "match_winner", "toss_name")

# Step 2: Filter and Aggregate Data for Teams Winning When Chasing
# Filter for matches where the winning team chased. If the toss winner chose to bat but did not win, or if the toss winner
# chose to field and won, then the team was chasing. Group by season and match winner to count wins while chasing.
chasing_first_winning_teams_df = winning_teams_df \
    .filter(
        ((F.col("toss_winner") != F.col("match_winner")) & (F.col("toss_name") == 'bat')) |  # Toss winner batted, but lost
        ((F.col("toss_winner") == F.col("match_winner")) & (F.col("toss_name") == 'field'))  # Toss winner fielded and won
    ) \
    .groupBy("season_year", "match_winner") \
    .agg(F.count("*").alias("chasing_win_count")) \
    .orderBy(F.col("season_year").asc(), F.col("chasing_win_count").desc()) 

output_path = "dbfs:/FileStore/tables/winning_teams_by_chasing"
chasing_first_winning_teams_df.write.csv(output_path, header=True, mode="overwrite")

# Step 3: Filter and Aggregate Data for Teams Winning When Batting First
# Filter for matches where the winning team batted first. If the toss winner chose to field but did not win, or if the toss 
# winner chose to bat and won, then the team batted first. Group by season and match winner to count wins while batting first.
batting_first_winning_teams_df = winning_teams_df \
    .filter(
        ((F.col("toss_winner") != F.col("match_winner")) & (F.col("toss_name") == 'field')) |  # Toss winner fielded, but lost
        ((F.col("toss_winner") == F.col("match_winner")) & (F.col("toss_name") == 'bat'))      # Toss winner batted and won
    ) \
    .groupBy("season_year", "match_winner") \
    .agg(F.count("*").alias("batting_win_count")) \
    .orderBy(F.col("season_year").asc(), F.col("batting_win_count").desc()) 

output_path = "dbfs:/FileStore/tables/winning_teams_by_batting_first"
batting_first_winning_teams_df.write.csv(output_path, header=True, mode="overwrite")

# COMMAND ----------

# Analyze top bowlers based on the number of wickets taken each season

# Filter for deliveries resulting in a wicket, group by season, team, and bowler, then count total wickets.
top_bowlers_by_wickets_df = ball_by_ball_df \
    .filter(F.col("bowler_wicket") == 'true') \
    .groupBy("season", "team_bowling", "bowler") \
    .agg(F.count("*").alias("wicket_taken_season_wise")) \
    .join(player_df, ball_by_ball_df["bowler"] == player_df["player_id"], "inner") \
    .join(team_df, ball_by_ball_df["team_bowling"] == team_df["team_id"], "inner") \
    .select("season", "player_name", "team_name", "country_name", "wicket_taken_season_wise") 

output_path = "dbfs:/FileStore/tables/top_bowlers_by_wickets"
top_bowlers_by_wickets_df.write.csv(output_path, header=True, mode="overwrite")

# COMMAND ----------

# Analyze top bowlers based on their economy rate

# Step 1: Calculate Runs per Ball for Each Bowler
# Select relevant columns and calculate runs scored per ball, including extras.
bowlers_economy_df = ball_by_ball_df \
    .select("season", "bowler", "team_bowling", "runs_scored", "bowler_extras") \
    .withColumn('runs_per_ball', F.col("runs_scored") + F.col("bowler_extras"))

# Step 2: Calculate Overall Economy Rate for Each Bowler
# Group by team and bowler, sum the runs per ball and count the number of balls bowled.
top_bowlers_economy_all_time_df = bowlers_economy_df \
    .groupBy("team_bowling" ,"bowler") \
    .agg(F.sum("runs_per_ball").alias("sum_runs_per_ball"), F.count("*").alias("count_balls")) \
    .filter(F.col("count_balls") >= 20) \
    .withColumn('overs_bowled', F.round(( F.col("count_balls") / 6 ), 2)) \
    .withColumn('bowler_economy', F.round(( F.col("sum_runs_per_ball") / F.col("overs_bowled") ), 2)) \
    .join(player_df, ball_by_ball_df["bowler"] == player_df["player_id"], "inner") \
    .join(team_df, ball_by_ball_df["team_bowling"] == team_df["team_id"], "inner") \
    .select("team_name", "player_name", "country_name", "bowler_economy") \
    .orderBy(F.col("bowler_economy").asc())

output_path = "dbfs:/FileStore/tables/top_bowlers_economy_all_time"
top_bowlers_economy_all_time_df.write.csv(output_path, header=True, mode="overwrite")

# Step 3: Calculate Economy Rate per Bowler for Each Season
top_bowlers_economy_per_season_df = bowlers_economy_df \
    .groupBy("season", "team_bowling" ,"bowler") \
    .agg(F.sum("runs_per_ball").alias("sum_runs_per_ball"), F.count("*").alias("count_balls")) \
    .filter(F.col("count_balls") >= 20) \
    .withColumn('overs_bowled', F.round(( F.col("count_balls") / 6 ), 2)) \
    .withColumn('bowler_economy', F.round(( F.col("sum_runs_per_ball") / F.col("overs_bowled") ), 2)) \
    .join(player_df, ball_by_ball_df["bowler"] == player_df["player_id"], "inner") \
    .join(team_df, ball_by_ball_df["team_bowling"] == team_df["team_id"], "inner") \
    .orderBy(F.col("season"), F.col("bowler_economy").asc()) \
    .select("season", "team_name", "player_name", "country_name", "bowler_economy")

output_path = "dbfs:/FileStore/tables/top_bowlers_economy_per_season"
top_bowlers_economy_per_season_df.write.csv(output_path, header=True, mode="overwrite")

# COMMAND ----------

# Analyze the most common types of dismissals in cricket

# Filter for deliveries that resulted in a wicket, group by season and out type, then count occurrences.
common_out_types_df = ball_by_ball_df \
    .filter(F.col("bowler_wicket") == 'true') \
    .groupBy("season", "out_type") \
    .agg(F.count("*").alias("wicket_taken_season_out_type_wise")) \
    .select("season", "out_type", "wicket_taken_season_out_type_wise")

output_path = "dbfs:/FileStore/tables/common_out_types"
common_out_types_df.write.csv(output_path, header=True, mode="overwrite")

# COMMAND ----------

# Analyze which bowlers excel at specific dismissal types

# Filter for deliveries that resulted in a wicket, group by season, team, bowler, and out type, then count occurrences.
bowler_wickets_out_type_wise_df = ball_by_ball_df \
    .filter(F.col("bowler_wicket") == 'true') \
    .groupBy("season", "team_bowling" ,"bowler", "out_type") \
    .agg(F.count("*").alias("wicket_taken_season_out_type_wise")) \
    .join(player_df, ball_by_ball_df["bowler"] == player_df["player_id"], "inner") \
    .join(team_df, ball_by_ball_df["team_bowling"] == team_df["team_id"], "inner") \
    .select("season", "player_name", "team_name", "country_name", "out_type", "wicket_taken_season_out_type_wise")

output_path = "dbfs:/FileStore/tables/bowler_wickets_out_type_wise"
bowler_wickets_out_type_wise_df.write.csv(output_path, header=True, mode="overwrite")



# COMMAND ----------

# Analyze how a team's toss decision (bat or field) impacts match outcomes

# Step 1: Select Relevant Columns and Filter for Winning Teams Based on Toss Decision
toss_outcome_df = match_df \
    .select("season_year", "toss_winner", "match_winner", "toss_name") \
    .filter(F.col("toss_winner") == F.col("match_winner")) \
    .withColumnRenamed("season_year","season") \
    .withColumnRenamed("toss_winner","team_name") \

# Step 2: Calculate Wins for Teams that Elected to Field
winning_teams_by_fielding_df = toss_outcome_df \
    .filter(F.col("toss_name") == "field") \
    .groupBy("season", "team_name") \
    .agg(F.count("*").alias("match_wins_by_fielding")) \
    .withColumnRenamed("season", "field_season") \
    .withColumnRenamed("team_name", "field_team_name")

# Step 3: Calculate Wins for Teams that Elected to Bat
winning_teams_by_batting_df = toss_outcome_df \
    .filter(F.col("toss_name") == "bat") \
    .groupBy("season", "team_name") \
    .agg(F.count("*").alias("match_wins_by_batting")) \
    .withColumnRenamed("season", "bat_season") \
    .withColumnRenamed("team_name", "bat_team_name")

# Step 4: Join Fielding and Batting Win Dataframes to Analyze Toss Impact
teams_toss_effect_on_type = winning_teams_by_fielding_df \
    .join(
        winning_teams_by_batting_df, 
        (winning_teams_by_fielding_df["field_season"] == winning_teams_by_batting_df["bat_season"]) & 
        (winning_teams_by_fielding_df["field_team_name"] == winning_teams_by_batting_df["bat_team_name"]), 
        "inner"
    ) \
    .withColumn("total_wins", F.col("match_wins_by_fielding") + F.col("match_wins_by_batting")) \
    .withColumn("win_percentage_field", F.round(F.col("match_wins_by_fielding") / F.col("total_wins") * 100)) \
    .withColumn("win_percentage_bat", 100 - F.col("win_percentage_field")) \
    .select(
        winning_teams_by_batting_df["bat_season"].alias("season"), 
        winning_teams_by_batting_df["bat_team_name"].alias("team_name"), 
        "win_percentage_field",
        "win_percentage_bat"
    ) \
    .orderBy(F.col("season").desc())

# Step 5: Analyze Teams Winning by Fielding
teams_winning_by_fielding = teams_toss_effect_on_type \
    .select(
        "season", 
        "team_name", 
        "win_percentage_field"
    ) \
    .orderBy(F.col("win_percentage_field").desc())

output_path = "dbfs:/FileStore/tables/teams_winning_by_fielding"
teams_winning_by_fielding.write.csv(output_path, header=True, mode="overwrite")


# Step 6: Analyze Teams Winning by Batting
teams_winning_by_batting = teams_toss_effect_on_type \
    .select(
        "season", 
        "team_name", 
        "win_percentage_bat"
    ) \
    .orderBy(F.col("win_percentage_bat").desc())

output_path = "dbfs:/FileStore/tables/teams_winning_by_batting"
teams_winning_by_batting.write.csv(output_path, header=True, mode="overwrite")

# COMMAND ----------

# Analyze which teams consistently perform well across seasons

# Step 1: Count occurrences where the team is team1
team1_count_df = match_df.groupBy("season_year", "team1") \
    .agg(F.count("*").alias("team1_count")) \
    .withColumnRenamed("team1", "team_name")

# Step 2: Count occurrences where the team is team2
team2_count_df = match_df.groupBy("season_year", "team2") \
    .agg(F.count("*").alias("team2_count")) \
    .withColumnRenamed("team2", "team_name")

# Step 3: Count winning occurrences for each team
winning_team_count = match_df.groupBy("season_year", "match_winner") \
    .agg(F.count("*").alias("match_win_count")) \
    .withColumnRenamed("match_winner", "team_name")

# Step 4: Merge counts to get total matches played by each team in each season
total_matches_df = team1_count_df.join(
    team2_count_df,
    on=["season_year", "team_name"],
    how="outer"
).join(
    winning_team_count, 
    on=["season_year", "team_name"],
    how="outer"
) \
.fillna(0, ["team1_count", "team2_count"]) \
.withColumn("total_matches", F.col("team1_count") + F.col("team2_count")) \
.withColumn("winning_percentage", F.round((F.col("match_win_count")/F.col("total_matches")*100), 2) ) \
.select("season_year", "team_name", "total_matches", "match_win_count", "winning_percentage")


# Step 5: Save the results
total_matches_df.orderBy("season_year", F.col("winning_percentage").desc())

output_path = "dbfs:/FileStore/tables/teams_winning_percentage"
total_matches_df.write.csv(output_path, header=True, mode="overwrite")
