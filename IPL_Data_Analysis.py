# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType
from pyspark.sql.functions import col, to_date, length, row_number
from pyspark.sql import Window


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
    .withColumn("match_id", col("MatcH_id").cast(IntegerType())) \
    .withColumn("over_id", col("Over_id").cast(IntegerType())) \
    .withColumn("ball_id", col("Ball_id").cast(IntegerType())) \
    .withColumn("innings_no", col("Innings_No").cast(IntegerType())) \
    .withColumn("team_batting", col("Team_Batting").cast(StringType())) \
    .withColumn("team_bowling", col("Team_Bowling").cast(StringType())) \
    .withColumn("striker_batting_position", col("Striker_Batting_Position").cast(IntegerType())) \
    .withColumn("extra_type", col("Extra_Type").cast(StringType())) \
    .withColumn("runs_scored", col("Runs_Scored").cast(IntegerType())) \
    .withColumn("extra_runs", col("Extra_runs").cast(IntegerType())) \
    .withColumn("wides", col("Wides").cast(IntegerType())) \
    .withColumn("legbyes", col("Legbyes").cast(IntegerType())) \
    .withColumn("byes", col("Byes").cast(IntegerType())) \
    .withColumn("noballs", col("Noballs").cast(IntegerType())) \
    .withColumn("penalty", col("Penalty").cast(IntegerType())) \
    .withColumn("bowler_extras", col("Bowler_Extras").cast(IntegerType())) \
    .withColumn("out_type", col("Out_type").cast(StringType())) \
    .withColumn("caught", col("Caught").cast(BooleanType())) \
    .withColumn("bowled", col("Bowled").cast(BooleanType())) \
    .withColumn("run_out", col("Run_out").cast(BooleanType())) \
    .withColumn("lbw", col("LBW").cast(BooleanType())) \
    .withColumn("retired_hurt", col("Retired_hurt").cast(BooleanType())) \
    .withColumn("stumped", col("Stumped").cast(BooleanType())) \
    .withColumn("caught_and_bowled", col("caught_and_bowled").cast(BooleanType())) \
    .withColumn("hit_wicket", col("hit_wicket").cast(BooleanType())) \
    .withColumn("obstructingfeild", col("ObstructingFeild").cast(BooleanType())) \
    .withColumn("bowler_wicket", col("Bowler_Wicket").cast(BooleanType())) \
    .withColumn("match_date", to_date(col("match_date"), "M/d/yyyy")) \
    .withColumn("season", col("Season").cast(IntegerType())) \
    .withColumn("striker", col("Striker").cast(IntegerType())) \
    .withColumn("non_striker", col("Non_Striker").cast(IntegerType())) \
    .withColumn("bowler", col("Bowler").cast(IntegerType())) \
    .withColumn("player_out", col("Player_Out").cast(IntegerType())) \
    .withColumn("fielders", col("Fielders").cast(IntegerType())) \
    .withColumn("striker_match_sk", col("Striker_match_SK").cast(IntegerType())) \
    .withColumn("strikersk", col("StrikerSK").cast(IntegerType())) \
    .withColumn("nonstriker_match_sk", col("NonStriker_match_SK").cast(IntegerType())) \
    .withColumn("nonstriker_sk", col("NONStriker_SK").cast(IntegerType())) \
    .withColumn("fielder_match_sk", col("Fielder_match_SK").cast(IntegerType())) \
    .withColumn("fielder_sk", col("Fielder_SK").cast(IntegerType())) \
    .withColumn("bowler_match_sk", col("Bowler_match_SK").cast(IntegerType())) \
    .withColumn("bowler_sk", col("BOWLER_SK").cast(IntegerType())) \
    .withColumn("playerout_match_sk", col("PlayerOut_match_SK").cast(IntegerType())) \
    .withColumn("battingteam_sk", col("BattingTeam_SK").cast(IntegerType())) \
    .withColumn("bowlingteam_sk", col("BowlingTeam_SK").cast(IntegerType())) \
    .withColumn("keeper_catch", col("Keeper_Catch").cast(BooleanType())) \
    .withColumn("player_out_sk", col("Player_out_sk").cast(IntegerType())) \
    .withColumn("matchdatesk", to_date(col("MatchDateSK"), "yyyyMMdd"))

# COMMAND ----------

ball_by_ball_df.show(1)

# COMMAND ----------

match_df_raw = spark.read.format("csv").option("header","true").load("s3://ipl-data-analysis-project/Match.csv")
match_df_raw.printSchema()

# COMMAND ----------

match_df = match_df_raw \
    .withColumn("match_sk", col("Match_SK").cast(IntegerType())) \
    .withColumn("match_id", col("match_id").cast(IntegerType())) \
    .withColumn("match_date", to_date("match_date","M/d/yyyy")) \
    .withColumn("season_year", col("season_year").cast(IntegerType())) \
    .withColumn("win_margin", col("win_margin").cast(IntegerType())) \
    .withColumn("country_id", col("country_id").cast(IntegerType()))


# COMMAND ----------

match_df.show(1)

# COMMAND ----------

player_df_raw = spark.read.format("csv").option("header","true").option("dateFormat","M/d/yyyy").load("s3://ipl-data-analysis-project/Player.csv")
player_df_raw.printSchema()

# COMMAND ----------

player_df = player_df_raw \
    .withColumn("player_sk", col("player_sk").cast(IntegerType())) \
    .withColumn("player_id", col("player_id").cast(IntegerType())) \
    .withColumn("dob", to_date("dob", "M/d/yyyy"))

# COMMAND ----------

player_df.show(1)

# COMMAND ----------

player_match_df_raw = spark.read.format("csv").option("header","true").load("s3://ipl-data-analysis-project/Player_match.csv")
player_match_df_raw.printSchema()

# COMMAND ----------

player_match_df = player_match_df_raw \
    .withColumn("player_match_sk", col("player_match_sk").cast(IntegerType())) \
    .withColumn("playermatch_key", col("playermatch_key").cast(DecimalType())) \
    .withColumn("match_id", col("match_id").cast(IntegerType())) \
    .withColumn("player_id", col("player_id").cast(IntegerType())) \
    .withColumn("dob", to_date("dob","M/d/yyyy")) \
    .withColumn("season_year", col("season_year").cast(IntegerType())) \
    .withColumn("age_as_on_match", col("age_as_on_match").cast(IntegerType())) \
    .withColumn("is_manofthematch", col("is_manofthematch").cast(BooleanType())) \
    .withColumn("isplayers_team_won", col("isplayers_team_won").cast(BooleanType())) 


# COMMAND ----------

player_match_df.show(5)

# COMMAND ----------

team_df_raw = spark.read.format("csv").option("header","true").load("s3://ipl-data-analysis-project/Team.csv")
team_df_raw.printSchema()

# COMMAND ----------

team_df = team_df_raw \
    .withColumn("team_sk", col("team_sk").cast(IntegerType())) \
    .withColumn("team_id", col("team_id").cast(IntegerType()))

# COMMAND ----------

team_df.show(5)

# COMMAND ----------

# No of bowler extras by each team per season

joined_df = ball_by_ball_df.join(team_df, ball_by_ball_df["team_bowling"] == team_df["team_id"], "inner")
grouped_df = joined_df.groupBy("team_name", "season").sum("bowler_extras").withColumnRenamed("sum(bowler_extras)", "total_bowler_extras")
sorted_df = grouped_df.orderBy(grouped_df.season.asc(), grouped_df.total_bowler_extras.desc()).show()


# COMMAND ----------

# No of extra runs by each team per season

joined_df = ball_by_ball_df.join(team_df, ball_by_ball_df["team_batting"] == team_df["team_id"], "inner")
grouped_df = joined_df.groupBy("team_name", "season").sum("extra_runs").withColumnRenamed("sum(extra_runs)", "total_extra_runs")
sorted_df = grouped_df.orderBy(grouped_df.season.asc(), grouped_df.total_extra_runs.desc()).show()

# COMMAND ----------

# No of extras by top 5 bowlers each season

grouped_df = ball_by_ball_df.groupBy("bowler", "season").sum("bowler_extras").withColumnRenamed("sum(bowler_extras)", "total_bowler_extras")
joined_df = grouped_df.join(player_df, grouped_df["bowler"] == player_df["player_id"], "inner")
select_df = joined_df.select("player_name", "country_name", "season", "total_bowler_extras")
window_spec = Window.partitionBy("season").orderBy(select_df.total_bowler_extras.desc())
ranked_df = select_df.withColumn("rank", row_number().over(window_spec))
top_bowlers_df = ranked_df.filter(ranked_df.rank <= 5).show()



# COMMAND ----------

# No of sixes by top 5 batters each season

filter_sixes_df = ball_by_ball_df.filter(col("runs_scored") == 6)
grouped_df = filter_sixes_df.groupBy("striker", "season").count().withColumnRenamed("count", "total_sixes")
joined_df = grouped_df.join(player_df, grouped_df["striker"] == player_df["player_id"] , "inner")
select_df = joined_df.select("player_name", "country_name", "season", "total_sixes")
window_spec = Window.partitionBy("season").orderBy(select_df.total_sixes.desc())
ranked_df = select_df.withColumn("rank", row_number().over(window_spec))
top_batters = ranked_df.filter(ranked_df.rank <= 5).show()

