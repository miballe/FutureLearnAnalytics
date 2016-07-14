################################################################
# DAMOOCP - Main Script
################################################################
# This is the main script that guides the general execution of
# all pipeline operations. The file is organized in pipeline
# stages sections with the aim to keep processes as isolated
# as possible, as well as to avoid the unnecessary execution
# of stages when not required.

# Cleans the workspace
rm( list = ls() )


########## Libraries and required scripts ##########
library("log4r")
source("01_load.r")
source("02_clean.r")

logger <- create.logger()
logfile(logger) <- file.path("./damoocp.log")
level(logger) <- "DEBUG"

info(logger, "***** MAIN EXECUTION START *****")


########## Initialization ##########
LOAD_DOWNLOADED_DATA <- TRUE
SAVE_RAW_DATA <- FALSE
LOAD_RAW_DATA <- FALSE
PROCESS_DATA_CLEANING <- FALSE
SAVE_CLEAN_DATA <- FALSE
LOAD_CLEAN_DATA <- FALSE
PROCESS_DATA_TRANSFORMATION <- FALSE
SAVE_TRANSFORMED_DATA <- FALSE
LOAD_TRANSFORMED_DATA <- FALSE


########## Data load ##########
# Functions from 01_load.r
if(LOAD_DOWNLOADED_DATA){
  info(logger, "- START - Load Downloaded Data")
  df_course_list <- load_from_csv_course_list()
  df_course_details <- load_from_csv_course_details()
  df_comments <- load_downloaded_comments(df_course_list[,1])
  df_enrolments <- load_downloaded_enrolments(df_course_list[,1])
  df_pr_assignments <- load_downloaded_peer_review_assignments(df_course_list[,1])
  df_pr_reviews <- load_downloaded_peer_review_reviews(df_course_list[,1])
  df_question_response <- load_downloaded_question_response(df_course_list[,1])
  df_step_activity <- load_downloaded_step_activity(df_course_list[,1])
  info(logger, "- END - Load Downloaded Data")
} else {
  info(logger, "- IGNORED - Load Downloaded Data")
}

if(SAVE_RAW_DATA) {
  info(logger, "- START - Save Raw Data")
  write.csv(df_course_list, "./Data_Raw/df_course_list_raw.csv", row.names = FALSE)
  write.csv(df_course_details, "./Data_Raw/df_course_details_raw.csv", row.names = FALSE)
  write.csv(df_comments, "./Data_Raw/df_comments_raw.csv", row.names = FALSE)
  write.csv(df_enrolments, "./Data_Raw/df_enrolments_raw.csv", row.names = FALSE)
  write.csv(df_pr_assignments, "./Data_Raw/df_pr_assignments_raw.csv", row.names = FALSE)
  write.csv(df_pr_reviews, "./Data_Raw/df_pr_reviews_raw.csv", row.names = FALSE)
  write.csv(df_question_response, "./Data_Raw/df_question_response_raw.csv", row.names = FALSE)
  write.csv(df_step_activity, "./Data_Raw/df_step_activity_raw.csv", row.names = FALSE)
  info(logger, "- END - Save Raw Data")
} else {
  info(logger, "- IGNORED - Save Raw Data")
}

########## Data cleaning ##########
# Functions from 02_clean.r
if(LOAD_RAW_DATA){
  info(logger, "- START - Load Raw Data")
  df_course_list = read.csv("./Data_Raw/df_course_list_raw.csv")
  df_course_details = read.csv("./Data_Raw/df_course_details_raw.csv")
  df_comments = read.csv("./Data_Raw/df_comments_raw.csv")
  df_enrolments = read.csv("./Data_Raw/df_enrolments_raw.csv")
  df_assignments = read.csv("./Data_Raw/df_assignments_raw.csv")
  df_reviews = read.csv("./Data_Raw/df_reviews_raw.csv")
  df_questions = read.csv("./Data_Raw/df_questions_raw.csv")
  df_steps = read.csv("./Data_Raw/df_steps_raw.csv")
  info(logger, "- END - Load Raw Data")
} else {
  info(logger, "- IGNORED - Load Raw Data")
}

if(PROCESS_DATA_CLEANING) {
  info(logger, "- START - Process Data Cleaning")
  df_comments <- clean_fl_comments_df(df_comments)
  info(logger, "- END - Process Data Cleaning")
} else {
  info(logger, "- IGNORED - Process Data Cleaning")
}

if(SAVE_CLEAN_DATA) {
  info(logger, "- START - Save Clean Data")
  info(logger, "- END - Save Clean Data")
} else {
  info(logger, "- IGNORED - Save Clean Data")
}

########## Data transformation and feature engineering ##########
# Functions from 03_transform.r
if(LOAD_CLEAN_DATA){
  info(logger, "- START - Load Clean Data")
  info(logger, "- END - Load Clean Data")
} else {
  info(logger, "- IGNORED - Load Clean Data")
}

if(PROCESS_DATA_TRANSFORMATION) {
  info(logger, "- START - Process Data Transformation")
  info(logger, "- END - Process Data Transformation")
} else {
  info(logger, "- IGNORED - Process Data Transformation")
}

if(SAVE_TRANSFORMED_DATA) {
  info(logger, "- START - Save Transformed Data")
  info(logger, "- END - Save Transformed Data")
} else {
  info(logger, "- IGNORED - Save Transformed Data")
}

if(LOAD_TRANSFORMED_DATA) {
  info(logger, "- START - Load Transformed Data")
  info(logger, "- END - Load Transformed Data")
} else {
  info(logger, "- IGNORED - Load Transformed Data")
}

info(logger, "***** MAIN EXECUTION END *****")