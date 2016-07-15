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

# Logging object creation and initialization
logger <- create.logger()
logfile(logger) <- file.path("./damoocp.log")
level(logger) <- "DEBUG"
info(logger, "***** MAIN EXECUTION START *****")


########## Execution Sequence (enable/disable) ##########
LOAD_DOWNLOADED_DATA <- TRUE
SAVE_RAW_DATA <- TRUE
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

########## Data write ##########
# Functions from 01_load.r
if(SAVE_RAW_DATA) {
  info(logger, "- START - Save Raw Data")
  save_raw_course_list()
  save_raw_course_details()
  save_raw_comments()
  save_raw_enrolments()
  save_raw_pr_assignments()
  save_raw_pr_reviews()
  save_raw_question_response()
  save_raw_step_activity()
  info(logger, "- END - Save Raw Data")
} else {
  info(logger, "- IGNORED - Save Raw Data")
}

########## Data cleaning ##########
# Functions from 02_clean.r
if(LOAD_RAW_DATA){
  info(logger, "- START - SECTION - Load Raw Data")
  df_course_list = read.csv("./Data_Raw/df_course_list_raw.csv")
  df_course_details = read.csv("./Data_Raw/df_course_details_raw.csv")
  df_comments = read.csv("./Data_Raw/df_comments_raw.csv")
  df_enrolments = read.csv("./Data_Raw/df_enrolments_raw.csv")
  df_assignments = read.csv("./Data_Raw/df_assignments_raw.csv")
  df_reviews = read.csv("./Data_Raw/df_reviews_raw.csv")
  df_questions = read.csv("./Data_Raw/df_questions_raw.csv")
  df_steps = read.csv("./Data_Raw/df_steps_raw.csv")
  info(logger, "- END - SECTION- Load Raw Data")
} else {
  info(logger, "- IGNORED - SECTION - Load Raw Data")
}

if(PROCESS_DATA_CLEANING) {
  info(logger, "- START - SECTION - Process Data Cleaning")
  df_comments <- clean_fl_comments_df(df_comments)
  info(logger, "- END - Process Data Cleaning")
} else {
  info(logger, "- IGNORED - SECTION - Process Data Cleaning")
}

if(SAVE_CLEAN_DATA) {
  info(logger, "- START - SECTION - Save Clean Data")
  info(logger, "- END - SECTION - Save Clean Data")
} else {
  info(logger, "- IGNORED - SECTION - Save Clean Data")
}

########## Data transformation and feature engineering ##########
# Functions from 03_transform.r
if(LOAD_CLEAN_DATA){
  info(logger, "- START - SECTION - Load Clean Data")
  info(logger, "- END - SECTION - Load Clean Data")
} else {
  info(logger, "- IGNORED - SECTION - Load Clean Data")
}

if(PROCESS_DATA_TRANSFORMATION) {
  info(logger, "- START - SECTION - Process Data Transformation")
  info(logger, "- END - SECTION - Process Data Transformation")
} else {
  info(logger, "- IGNORED - SECTION - Process Data Transformation")
}

if(SAVE_TRANSFORMED_DATA) {
  info(logger, "- START - SECTION - Save Transformed Data")
  info(logger, "- END - SECTION - Save Transformed Data")
} else {
  info(logger, "- IGNORED - SECTION - Save Transformed Data")
}

if(LOAD_TRANSFORMED_DATA) {
  info(logger, "- START - SECTION - Load Transformed Data")
  info(logger, "- END - SECTION - Load Transformed Data")
} else {
  info(logger, "- IGNORED - SECTION - Load Transformed Data")
}

info(logger, "***** MAIN EXECUTION END *****")