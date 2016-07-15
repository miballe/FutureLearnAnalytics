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
sstart_time <- proc.time()
info(logger, "***** MAIN EXECUTION START *****")


########## Execution Sequence (enable/disable) ##########
LOAD_DOWNLOADED_DATA <- FALSE
SAVE_RAW_DATA <- FALSE
LOAD_RAW_DATA <- TRUE
PROCESS_DATA_CLEANING <- FALSE
SAVE_CLEAN_DATA <- FALSE
LOAD_CLEAN_DATA <- FALSE
PROCESS_DATA_TRANSFORMATION <- FALSE
SAVE_TRANSFORMED_DATA <- FALSE
LOAD_TRANSFORMED_DATA <- FALSE


########## Load Downloaded Data ##########
# Functions from 01_load.r
if(LOAD_DOWNLOADED_DATA){
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Load Downloaded Data")
  df_course_list <- load_from_csv_course_list()
  df_course_details <- load_from_csv_course_details()
  df_comments <- load_downloaded_comments(df_course_list[,1])
  df_enrolments <- load_downloaded_enrolments(df_course_list[,1])
  df_pr_assignments <- load_downloaded_peer_review_assignments(df_course_list[,1])
  df_pr_reviews <- load_downloaded_peer_review_reviews(df_course_list[,1])
  df_question_response <- load_downloaded_question_response(df_course_list[,1])
  df_step_activity <- load_downloaded_step_activity(df_course_list[,1])
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Load Downloaded Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Load Downloaded Data")
}

########## Save Raw Data ##########
# Functions from 01_load.r
if(SAVE_RAW_DATA) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Save Raw Data")
  save_raw_course_list()
  save_raw_course_details()
  save_raw_comments()
  save_raw_enrolments()
  save_raw_pr_assignments()
  save_raw_pr_reviews()
  save_raw_question_response()
  save_raw_step_activity()
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Save Raw Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Save Raw Data")
}

########## Load Raw Data ##########
# Functions from 01_load.r
if(LOAD_RAW_DATA){
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Load Raw Data")
  df_course_list <- load_raw_course_list()
  df_course_details <- load_raw_course_details()
  df_comments <- load_raw_comments()
  df_enrolments <- load_raw_enrolments()
  df_pr_assignments <- load_raw_peer_review_assignments()
  df_pr_reviews <- load_raw_peer_review_reviews()
  df_question_response <- load_raw_question_response()
  df_step_activity <- load_raw_step_activity()
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Load Raw Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Load Raw Data")
}

########## Process Data Cleaning ##########
# Functions from 02_clean.r
if(PROCESS_DATA_CLEANING) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Process Data Cleaning")
  # df_comments <- clean_fl_comments_df(df_comments)
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Process Data Cleaning - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Process Data Cleaning")
}

########## Save Clean Data ##########
# Functions from 02_clean.r
if(SAVE_CLEAN_DATA) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Save Clean Data")
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Save Clean Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Save Clean Data")
}

########## Load Clean Data ##########
# Functions from 02_clean.r
if(LOAD_CLEAN_DATA){
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Load Clean Data")
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Load Clean Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Load Clean Data")
}

########## Process Data Transformation ##########
# Functions from 03_transform.r
if(PROCESS_DATA_TRANSFORMATION) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Process Data Transformation")
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Process Data Transformation - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Process Data Transformation")
}

########## Save Transformed Data ##########
# Functions from 03_transform.r
if(SAVE_TRANSFORMED_DATA) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Save Transformed Data")
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Save Transformed Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Save Transformed Data")
}

########## Load Transformed Data ##########
# Functions from 03_transform.r
if(LOAD_TRANSFORMED_DATA) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Load Transformed Data")
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Load Transformed Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Load Transformed Data")
}

########## Script Final Operations ##########
sstop_time <- proc.time() - sstart_time
info(logger, paste("***** MAIN EXECUTION END ***** - Elapsed:", sstop_time[3], "s"))
