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
info(logger, "***** MAIN_EXECUTION START *****")


########## Execution Sequence (enable/disable) ##########
LOAD_DOWNLOADED_DATA <- TRUE
SAVE_RAW_DATA <- TRUE
LOAD_RAW_DATA <- TRUE
PROCESS_DATA_CLEANING <- TRUE
SAVE_CLEAN_DATA <- TRUE
LOAD_CLEAN_DATA <- FALSE
PROCESS_DATA_TRANSFORMATION <- FALSE
SAVE_TRANSFORMED_DATA <- FALSE
LOAD_TRANSFORMED_DATA <- FALSE


########## Load Downloaded Data ##########
# Functions from 01_load.r
if(LOAD_DOWNLOADED_DATA){
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Load_Downloaded_Data")
  
  df_course_list <- load_from_csv_course_list()
  df_course_details <- load_from_csv_course_details()
  df_comments <- load_downloaded_comments(df_course_list[,1])
  df_enrolments <- load_downloaded_enrolments(df_course_list[,1])
  df_pr_assignments <- load_downloaded_peer_review_assignments(df_course_list[,1])
  df_pr_reviews <- load_downloaded_peer_review_reviews(df_course_list[,1])
  df_question_response <- load_downloaded_question_response(df_course_list[,1])
  df_step_activity <- load_downloaded_step_activity(df_course_list[,1])
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Load_Downloaded_Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Load_Downloaded_Data")
}

########## Save Raw Data ##########
# Functions from 01_load.r
if(SAVE_RAW_DATA) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Save_Raw_Data")
  
  save_raw_course_list(df_course_list)
  save_raw_course_details(df_course_details)
  save_raw_comments(df_comments)
  save_raw_enrolments(df_enrolments)
  save_raw_pr_assignments(df_pr_assignments)
  save_raw_pr_reviews(df_pr_reviews)
  save_raw_question_response(df_question_response)
  save_raw_step_activity(df_step_activity)
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Save_Raw_Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Save_Raw_Data")
}

########## Load Raw Data ##########
# Functions from 01_load.r
if(LOAD_RAW_DATA){
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Load_Raw_Data")
  
  df_course_list <- load_raw_course_list()
  df_course_details <- load_raw_course_details()
  df_comments <- load_raw_comments()
  df_enrolments <- load_raw_enrolments()
  df_pr_assignments <- load_raw_peer_review_assignments()
  df_pr_reviews <- load_raw_peer_review_reviews()
  df_question_response <- load_raw_question_response()
  df_step_activity <- load_raw_step_activity()
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Load_Raw_Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Load_Raw_Data")
}

########## Process Data Cleaning ##########
# Functions from 02_clean.r
if(PROCESS_DATA_CLEANING) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Process_Data_Cleaning")
  
  df_course_list <- clean_course_list(df_course_list)
  df_course_details <- clean_course_details(df_course_details)
  df_comments <- clean_comments(df_comments)
  df_enrolments <- clean_enrolments(df_enrolments)
  df_pr_assignments <- clean_pr_assignments(df_pr_assignments)
  df_pr_reviews <- clean_pr_reviews(df_pr_reviews)
  df_question_response <- clean_question_response(df_question_response)
  df_step_activity <- clean_step_activity(df_step_activity)
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Process_Data_Cleaning - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Process_Data_Cleaning")
}

########## Save Clean Data ##########
# Functions from 02_clean.r
if(SAVE_CLEAN_DATA) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Save_Clean_Data")
  
  obj_to_save <- c("df_course_list", "df_course_details", "df_comments", "df_enrolments",
                   "df_pr_assignments", "df_pr_reviews", "df_question_response", "df_step_activity")
  save(list = obj_to_save, file = paste("./", DATA_CLEAN, "/data_clean.RData", sep = ""))
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Save_Clean_Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Save_Clean_Data")
}

########## Load Clean Data ##########
# Functions from 02_clean.r
if(LOAD_CLEAN_DATA){
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Load_Clean_Data")
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Load_Clean_Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Load_Clean_Data")
}

########## Process Data Transformation ##########
# Functions from 03_transform.r
if(PROCESS_DATA_TRANSFORMATION) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Process_Data_Transformation")
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Process_Data_Transformation - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Process_Data_Transformation")
}

########## Save Transformed Data ##########
# Functions from 03_transform.r
if(SAVE_TRANSFORMED_DATA) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Save_Transformed_Data")
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Save_Transformed_Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Save_Transformed_Data")
}

########## Load Transformed Data ##########
# Functions from 03_transform.r
if(LOAD_TRANSFORMED_DATA) {
  cstart_time <- proc.time()
  info(logger, "- START - SECTION - Load_Transformed_Data")
  
  cstop_time <- proc.time() - cstart_time
  info(logger, paste("- END - SECTION - Load_Transformed_Data - Elapsed:", cstop_time[3], "s"))
} else {
  info(logger, "- IGNORED - SECTION - Load_Transformed_Data")
}

########## Script Final Operations ##########
sstop_time <- proc.time() - sstart_time
info(logger, paste("***** MAIN_EXECUTION END ***** - Elapsed:", sstop_time[3], "s"))
