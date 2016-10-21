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


########## Sourcing Eependent Scripts ##########
source("00_config.r")
source("01_load.r")
source("02_clean.r")
source("03_transform.r")
source("04_model.r")
source("ggtheme_data.r")
source("ggtheme_stata.r")

########## Execution Sequence (enable/disable) ##########
EXECUTE_LOAD_DOWNLOADED_DATA <- FALSE
EXECUTE_SAVE_RAW_DATA <- FALSE
EXECUTE_LOAD_RAW_DATA <- FALSE
EXECUTE_DATA_CLEANING <- FALSE
EXECUTE_SAVE_CLEAN_DATA <- FALSE
EXECUTE_LOAD_CLEAN_DATA <- TRUE
EXECUTE_DATA_TRANSFORMATION <- TRUE
EXECUTE_SAVE_TRANSFORMED_DATA <- TRUE
EXECUTE_LOAD_TRANSFORMED_DATA <- FALSE
EXECUTE_LOAD_MODELS_DATA <- FALSE

# Global errors and warnings containers initalization
execution_errors <- list()
execution_warnings <- list()


# Process logging start
sstart_time <- proc.time()
log_new_info("***** MAIN_EXECUTION START *****")

predictive_data <- list()
predictive_models <- list()
predictive_rocs <- list()

########## Load Downloaded Data Section ##########
# Functions from 01_load.r
if(EXECUTE_LOAD_DOWNLOADED_DATA){
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Load_Downloaded_Data")
  
  df_course_list <- load_from_csv_course_list()
  df_course_details <- load_from_csv_course_details()
  # Since further CSV loads depends on the short_code field from course_list,
  # the process cannot continue unless there are 1 or more entries.
  if( nrow(df_course_list) > 0 ) {
    df_comments <- load_downloaded_comments(df_course_list[,1])
    df_enrolments <- load_downloaded_enrolments(df_course_list[,1])
    df_pr_assignments <- load_downloaded_peer_review_assignments(df_course_list[,1])
    df_pr_reviews <- load_downloaded_peer_review_reviews(df_course_list[,1])
    df_question_response <- load_downloaded_question_response(df_course_list[,1])
    df_step_activity <- load_downloaded_step_activity(df_course_list[,1])
  }
  else {
    error_new("Course List CSV file is empty or does not exist. Aborting the process!")
  }
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Load_Downloaded_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Load_Downloaded_Data")
}


########## Save Raw Data ##########
# Functions from 01_load.r
if(EXECUTE_SAVE_RAW_DATA & length(execution_errors) == 0) {
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Save_Raw_Data")
  
  save_raw_course_list(df_course_list)
  save_raw_course_details(df_course_details)
  save_raw_comments(df_comments)
  save_raw_enrolments(df_enrolments)
  save_raw_pr_assignments(df_pr_assignments)
  save_raw_pr_reviews(df_pr_reviews)
  save_raw_question_response(df_question_response)
  save_raw_step_activity(df_step_activity)
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Save_Raw_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Save_Raw_Data")
}


########## Load Raw Data ##########
# Functions from 01_load.r
if(EXECUTE_LOAD_RAW_DATA & length(execution_errors) == 0){
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Load_Raw_Data")
  
  df_course_list <- load_raw_course_list()
  df_course_details <- load_raw_course_details()
  df_comments <- load_raw_comments()
  df_enrolments <- load_raw_enrolments()
  df_pr_assignments <- load_raw_peer_review_assignments()
  df_pr_reviews <- load_raw_peer_review_reviews()
  df_question_response <- load_raw_question_response()
  df_step_activity <- load_raw_step_activity()
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Load_Raw_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Load_Raw_Data")
}


########## Process Data Cleaning ##########
# Functions from 02_clean.r
if(EXECUTE_DATA_CLEANING & length(execution_errors) == 0) {
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Process_Data_Cleaning")
  
  df_course_list <- clean_course_list(df_course_list)
  df_course_details <- clean_course_details(df_course_details)
  df_comments <- clean_comments(df_comments)
  df_enrolments <- clean_enrolments(df_enrolments)
  df_pr_assignments <- clean_pr_assignments(df_pr_assignments)
  df_pr_reviews <- clean_pr_reviews(df_pr_reviews)
  df_question_response <- clean_question_response(df_question_response)
  df_step_activity <- clean_step_activity(df_step_activity)
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Process_Data_Cleaning - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Process_Data_Cleaning")
}


########## Save Clean Data ##########
# Functions from 02_clean.r
if(EXECUTE_SAVE_CLEAN_DATA & length(execution_errors) == 0) {
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Save_Clean_Data")
  
  # Lists all objects with name starting with "df_" which is the convention for data frames
  obj_to_save <- as.vector(ls(pattern =  "^df_.*"))
  save_clean_data_file(obj_to_save, DATA_CLEAN)
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Save_Clean_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Save_Clean_Data")
}


########## Load Clean Data ##########
# Functions from 02_clean.r
if(EXECUTE_LOAD_CLEAN_DATA & length(execution_errors) == 0){
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Load_Clean_Data")
  
  load_clean_data_file(DATA_CLEAN)
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Load_Clean_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Load_Clean_Data")
}


########## Process Data Transformation ##########
# Functions from 03_transform.r
if(EXECUTE_DATA_TRANSFORMATION & length(execution_errors) == 0) {
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Process_Data_Transformation")
  
  remove_admin_activity()
  transform_default_tables()
  df_course_facts <- transform_course_facts()
  df_participant_facts <- transform_participant_facts()
  df_total_events <- get_total_event_dates()
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Process_Data_Transformation - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Process_Data_Transformation")
}


########## Save Transformed Data ##########
# Functions from 03_transform.r
if(EXECUTE_SAVE_TRANSFORMED_DATA & length(execution_errors) == 0) {
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Save_Transformed_Data")
  
  # Lists all objects with name starting with "df_" which is the convention for data frames
  obj_to_save <- as.vector(ls(pattern =  "^df_.*"))
  save_transformed_data_file(obj_to_save, DATA_TRANSFORMED)
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Save_Transformed_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Save_Transformed_Data")
}


########## Load Transformed Data ##########
# Functions from 03_transform.r
if(EXECUTE_LOAD_TRANSFORMED_DATA & length(execution_errors) == 0) {
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Load_Transformed_Data")
  
  load_transformed_data_file(DATA_TRANSFORMED)
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Load_Transformed_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Load_Transformed_Data")
}


########## Load Transformed Data ##########
# Functions from 03_transform.r
if(EXECUTE_LOAD_MODELS_DATA & length(execution_errors) == 0) {
  cstart_time <- proc.time()
  log_new_info("- START - SECTION - Load_Models_Data")
  
  load_models_data_file(DATA_MODELS)
  
  cstop_time <- proc.time() - cstart_time
  log_new_info(paste("- END - SECTION - Load_Models_Data - Elapsed:", cstop_time[3], "s"))
} else {
  log_new_info("- IGNORED - SECTION - Load_Models_Data")
}


########## Script Final Operations ##########
log_new_info(paste("Process Completed with", length(execution_errors), "errors and", length(execution_warnings), "warnings"))
sstop_time <- proc.time() - sstart_time
log_new_info(paste("***** MAIN_EXECUTION END ***** - Elapsed:", sstop_time[3], "s"))
rm(cstart_time)
rm(cstop_time)
rm(sstart_time)
rm(sstop_time)
