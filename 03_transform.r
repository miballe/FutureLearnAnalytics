####################################################
# DAMOOCP data Transformation
####################################################
# The functions inside this file modifies data structures by applying
# Feature Engineering techniques


########## Generic Transformation Operations ##########



########## Specific Data Transformation Operations ##########
transform_course_list <- function(df_clean_course_list){
  

  return(df_clean_course_list)
}

transform_course_details <- function(df_clean_course_details) {
  

  return(df_clean_course_details)
}

calculate_duration_step_activity <- function() {
  duration_summary <- select(df_step_activity, short_code, week_number, step_number, total_seconds) %>% group_by(short_code, week_number, step_number) %>% select(short_code, week_number, step_number, total_seconds) %>% summarize(duration_reference = as.integer(median(total_seconds, na.rm = TRUE)))
  df_course_details <<- merge(df_course_details, duration_summary, intersect(names(df_course_details), names(duration_summary)), all = TRUE)
}


########## File read/write Operations ##########
save_transformed_data_file <- function(objects, file_name) {
  fstart_time <- proc.time()
  log_new_info("- START - save_transformed_data_file")
  
  save(list = objects, file = paste("./", DATA_TRANSFORMED, "/", file_name, ".RData", sep = ""))
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - save_transformed_data_file - Elapsed:", fstop_time[3], "s"))
}


load_transformed_data_file <- function(file_name) {
  fstart_time <- proc.time()
  log_new_info("- START - load_transformed_data_file")
  
  load( file = paste("./", DATA_TRANSFORMED, "/", file_name, ".RData", sep = ""), envir = .GlobalEnv )
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - load_transformed_data_file - Elapsed:", fstop_time[3], "s"))
}



