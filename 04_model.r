####################################################
# DAMOOCP data Modeling
####################################################
# The functions inside this file uses the transformed data to feed
# different data models to understand and predict learning behaviors





########## File read/write Operations ##########
save_transformed_data_file <- function(objects, file_name) {
  fstart_time <- proc.time()
  log_new_info("- START - save_transformed_data_file")
  
  save(list = objects, file = paste("./", DATA_TRANSFORMED, "/", file_name, ".RData", sep = ""))
  write.csv(df_course_facts, paste("./", DATA_TRANSFORMED, "/course_facts.csv", sep = ""))
  write.csv(df_participant_facts, paste("./", DATA_TRANSFORMED, "/participant_facts.csv", sep = ""))
  
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



