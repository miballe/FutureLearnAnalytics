####################################################
# DAMOOCP data Transformation
####################################################
# The functions inside this file modifies data structures by applying
# Feature Engineering techniques


########## Generic Transformation Operations ##########



########## Specific Data Transformation Operations ##########




########## File read/write Operations ##########
save_transformed_data_file <- function(objects, file_name) {
  fstart_time <- proc.time()
  info(logger, "- START - save_transformed_data_file")
  
  save(list = objects, file = paste("./", DATA_TRANSFORMED, "/", file_name, ".RData", sep = ""))
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - save_transformed_data_file - Elapsed:", fstop_time[3], "s"))
}


load_transformed_data_file <- function(file_name) {
  fstart_time <- proc.time()
  info(logger, "- START - load_transformed_data_file")
  
  load( file = paste("./", DATA_TRANSFORMED, "/", file_name, ".RData", sep = ""), envir = .GlobalEnv )
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_transformed_data_file - Elapsed:", fstop_time[3], "s"))
}



