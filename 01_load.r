#----------------------------------------------------------------------
# DAMOOCP data load
#----------------------------------------------------------------------
# The functions contained in this file load the data according
# to a file name convention to R data frames. The functions 
# basically load the data and ensure that any read.csv or merge
# operation are working correctly. Any data type change or field
# validations are performed in the step that follows: 02_clean
#
# Known Issues: Exceptionally, one file was found to have a NUL entry
# and this may occur again. A warning is displayed in case it is found
# and some observations are lost. To fix this issue, open the CSV file
# in Notpead++, find the character \0 and delete all its entries.
#


##### Generic CSV Read functions #####
load_local_csv <- function(data_folder, file_name) {
  df_return <- data.frame()
  file_path <- paste("./", data_folder, "/", file_name, ".csv", sep = "")
  if(file.exists(file_path)) {
    frstart_time <- proc.time()
    log4r::debug(logger, paste("- READING - Local CSV", file_path))
    df_return <- read.csv(file_path, stringsAsFactors=FALSE)
    frstop_time <- proc.time() - frstart_time
    log4r::debug(logger, paste("- COMPLETE - Local CSV", file_path, "- Elapsed:", frstop_time[3], "s"))
  } else {
    log4r::error(logger, paste("The file", file_path, "doesn't exist!"))
  }
  return(df_return)
}

load_merge_downloaded_csvs <- function(short_ids, data_folder, suffix) {
  df_return <- data.frame()
  first_loop <- TRUE
  for(short_id in short_ids) {
    file_path <- paste("./", data_folder, "/", short_id, "_", suffix, ".csv", sep = "")
    if(file.exists(file_path)) {
      frstart_time <- proc.time()
      log4r::debug(logger, paste("- READING - Downloaded CSV", file_path))
      df_temp <- read.csv(file_path, stringsAsFactors=FALSE)
      if(nrow(df_temp) > 0) {
        df_temp$short_code <- short_id
        df_return <- merge(df_return, df_temp, all = TRUE)
        if(first_loop == TRUE) {
          first_loop <- FALSE
          df_return <- merge(df_return, df_temp, all = TRUE)
        }
      }
      frstop_time <- proc.time() - frstart_time
      log4r::debug(logger, paste("- COMPLETE - Downloaded CSV", file_path, "- Elapsed:", frstop_time[3], "s"))
    } else {
      log4r::error(logger, paste("The file", file_path, "doesn't exist!"))
    }
  }
  return(df_return)
}

# Write RAW Course List
save_local_csv <- function(data_frame, file_name, data_folder) {
  frstart_time <- proc.time()
  file_path <- paste("./", data_folder, "/", file_name, ".csv", sep = "")
  log4r::debug(logger, paste("- WRITING - Local File", file_path))
  write.csv(data_frame, file_path, row.names = FALSE)
  frstop_time <- proc.time() - frstart_time
  log4r::debug(logger, paste("- COMPLETE - Local File", file_path, "- Elapsed:", frstop_time[3], "s"))
}

##### CUSTOM COURSES CSV READ Operations #####

# LIST custom CSV file (not FL provided)
load_from_csv_course_list <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_from_csv_course_list")

  course_list <- load_local_csv(DATA_DOWNLOADED, FILE_COURSELIST)

  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_from_csv_course_list - Elapsed:", fstop_time[3], "s"))
  return(course_list)
}


# DETAILS custom CSV file (not FL provided)
load_from_csv_course_details <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_from_csv_course_details")

  course_details <- load_local_csv(DATA_DOWNLOADED, FILE_COURSEDETAILS)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_from_csv_course_details - Elapsed:", fstop_time[3], "s"))
  return(course_details)
}


##### FutureLearn Downloaded CSV READ Operations #####

# Downloaded Comments CSV Load
load_downloaded_comments <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_comments")

  dwnld_comments <- load_merge_downloaded_csvs(code_list, DATA_DOWNLOADED, SUFFIX_COMMENTS)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_comments - Elapsed:", fstop_time[3], "s"))
  return(dwnld_comments)
}


# Downloaded Enrolments CSV Load
load_downloaded_enrolments <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_enrolments")

  dwnld_enrolments <- load_merge_downloaded_csvs(code_list, DATA_DOWNLOADED, SUFFIX_ENROLMENTS)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_enrolments - Elapsed:", fstop_time[3], "s"))
  return(dwnld_enrolments)
}


# Downloaded Peer Review Assignments CSV Load
load_downloaded_peer_review_assignments <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_peer_review_assignments")

  dwnld_peer_review_assignments <- load_merge_downloaded_csvs(code_list, DATA_DOWNLOADED, SUFFIX_PRASSIGNMENTS)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_peer_review_assignments - Elapsed:", fstop_time[3], "s"))
  return(dwnld_peer_review_assignments)
}


# Downloaded Peer Review Reviews CSV Load
load_downloaded_peer_review_reviews <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_peer_review_reviews")

  dwnld_peer_review_reviews <- load_merge_downloaded_csvs(code_list, DATA_DOWNLOADED, SUFFIX_PRREVIEWS)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_peer_review_reviews - Elapsed:", fstop_time[3], "s"))
  return(dwnld_peer_review_reviews)
}


# Downloaded Question Response CSV Load
load_downloaded_question_response <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_question_response")

  dwnld_question_response <- load_merge_downloaded_csvs(code_list, DATA_DOWNLOADED, SUFFIX_QUESTIONRESPONSE)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_question_response - Elapsed:", fstop_time[3], "s"))
  return(dwnld_question_response)
}


# Downloaded Step Activity CSV Load
load_downloaded_step_activity <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_step_activity")

  dwnld_step_activity <- load_merge_downloaded_csvs(code_list, DATA_DOWNLOADED, SUFFIX_STEPACTIVITY)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_step_activity - Elapsed:", fstop_time[3], "s"))
  return(dwnld_step_activity)
}


##### RAW Data WRITE Operations #####



save_raw_course_list <- function(df_to_save) {
  fstart_time <- proc.time()
  info(logger, "- START - save_raw_course_list")
  if(exists("df_to_save")) {
    save_local_csv(df_to_save, FILE_COURSELIST, DATA_RAW)
  } else {
    log4r::error(logger, "The Data Frame to save doesn't exist!")
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - save_raw_course_list - Elapsed:", fstop_time[3], "s"))
}

save_raw_course_details <- function(df_to_save) {
  fstart_time <- proc.time()
  info(logger, "- START - save_raw_course_details")
  if(exists("df_to_save")) {
    save_local_csv(df_to_save, FILE_COURSEDETAILS, DATA_RAW)
  } else {
    log4r::error(logger, "The Data Frame to save doesn't exist!")
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - save_raw_course_details - Elapsed:", fstop_time[3], "s"))
}

save_raw_comments <- function(df_to_save) {
  fstart_time <- proc.time()
  info(logger, "- START - save_raw_comments")
  if(exists("df_to_save")) {
    save_local_csv(df_to_save, SUFFIX_COMMENTS, DATA_RAW)
  } else {
    log4r::error(logger, "The Data Frame to save doesn't exist!")
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - save_raw_comments - Elapsed:", fstop_time[3], "s"))
}

save_raw_enrolments <- function(df_to_save) {
  fstart_time <- proc.time()
  info(logger, "- START - save_raw_enrolments")
  if(exists("df_to_save")) {
    save_local_csv(df_to_save, SUFFIX_ENROLMENTS, DATA_RAW)
  } else {
    log4r::error(logger, "The Data Frame to save doesn't exist!")
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - save_raw_enrolments - Elapsed:", fstop_time[3], "s"))
}

save_raw_pr_assignments <- function(df_to_save) {
  fstart_time <- proc.time()
  info(logger, "- START - save_raw_pr_assignments")
  if(exists("df_to_save")) {
    save_local_csv(df_to_save, SUFFIX_PRASSIGNMENTS, DATA_RAW)
  } else {
    log4r::error(logger, "The Data Frame to save doesn't exist!")
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - save_raw_pr_assignments - Elapsed:", fstop_time[3], "s"))
}

save_raw_pr_reviews <- function(df_to_save) {
  fstart_time <- proc.time()
  info(logger, "- START - save_raw_pr_reviews")
  if(exists("df_to_save")) {
    save_local_csv(df_to_save, SUFFIX_PRREVIEWS, DATA_RAW)
  } else {
    log4r::error(logger, "The Data Frame to save doesn't exist!")
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - save_raw_pr_reviews - Elapsed:", fstop_time[3], "s"))
}

save_raw_question_response <- function(df_to_save) {
  fstart_time <- proc.time()
  info(logger, "- START - save_raw_question_response")
  if(exists("df_to_save")) {
    save_local_csv(df_to_save, SUFFIX_QUESTIONRESPONSE, DATA_RAW)
  } else {
    log4r::error(logger, "The Data Frame to save doesn't exist!")
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - save_raw_question_response - Elapsed:", fstop_time[3], "s"))
}

save_raw_step_activity <- function(df_to_save) {
  fstart_time <- proc.time()
  info(logger, "- START - save_raw_step_activity")
  if(exists("df_to_save")) {
    save_local_csv(df_to_save, SUFFIX_STEPACTIVITY, DATA_RAW)
  } else {
    log4r::error(logger, "The Data Frame to save doesn't exist!")
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - save_raw_step_activity - Elapsed:", fstop_time[3], "s"))
}


##### RAW Data READ Operations #####



load_raw_course_list <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_raw_course_list")
  
  raw_course_list <- load_local_csv(DATA_RAW, FILE_COURSELIST)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_raw_course_list - Elapsed:", fstop_time[3], "s"))
  return(raw_course_list)
}

load_raw_course_details <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_raw_course_details")
  
  raw_course_details <- load_local_csv(DATA_RAW, FILE_COURSEDETAILS)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_raw_course_details - Elapsed:", fstop_time[3], "s"))
  return(raw_course_details)
}

load_raw_comments <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_raw_comments")
  
  raw_comments <- load_local_csv(DATA_RAW, SUFFIX_COMMENTS)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_raw_comments - Elapsed:", fstop_time[3], "s"))
  return(raw_comments)
}

load_raw_enrolments <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_raw_enrolments")
  
  raw_enrolments <- load_local_csv(DATA_RAW, SUFFIX_ENROLMENTS)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_raw_enrolments - Elapsed:", fstop_time[3], "s"))
  return(raw_enrolments)
}

load_raw_peer_review_assignments <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_raw_peer_review_assignments")
  
  raw_pr_assignments <- load_local_csv(DATA_RAW, SUFFIX_PRASSIGNMENTS)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_raw_peer_review_assignments - Elapsed:", fstop_time[3], "s"))
  return(raw_pr_assignments)
}

load_raw_peer_review_reviews <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_raw_peer_review_reviews")
  
  raw_pr_reviews <- load_local_csv(DATA_RAW, SUFFIX_PRREVIEWS)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_raw_peer_review_reviews - Elapsed:", fstop_time[3], "s"))
  return(raw_pr_reviews)
}

load_raw_question_response <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_raw_question_response")
  
  raw_question_response <- load_local_csv(DATA_RAW, SUFFIX_QUESTIONRESPONSE)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_raw_question_response - Elapsed:", fstop_time[3], "s"))
  return(raw_question_response)
}

load_raw_step_activity <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_raw_step_activity")
  
  raw_step_activity <- load_local_csv(DATA_RAW, SUFFIX_STEPACTIVITY)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_raw_step_activity - Elapsed:", fstop_time[3], "s"))
  return(raw_step_activity)
}
