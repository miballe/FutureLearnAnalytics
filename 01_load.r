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


########## Script wide constants ##########
DATA_DOWNLOADED = "Data_Downloaded"
DATA_RAW = "Data_Raw"
DATA_CLEAN = "Data_Clean"
DATA_TRANSFORMED = "Data_Transformed"
FILE_COURSELIST = "course-list.csv"
FILE_COURSEDETAILS = "course-details.csv"
DEFAULT_NADATE = "2000-01-01 00:00:00 UTC"
SUFFIX_COMMENTS = "comments"
SUFFIX_ENROLMENTS = "enrolments"
SUFFIX_PRASSIGNMENTS = "peer-review-assignments"
SUFFIX_PRREVIEWS = "peer-review-reviews"
SUFFIX_QUESTIONRESPONSE = "question-response"
SUFFIX_STEPACTIVITY = "step-activity"


##### CUSTOM COURSES CSV READ Operations #####

# LIST custom CSV file (not FL provided)
load_from_csv_course_list <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_from_csv_course_list")
  raw_coursel <- data.frame(short_code = character(), full_name = character(),
                             start_date = as.Date(character()), end_date = as.Date(character()),
                             run_number = integer(), department = character() )

  file_name <- paste("./", DATA_DOWNLOADED, "/", FILE_COURSELIST, sep = "")
  log4r::debug(logger, paste("- READING - ", file_name))
  raw_coursel <- read.csv(file_name)
  log4r::debug(logger, paste("- COMPLETE - ", file_name))
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_from_csv_course_list", fstop_time[3], "s"))
  return(raw_coursel)
}


# DETAILS custom CSV file (not FL provided)
load_from_csv_course_details <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - load_from_csv_course_details")
  raw_coursed <- data.frame(short_code = character(), week_number = integer(),
                             step_number = integer(),  material_type = character(),
                             estimated_time = integer(), release_date = as.Date(character()),
                             unavailable_date = as.Date(character()) )
                            
  file_name <- paste("./", DATA_DOWNLOADED, "/", FILE_COURSEDETAILS, sep = "")
  log4r::debug(logger, paste("- READING - ", file_name))
  raw_coursed <- read.csv(file_name)
  log4r::debug(logger, paste("- COMPLETE - ", file_name))
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_from_csv_course_details", fstop_time[3], "s"))
  return(raw_coursed)
}


##### FutureLearn Downloaded CSV READ Operations #####

# Downloaded Comments CSV Load
load_downloaded_comments <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_comments")
  raw_comments <- data.frame(id = integer(), short_code = character(), author_id = character(),
                            parent_id = integer(), step = character(),
                            week_number = integer(), step_number = integer(),
                            text = character(), timestamp = as.Date(character()),
                            moderated = as.Date(character()), likes = integer() )
  
  for(c in code_list) {
    file_name <- paste("./", DATA_DOWNLOADED, "/", c, "_", SUFFIX_COMMENTS, ".csv", sep = "")
    print(file_name)
    if(file.exists(file_name)) {
      log4r::debug(logger, paste("- READING - ", file_name))
      temp_read <- read.csv(file_name)
      if(nrow(temp_read) > 0) {
        log4r::debug(logger, paste("- MERGING - ", file_name))
        temp_read$short_code <- c
        raw_comments <- merge(raw_comments, temp_read, all = TRUE)
      }
      log4r::debug(logger, paste("- COMPLETE - ", file_name))
    }
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_comments", fstop_time[3], "s"))
  return(raw_comments)
}


# Downloaded Enrolments CSV Load
load_downloaded_enrolments <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_enrolments")
  raw_enrolments <- data.frame(short_code = character(), learner_id = character(), 
                             enrolled_at = as.Date(character()), unenrolled_at = character(),
                             role = character(), fully_participated_at = character(),
                             purchased_statemet_at = character(), gender = character(),
                             country = character(), age_range = character(),
                             highest_education_level = character(), employment_status = character(), 
                             employment_area = character())
  
  for(c in code_list) {
    file_name <- paste("./", DATA_DOWNLOADED, "/", c, "_", SUFFIX_ENROLMENTS, ".csv", sep = "")
    if(file.exists(file_name)) {
      log4r::debug(logger, paste("- READING - ", file_name))
      temp_read <- read.csv(file_name)
      if(nrow(temp_read) > 0) {
        log4r::debug(logger, paste("- MERGING - ", file_name))
        temp_read$short_code <- c
        raw_enrolments <- merge(raw_enrolments, temp_read, all = TRUE)
      }
      log4r::debug(logger, paste("- COMPLETE", file_name))
    }
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_enrolments", fstop_time[3], "s"))
  return(raw_enrolments)
}


# Downloaded Peer Review Assignments CSV Load
load_downloaded_peer_review_assignments <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_peer_review_assignments")
  raw_assignments <- data.frame(id = integer(), short_code = character(), step = character(), 
                               week_number = integer(), step_number = integer(),
                               author_id = character(), text = character(),
                               first_viewed_at = character(), submitted_at = character(),
                               moderated = character(), review_count = integer() )
  
  for(c in code_list) {
    file_name <- paste("./", DATA_DOWNLOADED, "/", c, "_", SUFFIX_PRASSIGNMENTS, ".csv", sep = "")
    if(file.exists(file_name)) {
      log4r::debug(logger, paste("- READING - ", file_name))
      temp_read <- read.csv(file_name)
      if(nrow(temp_read) > 0) {
        log4r::debug(logger, paste("- MERGING - ", file_name))
        temp_read$short_code <- c
        raw_assignments <- merge(raw_assignments, temp_read, all = TRUE)
      }
      log4r::debug(logger, paste("- COMPLETE - ", file_name))
    }
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_peer_review_assignments", fstop_time[3], "s"))
  return(raw_assignments)
}


# Downloaded Peer Review Reviews CSV Load
load_downloaded_peer_review_reviews <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_peer_review_reviews")
  raw_reviews <- data.frame(id = integer(), short_code = character(), step = character(), 
                            week_number = integer(), step_number = integer(),
                            reviewer_id = character(), assignment_id = integer(),
                            guideline_one_feedback = character(), guideline_two_feedback = character(),
                            guideline_three_feedback = character(), created_at = character() )
  
  for(c in code_list) {
    file_name <- paste("./", DATA_DOWNLOADED, "/", c, "_", SUFFIX_PRREVIEWS, ".csv", sep = "")
    if(file.exists(file_name)) {
      log4r::debug(logger, paste("- READING - ", file_name))
      temp_read <- read.csv(file_name)
      if(nrow(temp_read) > 0) {
        log4r::debug(logger, paste("- MERGING - ", file_name))
        temp_read$short_code <- c
        raw_reviews <- merge(raw_reviews, temp_read, all = TRUE)
      }
      log4r::debug(logger, paste("- COMPLETE - ", file_name))
    }
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_peer_review_reviews", fstop_time[3], "s"))
  return(raw_reviews)
}


# Downloaded Question Response CSV Load
# The option stringsAsFactors = FALSE was added to avoid a conflict when loading
# some files in sequence which recognize a fixed amount of items as factors and
# later merges fail, fillingn the value as NA
load_downloaded_question_response <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_question_response")
  raw_questions <- data.frame(learner_id = character(), short_code = character(), quiz_question = character(), 
                            week_number = character(), step_number = character(),
                            question_number = character(), response = character(),
                            submitted_at = character(), correct = character() )
  
  for(c in code_list) {
    file_name <- paste("./", DATA_DOWNLOADED, "/", c, "_", SUFFIX_QUESTIONRESPONSE, ".csv", sep = "")
    if(file.exists(file_name)) {
      log4r::debug(logger, paste("- READING - ", file_name))
      temp_read <- read.csv(file_name, stringsAsFactors=FALSE)
      if(nrow(temp_read) > 0) {
        log4r::debug(logger, paste("- MERGING - ", file_name))
        temp_read$short_code <- c
        raw_questions <- merge(raw_questions, temp_read, all = TRUE)
      }
      log4r::debug(logger, paste("- COMPLETE - ", file_name))
    }
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_question_response", fstop_time[3], "s"))
  return(raw_questions)
}


# Downloaded Step Activity CSV Load
load_downloaded_step_activity <- function(code_list) {
  fstart_time <- proc.time()
  info(logger, "- START - load_downloaded_step_activity")
  raw_steps <- data.frame(learner_id = character(), short_code = character(), step = character(), 
                         week_number = character(), step_number = character(),
                         first_visited_at = character(), last_completed_at = character() )
  
  for(c in code_list) {
    file_name <- paste("./", DATA_DOWNLOADED, "/", c, "_", SUFFIX_STEPACTIVITY, ".csv", sep = "")
    if(file.exists(file_name)) {
      log4r::debug(logger, paste("- READING - ", file_name))
      temp_read <- read.csv(file_name, stringsAsFactors=FALSE)
      if(nrow(temp_read) > 0) {
        log4r::debug(logger, paste("- MERGING - ", file_name))
        temp_read$short_code <- c
        raw_steps <- merge(raw_steps, temp_read, all = TRUE)
      }
      log4r::debug(logger, paste("- COMPLETE - ", file_name))
    }
  }
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - load_downloaded_step_activity", fstop_time[3], "s"))
  return(raw_steps)
}


##### RAW Data WRITE Operations #####

# Write RAW Course List
save_raw_course_list <- function() {
  fstart_time <- proc.time()
  info(logger, "- START - save_raw_course_list")
  
  
  log4r::debug(logger, paste("- WRITING - ", file_name))
  write.csv(file_name, stringsAsFactors=FALSE)
  log4r::debug(logger, paste("- COMPLETE - ", file_name))

  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - save_raw_course_list", fstop_time[3], "s"))
}






##### RAW Data READ Operations #####
