####################################################
# DAMOOCP data cleaning
####################################################
# The functions inside this file transform the raw read data
# into R data frames to ensure data types are correct, NA and
# NaN values are either replaced or assigned, values are within
# expected ranges and in general the data is ready to get
# reliable and closer to reality statistics. In particular
# there are some files (older datasets) that have a smaller
# schema than the new ones, so the missing values are assigned
# either with a calculation, or simply assigning a default value.


########## Script wide variables ##########
DEFAULT_NADATE = "2000-01-01 00:00:00 UTC"


########## Generic Clean Operations ##########



########## Specific Data Clean Operations ##########
clean_course_list <- function(raw_course_list) {
  fstart_time <- proc.time()
  info(logger, "- START - clean_course_list")
  
  return_df <- data.frame(raw_course_list$short_code)
  return_df$short_code <- factor(raw_course_list$short_code)
  return_df$raw_course_list.short_code <- NULL
  return_df$full_name <- factor(raw_course_list$full_name)
  return_df$start_date <- strptime(raw_course_list$start_date, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$end_date <- strptime(raw_course_list$end_date, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$run_number <- raw_course_list$run_number
  return_df$departmet <- factor(raw_course_list$department)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - clean_course_list - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}

clean_course_details <- function(raw_course_details) {
  fstart_time <- proc.time()
  info(logger, "- START - clean_course_details")
  
  return_df <- data.frame(raw_course_details$short_code)
  return_df$short_code <- factor(raw_course_details$short_code)
  return_df$raw_course_details.short_code <- NULL
  return_df$week_number <- raw_course_details$week_number
  return_df$step_number <- raw_course_details$step_number
  return_df$material_type <- factor(raw_course_details$material_type)
  return_df$estimated_time <- raw_course_details$estimated_time
  return_df$week_start_date <- strptime(raw_course_details$week_start_date, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$week_end_date <- strptime(raw_course_details$week_end_date, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - clean_course_details - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_comments <- function(raw_comments) {
  fstart_time <- proc.time()
  info(logger, "- START - clean_comments")
  
  return_df <- data.frame(raw_comments$id)
  return_df$short_code <- factor(raw_comments$short_code)
  return_df$id <- raw_comments$id
  return_df$raw_comments.id <- NULL
  return_df$author_id <- factor(raw_comments$author_id)
  return_df$parent_id <- factor(raw_comments$parent_id)
  return_df$week_number <- raw_comments$week_number
  return_df$step_number <- raw_comments$step_number
  return_df$text <- raw_comments$text
  return_df$timestamp <- strptime(raw_comments$timestamp, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$moderated <- strptime(raw_comments$moderated, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$likes <- raw_comments$likes

  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - clean_comments - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_enrolments <- function(raw_enrolments) {
  fstart_time <- proc.time()
  info(logger, "- START - clean_enrolments")
  
  return_df <- data.frame(raw_enrolments$learner_id)
  return_df$short_code <- factor(raw_enrolments$short_code)
  return_df$learner_id <- factor(raw_enrolments$learner_id)
  return_df$raw_enrolments.learner_id <- NULL
  return_df$enrolled_at <- strptime(raw_enrolments$enrolled_at, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$role <- factor(raw_enrolments$role)
  return_df$fully_participated_at <- strptime(raw_enrolments$fully_participated_at, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$purchased_statement_at <- strptime(raw_enrolments$purchased_statement_at, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$gender <- factor(raw_enrolments$gender)  # TODO: Remove entries with "nonbinary" and "other"
  return_df$country <- factor(raw_enrolments$country) 
  return_df$age_range <- factor(raw_enrolments$age_range)
  return_df$highest_education_level <- factor(raw_enrolments$highest_education_level)
  return_df$employment_status <- factor(raw_enrolments$employment_status)
  return_df$employment_area <- factor(raw_enrolments$employment_area)
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - clean_enrolments - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_pr_assignments <- function(raw_pr_assignments) {
  fstart_time <- proc.time()
  info(logger, "- START - clean_pr_assignments")
  
  return_df <- data.frame(raw_pr_assignments$id)
  return_df$short_code <- factor(raw_pr_assignments$short_code)
  return_df$id <- raw_pr_assignments$id
  return_df$raw_pr_assignments.id <- NULL
  return_df$week_number <- raw_pr_assignments$week_number
  return_df$step_number <- raw_pr_assignments$step_number
  return_df$author_id <- factor(raw_pr_assignments$author_id)
  return_df$text <- raw_pr_assignments$text
  return_df$first_viewed_at <- strptime(raw_pr_assignments$first_viewed_at, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$submitted_at <- strptime(raw_pr_assignments$submitted_at, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$moderated <- strptime(raw_pr_assignments$moderated, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$review_count <- raw_pr_assignments$review_count

  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - clean_pr_assignments - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_pr_reviews <- function(raw_pr_reviews) {
  fstart_time <- proc.time()
  info(logger, "- START - clean_pr_reviews")
  
  return_df <- data.frame(raw_pr_reviews$id)
  return_df$short_code <- factor(raw_pr_reviews$short_code)
  return_df$id <- raw_pr_reviews$id
  return_df$raw_pr_reviews.id <- NULL
  return_df$week_number <- raw_pr_reviews$week_number
  return_df$step_number <- raw_pr_reviews$step_number
  return_df$reviewer_id <- factor(raw_pr_reviews$reviewer_id)
  return_df$assignment_id <- raw_pr_reviews$assignment_id
  return_df$guideline_one_feedback <- raw_pr_reviews$guideline_one_feedback
  return_df$guideline_two_feedback <- raw_pr_reviews$guideline_two_feedback
  return_df$guideline_three_feedback <- raw_pr_reviews$guideline_three_feedback
  return_df$created_at <- strptime(raw_pr_reviews$created_at, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  
  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - clean_pr_reviews - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_question_response <- function(raw_question_response) {
  fstart_time <- proc.time()
  info(logger, "- START - clean_question_response")
  
  # Removes the lines with empty learner_id. It cannot be recovered.
  raw_question_response <- raw_question_response[raw_question_response$learner_id != "",]
  
  return_df <- data.frame(raw_question_response$learner_id)
  return_df$short_code <- factor(raw_question_response$short_code)
  return_df$learner_id <- factor(raw_question_response$learner_id)
  return_df$raw_question_response.learner_id <- NULL
  return_df$week_number <- raw_question_response$week_number
  return_df$step_number <- raw_question_response$step_number
  return_df$question_number <- raw_question_response$question_number
  return_df$response <- factor(raw_question_response$response)
  return_df$submitted_at <- strptime(raw_question_response$submitted_at, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$correct <- ifelse(tolower(raw_question_response$correct) == "true", TRUE, FALSE)

  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - clean_question_response - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_step_activity <- function(raw_step_activity) {
  fstart_time <- proc.time()
  info(logger, "- START - raw_step_activity")
  
  # Removes the lines with empty learner_id. It cannot be recovered.
  raw_step_activity <- raw_step_activity[raw_step_activity$learner_id != "",]
  
  return_df <- data.frame(raw_step_activity$learner_id)
  return_df$short_code <- factor(raw_step_activity$short_code)
  return_df$learner_id <- factor(raw_step_activity$learner_id)
  return_df$raw_step_activity.learner_id <- NULL
  return_df$week_number <- raw_step_activity$week_number
  return_df$step_number <- raw_step_activity$step_number
  return_df$first_visited_at <- strptime(raw_step_activity$first_visited_at, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  return_df$last_completed_at <- strptime(raw_step_activity$last_completed_at, "%Y-%m-%d %H:%M:%S", tz = "UTC")

  fstop_time <- proc.time() - fstart_time
  info(logger, paste("- END - raw_step_activity - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}
