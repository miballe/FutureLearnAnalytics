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


########## Specific Data Clean Operations ##########
clean_course_list <- function(raw_course_list) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_course_list")
  
  # Data type conversions
  return_df <- data.frame(raw_course_list$short_code)
  return_df$short_code <- factor(raw_course_list$short_code)
  return_df$raw_course_list.short_code <- NULL
  return_df$run_number <- raw_course_list$run_number
  return_df$short_name <- factor(raw_course_list$short_name)
  return_df$full_name <- factor(raw_course_list$full_name)
  return_df$start_date <- as.POSIXct(strptime(raw_course_list$start_date, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$end_date <- as.POSIXct(strptime(raw_course_list$end_date, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$institution <- factor(raw_course_list$institution)
  return_df$departmet <- factor(raw_course_list$department)
  
  # Verifications and default assignments
  return_df$total_weeks <- as.integer(difftime(raw_course_list$end_date, raw_course_list$start_date, units = "weeks"))
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_course_list - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_course_details <- function(raw_course_details) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_course_details")
  
  # Data type conversions
  return_df <- data.frame(raw_course_details$short_code)
  return_df$short_code <- factor(raw_course_details$short_code)
  return_df$raw_course_details.short_code <- NULL
  return_df$run_number <- raw_course_details$run_number
  return_df$week_number <- raw_course_details$week_number
  return_df$step_number <- raw_course_details$step_number
  return_df$content_type <- factor(raw_course_details$content_type)
  return_df$duration_estimated <- raw_course_details$duration_estimated
  return_df$week_start_date <- as.POSIXct(strptime(raw_course_details$week_start_date, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$week_end_date <- as.POSIXct(strptime(raw_course_details$week_end_date, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  
  # Verifications and default assignments
  return_df$total_days <- as.integer(difftime(raw_course_details$week_end_date, raw_course_details$week_start_date, units = "days"))
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_course_details - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_enrolments <- function(raw_enrolments) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_enrolments")
  
  # Data type conversions
  return_df <- data.frame(raw_enrolments$learner_id)
  return_df$short_code <- factor(raw_enrolments$short_code)
  return_df$learner_id <- factor(raw_enrolments$learner_id)
  return_df$raw_enrolments.learner_id <- NULL
  return_df$enrolled_at <- as.POSIXct(strptime(raw_enrolments$enrolled_at, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$unenrolled_at <- as.POSIXct(strptime(raw_enrolments$unenrolled_at, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$role <- factor(raw_enrolments$role)
  return_df$fully_participated_at <- as.POSIXct(strptime(raw_enrolments$fully_participated_at, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$purchased_statement_at <- as.POSIXct(strptime(raw_enrolments$purchased_statement_at, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$gender <- factor(raw_enrolments$gender)  # TODO: Remove entries with "nonbinary" and "other"
  return_df$country <- factor(raw_enrolments$country) 
  return_df$age_range <- factor(raw_enrolments$age_range)
  return_df$highest_education_level <- factor(raw_enrolments$highest_education_level)
  return_df$employment_status <- factor(raw_enrolments$employment_status)
  return_df$employment_area <- factor(raw_enrolments$employment_area)
  
  # Verifications and default assignments
  return_df[is.na(return_df$gender),c("gender")] <- "Unknown"
  return_df[is.na(return_df$country),c("country")] <- "Unknown"
  return_df[is.na(return_df$age_range),c("age_range")] <- "Unknown"
  return_df[is.na(return_df$highest_education_level),c("highest_education_level")] <- "Unknown"
  return_df[is.na(return_df$employment_status),c("employment_status")] <- "Unknown"
  return_df[is.na(return_df$employment_area),c("employment_area")] <- "Unknown"
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_enrolments - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_step_activity <- function(raw_step_activity) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_step_activity")
  
  # Removes the lines with empty learner_id. It cannot be recovered.
  raw_step_activity <- raw_step_activity[raw_step_activity$learner_id != "",]
  
  # Data type conversions
  return_df <- data.frame(raw_step_activity$learner_id)
  return_df$short_code <- factor(raw_step_activity$short_code)
  return_df$learner_id <- factor(raw_step_activity$learner_id)
  return_df$raw_step_activity.learner_id <- NULL
  return_df$week_number <- raw_step_activity$week_number
  return_df$step_number <- raw_step_activity$step_number
  return_df$first_visited_at <- as.POSIXct(strptime(raw_step_activity$first_visited_at, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$last_completed_at <- as.POSIXct(strptime(raw_step_activity$last_completed_at, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  
  # Verification and default assignments
  # observations_with_no_dates <- raw_step_activity[is.na(raw_step_activity$first_visited_at) & is.na(raw_step_activity$last_completed_at),]
  # learners_with_no_dates <- unique(as.vector(observations_with_no_dates$learner_id))
  return_df$total_seconds <- as.integer(difftime(return_df$last_completed_at, return_df$first_visited_at, units = "secs"))
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_step_activity - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_comments <- function(raw_comments) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_comments")
  
  # Data type conversions
  return_df <- data.frame(raw_comments$id)
  return_df$short_code <- factor(raw_comments$short_code)
  return_df$id <- raw_comments$id
  return_df$raw_comments.id <- NULL
  return_df$author_id <- factor(raw_comments$author_id)
  return_df$parent_id <- factor(raw_comments$parent_id)
  return_df$week_number <- raw_comments$week_number
  return_df$step_number <- raw_comments$step_number
  return_df$text <- raw_comments$text
  return_df$timestamp <- as.POSIXct(strptime(raw_comments$timestamp, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$moderated <- as.POSIXct(strptime(raw_comments$moderated, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$likes <- raw_comments$likes

  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_comments - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_pr_assignments <- function(raw_pr_assignments) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_pr_assignments")
  
  # Data type conversions
  return_df <- data.frame(raw_pr_assignments$id)
  return_df$short_code <- factor(raw_pr_assignments$short_code)
  return_df$id <- raw_pr_assignments$id
  return_df$raw_pr_assignments.id <- NULL
  return_df$week_number <- raw_pr_assignments$week_number
  return_df$step_number <- raw_pr_assignments$step_number
  return_df$author_id <- factor(raw_pr_assignments$author_id)
  return_df$text <- raw_pr_assignments$text
  return_df$first_viewed_at <- as.POSIXct(strptime(raw_pr_assignments$first_viewed_at, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$submitted_at <- as.POSIXct(strptime(raw_pr_assignments$submitted_at, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$moderated <- as.POSIXct(strptime(raw_pr_assignments$moderated, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$review_count <- raw_pr_assignments$review_count

  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_pr_assignments - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_pr_reviews <- function(raw_pr_reviews) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_pr_reviews")
  
  # Data type conversions
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
  return_df$created_at <- as.POSIXct(strptime(raw_pr_reviews$created_at, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_pr_reviews - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


clean_question_response <- function(raw_question_response) {
  fstart_time <- proc.time()
  log_new_info("- START - clean_question_response")
  
  # Removes the lines with empty learner_id. It cannot be recovered.
  raw_question_response <- raw_question_response[raw_question_response$learner_id != "",]
  
  # Data type conversions
  return_df <- data.frame(raw_question_response$learner_id)
  return_df$short_code <- factor(raw_question_response$short_code)
  return_df$learner_id <- factor(raw_question_response$learner_id)
  return_df$raw_question_response.learner_id <- NULL
  return_df$week_number <- raw_question_response$week_number
  return_df$step_number <- raw_question_response$step_number
  return_df$question_number <- raw_question_response$question_number
  return_df$response <- factor(raw_question_response$response)
  return_df$submitted_at <- as.POSIXct(strptime(raw_question_response$submitted_at, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  return_df$correct <- ifelse(tolower(raw_question_response$correct) == "true", TRUE, FALSE)

  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - clean_question_response - Elapsed:", fstop_time[3], "s"))
  return(return_df)
}


########## Clean File Operations ##########

save_clean_data_file <- function(objects, file_name) {
  fstart_time <- proc.time()
  log_new_info("- START - save_clean_data_file")
  
  save(list = objects, file = paste("./", DATA_CLEAN, "/", file_name, ".RData", sep = ""))
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - save_clean_data_file - Elapsed:", fstop_time[3], "s"))
}


load_clean_data_file <- function(file_name) {
  fstart_time <- proc.time()
  log_new_info("- START - load_clean_data_file")
  
  load( file = paste("./", DATA_CLEAN, "/", file_name, ".RData", sep = ""), envir = .GlobalEnv )
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - load_clean_data_file - Elapsed:", fstop_time[3], "s"))
}

########## Clean Notebook Specific Functions ##########

get_course_list <- function() {
  ret_value <- select(df_course_list, short_code, start_date, end_date, total_weeks)
  return(ret_value)
}

get_unexpected_course_details <- function() {
  ret_value <- select(df_course_details, short_code, week_number, step_number, week_start_date, week_end_date, total_days) %>% 
                 filter(total_days != 7)
  return(ret_value)  
}

summary_clean_data <- function() {
  list_dataframes <- ls(pattern =  "^df_.*")
  list_short_codes <- unique(df_course_list$short_code)
  
  count_course_list <- data.frame(table(df_course_list$short_code))
  count_course_details <- data.frame(table(df_course_details$short_code))
  count_enrolments <- data.frame(table(df_enrolments$short_code))
  count_step_activity <- data.frame(table(df_step_activity$short_code))
  count_comments <- data.frame(table(df_comments$short_code))
  count_pr_assignments <- data.frame(table(df_pr_assignments$short_code))
  count_pr_reviews <- data.frame(table(df_pr_reviews$short_code))
  count_question_response <- data.frame(table(df_question_response$short_code))
  
  names(count_course_list) <- c("short_code", "df_course_list")
  names(count_course_details) <- c("short_code", "df_course_details")
  names(count_enrolments) <- c("short_code", "df_enrolments")
  names(count_step_activity) <- c("short_code", "df_step_activity")
  names(count_comments) <- c("short_code", "df_comments")
  names(count_pr_assignments) <- c("short_code", "df_pr_assignments")
  names(count_pr_reviews) <- c("short_code", "df_pr_reviews")
  names(count_question_response) <- c("short_code", "df_question_response")
  
  count_total <- merge(count_course_list, count_course_details, all = TRUE)
  count_total <- merge(count_total, count_enrolments, all = TRUE)
  count_total <- merge(count_total, count_step_activity, all = TRUE)
  count_total <- merge(count_total, count_comments, all = TRUE)
  count_total <- merge(count_total, count_pr_assignments, all = TRUE)
  count_total <- merge(count_total, count_pr_reviews, all = TRUE)
  count_total <- merge(count_total, count_question_response, all = TRUE)
  count_total[is.na(count_total)] <- 0
  count_total
}

plot_weeks_steps_hist <- function(test_short_code) {
  course_steps_times <- df_step_activity[df_step_activity$short_code == test_short_code & !is.na(df_step_activity$total_seconds), c("short_code", "week_number", "step_number", "total_seconds")]
  course_steps_times <- course_steps_times[course_steps_times$week_number == 1 & course_steps_times$step_number < 6,]
  steps_stats <- select(course_steps_times[course_steps_times$short_code == test_short_code,], week_number, step_number, total_seconds) %>% 
    filter(week_number == 1 & step_number < 6) %>%
    group_by(week_number, step_number) %>% select(week_number, step_number, total_seconds) %>% 
    summarize(duration_median = as.integer(median(total_seconds, na.rm = TRUE)), duration_mean = as.integer(mean(total_seconds, na.rm = TRUE)), duration_std = as.integer(sd(total_seconds, na.rm = TRUE)))
  steps_stats <- merge(df_course_details[df_course_details$short_code == test_short_code, c("short_code", "week_number", "step_number", "duration_estimated")], steps_stats, all.y = TRUE)
  course_steps_times$week <- paste(course_steps_times$week_number, "-Week", sep = "")
  steps_stats$week <- paste(steps_stats$week_number, "-Week", sep = "")
  return_plot <- ggplot(course_steps_times) + 
    theme_stata() + 
    geom_histogram(aes(x = log(total_seconds)), color = "grey70", bins = 30) +
    geom_vline(aes(xintercept = log(duration_median)), data = steps_stats, colour = "forestgreen", size = 1) +
    geom_vline(aes(xintercept = log(duration_estimated)), data = steps_stats, linetype = "dashed", colour = "royalblue", size = 1) +
    geom_vline(aes(xintercept = log(duration_mean)), data = steps_stats, linetype = "dotted", colour = "red", size = 1) +
    facet_grid(week ~ step_number, switch = "y") +
    ggtitle(paste("Duration Time Histogram for partial Week-Step Combination \n for", test_short_code)) +
    labs(x = "Duration Seconds (Log scale)", y = "Number of Views")
  return(return_plot)
}

get_courses_count_participants <- function() {
  df_participants <- select(df_enrolments, learner_id, role) %>% 
                         filter(role == "learner") %>% 
                             group_by(learner_id) %>% 
                                 select(learner_id) %>% 
                                     summarise_(number_courses_taken = ~n())
  return(table(df_participants$number_courses_taken))
}

get_unknown_demographics <- function() {
  df_demographics <- select(df_enrolments, short_code, role, gender, country, age_range, highest_education_level, employment_status, employment_area) %>% filter(role == "learner") %>% select(-role)
  df_demographics$gender <- ifelse(df_demographics$gender == "Unknown", 1, 0)
  df_demographics$country <- ifelse(df_demographics$country == "Unknown", 1, 0)
  df_demographics$age_range <- ifelse(df_demographics$age_range == "Unknown", 1, 0)
  df_demographics$highest_education_level <- ifelse(df_demographics$highest_education_level == "Unknown", 1, 0)
  df_demographics$employment_status <- ifelse(df_demographics$employment_status == "Unknown", 1, 0)
  df_demographics$employment_area <- ifelse(df_demographics$employment_area == "Unknown", 1, 0)
  ret_value <- df_demographics %>% group_by(short_code) %>% summarise_each(funs(sum))
  course_count <- count(df_demographics, short_code)
  ret_value <- merge(ret_value, course_count, all = TRUE)
  ret_value[,2:7] <- ret_value[,2:7] / ret_value[,8]
  ret_value$n <- NULL
  return(ret_value)
}

plot_step_activity_daily <- function(course_code){
  tmp_activity <- select(df_step_activity, short_code, first_visited_at) %>% filter(short_code == course_code) %>% arrange(first_visited_at)
  tmp_activity$start_date <- as.Date(tmp_activity$first_visited_at)
  tmp_activity <- select(tmp_activity, first_visited_at, start_date) %>% group_by(start_date) %>% summarize_(day_visits = ~n())
  ggplot(tmp_activity, aes(start_date, day_visits)) + geom_line() +
    ggtitle(paste("Activity per day for", course_code)) +
    labs(x = "Day", y = "Number of Views")
}

get_user_role_activity_before <- function(course_code, reference_date) {
  activity_before <- select(df_step_activity, short_code, learner_id, first_visited_at) %>% filter(short_code == course_code & first_visited_at < as.POSIXct(reference_date))
  unique_user_ids <- unique(activity_before$learner_id)
  unique_user_ids <- data.frame(unique_user_ids)
  names(unique_user_ids) <- c("learner_id")
  users_and_roles <- merge(df_enrolments, unique_user_ids, by = "learner_id")
  users_and_roles <- select(users_and_roles, learner_id, role)
  users_and_roles <- unique(users_and_roles)
  return(users_and_roles)
}


########## Validate Clean Datasets ##########

validate_course_list <- function(df_validate_course_list) {
  fstart_time <- proc.time()
  log_new_info("- START - validate_course_list")
  
  end_gt_start <- ifelse(df_clean_course_list$end_date > df_clean_course_list$start_date, TRUE, FALSE)
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - validate_course_list - Elapsed:", fstop_time[3], "s"))
}

