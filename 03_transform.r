####################################################
# DAMOOCP data Transformation
####################################################
# The functions inside this file modifies data structures by applying
# Feature Engineering techniques


########## Generic Transformation Operations ##########



########## Specific Data Transformation Operations ##########

# This function removes all actions performed by admin users. The only dataset that may require not be filtered
# are the comments. Many of those are support comments in reply of a participant's message, and some other are
# motivation ones. Initial estimations won't take admin comments into account.
remove_admin_activity <- function() {
  fstart_time <- proc.time()
  log_new_info("- START - remove_admin_activity")
  
  admin_users <- select(df_enrolments, learner_id, role) %>% filter(role != "learner")
  admin_users <- unique(admin_users)
  admin_users <- admin_users$learner_id
  df_enrolments <<- df_enrolments[!df_enrolments$learner_id %in% admin_users,]
  df_step_activity <<- df_step_activity[!df_step_activity$learner_id %in% admin_users,]
  df_comments <<- df_comments[!df_comments$author_id %in% admin_users,]
  df_pr_assignments <<- df_pr_assignments[!df_pr_assignments$author_id %in% admin_users,]
  df_pr_reviews <<- df_pr_reviews[!df_pr_reviews$reviewer_id %in% admin_users,]
  df_question_response <<- df_question_response[!df_question_response$learner_id %in% admin_users,]
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - remove_admin_activity - Elapsed:", fstop_time[3], "s"))
}

# Function to calculate additional values to the default tables, by using cross-query results and creating some
# redundancy to the data, so the final transformations are less expensive in CPU.
transform_default_tables <- function() {
  fstart_time <- proc.time()
  log_new_info("- START - transform_default_tables")
  
  # Calculate the total number of participants for course run and store the value in a new field in course_list
  log_new_debug("- Calculating total enrolled...")
  tmp_course <- select(df_enrolments, short_code) %>% group_by(short_code) %>% summarize(total_enrolled = n())
  log_new_debug("- Merging total enrolled...")
  df_course_list <<- merge(df_course_list, tmp_course, all = TRUE)
  df_course_details <<- merge(df_course_details, select(df_course_list, short_code, short_name, total_enrolled), all = TRUE)
  log_new_debug("- Calculating total steps...")
  tmp_course <- select(df_course_details, short_code) %>% group_by(short_code) %>% summarize(total_steps = n())
  log_new_debug("- Merging total steps...")
  df_course_list <<- merge(df_course_list, tmp_course, all = TRUE)
  
  # Adds to the activity table the course level calculated fields
  log_new_debug("- Merging course level data...")
  df_step_activity <<- merge(df_step_activity, select(df_course_list, short_code, total_enrolled, total_steps), all = TRUE)
  
  # Calculates statistic durations for all activities and then stores them as a new field in the course details table
  log_new_debug("- Calculating duration subtotals...")
  duration_summary <- select(df_step_activity, short_code, week_number, step_number, total_seconds) %>%
    group_by(short_code, week_number, step_number) %>% 
    select(short_code, week_number, step_number, total_seconds) %>% 
    summarize(duration_median = as.integer(median(total_seconds, na.rm = TRUE)), 
              duration_mean = as.integer(mean(total_seconds, na.rm = TRUE)))
  log_new_debug("- Merging duration subtotals...")
  df_course_details <<- merge(df_course_details, duration_summary, intersect(names(df_course_details), names(duration_summary)), all = TRUE)
  
  # Adds to the activity table the course details level calculated fields
  log_new_debug("- Merging precalculated fields to activity...")
  df_step_activity <<- merge(df_step_activity, select(df_course_details, short_code, week_number, step_number, week_start_date, week_end_date, duration_median, duration_mean), all = TRUE)
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - transform_default_tables - Elapsed:", fstop_time[3], "s"))
}


# This function creates a new table using the course details as starting point. Some aggregated values are obtained
# from the enrolment and activity tables and summarized in this one as facts of the course. This will help understanding
# the behaviors from the course design perspective.
transform_course_facts <- function() {
  fstart_time <- proc.time()
  log_new_info("- START - transform_course_facts")

  # Creates the base object to return
  log_new_debug("- Creating base return data frame...")
  df_return <- select(df_course_details, short_code, short_name, run_number, week_number, step_number, content_type, duration_estimated, duration_median, duration_mean, week_start_date, week_end_date) %>% 
    arrange(short_code, week_number, step_number)
  df_return$step_absolute_number <- NA
  log_new_debug("- Assigning absolute step numbers...")
  for(course_code in df_course_list$short_code) {
    total_steps <- df_course_list[df_course_list$short_code == course_code,]$total_steps[1]
    df_return[df_return$short_code == course_code,]$step_absolute_number <- c(1:total_steps)
  }
  
  # Calculates features based on activity fields and grouped by course-week-step
  log_new_debug("- Calculating course activity aggregates...")
  course_activity <- df_step_activity %>%
    group_by(short_code, total_enrolled, week_number, step_number) %>%
    summarize(visits_number = n(),
              visited = visits_number,
              completed = sum(!is.na(last_completed_at)),
              seen_before_week = sum(first_visited_at < week_start_date),
              seen_during_week = sum(first_visited_at > week_start_date &  first_visited_at < week_end_date ),
              seen_after_week = sum(first_visited_at > week_end_date) )
  
  log_new_debug("- Calculating activity aggregate proportions...")
  course_activity$visited <- course_activity$visited / course_activity$total_enrolled
  course_activity$completed <- course_activity$completed / course_activity$total_enrolled
  course_activity$seen_before_week <- course_activity$seen_before_week / course_activity$visits_number
  course_activity$seen_during_week <- course_activity$seen_during_week / course_activity$visits_number
  course_activity$seen_after_week <- course_activity$seen_after_week / course_activity$visits_number

  log_new_debug("- Merging activity aggregates...")
  df_return <- merge(df_return, course_activity, intersect(names(df_return), names(course_activity)), all = TRUE)

  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - transform_course_facts - Elapsed:", fstop_time[3], "s"))
  return(df_return)
}


# This function calculates the participant main facts by using the enrolments table as starting point and
# adding calculated values using mainly the activities source with aggregated operations, or finding items
# matching particular conditions.

transform_participant_facts <- function() {
  fstart_time <- proc.time()
  log_new_info("- START - transform_participant_facts")
  
  log_new_debug("- Creating base return data frame...")
  df_return <- select(df_enrolments, short_code, learner_id, enrolled_at, fully_participated_at, purchased_statement_at, gender, country, age_range, highest_education_level, employment_status, employment_area) %>%
                 arrange(short_code, learner_id)
  
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - transform_participant_facts - Elapsed:", fstop_time[3], "s"))
  return(df_return)
}



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



