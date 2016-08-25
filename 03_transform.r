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
  df_course_list <<- merge(df_course_list, tmp_course, all.x = TRUE)
  df_course_details <<- merge(df_course_details, select(df_course_list, short_code, short_name, total_enrolled), all.x = TRUE)
  log_new_debug("- Calculating total steps...")
  tmp_course <- select(df_course_details, short_code) %>% group_by(short_code) %>% summarize(total_steps = n())
  log_new_debug("- Merging total steps...")
  df_course_list <<- merge(df_course_list, tmp_course, all = TRUE)
  log_new_debug("- Assigning absolute step numbers...")
  step_numbering <- df_course_details %>% 
    arrange(short_code, week_number, step_number)
  step_numbering$step_absolute_number <- NA
  for(course_code in df_course_list$short_code) {
    total_steps <- df_course_list[df_course_list$short_code == course_code,]$total_steps[1]
    step_numbering[step_numbering$short_code == course_code,]$step_absolute_number <- c(1:total_steps)
  }
  df_course_details <<- step_numbering
  step_numbering <- NULL
  
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
  df_course_details <<- merge(df_course_details, select(duration_summary, short_code, week_number, step_number, duration_median, duration_mean), all= TRUE)
  
  # Adds to the activity table the course details level calculated fields
  log_new_debug("- Merging precalculated fields to activity...")
  df_step_activity <<- merge(df_step_activity, select(df_course_details, short_code, week_number, step_number, step_absolute_number, week_start_date, week_end_date, duration_estimated, duration_median, duration_mean), all = TRUE)
  
  # Adds to the comments data some calculated fields to ease further aggregations
  log_new_debug("- Calculating comments fields...")
  df_comments$clean_text <<- tolower(df_comments$text)
  df_comments$clean_text <<- removeNumbers(df_comments$clean_text)
  df_comments$clean_text <<- removeWords(df_comments$clean_text, stopwords("english"))
  df_comments$clean_text <<- removePunctuation(df_comments$clean_text, preserve_intra_word_dashes = TRUE)
  df_comments$clean_text <<- stripWhitespace(df_comments$clean_text)
  df_comments$clean_words_number <<- stri_count_fixed(df_comments$clean_text, " ")
  
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
  df_return <- select(df_course_details, short_code, short_name, run_number, week_number, step_number, step_absolute_number, content_type, duration_estimated, duration_median, duration_mean, week_start_date, week_end_date) %>% 
    arrange(short_code, week_number, step_number)

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
  
  # creates the base object to return
  log_new_debug("- Creating base return data frame...")
  df_return <- select(df_enrolments, short_code, learner_id, enrolled_at, unenrolled_at, fully_participated_at, purchased_statement_at, gender, country, age_range, highest_education_level, employment_status, employment_area) %>%
                 arrange(short_code, learner_id)
  
  # Extracts relevant course info data and merges it in the return data frame. This will simplify further calculations.
  log_new_debug("- Adding course info data...")
  course_info <- select(df_course_list, short_code, run_number, short_name, start_date, end_date, total_steps)
  df_return <- merge(df_return, course_info, c("short_code"), all.x = TRUE)
  
  # Extracts activity info grouped by participant (learner_id) and calculate the total visited steps and the max visited week
  log_new_debug("- Adding step activity aggregated values...")
  activity_week_info <- df_step_activity %>% 
                          group_by(short_code, learner_id) %>% 
                            summarize(visited_steps = n(), 
                                      max_week = max(week_number),
                                      max_absolute_step = max(step_absolute_number),
                                      completed_steps = sum(!is.na(last_completed_at)),
                                      seen_before_week = sum(first_visited_at < week_start_date),
                                      seen_during_week = sum(first_visited_at > week_start_date &  first_visited_at < week_end_date ),
                                      seen_after_week = sum(first_visited_at > week_end_date) )
  df_return <- merge(df_return, select(activity_week_info, short_code, learner_id, visited_steps, max_week, max_absolute_step, completed_steps, seen_before_week, seen_during_week, seen_after_week) , all.x = TRUE)
  df_return[is.na(df_return$visited_steps),]$visited_steps <- 0
  df_return[is.na(df_return$max_week),]$max_week <- 0
  df_return[is.na(df_return$completed_steps),]$completed_steps <- 0
  df_return[is.na(df_return$seen_before_week),]$seen_before_week <- 0
  df_return[is.na(df_return$seen_during_week),]$seen_during_week <- 0
  df_return[is.na(df_return$seen_after_week),]$seen_after_week <- 0
  
  # Calculates ratios and indicators with existing fields in the df_return table
  log_new_debug("- Calculating visited, completed and seen metrics...")
  df_return$days_enrolled_before_start <- as.integer(difftime(df_return$start_date, df_return$enrolled_at, units = "days"))
  df_return$purchased_statement <- ifelse(!is.na(df_return$purchased_statement_at), 1, 0)
  df_return$visited <- df_return$visited_steps / df_return$total_steps
  df_return$completed <- df_return$completed_steps / df_return$total_steps
  df_return$unenrolled <- ifelse(!is.na(df_return$unenrolled_at), 1, 0)
  df_return$seen_before_week <- df_return$seen_before_week / df_return$visited_steps
  df_return$seen_during_week <- df_return$seen_during_week / df_return$visited_steps
  df_return$seen_after_week <- df_return$seen_after_week / df_return$visited_steps
  df_return[is.nan(df_return$seen_before_week),]$seen_before_week <- 0
  df_return[is.nan(df_return$seen_during_week),]$seen_during_week <- 0
  df_return[is.nan(df_return$seen_after_week),]$seen_after_week <- 0
  df_return[is.na(df_return$max_absolute_step),]$max_absolute_step <- 0
  
  # Aggregates and calculates social related data
  log_new_debug("- Calculating aggregated social interactions...")
  comments_particpant <- select(df_comments, author_id, short_code, week_number, step_number, likes, clean_words_number)
  names(comments_particpant) <- c("learner_id", "short_code", "week_number", "step_number", "likes", "clean_words_number")
  comments_aggregated <- comments_particpant %>%
                           group_by(learner_id, short_code ) %>%
                             summarize(number_social_interactions = n(),
                                       total_words = sum(clean_words_number),
                                       total_likes = sum(likes),
                                       avg_words_comment = total_words/number_social_interactions)
  df_return <- merge(df_return, select(comments_aggregated, short_code, learner_id, number_social_interactions, total_words, total_likes, avg_words_comment), c("short_code", "learner_id"), all.x = TRUE)
  df_return[is.na(df_return$number_social_interactions),]$number_social_interactions <- 0
  df_return[is.na(df_return$total_words),]$total_words <- 0
  df_return[is.na(df_return$total_likes),]$total_likes <- 0
  df_return[is.na(df_return$avg_words_comment),]$avg_words_comment <- 0
  df_return$social_interactions_rate <- df_return$number_social_interactions / df_return$visited_steps
  # df_return[is.nan(df_return$social_interactions_rate),]$social_interactions_rate <- 0
  df_return[is.nan(df_return$social_interactions_rate),"social_interactions_rate"] <- 0
  df_return$skipped_content <- ifelse(df_return$max_absolute_step != 0, (df_return$max_absolute_step - df_return$visited_steps) / df_return$max_absolute_step, 0)
  
  # Variables to predict
  df_return$fully_participating_learner <- ifelse(df_return$completed > 0.5, 1, 0)
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - transform_participant_facts - Elapsed:", fstop_time[3], "s"))
  return(df_return)
}


get_total_event_dates <- function() {
  fstart_time <- proc.time()
  log_new_info("- START - get_total_event_dates")
  
  df_return <- data.frame()

  tmp_read <- select(df_step_activity, short_code, learner_id, first_visited_at, last_completed_at)
  tmp_subset <- select(tmp_read, short_code, learner_id, first_visited_at)
  names(tmp_subset) <- c("short_code", "learner_id", "event_date")
  tmp_subset$event_type <- "step_visit"
  df_return <- tmp_subset
  tmp_subset <- select(tmp_read, short_code, learner_id, last_completed_at) %>% filter(!is.na(last_completed_at))
  names(tmp_subset) <- c("short_code", "learner_id", "event_date")
  tmp_subset$event_type <- "step_complete"
  df_return <- rbind(df_return, tmp_subset)
  
  tmp_subset <- select(df_comments, short_code, author_id, timestamp)
  names(tmp_subset) <- c("short_code", "learner_id", "event_date")
  tmp_subset$event_type <- "comment"
  df_return <- rbind(df_return, tmp_subset)
  
  tmp_read <- select(df_enrolments, short_code, learner_id, unenrolled_at, fully_participated_at, purchased_statement_at)
  tmp_subset <- select(tmp_read, short_code, learner_id, unenrolled_at) %>% filter(!is.na(unenrolled_at))
  names(tmp_subset) <- c("short_code", "learner_id", "event_date")
  tmp_subset$event_type <- "unenrollment"
  df_return <- rbind(df_return, tmp_subset)
  tmp_subset <- select(tmp_read, short_code, learner_id, fully_participated_at) %>% filter(!is.na(fully_participated_at))
  names(tmp_subset) <- c("short_code", "learner_id", "event_date")
  tmp_subset$event_type <- "certificate"
  df_return <- rbind(df_return, tmp_subset)
  tmp_subset <- select(tmp_read, short_code, learner_id, purchased_statement_at) %>% filter(!is.na(purchased_statement_at))
  names(tmp_subset) <- c("short_code", "learner_id", "event_date")
  tmp_subset$event_type <- "statement"
  df_return <- rbind(df_return, tmp_subset)
  
  tmp_read <- select(df_pr_assignments, short_code, author_id, first_viewed_at, submitted_at)
  tmp_subset <- select(tmp_read, short_code, author_id, first_viewed_at) %>% filter(!is.na(first_viewed_at))
  names(tmp_subset) <- c("short_code", "learner_id", "event_date")
  tmp_subset$event_type <- "assignment_view"
  df_return <- rbind(df_return, tmp_subset)
  tmp_subset <- select(tmp_read, short_code, author_id, submitted_at) %>% filter(!is.na(submitted_at))
  names(tmp_subset) <- c("short_code", "learner_id", "event_date")
  tmp_subset$event_type <- "assignment_submit"
  df_return <- rbind(df_return, tmp_subset)
  
  tmp_subset <- select(df_pr_reviews, short_code, reviewer_id, created_at)
  names(tmp_subset) <- c("short_code", "learner_id", "event_date")
  tmp_subset$event_type <- "assignment_review"
  df_return <- rbind(df_return, tmp_subset)
  
  tmp_subset <- select(df_question_response, short_code, learner_id, submitted_at)
  names(tmp_subset) <- c("short_code", "learner_id", "event_date")
  tmp_subset$event_type <- "question_response"
  df_return <- rbind(df_return, tmp_subset)
  
  df_return <- merge(df_return, select(df_course_list, short_code, start_date, end_date), "short_code", all.x = TRUE)
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - get_total_event_dates - Elapsed:", fstop_time[3], "s"))
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



