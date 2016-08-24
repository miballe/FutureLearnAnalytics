####################################################
# DAMOOCP data Modeling
####################################################
# The functions inside this file uses the transformed data to feed
# different data models to understand and predict learning behaviors


########## Generic Transformation Operations ##########

# Updates fields in the parameters (all of type POSIXct) to NA
# when the value is higher than cut_date
update_date_field_na <- function(df_change, fields, cut_date) {
  fstart_time <- proc.time()
  log_new_info("- START - update_date_field_na")
  
  for(field in fields) {
    if(nrow(df_change[!is.na(df_change[,c(field)]) & df_change[,c(field)] > cut_date,]) == 0) {
      log_new_debug(paste("- No records to change for field", field))
      # temp_read[!is.na(temp_read$unenrolled_at) & temp_read$unenrolled_at > cut_date,]$unenrolled_at <- NA
    } else {
      log_new_debug(paste("- Records found to change for field", field))
      df_change[!is.na(df_change[,c(field)]) & df_change[,c(field)] > cut_date, c(field)] <- NA
    }
  }
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - update_date_field_na - Elapsed:", fstop_time[3], "s"))
  return(df_change)
}


# Filters the default data frames by removing all activity, entries or values beyond the cut_week end date
# for the selected course list
filter_default_tables_cut_date <- function(course_list, cut_week) {
  fstart_time <- proc.time()
  log_new_info("- START - filter_default_tables_cut_date")
  
  dfp_enrolments <- data.frame()
  dfp_step_activity <- data.frame()
  dfp_comments <- data.frame()
  dfp_pr_assignments <- data.frame()
  dfp_pr_reviews <- data.frame()
  dfp_question_response <- data.frame()
  
  log_new_info("- Parameters OK, data frames initialized.")
  
  # Filtering courses default variables with the course_list values
  df_course_list <<- df_course_list[df_course_list$short_code %in% course_list,]
  df_course_details <<- df_course_details[df_course_details$short_code %in% course_list,]
  
  # Loop that filters the data for each table, according to the provided week'e end-date
  for(course_code in course_list) {
    log_new_debug(paste("- Filtering data frames for the course", course_code))
    cut_date <- select(df_course_details, short_code, week_number, week_end_date) %>% 
      filter(short_code == course_code & week_number == cut_week) %>% 
      group_by(short_code) %>% 
      summarize(max_end_date = max(week_end_date)) %>% 
      select(max_end_date)
    cut_date <- cut_date$max_end_date[1]
    
    # Filtering enrolments events as per cut_date
    log_new_debug(paste("- Filtering enrolments for", course_code))
    temp_read <- df_enrolments %>% 
      filter(short_code == course_code & enrolled_at < cut_date)
    temp_read <- update_date_field_na(temp_read, c("unenrolled_at", "fully_participated_at", "purchased_statement_at"), cut_date)
    ifelse(nrow(dfp_enrolments) == 0, dfp_enrolments <- temp_read, dfp_enrolments <- rbind(dfp_enrolments, temp_read))
    
    # Filtering activities per cut_date
    log_new_debug(paste("- Filtering activities for", course_code))
    temp_read <- df_step_activity %>% 
      filter(short_code == course_code & first_visited_at < cut_date)
    temp_read <- update_date_field_na(temp_read, c("last_completed_at"), cut_date)
    ifelse(nrow(dfp_step_activity) == 0, dfp_step_activity <- temp_read, dfp_step_activity <- rbind(dfp_step_activity, temp_read))
    
    # Filtering comments per cut_date
    log_new_debug(paste("- Filtering comments for", course_code))
    temp_read <- df_comments %>% 
      filter(short_code == course_code & timestamp < cut_date)
    temp_read <- update_date_field_na(temp_read, c("moderated"), cut_date)
    ifelse(nrow(dfp_comments) == 0, dfp_comments <- temp_read, dfp_comments <- rbind(dfp_comments, temp_read))
    
    # Filtering peer-review assignments per cut_date
    log_new_debug(paste("- Filtering pr-assignments for", course_code))
    temp_read <- df_pr_assignments %>% 
      filter(short_code == course_code & first_viewed_at < cut_date)
    temp_read <- update_date_field_na(temp_read, c("submitted_at", "moderated"), cut_date)
    ifelse(nrow(dfp_pr_assignments) == 0, dfp_pr_assignments <- temp_read, dfp_pr_assignments <- rbind(dfp_pr_assignments, temp_read))
    
    # Filtering peer-review reviews per cut_date
    log_new_debug(paste("- Filtering pr-reviews for", course_code))
    temp_read <- df_pr_reviews %>% 
      filter(short_code == course_code & created_at < cut_date)
    ifelse(nrow(dfp_pr_reviews) == 0, dfp_pr_reviews <- temp_read, dfp_pr_reviews <- rbind(dfp_pr_reviews, temp_read))
    
    # Filtering question response per cut_date
    log_new_debug(paste("- Filtering question-response for", course_code))
    temp_read <- df_question_response %>% 
      filter(short_code == course_code & submitted_at < cut_date)
    ifelse(nrow(dfp_question_response) == 0, dfp_question_response <- temp_read, dfp_question_response <- rbind(dfp_question_response, temp_read))
    
  }
  
  # The default data frames are updated in the Global Environment
  df_enrolments <<- dfp_enrolments
  df_step_activity <<- dfp_step_activity
  df_comments <<- dfp_comments
  df_pr_assignments <<- dfp_pr_assignments
  df_pr_reviews <<- dfp_pr_reviews
  df_question_response <<- dfp_question_response
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - filter_default_tables_cut_date - Elapsed:", fstop_time[3], "s"))
}


# Look for participant's activity after the designated dates for each week and sets a boolean
# value accordingly
get_participant_activity_boolean <- function(participant_entry, field) {
  
  cut_date <- ifelse(field == "one_week_ahead", participant_entry$date_one_week_ahead, participant_entry$date_two_weeks_ahead)
  course_code <- as.character(participant_entry$short_code)
  participant_id <- as.character(participant_entry$learner_id)
  
  df_activity <- select(df_step_activity, short_code, learner_id, first_visited_at, last_completed_at) %>%
                   filter(short_code == course_code & learner_id == participant_id &
                            (first_visited_at > cut_date | last_completed_at > cut_date))
  if(nrow(df_activity) > 0) { return(1) }
  
  # TODO: Test other tables and conditions!    *******************************************
  
  return(0)
}



# This function claculates a boolean variable specifying if there is any activity beyond the
# week prediction end date for each user, and return the data frame with two new fields
# one and two weeks ahead.
get_prediction_weeks_activity <- function(df_prediction, week_prediction) {
  fstart_time <- proc.time()
  log_new_info("- START - filter_default_tables_cut_date")
  
  course_list <- unique(df_prediction$short_code)
  df_prediction$one_week_ahead <- NA
  df_prediction$two_weeks_ahead <- NA
  df_prediction$date_one_week_ahead <- as.POSIXct("1960-01-01")
  df_prediction$date_two_weeks_ahead <- as.POSIXct("1960-01-01")
  
  for(course_code in course_list) {
    log_new_debug(paste("- Calculating the cut date...", course_code))
    cut_date <- select(df_course_details, short_code, week_number, week_end_date) %>% 
      filter(short_code == course_code & week_number == week_prediction) %>% 
        group_by(short_code) %>% 
          summarize(max_end_date = max(week_end_date)) %>% 
            select(max_end_date)
    cut_date <- as.POSIXct(cut_date$max_end_date[1])
    
    df_prediction[df_prediction$short_code == course_code, "date_one_week_ahead"] <- cut_date + weeks(1)
    df_prediction[df_prediction$short_code == course_code, "date_two_weeks_ahead"] <- cut_date + weeks(2)
  }
  
  df_prediction[,"one_week_ahead"] <- apply(df_prediction, 1, FUN = get_participant_activity_boolean, "one_week_ahead")
  df_prediction[,"two_weeks_ahead"] <- apply(df_prediction, 1, FUN = get_participant_activity_boolean, "two_weeks_ahead")
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - filter_default_tables_cut_date - Elapsed:", fstop_time[3], "s"))
  return(df_prediction)
}


########## Predictive Data Operations ##########

# The goal of get_participant_predictive_data is to obtain a dataset with all metrics relevant for 
# predictive models. It's not intended to be used for a single algorithm, but a generic
# method for any of those. The returned dataset is not split between training and test sets
# Those have to later be splitted according to the algorithm requirements. However, the 
# dataset is basically a snapshot of each course's status at the requested week. This with the
# aim to replicate as close as possible the same conditions attempted to be predicted.
#
# This function basically reads again the clean data, filters all activity before the cut date
# and execute the regular transformation functions. Finally calculates the predictive variables
# for the two weeks that follows the passed cut_week.
get_participants_predictive_data <- function(course_list, cut_week) {
  fstart_time <- proc.time()
  log_new_info("- START - get_predictive_data")
  
  # Initializes the return object in case the basic conditions are not met
  df_return <- data.frame()
  
  course_info <- select(df_course_list, short_code, start_date, end_date, total_weeks) %>%
                   filter(short_code %in% course_list)
  
  if(length(unique(course_info$total_weeks)) == 1 & course_info$total_weeks[1] >= cut_week + 2) {
    log_new_debug(paste("- The following are the courses to be used as predictive data:", unique(course_info$short_code)))
  } else {
    return(df_return)
  }
  
  load_clean_data_file(DATA_CLEAN)                      # Loads again the clean data for a new filtered transformation
  filter_default_tables_cut_date(course_list, cut_week) # Filters the default datasets 
  remove_admin_activity()                               # Removes the non-learner activity
  transform_default_tables()                            # Apply the same transformations as in the general tranformation process
  df_return <- transform_participant_facts()            # Calculates the participants facts with filtered data
  
  load_clean_data_file(DATA_CLEAN)                                 # Loads the data again to extract activity beyond the cut_date
  df_return <- get_prediction_weeks_activity(df_return, cut_week)  # Calculates the variables to predict

  # df_return$fully_participating_learner merge with df_participant_facts to get the last outcome
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - get_predictive_data - Elapsed:", fstop_time[3], "s"))
  return(df_return)
}




########## Predictive Models Operations ##########

predict_fpl_logreg <- function() {
  training_courses <- c("research-project-1", "research-project-2", "research-project-3", "research-project-4")
  training_set <- get_participant_facts_training_set(training_courses)
  
  logreg = glm(fully_participating_learner ~ ., family=binomial(logit), data=training_set)
  
  hist(logreg$fitted.values)
  
}






