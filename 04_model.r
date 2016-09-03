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
filter_default_tables_cut_week <- function(course_list, cut_week) {
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


# This function claculates a boolean variable specifying if there is any activity beyond the
# week prediction end date for each user, and return the data frame with two new fields
# one and two weeks ahead.
get_prediction_weeks_activity <- function(df_prediction, week_prediction) {
  fstart_time <- proc.time()
  log_new_info("- START - filter_default_tables_cut_date")
  
  course_list <- unique(df_prediction$short_code)
  max_total_weeks <- max(df_course_list[df_course_list$short_code %in% course_list, "total_weeks"])
  df_prediction$one_week_ahead <- NA
  df_prediction$two_weeks_ahead <- NA
  df_prediction$fully_participating_learner <- NULL

  for(course_code in course_list) {
    log_new_debug(paste("- Calculating the cut date...", course_code))
    cut_date <- select(df_course_details, short_code, week_number, week_end_date) %>% 
      filter(short_code == course_code & week_number == week_prediction) %>% 
        group_by(short_code) %>% 
          summarize(max_end_date = max(week_end_date)) %>% 
            select(max_end_date)
    cut_date <- as.POSIXct(cut_date$max_end_date[1])
    
    log_new_debug(paste("- Extracting events week 1...", course_code))
    course_events <- df_total_events %>%
                       filter(short_code == course_code & event_date > cut_date & event_date < end_date) %>%
                         group_by(short_code, learner_id) %>%
                           summarize(number_events = n())

    log_new_debug(paste("- Assigning predictor week 1...", course_code))
    course_events$week_one <- 1
    df_prediction <- merge(df_prediction, select(course_events, short_code, learner_id, week_one), c("short_code", "learner_id"), all.x = TRUE)
    df_prediction[!is.na(df_prediction$week_one), "one_week_ahead"] <- df_prediction[!is.na(df_prediction$week_one), "week_one"]
    df_prediction$week_one <- NULL
    
    if(week_prediction + 1 < max_total_weeks) {
      log_new_debug(paste("- Extracting events week 2...", course_code))
      course_events <- df_total_events %>%
                         filter(short_code == course_code &  event_date > cut_date + weeks(1) & event_date < end_date) %>%
                           group_by(short_code, learner_id) %>%
                             summarize(number_events = n())
  
      log_new_debug(paste("- Assigning predictor week 2...", course_code))
      course_events$week_two <- 1
      df_prediction <- merge(df_prediction, select(course_events, short_code, learner_id, week_two), c("short_code", "learner_id"), all.x = TRUE)
      df_prediction[!is.na(df_prediction$week_two), "two_weeks_ahead"] <- df_prediction[!is.na(df_prediction$week_two), "week_two"]
      df_prediction$week_two <- NULL
    }
  }

  df_prediction[is.na(df_prediction$one_week_ahead), "one_week_ahead"] <- 0
  df_prediction[is.na(df_prediction$two_weeks_ahead), "two_weeks_ahead"] <- 0

  # Extracts the cetificate info from the participant facts by obtaining the fully_participating_learner which
  # indicates whether the user is eligible or not for certificate. The term certificate is used in the prediction
  # model to ease the comparison to other studies.
  end_certificate <- select(df_participant_facts, short_code, learner_id, fully_participating_learner) %>%
                       filter(short_code %in% course_list & fully_participating_learner == 1)
  df_prediction <- merge(df_prediction, end_certificate, c("short_code", "learner_id"), all.x = TRUE)
  df_prediction$certificate <- 0
  df_prediction[is.na(df_prediction$fully_participating_learner), "fully_participating_learner"] <- 0  # default value to ensure no NAs are left
  df_prediction$certificate <- df_prediction$fully_participating_learner
  df_prediction$fully_participating_learner <- NULL
  
  if(week_prediction + 1 == max_total_weeks) {
    log_new_info("- Week 2 reached the course max week duration, therefore, certificate value is assigned")
    df_prediction$two_weeks_ahead <- df_prediction$certificate
  }
  
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
  
  features_variables <- c("short_code", "learner_id",
                          # Add variables from this point
                          "days_enrolled_before_start", "unenrolled", "purchased_statement",
                          "max_week", "max_absolute_step", "visited", "completed", "total_likes",
                          "avg_words_comment", "social_interactions_rate", "skipped_content",
                          # Don't modify the folliwing ones
                          "one_week_ahead", "two_weeks_ahead", "certificate")
  
  # Initializes the return object in case the basic conditions are not met
  df_return <- data.frame()
  
  course_info <- select(df_course_list, short_code, start_date, end_date, total_weeks) %>%
                   filter(short_code %in% course_list)
  
  if(length(unique(course_info$total_weeks)) == 1 & course_info$total_weeks[1] >= cut_week + 1) {
    log_new_debug(paste("- The following are the courses to be used as predictive data:", unique(course_info$short_code)))
  } else {
    log_new_error("- No predictive data could be processed. Number of weeks requested not valid.")
    return(df_return)
  }
  
  load_clean_data_file(DATA_CLEAN)                      # Loads again the clean data for a new filtered transformation
  filter_default_tables_cut_week(course_list, cut_week) # Filters the default datasets 
  remove_admin_activity()                               # Removes the non-learner activity
  transform_default_tables()                            # Apply the same transformations as in the general tranformation process
  df_return <- transform_participant_facts()            # Calculates the participants facts with filtered data
  df_return <- get_prediction_weeks_activity(df_return, cut_week)  # Calculates the variables to predict from the total events table
  
  df_return <- df_return[,features_variables] %>% arrange(short_code, learner_id)

  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - get_predictive_data - Elapsed:", fstop_time[3], "s"))
  return(df_return)
}


# In this function the predictive data is partitioned in training and test set, according to the chosen method.
partition_participants_predictive_data <- function(predictive_data, partition_method) {
  fstart_time <- proc.time()
  log_new_info("- START - partition_participants_predictive_data")
  
  list_return <- list()
  random_training_percentage <- 0.75
  course_list <- unique(predictive_data$short_code)
  
  # Verifies if the method is random or a course name. In the second case it is verified against the course list
  # coming in the predictive data. If those conditions are not met, an error is returned.
  if(partition_method == "random") {
    set.seed(71)
    index_training <- createDataPartition(y = predictive_data$certificate, p = random_training_percentage, list = FALSE)
    training_part <- total_data[index_training,]
    test_part <- total_data[-index_training,]
    list_return <- list(training_data = training_part, test_data = test_part)
  } else if(partition_method %in% course_list) {
    course_list <- setdiff(course_list, partition_method)
    training_part <- predictive_data[predictive_data$short_code %in% course_list,]
    test_part <- predictive_data[predictive_data$short_code == partition_method,]
    list_return <- list(training_data = training_part, test_data = test_part)
  } else {
    list_return <- list(error = "Expected \"random\" or a valid course short code within the predictive data")
  }

  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - partition_participants_predictive_data - Elapsed:", fstop_time[3], "s"))
  return(list_return)
}


########## Predictive Models Operations ##########

# Function that creates a Logistic Regression model according to the passed parameters.
# The data structure is expected to have always two identification sets at the beginning (short_code, learner_id)
# and at the end the variables to predict. Everything else in between are predicting features as numeric values.
create_train_participants_model_logreg <- function(training_data, prediction_variable) {
  fstart_time <- proc.time()
  log_new_info("- START - create_train_participants_model_logreg")
  
  columns_to_remove <- c("short_code", "learner_id", "one_week_ahead", "two_weeks_ahead", "certificate")
  
  # TODO: Consider training the model for X-weeks-ahead including the certificate. However, such variable won't be available at test. Check!
  
  if(prediction_variable == "certificate") {
    columns_to_remove <- setdiff(columns_to_remove, "certificate")
    model_return = glm(certificate ~ ., family = binomial(logit), data = training_data[,!names(training_data) %in% columns_to_remove])
    return(model_return)
  } else if(prediction_variable == "one_week_ahead") {
    columns_to_remove <- setdiff(columns_to_remove, "one_week_ahead")
    model_return = glm(one_week_ahead ~ ., family = binomial(logit), data = training_data[,!names(training_data) %in% columns_to_remove])
    return(model_return)
  } else if(prediction_variable == "two_weeks_ahead") {
    columns_to_remove <- setdiff(columns_to_remove, "two_weeks_ahead")
    model_return = glm(two_weeks_ahead ~ ., family = binomial(logit), data = training_data[,!names(training_data) %in% columns_to_remove])
    return(model_return)
  }
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - create_train_participants_model_logreg - Elapsed:", fstop_time[3], "s"))
  return(NULL)
}


# Function that creates an XGBoost model according to the passed parameters.
# The data structure is expected to have always two identification sets at the beginning (short_code, learner_id)
# and at the end the variables to predict. Everything else in between are predicting features as numeric values.
create_train_participants_model_xgboost <- function(training_data, prediction_variable) {
  fstart_time <- proc.time()
  log_new_info("- START - create_train_participants_model_xgboost")
  
  columns_to_remove <- c("short_code", "learner_id", "one_week_ahead", "two_weeks_ahead", "certificate")
  
  # TODO: Consider training the model for X-weeks-ahead including the certificate. However, such variable won't be available at test. Check!
  
  if(prediction_variable == "certificate") {
    model_return = xgboost(data = as.matrix(training_data[,!names(training_data) %in% columns_to_remove]), label = as.matrix(training_data[,"certificate"]), eval_metric = "auc", max_depth = 8, nrounds = 10, objective = "binary:logistic")
    return(model_return)
  } else if(prediction_variable == "one_week_ahead") {
    model_return = xgboost(data = as.matrix(training_data[,!names(training_data) %in% columns_to_remove]), label = as.matrix(training_data[,"one_week_ahead"]), eval_metric = "auc", max_depth = 8, nrounds = 2, objective = "binary:logistic")
    return(model_return)
  } else if(prediction_variable == "two_weeks_ahead") {
    model_return = xgboost(data = as.matrix(training_data[,!names(training_data) %in% columns_to_remove]), label = as.matrix(training_data[,"two_weeks_ahead"]), eval_metric = "auc", max_depth = 8, nrounds = 2, objective = "binary:logistic")
    return(model_return)
  }
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - create_train_participants_model_xgboost - Elapsed:", fstop_time[3], "s"))
  return(NULL)
}

predict_participants_model <- function(prediction_model, test_data, prediction_variable, model_type) {
  fstart_time <- proc.time()
  log_new_info("- START - predict_participants_model")
  
  columns_to_remove <- c("short_code", "learner_id", "one_week_ahead", "two_weeks_ahead", "certificate")
  
  if(model_type == "logreg"){
    predicted_data = data.frame(variable_prob = predict(prediction_model, newdata = test_data[,!names(test_data) %in% columns_to_remove], type="response"))
  } else if(model_type == "xgboost") {
    xgb_test_data <- as.matrix(test_data[,!names(test_data) %in% columns_to_remove])
    predicted_data = data.frame(variable_prob = predict(prediction_model, newdata = xgb_test_data))
  }
  predicted_data$short_code <- test_data$short_code
  predicted_data$learner_id <- test_data$learner_id

  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - predict_participants_model - Elapsed:", fstop_time[3], "s"))
  return(predicted_data)
}


# Function to test the models
calculate_models_predictions <- function() {
  fstart_time <- proc.time()
  log_new_info("- START - manually_executed_prediction")
  
  course_list <- c("research-project-1", "research-project-2", "research-project-3", "research-project-4", "research-project-5")
  predictive_variables <- c("certificate", "one_week_ahead", "two_weeks_ahead")
  short_name <- "rp"
  test_set_partition <- "research-project-5"
  
  log_new_debug("- Starting the new prediction results collection")
  for(n_week in 1:7) {
    log_new_debug(paste("- Processing predictive data+models for the week", n_week))
    for(p_variable in predictive_variables){
      log_new_debug(paste("- Processing predictive data+models for the dependent variable", p_variable))
      # if the predictive data is already in the list, the item is returned. Otherwise, it is fully calculated.
      if(paste(short_name, "_w", n_week, sep = "") %in% names(predictive_data)) {
        log_new_debug("- Predictive Data - Found in the global list")
        total_data <- predictive_data[[ paste(short_name, "_w", n_week, sep = "") ]]
      } else {
        log_new_debug("- Predictive Data - Calculating...")
        total_data <- get_participants_predictive_data(course_list, n_week)
        predictive_data[[ length(predictive_data) + 1 ]] <<- total_data
        names(predictive_data)[length(predictive_data)] <<- paste(short_name, "_w", n_week, sep = "")
        log_new_debug("- Predictive Data - Added to the global list")
      }
      # Partitions the predictive data in Training and Test set, both contained in a list
      log_new_debug(paste("- Data with", nrow(total_data), "observations to be partitioned by", test_set_partition))
      list_data <- partition_participants_predictive_data(total_data, test_set_partition) 
      log_new_debug(paste("- Training a new LogReg model to predict", p_variable, "with", nrow(list_data$training_data), "observations"))
      
      # Trains the LogReg model for the combination week-variable and then saves it in the global model list
      pred_model <- create_train_participants_model_logreg(list_data$training_data, p_variable)
      predictive_models[[ length(predictive_models) + 1 ]] <<- pred_model
      names(predictive_models)[length(predictive_models)] <<- paste(short_name, "_w", n_week, "_logreg_", p_variable, "_model", sep = "")
      # Tests the LogReg model for the combination week-variable and saves the result in the global model list
      predicted_values <- predict_participants_model(pred_model, list_data$test_data, p_variable, "logreg")
      predictive_models[[ length(predictive_models) + 1 ]] <<- predicted_values
      names(predictive_models)[length(predictive_models)] <<- paste(short_name, "_w", n_week, "_logreg_", p_variable, "_prediction", sep = "")
    
      # Trains the LogReg model for the combination week-variable and then saves it in the global model list
      pred_model <- create_train_participants_model_xgboost(list_data$training_data, p_variable)
      predictive_models[[ length(predictive_models) + 1 ]] <<- pred_model
      names(predictive_models)[length(predictive_models)] <<- paste(short_name, "_w", n_week, "_xgboost_", p_variable, "_model", sep = "")
      # Tests the LogReg model for the combination week-variable and saves the result in the global model list
      predicted_values <- predict_participants_model(pred_model, list_data$test_data, p_variable, "xgboost")
      predictive_models[[ length(predictive_models) + 1 ]] <<- predicted_values
      names(predictive_models)[length(predictive_models)] <<- paste(short_name, "_w", n_week, "_xgboost_", p_variable, "_prediction", sep = "")
    }
  }
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - manually_executed_prediction - Elapsed:", fstop_time[3], "s"))
}


calculate_allweeks_model_roc <- function(pred_variable, model_type) {
  fstart_time <- proc.time()
  log_new_info("- START - calculate_allweeks_model_roc")
  
  for(n_week in 1:7) {
    pred_name <- paste("rp_w", n_week, "_", model_type, "_", pred_variable, "_prediction", sep = "")
    prediction_data <- predictive_models[[pred_name]]
    test_data <- predictive_data[[paste("rp_w", n_week, sep = "")]]
    ftest_data <- test_data[test_data$short_code == "research-project-5", pred_variable]
    cut_values <- seq(.01,.99,length=1000)
    conf_matrix_values <- data.frame(cut_values)
    conf_matrix_values$sensitivity <- 0
    conf_matrix_values$specificity <- 0
    conf_matrix_values$dropout <- 0
    conf_matrix_values$dist <- 0
    for(cvalue in 1:length(cut_values)) {
      predicted_values <- ifelse(prediction_data$variable_prob > cut_values[[cvalue]], 1, 0)
      conf_matrix <- confusionMatrix(data = predicted_values, reference = ftest_data)
      conf_matrix_values[cvalue, "sensitivity"] <- conf_matrix[[4]][[1]]
      conf_matrix_values[cvalue, "specificity"] <- conf_matrix[[4]][[2]]
      conf_matrix_values[cvalue, "dropout"] <- (conf_matrix[[2]][[1]] + conf_matrix[[2]][[3]]) / (conf_matrix[[2]][[1]] + conf_matrix[[2]][[2]] + conf_matrix[[2]][[3]] + conf_matrix[[2]][[4]])
      conf_matrix_values[cvalue, "dist"] <- sqrt( (1 - conf_matrix_values[cvalue, "sensitivity"])^2 + (1 - conf_matrix_values[cvalue, "specificity"])^2 )
    }
    predictive_rocs[[ length(predictive_rocs) + 1 ]] <<- conf_matrix_values
    names(predictive_rocs)[length(predictive_rocs)] <<- paste(model_type, "_", pred_variable, "_w", n_week, sep = "")
  }
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - calculate_allweeks_model_roc - Elapsed:", fstop_time[3], "s"))
}



calculate_all_rocs <- function() {
  fstart_time <- proc.time()
  log_new_info("- START - calculate_all_rocs")
  
  model_types <- c("logreg", "xgboost")
  pred_variables <- c("certificate", "one_week_ahead", "two_weeks_ahead")
  
  for(m_type in model_types) {
    for(p_variable in pred_variables) {
      calculate_allweeks_model_roc(p_variable, m_type)
    }
  }
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - calculate_all_rocs - Elapsed:", fstop_time[3], "s"))
}



plot_prediction_model_roc_allweeks <- function(model_type, pred_variable) {

  gp <- ggplot() + 
          theme_stata() + 
          ggtitle(paste("All Weeks ROC for", model_type, "-", pred_variable))
  legend_text <- c()
  
  for(n_week in 1:7) {
    item_name <- paste(model_type, "_", pred_variable, "_w", n_week, sep = "")
    roc_data <- predictive_rocs[[item_name]]
    week_name <- paste("Week_", n_week)
    gp <- gp + geom_line(data = roc_data, aes(x = 1-specificity, y = sensitivity, linetype = "Week"), linetype = n_week, size = 1)
    legend_text <- c(legend_text, week_name = n_week)
    # ggplot(conf_matrix_values, aes(1 - specificity)) + 
    # conf_matrix_values$abs_diff <- abs(conf_matrix_values$sensitivity - conf_matrix_values$specificity)
    # conf_matrix_values$dropout_diff <- abs(conf_matrix_values$dropout - dropout_reference)
    # min_cut_value <- which.min(conf_matrix_values$abs_diff)
    # min_dropout_diff <- which.min(conf_matrix_values$dropout_diff)
    # min_dist <- which.min(conf_matrix_values$dist)
  
  }
  gp <- gp + labs(x = "False Positives Rate", y = "True Positives Rate") +
    scale_linetype_stata("Week", legend_text)
    
  return(gp)
}


generate_allrocs_rp <- function() {
  fstart_time <- proc.time()
  log_new_info("- START - generate_allrocs_rp")
  
  model_types <- c("logreg", "xgboost")
  pred_variables <- c("certificate", "one_week_ahead", "two_weeks_ahead")
  
  for(m_type in model_types) {
    for(p_variable in pred_variables) {
      rocp <- plot_prediction_model_roc_allweeks(m_type, p_variable)
      file_name <- paste("./", m_type, "_", p_variable, ".png", sep = "")
      ggsave(filename = file_name, plot = rocp, device = "png")
    }
  }
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - generate_allrocs_rp - Elapsed:", fstop_time[3], "s"))
}


generate_allroc_table_rp <- function() {
  fstart_time <- proc.time()
  log_new_info("- START - generate_allroc_table_rp")
  
  model_types <- c("logreg", "xgboost")
  pred_variables <- c("certificate", "one_week_ahead", "two_weeks_ahead")
  df_return <- data.frame(week_number = 1:7)
  
  for(m_type in model_types) {
    for(p_variable in pred_variables) {
      week_roc <- c()
      for(n_week in 1:7) {
        pred_name <- paste("rp_w", n_week, "_", m_type, "_", p_variable, "_prediction", sep = "")
        roc_probabilities <- predictive_models[[pred_name]]
        roc_probabilities <- roc_probabilities$variable_prob
        data_name <- paste("rp_w", n_week, sep = "")
        roc_reference <- predictive_data[[data_name]]
        roc_reference <- roc_reference[roc_reference$short_code == "research-project-5", p_variable]
        roc_prediction <- prediction(roc_probabilities, roc_reference)
        auc_value <- performance(roc_prediction, measure = "auc")
        week_roc <- c(week_roc, auc_value@y.values[[1]])
      }
      ret_name <- paste(m_type, "_", p_variable, sep = "")
      df_return[, ret_name] <- week_roc
    }
  }
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - generate_allroc_table_rp - Elapsed:", fstop_time[3], "s"))
  return(df_return)
}


manually_generated_plots <- function() {
  plot_prediction_model_roc_allweeks("logreg", "certificate")
  plot_prediction_model_roc_allweeks("logreg", "one_week_ahead")
  plot_prediction_model_roc_allweeks("logreg", "two_weeks_ahead")
  plot_prediction_model_roc_allweeks("xgboost", "certificate")
  plot_prediction_model_roc_allweeks("xgboost", "one_week_ahead")
  plot_prediction_model_roc_allweeks("xgboost", "two_weeks_ahead")
}

plot_prediciton_model_cutoff <- function() {
  item_name <- "xgboost_certificate_w7"
  conf_matrix_values <- predictive_rocs[[item_name]]
  cut_values <- seq(.01,.99,length=1000)
  ggplot(conf_matrix_values, aes(cut_values)) + 
    theme_stata() +
    geom_line(aes(y = sensitivity, color = "Sensitivity", linetype = "Sensitivity"), size = 1) + 
    geom_line(aes(y = specificity, color = "Specificity", linetype = "Specificity"), size = 1) +
    geom_line(aes(y = dropout, color = "Dropout", linetype = "Dropout"), size = 1) +
    geom_line(aes(y = dist, color = "Distance", linetype = "Distance"), size = 1) +
    scale_linetype_manual("", values = c("Sensitivity" = 1, "Specificity" = 3, "Dropout" = 4, "Distance" = 5)) +
    labs(title = "LogRegression Cutoff Analysis", x = "Cut Values", y = "Value", color = "")
}

plot_pred_prob_hist <- function() {
  hist_data <- data.frame(obs_class = predictive_data$rp_w1$certificate)
  ggplot(aes(x = hist_data$obs_class)) +
    theme_stata() +
    geom_histogram(aes(x = obs_class)) +
    labs(title = "Observed classes for Certificate - Week 1", x = "Class", y = "Frequency", color = "")
    
}


########## File read/write Operations ##########
save_models_data_file <- function(file_name) {
  fstart_time <- proc.time()
  log_new_info("- START - save_models_data_file")
  
  objects <- c("predictive_data", "predictive_models", "predictive_rocs")
  
  save(list = objects, file = paste("./", DATA_MODELS, "/", file_name, ".RData", sep = ""))

  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - save_models_data_file - Elapsed:", fstop_time[3], "s"))
}


load_models_data_file <- function(file_name) {
  fstart_time <- proc.time()
  log_new_info("- START - load_models_data_file")
  
  load( file = paste("./", DATA_MODELS, "/", file_name, ".RData", sep = ""), envir = .GlobalEnv )
  
  fstop_time <- proc.time() - fstart_time
  log_new_info(paste("- END - load_models_data_file - Elapsed:", fstop_time[3], "s"))
}

