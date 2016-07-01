####################################################
# DAMOOCP data load
####################################################

########## Script wide variables ##########
FOLDER_DATA = "CoursesData"
FILE_COURSELIST = "courselist.csv"
FILE_COURSEDETAILS = "coursedetails.csv"
DEFAULT_NADATE = "2000-01-01 00:00:00 UTC"
SUFFIX_COMMENTS = "cmts"
SUFFIX_ENROLMENTS = "erts"
SUFFIX_ASSIGNMENTS = "pras"
SUFFIX_REVIEWS = "prrv"
SUFFIX_QUESTION = "qtrp"
SUFFIX_STEPACTIVITY = "stac"


########## Data load operations ##########

# COURSES LIST custom CSV file (not FL provided)
load_fl_courses_list_from_csv <- function() {
  
  raw_coursel <- data.frame(short_code = character(), full_name = character(),
                             start_date = as.Date(character()), end_date = as.Date(character()),
                             run_number = integer(), department = character() )

  file_name <- paste("./", FOLDER_DATA, "/", FILE_COURSELIST, sep = "")
  raw_coursel <- read.csv(file_name)

  return(raw_coursel)
}


# COURSES DETAILS custom CSV file (not FL provided)
load_fl_courses_details_from_csv <- function() {
  
  raw_coursed <- data.frame(short_code = character(), week_number = integer(),
                             step_number = integer(),  material_type = character(),
                             estimated_time = integer(), release_date = as.Date(character()),
                             unavailable_date = as.Date(character()) )
                            
  file_name <- paste("./", FOLDER_DATA, "/", FILE_COURSEDETAILS, sep = "")
  raw_coursed <- read.csv(file_name)
  
  return(raw_coursed)
}


# COMMENTS CSV LOAD
load_fl_comments_from_csv <- function(code_list) {
  
  raw_comments <- data.frame(id = integer(), short_code = character(), author_id = character(),
                            parent_id = integer(), step = character(),
                            week_number = integer(), step_number = integer(),
                            text = character(), timestamp = as.Date(character()),
                            moderated = as.Date(character()), likes = integer() )
  
  for(c in code_list) {
    file_name <- paste("./", FOLDER_DATA, "/", c, "_", SUFFIX_COMMENTS, ".csv", sep = "")
    if(file.exists(file_name)) {
      temp_read <- read.csv(file_name)
      temp_read$short_code <- c
      raw_comments <- merge(raw_comments, temp_read, all = TRUE)
    }
  }
  
  return(raw_comments)
}


# ENROLMENTS CSV LOAD
load_fl_enrolments_from_csv <- function(code_list) {
  
  raw_enrolments <- data.frame(short_code = character(), learner_id = character(), 
                             enrolled_at = as.Date(character()), unenrolled_at = character(),
                             role = character(), fully_participated_at = character(),
                             purchased_statemet_at = character(), gender = character(),
                             country = character(), age_range = character(),
                             highest_education_level = character(), employment_status = character(), 
                             employment_area = character())
  
  for(c in code_list) {
    file_name <- paste("./", FOLDER_DATA, "/", c, "_", SUFFIX_ENROLMENTS, ".csv", sep = "")
    if(file.exists(file_name)) {
      temp_read <- read.csv(file_name)
      temp_read$short_code <- c
      raw_enrolments <- merge(raw_enrolments, temp_read, all = TRUE)
    }
  }
  
  return(raw_enrolments)
}


# ASSIGNMENTS CSV LOAD
load_fl_assignments_from_csv <- function(code_list) {
  
  raw_assignments <- data.frame(id = integer(), short_code = character(), step = character(), 
                               week_number = integer(), step_number = integer(),
                               author_id = character(), text = character(),
                               first_viewed_at = character(), submitted_at = character(),
                               moderated = character(), review_count = integer() )
  
  for(c in code_list) {
    file_name <- paste("./", FOLDER_DATA, "/", c, "_", SUFFIX_ASSIGNMENTS, ".csv", sep = "")
    if(file.exists(file_name)) {
      temp_read <- read.csv(file_name)
      temp_read$short_code <- c
      raw_assignments <- merge(raw_assignments, temp_read, all = TRUE)
    }
  }
  
  return(raw_assignments)
}


# REVIEWS CSV LOAD
load_fl_reviews_from_csv <- function(code_list) {
  
  raw_reviews <- data.frame(id = integer(), short_code = character(), step = character(), 
                            week_number = integer(), step_number = integer(),
                            reviewer_id = character(), assignment_id = integer(),
                            guideline_one_feedback = character(), guideline_two_feedback = character(),
                            guideline_three_feedback = character(), created_at = character() )
  
  for(c in code_list) {
    file_name <- paste("./", FOLDER_DATA, "/", c, "_", SUFFIX_REVIEWS, ".csv", sep = "")
    if(file.exists(file_name)) {
      temp_read <- read.csv(file_name)
      temp_read$short_code <- c
      raw_reviews <- merge(raw_reviews, temp_read, all = TRUE)
    }
  }
  
  return(raw_reviews)
}


# QUESTIONS RESPONSES CSV LOAD
load_fl_questions_from_csv <- function(code_list) {
  
  raw_questions <- data.frame(learner_id = character(), short_code = character(), quiz_question = character(), 
                            week_number = character(), step_number = character(),
                            question_number = character(), response = character(),
                            submitted_at = character(), correct = character() )
  
  for(c in code_list) {
    file_name <- paste("./", FOLDER_DATA, "/", c, "_", SUFFIX_QUESTION, ".csv", sep = "")
    if(file.exists(file_name)) {
      temp_read <- read.csv(file_name)
      temp_read$short_code <- c
      raw_questions <- merge(raw_questions, temp_read, all = TRUE)
    }
  }
  
  return(raw_questions)
}


# STEPS ACTIONS CSV LOAD
load_fl_steps_from_csv <- function(code_list) {
  
  raw_steps <- data.frame(learner_id = character(), short_code = character(), step = character(), 
                         week_number = character(), step_number = character(),
                         first_visited_at = character(), last_completed_at = character() )
  
  for(c in code_list) {
    file_name <- paste("./", FOLDER_DATA, "/", c, "_", SUFFIX_STEPACTIVITY, ".csv", sep = "")
    if(file.exists(file_name)) {
      temp_read <- read.csv(file_name, stringsAsFactors=FALSE)
      temp_read$short_code <- c
      raw_steps <- merge(raw_steps, temp_read, all = TRUE)
    }
  }
  
  return(raw_steps)
}


