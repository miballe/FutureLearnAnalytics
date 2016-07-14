####################################################
# DAMOOCP - Main Script
####################################################
# This is the main script that guides the general execution of
# all pipeline operations. The file is organized in pipeline
# stages sections with the aim to keep processes as isolated
# as possible, as well as to avoid the unnecessary execution
# of stages when not required.

# Cleans the workspace
rm( list = ls() )

########## Libraries and required scripts ##########
source("01_load.r")
source("02_clean.r")


########## Initialization ##########


########## Data load from CSV files ##########
# Functions from 01_load.r
df_course_list <- load_fl_courses_list_from_csv()
df_course_details <- load_fl_courses_details_from_csv()
df_comments <- load_fl_comments_from_csv(df_course_list[,1])
df_enrolments <- load_fl_enrolments_from_csv(df_course_list[,1])
df_assignments <- load_fl_assignments_from_csv(df_course_list[,1])
df_reviews <- load_fl_reviews_from_csv(df_course_list[,1])
df_questions <- load_fl_questions_from_csv(df_course_list[,1])  #TODO: Error in `$<-.data.frame`(`*tmp*`, "short_code", value = "web-science-4") : replacement has 1 row, data has 0 
df_steps <- load_fl_steps_from_csv(df_course_list[,1])
# TODO: When running the load operations for questions and steps, some
#       warnings are displayed. Exported files don't evidence a problem
#       but this has to be monitored in case an issue araises later.


########## Data cleaning ##########
# Functions from 02_clean.r
# df_comments <- clean_fl_comments_df(df_comments)


########## Data transformation and feature engineering ##########
# Functions from 03_transform.r




