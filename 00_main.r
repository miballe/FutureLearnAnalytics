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


########## Data load from raw CSV files ##########
# Functions from 01_load.r
df_course_list <- load_fl_courses_list_from_csv()
df_course_details <- load_fl_courses_details_from_csv()
df_comments <- load_fl_comments_from_csv(df_course_list[,1])
df_enrolments <- load_fl_enrolments_from_csv(df_course_list[,1])
df_assignments <- load_fl_assignments_from_csv(df_course_list[,1])
df_reviews <- load_fl_reviews_from_csv(df_course_list[,1])
df_questions <- load_fl_questions_from_csv(df_course_list[,1])
df_steps <- load_fl_steps_from_csv(df_course_list[,1])

write.csv(df_course_list, "./Data_Raw/df_course_list_raw.csv", row.names = FALSE)
write.csv(df_course_details, "./Data_Raw/df_course_details_raw.csv", row.names = FALSE)
write.csv(df_comments, "./Data_Raw/df_comments_raw.csv", row.names = FALSE)
write.csv(df_enrolments, "./Data_Raw/df_enrolments_raw.csv", row.names = FALSE)
write.csv(df_assignments, "./Data_Raw/df_assignments_raw.csv", row.names = FALSE)
write.csv(df_reviews, "./Data_Raw/df_reviews_raw.csv", row.names = FALSE)
write.csv(df_questions, "./Data_Raw/df_questions_raw.csv", row.names = FALSE)
write.csv(df_steps, "./Data_Raw/df_steps_raw.csv", row.names = FALSE)

########## Data cleaning ##########
# Functions from 02_clean.r
df_comments <- clean_fl_comments_df(df_comments)


########## Data transformation and feature engineering ##########
# Functions from 03_transform.r




