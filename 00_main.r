####################################################
# DAMOOCP - Main Script
####################################################

# Cleans the workspace
rm( list = ls() )

########## Libraries and required scripts ##########
source("01_load.r")


########## Initialization ##########




########## Data load from CSV files ##########
# All methods load the raw data to new data frames. Then
# NA values are completed and types are changed. The final
# outcome is a table with valid values and exactly the 
# original columns

df_course_list <- load_fl_courses_list_from_csv()
df_course_details <- load_fl_courses_details_from_csv()
df_comments <- load_fl_comments_from_csv(df_course_list[,1])
df_enrolments <- load_fl_enrolments_from_csv(df_course_list[,1])
df_assignments <- load_fl_assignments_from_csv(df_course_list[,1])
df_reviews <- load_fl_reviews_from_csv(df_course_list[,1])
df_questions <- load_fl_questions_from_csv(df_course_list[,1])
df_steps <- load_fl_steps_from_csv(df_course_list[,1])


########## Data cleaning ##########
# All methods remove, fill and assign default values to the 
# raw read datasets with the aim to have a standard raw data
# structure, avoiding future errors or distortions, and 
# using the most of the existing records



########## Data transformation and feature engineering ##########


