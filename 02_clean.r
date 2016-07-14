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



########## Data load operations ##########
clean_fl_comments_df <- function(ddata) {
  # Apparently, the variables id, author_id, step, text and 
  # timestamp are validated at FL at export time and are not 
  # expected to be NA
  ddata$timestamp <- strptime(ddata$timestamp, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  ddata$moderated <- strptime(ddata$moderated, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  step_numbers <- strsplit(as.character(ddata$step), ".", fixed = TRUE)
  ddata$week_number <- step_numbers[,2]
  
  return(ddata)
}




