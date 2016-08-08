################################################################
# DAMOOCP - Configuration file
################################################################
# This file contains the contstants and values used by all other
# scripts. This file has to be loaded only once per process
# execution. This means that it is loaded only at the 00_main
# script, and has also to be loaded at the beginning of each
# notebook.

suppressMessages(library(dplyr))

########## Global constants and settings ##########
# Even if suffixes are originally matching FL conventions, the same names
# are used for objects at cleand and tranformation stages.
DATA_DOWNLOADED = "Data_Downloaded"               # Downloaded data folder name
DATA_RAW = "Data_Raw"                             # Raw data folder name
DATA_CLEAN = "Data_Clean"                         # Clean data folder name
DATA_TRANSFORMED = "Data_Transformed"             # Transformed data folder name
SUFFIX_COMMENTS = "comments"                      # Suffix for comments CSV files from FL
SUFFIX_ENROLMENTS = "enrolments"                  # Suffix for enrolments CSV files from FL
SUFFIX_PRASSIGNMENTS = "peer-review-assignments"  # Suffix for Peer to peer review assignments CSV files from FL
SUFFIX_PRREVIEWS = "peer-review-reviews"          # Suffix for peer to peer review reviews CSV files from FL
SUFFIX_QUESTIONRESPONSE = "question-response"     # Suffix for question response CSV files from FL
SUFFIX_STEPACTIVITY = "step-activity"             # Suffix for step activity CSV files from FL
FILE_COURSELIST = "course-list"                   # Course list file name - Custom file manually created. See ./Data_Downloaded/readme.md for details
FILE_COURSEDETAILS = "course-details"             # Course details file name - custom file manually created. See ./Data_Downloaded/readme.md for details
FILE_LOG = "damoocp"                              # Log file name, to be created/appended at the project root folder.


########## Logging System Initialization ##########
# Logging, warning and error object creation and control
logger <- log4r::create.logger()
log4r::logfile(logger) <- file.path(paste("./", FILE_LOG, ".log", sep = ""))
log4r::level(logger) <- "DEBUG"

# Functions to write the log and screen progress
# All methods are called as log4r::XXXX to avoid loading the package and the
# potential "debug" function conflict.
log_new_error <- function(error_message) {
  log4r::error(logger, error_message)
  execution_errors[[length(execution_errors) + 1]] <- error_message
  print(paste("ERROR", error_message))
}

log_new_warning <- function(warning_message) {
  log4r::warn(logger, warning_message)
  execution_warnings[[length(execution_warnings) + 1]] <- warning_message
  print(paste("WARNING", warning_message))
}

log_new_info <- function(info_message) {
  log4r::info(logger, info_message)
  print(paste("INFO", info_message))
}

log_new_debug <- function(debug_message) {
  log4r::debug(logger, debug_message)
  print(paste("DEBUG", debug_message))
}

