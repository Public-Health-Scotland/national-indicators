#' Code to connect to SMRA database
#' @description You will be asked for a username and password when using this function
#'
#' @return Formal class Oracle object
#' @export
connect_to_smra <- function() {
  smra_connect <- odbc::dbConnect(odbc::odbc(),
            dsn = "SMRA",
            uid = "", #.rs.askForPassword("SMRA Username:"),
            pwd = "" #.rs.askForPassword("SMRA Password:")
  )
  return(smra_connect)
}

#' Query to extract NI14 data from SMRA
#'
#' @return An SQL query as a text string
#' @export
ni14_smra_query <- function() {
  query <- "SELECT LINK_NO, CIS_MARKER, ADMISSION_DATE, DISCHARGE_DATE,
  ADMISSION_TYPE, DISCHARGE_TYPE, DR_POSTCODE as postcode
  FROM ANALYSIS.SMR01_PI
  WHERE ((ANALYSIS.SMR01_PI.RECORD_TYPE = '01B')
  AND (DISCHARGE_DATE >= '01-APR-2013')
  AND (AGE_IN_YEARS >= 18)
  AND LINK_NO is not NULL)"
  return(query)
}

#' Query to extract NI14 deaths data from SMRA
#'
#' @return An SQL query as a text string
#' @export
ni14_gro_query <- function() {
  "SELECT MAX(DATE_OF_DEATH) as death_date, LINK_NO
  FROM ANALYSIS.GRO_DEATHS_C
  WHERE (DATE_OF_DEATH >= '01-APR-2013')
  GROUP BY LINK_NO"
}

#' Query to extract NI16 data from SMRA
#'
#' @return An SQL query as a text string
#' @export
ni16_smra_query <- function() {
  "SELECT DISCHARGE_DATE, DR_POSTCODE as postcode,
  MAIN_CONDITION, OTHER_CONDITION_1, OTHER_CONDITION_2,
  OTHER_CONDITION_3, OTHER_CONDITION_4, OTHER_CONDITION_5
  FROM ANALYSIS.SMR01_PI
  WHERE ((DISCHARGE_DATE >= '01-APR-2013')
  AND (AGE_IN_YEARS >= 65)
  AND (INPATIENT_DAYCASE_IDENTIFIER = 'I')
  AND (ADMISSION_TYPE Between 33 AND 35)
  AND (SEX In ('1', '2'))
  AND (DR_Postcode is not NULL))
  Order By DR_Postcode"
}


