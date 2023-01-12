#' Code to connect to SMRA database
#' @description You will be asked for a username and password when using this function
#'
#' @return Formal class Oracle object
#' @export
connect_to_smra <- function() {
  smra_connect <- odbc::dbConnect(odbc::odbc(),
    dsn = "SMRA",
    uid = "", # .rs.askForPassword("SMRA Username:"),
    pwd = "" # .rs.askForPassword("SMRA Password:")
  )
  return(smra_connect)
}
