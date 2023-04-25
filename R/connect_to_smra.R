#' Code to connect to SMRA database
#' @description You will be asked for a username and password when using this function
#'
#' @return Formal class Oracle object
#' @export
connect_to_smra <- function() {
  username <- Sys.getenv("USER")
  if (username == "") {
    username <- .rs.askForPassword("SMRA Username:")
  }

  smra_connect <- odbc::dbConnect(odbc::odbc(),
    dsn = "SMRA",
    uid = username,
    pwd = .rs.askForPassword("SMRA Password:")
  )
  return(smra_connect)
}
