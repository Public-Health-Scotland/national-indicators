#' Use postcode to determine the HSCP locality of residence
#'
#' @param data Any data frame containing a variable "postcode"
#' @param spd_path Path to the SPD lookup, defaults to [get_spd_path()]
#' @param locality_path Path to the HSCP locality lookup, defaults to [get_locality_path()]
#'
#' @return A data frame with additional geographies
#' @export
get_locality_from_postcode <- function(data,
                                       spd_path = get_spd_path(),
                                       locality_path = get_locality_path()) {
  return_data <- dplyr::left_join(
    data,
    readr::read_rds(spd_path),
    by = dplyr::join_by("postcode" == "pc7")
  ) %>%
    dplyr::left_join(
      readr::read_rds(locality_path) %>%
        dplyr::select("datazone2011", "hscp_locality"),
      by = "datazone2011"
    )

  return(return_data)
}
