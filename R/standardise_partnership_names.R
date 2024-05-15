#' Standardise partnership names for NIs
#'
#' @param partnership_name A vector of existing partnership names
#'
#' @return A vector of partnership names in the NI expected format
#' @export
standardise_partnership_names <- function(partnership_name) {
  fixed_partnership <-
    dplyr::case_when(
      stringr::str_detect(partnership_name, "^City of Edinburgh$") ~ "Edinburgh",
      partnership_name == "Edinburgh, City of" ~ "Edinburgh",
      stringr::str_detect(partnership_name, "(siar$|Siar$)") ~ "Western Isles",
      stringr::str_detect(partnership_name, "&") ~ stringr::str_replace(partnership_name, "&", "and"),
      stringr::str_detect(partnership_name, "(^Orkney$|^Shetland$)") ~
        stringr::str_replace(partnership_name, "(^Orkney$|^Shetland$)", "\\1 Islands"),
      TRUE ~ partnership_name
    )
  return(fixed_partnership)
}
