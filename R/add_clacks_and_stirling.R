#' Add rows for Clackmannanshire & Stirling as a partnership
#'
#' @param data The data frame containing a column with council area names
#' @param council_variable The variable to use for council names
#'
#' @return A data frame with additional rows for C&S
#' @export
add_clacks_and_stirling <- function(data, council_variable) {
  data_inc_c_and_s <- data %>%
    dplyr::mutate(
      temp_part =
        dplyr::if_else({{ council_variable }} %in% c("Clackmannanshire", "Stirling"),
          "Clackmannanshire & Stirling",
          NA_character_
        )
    ) %>%
    tidyr::pivot_longer(
      cols = c("partnership", "temp_part"),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(!"name")

  return(data_inc_c_and_s)
}
