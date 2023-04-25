#' Add an 'all' grouping to a variable
#'
#' @description Adding an 'all' grouping means that, for example, in the case of 'age'
#' we can use this function to keep the ages in that column but also add rows for 'all ages'.
#'
#' @param data The data frame to transform
#' @param group_var The name of the variable to transform
#' @param group The name of the 'all' grouping
#'
#' @return The data frame with added rows with the 'all' grouping
#' @export
#'
#' @examples
#' tibble::tribble(~age, ~value, "1", 3, "2", 5, "3", 6) %>%
#' add_all_grouping("age", "All ages")
add_all_grouping <- function(data, group_var, group) {
  return_data <- data %>%
    dplyr::mutate(temp = {{ group }}) %>%
    tidyr::pivot_longer(c(group_var, "temp"), values_to = group_var) %>%
    dplyr::select(-name)
  return(return_data)
}
