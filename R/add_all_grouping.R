add_all_grouping <- function(data, group_var, group) {
  return_data <- data %>%
    dplyr::mutate(temp = {{ group }}) %>%
    tidyr::pivot_longer(c(group_var, "temp"), values_to = group_var) %>%
    dplyr::select(-name)
  return(return_data)
}
