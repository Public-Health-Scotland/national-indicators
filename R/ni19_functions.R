#' Calculate quarterly and annual figures for NI19
#'
#' @param file_from_dd_team The name of the file sent from the Delayed Discharges team
#' @param write_to_disk When TRUE, will write the output to disk
#'
#' @seealso [get_ni_input_dir()] and [read_population_lookup()]
#'
#' @return The annual figures for NI19
#' @export
calculate_ni19 <- function(file_from_dd_team,
                           write_to_disk = FALSE) {

  # TODO return an error if the wrong DD file name is used

  ni19 <- readr::read_csv(glue::glue("{get_ni_input_dir()}/NI19/{file_from_dd_team}.csv"),
    col_names = c("partnership", "year", "age_group", "complex_needs", "Q1", "Q2", "Q3", "Q4", "Annual"),
    col_types = "cccciiiii",
    col_select = c(1, 2, 5:9),
    skip = 1
  ) %>%
    dplyr::mutate(
      pop_year = stringr::str_sub(year, 1, 4),
      partnership = stringr::str_replace(partnership, "&", "and"),
      partnership = dplyr::if_else(stringr::str_detect(partnership, "Comha") == TRUE, "Na h-Eileanan Siar", partnership),
      partnership = dplyr::if_else(partnership == "Orkney" | partnership == "Shetland", stringr::str_c(partnership, " Islands"), partnership)
    ) %>%
    dplyr::filter(!(partnership %in% c("Undetermined", "Other"))) %>%
    dplyr::mutate(temp_part = dplyr::if_else(partnership %in% c("Clackmannanshire", "Stirling"),
      "Clackmannanshire and Stirling", NA_character_
    )) %>%
    tidyr::pivot_longer(
      cols = c("partnership", "temp_part"),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    tidyr::pivot_longer(
      cols = c("Q1", "Q2", "Q3", "Q4", "Annual"),
      names_to = "time_period",
      values_to = "numerator"
    ) %>%
    dplyr::group_by(partnership, year, pop_year, time_period) %>%
    dplyr::summarise(numerator = sum(numerator)) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(., read_population_lookup(min_year = 2015, ages_required = "over75", type = "partnership")) %>%
    dplyr::rename(denominator = over75_pop) %>%
    dplyr::mutate(value = (numerator / denominator) * 1000)

  ni19_scotland_column <- create_scotland_column(ni19)

  ni19_w_scotland_column <- ni19 %>%
    dplyr::left_join(., ni19_scotland_column, by = c("year", "time_period")) %>%
    dplyr::mutate(indicator = "NI19")

  if (write_to_disk == TRUE) {
    arrow::write_parquet(glue::glue("{get_ni_output_dir()}/NI19_excel_output.parquet"))
  }

  return(ni19_w_scotland_column)
}
