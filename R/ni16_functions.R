#' Read the SMRA extract for NI16, filter to records involving falls,
#' and match on geographies
#'
#' @return A data frame
#'
#' @seealso [connect_to_smra()], [calculate_financial_month()], [get_spd_path()],
#' [get_locality_path()]
process_ni16_smra_extract <- function(min_date = "01-APR-2013") {
  smra_query <- glue::glue(readr::read_file("SQL/ni16_smra.sql"))

  return_data <-
    odbc::dbGetQuery(connect_to_smra(), smra_query) %>%
    tibble::as_tibble() %>%
    # For R-standard column names
    janitor::clean_names() %>%
    # Change diagnosis codes to first three characters
    dplyr::mutate(dplyr::across(main_condition:other_condition_5, ~ stringr::str_sub(.x, 1, 3))) %>%
    # Filter out any records where the diagnosis didn't involve a fall
    dplyr::filter(dplyr::if_any(
      c(main_condition:other_condition_5),
      ~ . %in% paste0("W", stringr::str_pad(0:19, 2, "left", "0"))
    )) %>%
    # Create variables for different date levels
    dplyr::mutate(
      fin_month = calculate_financial_month(discharge_date),
      fin_year = phsmethods::extract_fin_year(discharge_date),
      cal_month = lubridate::month(discharge_date),
      cal_year = lubridate::year(discharge_date)
    ) %>%
    dplyr::left_join(
      arrow::read_parquet(get_spd_path(),
        col_select = c("pc7", "ca2018", "datazone2011")
      ),
      by = c("postcode" = "pc7")
    ) %>%
    dplyr::filter(!is_missing(datazone2011)) %>%
    dplyr::left_join(
      readr::read_rds(get_locality_path()) %>%
        dplyr::select("datazone2011", "ca2019name", "hscp_locality"),
      by = "datazone2011"
    ) %>%
    dplyr::rename(partnership = ca2019name, locality = hscp_locality)

  return(return_data)
}

#' Aggregate the NI16 SMR extract to a chosen date level
#'
#' @param data The SMR extract derived from [process_ni16_smra_extract()]
#' @param date_level Either `calendar` or `financial`
#'
#' @return The extract aggregated to chosen date level, with falls as the summary variable
aggregate_to_date_level <- function(data, date_level = c("calendar", "financial")) {
  return_data <- data %>%
    # Either selects c("fin_year", "fin_month") or c("cal_year", "cal_month")
    dplyr::group_by(
      across(c(
        dplyr::starts_with(stringr::str_sub(date_level, 1, 3)),
        "partnership", "ca2018", "locality"
      ))
    ) %>%
    # Falls is the number of records
    dplyr::summarise(falls = dplyr::n()) %>%
    dplyr::ungroup() %>%
    # Renames the date variables to c("year", "month")
    dplyr::rename_with(
      ~ stringr::str_sub(.x, 5, -1),
      dplyr::starts_with(stringr::str_sub(date_level, 1, 3))
    ) %>%
    dplyr::mutate(month = as.character(month))

  return(return_data)
}

#' Add 'all' groups to data frame - annual totals, partnership totals,
#' Clackmannanshire & Stirling totals, and Soctland totals, then aggregate
#'
#' @param data A data frame
#'
#' @return A data frame with the additional totals
add_additional_groups_ni16 <- function(data) {
  return_data <- data %>%
    # Make annual totals
    add_all_grouping("month", "Annual") %>%
    # Make Clackmannanshire & Stirling totals
    dplyr::mutate(temp_part = dplyr::if_else(ca2018 %in% c("S12000005", "S12000030"),
      "Clackmannanshire and Stirling",
      NA_character_
    )) %>%
    tidyr::pivot_longer(
      cols = c(partnership, temp_part),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    # Make partnership totals
    add_all_grouping("locality", "All") %>%
    # Make Scotland totals
    dplyr::mutate(temp_part = dplyr::if_else(partnership == "Clackmannanshire and Stirling",
      NA_character_,
      "Scotland"
    )) %>%
    tidyr::pivot_longer(
      cols = c(partnership, temp_part),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    # Select out any rows where partnership is Scotland
    # and localities aren't 'all'
    dplyr::filter(partnership != "Scotland" | locality == "All") %>%
    # Aggregate
    dplyr::group_by(year, month, partnership, locality) %>%
    dplyr::summarise(falls = sum(falls)) %>%
    dplyr::ungroup()

  return(return_data)
}

#' Calculate the final NI16 values for calendar year and financial year
#'
#' @param write_to_disk Boolean parameter, choose to write the output to disk or not
#'
#' @return A list of data frames, one for calendar year named `calendar` and one
#' for financial year named `financial`
#' @export
#'
#' @seealso [process_ni16_smra_extract()], [aggregate_to_date_level()],
#' [add_additional_groups()], [create_population_lookup()]
calculate_ni16_final_output <- function(write_to_disk = TRUE) {
  smra_extract <- process_ni16_smra_extract()

  date_level_list <- list(
    calendar = aggregate_to_date_level(smra_extract, "calendar"),
    financial = aggregate_to_date_level(smra_extract, "financial")
  ) %>%
    purrr::map(~ add_additional_groups_ni16(.x)) %>%
    purrr::map(~ dplyr::mutate(.x, pop_year = stringr::str_sub(year, 1, 4)) %>%
      dplyr::left_join(
        read_population_lookup(
          ages_required = "over65",
          type = "locality"
        ),
        by = c("pop_year", "locality", "partnership")
      ) %>%
      # NI16 value is falls rate per 1000 population
      dplyr::mutate(
        value = (falls / over65_pop) * 1000,
        indicator = "NI16"
      ) %>%
      # Rename to standard variable names
      dplyr::rename(
        numerator = falls,
        denominator = over65_pop,
        time_period = month
      ))

  if (write_to_disk == TRUE) {
    arrow::write_parquet(date_level_list[["calendar"]], fs::path(get_ni_output_dir(), "ni16_calendar_year.parquet"))
    arrow::write_parquet(date_level_list[["financial"]], fs::path(get_ni_output_dir(), "ni16_financial_year.parquet"))
  }

  return(date_level_list)
}
