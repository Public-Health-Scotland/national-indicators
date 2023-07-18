#' Read in raw HACE data for manipulation
#'
#' @param hace_year The year of the published HACE results
#' @param sheet_num_hscp The Excel sheet number relevant to the HSCP-level data
#' @param sheet_num_loc The Excel sheet number relevant to the Locality-level data
#'
#' @return A list of length `2`, with the HSCP then the Locality data
read_raw_hace_data <- function(hace_year, sheet_num_hscp, sheet_num_loc) {
  hscp_data <- readxl::read_excel(
    path = fs::path(get_ni_input_dir(), "NI1_to_NI9", paste0("HSCP_", hace_year, "_final_results_For_LIST.xlsx")),
    sheet = sheet_num_hscp,
    col_names = TRUE
  )

  locality_data <- readxl::read_excel(
    path = fs::path(get_ni_input_dir(), "NI1_to_NI9", paste0("Locality_", hace_year, "_final_results_For_LIST.xlsx")),
    sheet = sheet_num_loc,
    col_names = TRUE
  )

  return(list(hscp_data = hscp_data, locality_data = locality_data))
}

#' Format the raw HACE data into the format we need
#'
#' @param data_list Output from [read_raw_hace_data()]
#' @param hace_year The year of the questionnaire
#'
#' @return A data frame
format_raw_hace_data <- function(data_list, hace_year) {
  # Format HSCP level data
  hscp_data <- data_list[["hscp_data"]] %>%
    janitor::clean_names() %>%
    dplyr::mutate(locality = "All") %>%
    dplyr::rename(
      value = glue::glue("hscp_percent_positive_{hace_year}"),
      upper_ci = glue::glue("wgt_percentpositive_upp_{hace_year}"),
      lower_ci = glue::glue("wgt_percentpositive_low_{hace_year}"),
      partnership = hscp_name
    )

  # Format Locality level data
  locality_data <- data_list[["locality_data"]] %>%
    janitor::clean_names() %>%
    # Column is formatted "hscp_name HSCP - locality_name" so we want to split those
    dplyr::bind_cols(., reshape2::colsplit(.$locality_name, pattern = " - ", names = c("partnership", "locality"))) %>%
    # Get rid of the 'HSCP' and 'Local Authority' strings
    dplyr::mutate(
      partnership =
        dplyr::case_when(
          stringr::str_detect(partnership, "HSCP") ~ stringr::str_replace(partnership, " HSCP", ""),
          stringr::str_detect(partnership, "Local Authority") ~ stringr::str_replace(partnership, " Local Authority", ""),
          TRUE ~ partnership
        )
    ) %>%
    dplyr::select(-locality_name) %>%
    # Rename to value and make sure it's a number.
    # Coerces NAs when value is '*' (suppressed data)
    dplyr::rename(
      value = glue::glue("locality_percent_positive_{hace_year}"),
      upper_ci = glue::glue("wgt_percentpositive_upp_{hace_year}"),
      lower_ci = glue::glue("wgt_percentpositive_low_{hace_year}")
    ) %>%
    dplyr::mutate(dplyr::across(c("value", "upper_ci", "lower_ci"), as.numeric)) %>%
    dplyr::filter(partnership != "Scotland")

  return_data <- dplyr::bind_rows(hscp_data, locality_data)

  return(return_data)
}

#' Calculate the data for indicators 1 to 9 using most recent HACE data
#'
#' @param hace_year The year of the HACE survey
#' @param sheet_num_hscp The sheet number in the HSCP Excel workbook to read from
#' @param sheet_num_loc The sheet number in the Locality Excel workbook to read from
#'
#' @return A final output for the outcome indicators
#' @export
calculate_outcome_indicators <- function(hace_year,
                                         sheet_num_hscp = 2,
                                         sheet_num_loc = 2,
                                         write_to_disk = FALSE) {
  fin_year <- paste0(
    stringr::str_sub(hace_year, 1, 2),
    as.character(as.numeric(stringr::str_sub(hace_year, 3, 4)) - 1),
    "/",
    stringr::str_sub(hace_year, 3, 4)
  )

  raw_list <- read_raw_hace_data(hace_year, sheet_num_hscp, sheet_num_loc)

  indicators <- format_raw_hace_data(raw_list, hace_year) %>%
    dplyr::mutate(
      indicator = dplyr::case_when(
        # Find indicator based on content of question, as the question numbers may change in the future
        stringr::str_detect(.data$description_2022, "In general,") ~ "NI1",
        stringr::str_detect(.data$description_2022, "independently") ~ "NI2",
        stringr::str_detect(.data$description_2022, "had a say") ~ "NI3",
        stringr::str_detect(.data$description_2022, "coordinated") ~ "NI4",
        stringr::str_detect(.data$description_2022, "exclude") ~ "NI5",
        stringr::str_detect(.data$description_2022, "GP") ~ "NI6",
        stringr::str_detect(.data$description_2022, "maintained") ~ "NI7",
        stringr::str_detect(.data$description_2022, "continue") ~ "NI8",
        stringr::str_detect(.data$description_2022, "safe") ~ "NI9",
        TRUE ~ "No"
      ),
      time_period = "Annual",
      year = fin_year,
      numerator = NA,
      denominator = NA,
      ind_no = as.numeric(stringr::str_sub(indicator, 3, 3))
    ) %>%
    dplyr::select(
      "year",
      "value",
      "partnership",
      "numerator",
      "locality",
      "indicator",
      "denominator",
      "time_period",
      "lower_ci",
      "upper_ci"
    )

  return_data <-
    dplyr::left_join(indicators,
      indicators %>% dplyr::filter(partnership == "Scotland") %>%
        dplyr::rename(scotland = value) %>%
        dplyr::select(
          "time_period",
          "indicator",
          "year",
          "scotland"
        ),
      by = c("year", "indicator", "time_period")
    )

  write_year <- stringr::str_replace(max(return_data$year), "/", "")

  if (write_to_disk) {
    # Spreadsheet output doesn't need Scotland column
    arrow::write_parquet(
      return_data %>%
        dplyr::select(-"scotland"),
      fs::path(get_ni_output_dir(), glue::glue("NI1_to_NI9_{write_year}_spreadsheet_output.parquet"))
    )
    # Tableau output doesn't need ind_no and doesn't need Scotland rows
    arrow::write_parquet(
      return_data %>%
        dplyr::select(-"ind_no") %>%
        dplyr::filter(partnership != "Scotland"),
      fs::path(get_ni_output_dir(), glue::glue("NI1_to_NI9_{write_year}_tableau_output.parquet"))
    )
  }

  return(return_data)
}
