#' Process the HACE outputs and save the outputs
#'
#' @param finyear The financial year to be shown in the column Year1, in "20XX/YY" format
#' @param archive Default to `TRUE`, when TRUE then will archive the previous NI1-9 Tableau output
#' @param write_to_disk Defaults to `TRUE`. Writes three files to disk, the 'all data and variables'
#' version, the Tableau output, and the Management Information output
#'
#' @return The final, unfiltered, NI1-9 output
#' @export
process_hace_outputs <- function(finyear, archive = TRUE, write_to_disk = TRUE) {
  # Pull out data from a stable previous version
  older_data_tableau <- haven::read_sav(glue::glue("{get_ni_tableau_output_dir()}/NI Tableau Final-L3-{previous_update()}.sav")) %>%
    dplyr::filter(Indicator1 %in% c("NI1", "NI2", "NI3", "NI4", "NI5", "NI6", "NI7", "NI8", "NI9")) %>%
    dplyr::select(-LA_Code)

  if (archive == TRUE) {
    # Archive old version
    readr::write_rds(
      older_data_tableau,
      glue::glue("{get_ni_dir()}/Z1 - Data Archive/NI 1-9-Tableau-pre-{Sys.Date()}.rds")
    )
  }

  # Read in data from two spreadsheets and wrangle
  # Get the HACE data at Partnership level
  raw_data_hscp <- read_raw_hace_data(latest_hace_year(), 2, 2)[[1]] %>%
    janitor::clean_names() %>%
    dplyr::mutate(locality = "All") %>%
    dplyr::rename(
      value = glue::glue("hscp_percent_positive_{latest_hace_year()}"),
      upper_ci = glue::glue("wgt_percentpositive_upp_{latest_hace_year()}"),
      lower_ci = glue::glue("wgt_percentpositive_low_{latest_hace_year()}"),
      partnership = hscp_name
    )
  # Read in the HACE data at locality level
  raw_data_loc <- read_raw_hace_data(latest_hace_year(), 2, 2)[[2]] %>%
    janitor::clean_names() %>%
    # Column is formatted "hscp_name HSCP - locality_name" so we want to split those
    dplyr::bind_cols(
      reshape2::colsplit(.$locality_name,
        pattern = " - ",
        names = c("partnership", "locality")
      )
    ) %>%
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
      value = glue::glue("locality_percent_positive_{latest_hace_year()}"),
      upper_ci = glue::glue("wgt_percentpositive_upp_{latest_hace_year()}"),
      lower_ci = glue::glue("wgt_percentpositive_low_{latest_hace_year()}")
    ) %>%
    dplyr::mutate(dplyr::across(c("value", "upper_ci", "lower_ci"), as.numeric)) %>%
    dplyr::filter(partnership != "Scotland")

  # Add locality and partnership data together
  one_to_nine <- dplyr::bind_rows(raw_data_loc, raw_data_hscp) %>%
    dplyr::mutate(
      indicator = dplyr::case_when(
        # Find indicator based on content of question, as the question numbers change every time
        stringr::str_detect(description_2022, "In general,") == TRUE ~ "NI1",
        stringr::str_detect(description_2022, "independently") == TRUE ~ "NI2",
        stringr::str_detect(description_2022, "had a say") == TRUE ~ "NI3",
        stringr::str_detect(description_2022, "coordinated") == TRUE ~ "NI4",
        stringr::str_detect(description_2022, "exclude") == TRUE ~ "NI5",
        stringr::str_detect(description_2022, "GP") == TRUE ~ "NI6",
        stringr::str_detect(description_2022, "maintained") == TRUE ~ "NI7",
        stringr::str_detect(description_2022, "continue") == TRUE ~ "NI8",
        stringr::str_detect(description_2022, "safe") == TRUE ~ "NI9",
        TRUE ~ "No"
      ),
      data = "Annual",
      year = finyear,
      numerator = NA,
      denominator = NA
    ) %>%
    dplyr::select(
      "year",
      "value",
      "partnership",
      "numerator",
      "locality",
      "indicator",
      "denominator",
      "data",
      "lower_ci",
      "upper_ci"
    )

  # Add Scotland totals as a column
  final <- dplyr::left_join(one_to_nine,
    one_to_nine %>% dplyr::filter(partnership == "Scotland") %>%
      dplyr::rename(scotland = value) %>%
      dplyr::select(
        "data",
        "indicator",
        "year",
        "scotland"
      ),
    by = c("year", "indicator", "data")
  ) %>%
    dplyr::select(
      "year",
      "value",
      "scotland",
      "partnership",
      "numerator",
      "locality",
      "indicator",
      "denominator",
      "data",
      "lower_ci",
      "upper_ci"
    ) %>%
    magrittr::set_colnames(c(
      "Year1", "Value", "Scotland", "Partnership1", "Numerator",
      "Locality", "Indicator1", "Denominator", "Data1", "LowerCI",
      "UpperCI"
    ))

  final <- dplyr::bind_rows(final, older_data_tableau)

  if (write_to_disk == TRUE) {
    # Final save outs
    readr::write_rds(
      final,
      glue::glue("{get_ni_dir()}/NI 1-9/NI 1-9-All Data and Vars.rds")
    )
    # No Scotland rows in Tableau output
    readr::write_rds(
      final %>% dplyr::filter(Partnership1 != "Scotland"),
      "{get_ni_dir}/NI 1-9/NI 1-9-Tableau-Format.rds"
    )
    # No individual localities in MI output
    readr::write_rds(
      final %>% dplyr::filter(Locality == "All"),
      "{get_ni_dir()}/NI 1-9/NI 1-9-MI-Format.rds"
    )
  }

  return(final)
}


# Section to get older CI files ----
# confidence <- dplyr::bind_rows(
#   haven::read_sav("NI 1-9/NI 1-9-With-CIs-Reshaped-1920.zsav"),
#   haven::read_sav("NI 1-9/NI 1-9-With-CIs-Reshaped-1314.zsav"),
#   haven::read_sav("NI 1-9/NI 1-9-With-CIs-Reshaped-1516.zsav"),
#   haven::read_sav("NI 1-9/NI 1-9-With-CIs-Reshaped-1718.zsav")) %>% janitor::clean_names() %>%
#   dplyr::select(-hscp_ques_resp_unweighted_2020, -hscp_percent_positive_2020)

# Section for taking messy column names from CI data and cleaning them ----
# column_names_1 <- c(glue::glue("NI{1:9}_number_of_responses"))
# column_names_2 <- c(glue::glue("NI{1:9}_percentage_positive"))
# column_names_3 <- c(glue::glue("NI{1:9}_lower_ci"))
# column_names_4 <- c(glue::glue("NI{1:9}_upper_ci"))
# column_names <- c("hscp")
# i <- 1
# while (i <= 9) {
#   column_names <- append(column_names, c(column_names_1[i], column_names_2[i], column_names_3[i], column_names_4[i]))
#   i <- i + 1
# }

# Reshape the CI info to be matched on to main HACE data
# reshape_ci <- function(year) {
#   terrible_data <-
#     readxl::read_excel("NI 1-9/Integration_indicators_1to9_confidence_intervals_Dec_2021.xlsx", sheet = glue::glue("HSCP {year}"), range = "A6:AS38") %>%
#     dplyr::select(1:5, 7:10, 12:15, 17:20, 22:25, 27:30, 32:35, 37:40, 42:45)
#
#   colnames(terrible_data) <- column_names
#
#   responses <- terrible_data %>%
#     tidyr::pivot_longer(
#       cols = c(contains("number_of_responses")),
#       names_to = "indicator",
#       values_to = "number_of_responses"
#     ) %>%
#     dplyr::mutate(indicator = stringr::str_sub(indicator, 1, 3)) %>%
#     dplyr::select(hscp, indicator, number_of_responses)
#
#   percentage <- terrible_data %>%
#     tidyr::pivot_longer(
#       cols = c(contains("percentage")),
#       names_to = "indicator",
#       values_to = "percentage_positive"
#     ) %>%
#     dplyr::mutate(indicator = stringr::str_sub(indicator, 1, 3)) %>%
#     dplyr::select(hscp, indicator, percentage_positive)
#
#   lowerci <- terrible_data %>%
#     tidyr::pivot_longer(
#       cols = c(contains("lower")),
#       names_to = "indicator",
#       values_to = "lower_ci"
#     ) %>%
#     dplyr::mutate(indicator = stringr::str_sub(indicator, 1, 3)) %>%
#     dplyr::select(hscp, indicator, lower_ci)
#
#   upperci <- terrible_data %>%
#     tidyr::pivot_longer(
#       cols = c(contains("upper")),
#       names_to = "indicator",
#       values_to = "upper_ci"
#     ) %>%
#     dplyr::mutate(indicator = stringr::str_sub(indicator, 1, 3)) %>%
#     dplyr::select(hscp, indicator, upper_ci)
#
#   return_df <- dplyr::left_join(responses, percentage) %>%
#     dplyr::left_join(., lowerci) %>%
#     dplyr::left_join(., upperci) %>%
#     dplyr::mutate(
#       year = stringr::str_c("20", stringr::str_sub({{ year }}, 1, 2), "/", stringr::str_sub({{ year }}, 3, 4)),
#       locality = "All",
#       data = "Annual"
#     )
#
#   return(return_df)
# }

# Get CIs for each year
# ci_values <- dplyr::bind_rows(
#   reshape_ci("1920"),
#   reshape_ci("1718"),
#   reshape_ci("1516"),
#   reshape_ci("1314")
# )

# all_data_and_vars <- dplyr::left_join(one_to_nine, ci_values, by = c("locality", "data", "indicator", "year", "partnership" = "hscp")) %>%
#   dplyr::select(year:data, upper_ci, lower_ci)
