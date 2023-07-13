#' Read in and format NI18 data from Social Care team
#'
#' @param path Path to spreadsheet received from Social Care team
#' @param min_year Minimum year to use for output
#'
#' @return A data frame
get_new_ni_18_data <- function(path, min_year) {
  readxl::read_excel(
    path = path,
    sheet = "T1 Data"
  ) %>%
    dplyr::rename(identifier = 2, partnership = 4) %>%
    dplyr::select("identifier", "partnership", dplyr::matches("\\d{4}")) %>%
    tidyr::pivot_longer(
      cols = dplyr::matches("\\d{4}"),
      names_to = "year",
      names_transform = as.integer,
      values_ptypes = numeric()
    ) %>%
    tidyr::pivot_wider(
      names_from = "identifier",
      values_from = "value"
    ) %>%
    janitor::clean_names() %>%
    dplyr::filter(year >= min_year) %>%
    dplyr::mutate(partnership = standardise_partnership_names(partnership)) %>%
    dplyr::mutate(
      numerator = .data$total_pc,
      denominator = .data$total_pc + .data$ch_res + .data$cc_census,
      rate = .data$percentage * 100,
      .keep = "unused"
    )
}

#' Read in raw NI18 data and calculate final NI18 output
#'
#' @param path Path to spreadsheet received from Social Care team
#' @param min_year Minimum year to use for output
#' @param write_to_disk Boolean, whether to write output or not
#'
#' @return A list of two outputs, one for the MI spreadsheet and one for Tableau
#' @export
calculate_ni18 <- function(path, min_year, write_to_disk = TRUE) {
  raw_data <- get_new_ni_18_data(
    path = path,
    min_year = min_year
  )

  c_and_s <- raw_data %>%
    dplyr::filter(partnership %in% c("Clackmannanshire", "Stirling")) %>%
    dplyr::mutate(partnership = "Clackmannanshire and Stirling") %>%
    dplyr::group_by(year, partnership) %>%
    dplyr::summarise(
      numerator = sum(numerator),
      denominator = sum(denominator)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(rate = numerator / denominator * 100)

  ni_18_data <- dplyr::bind_rows(raw_data, c_and_s)

  spreadsheet_output <- ni_18_data %>%
    dplyr::mutate(
      time_period = "Annual",
      indicator = "NI18",
      estimate = "No",
      ind_no = 18L
    ) %>%
    dplyr::select(
      "year",
      "time_period",
      "partnership",
      "indicator",
      "estimate",
      "numerator",
      "denominator",
      "rate",
      "ind_no"
    ) %>%
    dplyr::mutate(year = format_financial_year(year, "single_year"))

  max_year <- max(spreadsheet_output$year)

  ni18_tableau <- ni_18_data %>%
    dplyr::mutate(
      indicator = "NI18",
      locality = "All",
      time_period = "Annual"
    ) %>%
    dplyr::select(
      "year",
      "rate",
      "partnership",
      "numerator",
      "locality",
      "indicator",
      "denominator",
      "time_period"
    )

  ni18_final_tableau <- dplyr::left_join(
    ni18_tableau %>%
      dplyr::filter(partnership != "Scotland"),
    ni18_tableau %>% dplyr::filter(partnership == "Scotland") %>%
      dplyr::rename(scotland = rate) %>%
      dplyr::select(year, scotland),
    by = "year"
  ) %>%
    dplyr::relocate(scotland, .after = "rate") %>%
    dplyr::mutate(year = format_financial_year(year, "single_year"))

  if (write_to_disk) {
    arrow::write_parquet(spreadsheet_output, fs::path(get_ni_output_dir(), glue::glue("NI18_{max_year}_spreadsheet_output.parquet")))
    arrow::write_parquet(ni18_final_tableau, fs::path(get_ni_output_dir(), glue::glue("NI18_{max_year}_tableau_output.parquet")))
  }

  return(list(spreadsheet_output, ni18_final_tableau))
}
