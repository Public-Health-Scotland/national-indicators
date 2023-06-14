#' Prepare a check file comparing an indicator's values to the previous output
#'
#' @param previous_output_file File name of last Excel output
#' @param indicator_to_check Indicator to compare, in "NIXX" format
#' @param output_type Whether you're checking a publication output or an MI output
#'
#' @return A check file
#' @export
check_against_previous_output <- function(previous_output_file,
                                          indicator_to_check,
                                          output_type = c("mi", "publication")) {

  suffix <- ifelse(output_type == "mi", "excel", "publication")

  previous_data <- readxl::read_excel(fs::path(get_ni_excel_output_dir(), previous_output_file, ext = "xlsx")) %>%
    dplyr::filter(indicator == indicator_to_check)

  new_data <- arrow::read_parquet(fs::path(get_ni_output_dir(), glue::glue("{indicator_to_check}_{suffix}_output.parquet")))

  checks <- dplyr::left_join(new_data, previous_data, by = c("year", "time_period", "partnership")) %>%
    dplyr::mutate(num_diff = numerator.x - numerator.y,
                  denom_diff = denominator.x - denominator.y,
                  value_diff = value.x - value.y,
                  value_percent = scales::percent(value_diff/value.y, accuracy = 0.01)) %>%
    dplyr::select(year, partnership, num_diff, denom_diff, value_diff, value_percent) %>%
    dplyr::arrange(desc(value_percent))

  return(checks)

}

#' Create a crosstab showing how partnerships are represented over years in an output
#' @description The values in the crosstab will be TRUE when there are 34 unique partnerships represented in a year
#' (as this includes Scotland and C&S)
#'
#' @param output_file A full output file for NIs, in Excel format
#'
#' @return A crosstab
#' @export
create_year_crosstab <- function(output_file) {

  all_ind_check <- readxl::read_excel(glue::glue("{get_ni_excel_output_dir()}/{output_file}.xlsx")) %>%
    # Just in case
    dplyr::filter(partnership != "Other")

  year_check <- all_ind_check %>%
    dplyr::group_by(indicator, year) %>%
    dplyr::summarise(count = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(indicator, year) %>%
    dplyr::mutate(partnership_correct = count %% 34 == 0) %>%
    tidyr::pivot_wider(id_cols = "indicator",
                       names_from = year,
                       values_from = partnership_correct) %>%
    dplyr::select(order(colnames(year_check))) %>%
    dplyr::relocate(indicator)
}

