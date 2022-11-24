#' Create comparison between previous update and current update, based on indicator
#'
#' @param old_data Ideally the Tableau output from last run, as this is at Locality level
#' @param new_data The output from running one of the indicators
#' @param indicator As a string, the indicator we're creating the comparison for
#' @param write_to_disk Default TRUE, will save the comparison as an Excel workbook
#'
#' @return A data frame containing any records where the value, denominator, or numerator
#' difference is above 5%
#' @export
create_output_comparison <- function(old_data, new_data, indicator, write_to_disk = TRUE) {
  comparison <- dplyr::left_join(old_data %>% dplyr::filter(Indicator1 == indicator),
                                 new_data,
                                 by = c("Year1", "Partnership1",
                                        "Locality", "Indicator1", "Data1"),
                                 suffix = c("_old", "_new")) %>%
    janitor::clean_names() %>%
    dplyr::mutate(value_diff = value_new - value_old,
                  value_proportion = scales::percent(value_diff/value_old),
                  value_issue = !dplyr::between(value_diff/value_old, -.05, .05),
                  numerator_diff = numerator_new - numerator_old,
                  numerator_proportion = scales::percent(numerator_diff/numerator_old),
                  numerator_issue = !dplyr::between(numerator_diff/numerator_old, -.05, .05),
                  denominator_diff = denominator_new - denominator_old,
                  denominator_proportion = scales::percent(denominator_diff/denominator_old),
                  denominator_issue = !dplyr::between(denominator_diff/denominator_old, -.05, .05)) %>%
    dplyr::filter(dplyr::across(dplyr::contains("issue"), ~ .x == TRUE)) %>%
    dplyr::select("year1",
           "partnership1",
           "locality",
           "data1",
           dplyr::contains("value"),
           dplyr::contains("numerator"),
           dplyr::contains("denominator"))

  if (write_to_disk == TRUE) {
    openxlsx::write.xlsx(comparison,
                         glue::glue("checks/{indicator}-comparison-{latest_update()}.xlsx"))
  }

  return(comparison)
}
