#' Process SMR01 and SMR01e data for use with NI15
#'
#' @param extract_start Start date of extract in 'DD-MMM-YYYY' format
#' @param extract_end  End date of extract in 'DD-MMM-YYYY' format
#'
#' @return A tibble
#' @export
process_ni15_smr_extracts <- function(extract_start, extract_end) {
  # Extract from SMR01 has to start six months before the actual start date of represented data for safety
  extract_start_smr <- stringr::str_to_upper(format(lubridate::dmy(format(strptime(extract_start, format = "%d-%b-%Y"), "%d-%m-%Y")) - months(6), "%d-%b-%Y"))
  # Connect to SMRA
  smra_connect <- connect_to_smra()

  # Read SMR01 data
  smr01 <- odbc::dbGetQuery(smra_connect, glue::glue(readr::read_file("SQL/ni15_smr01.sql")))
  # Read SMR01e data
  smr01e <- odbc::dbGetQuery(smra_connect, glue::glue(readr::read_file("SQL/ni15_smr01e.sql")))

  smr01 <- dplyr::bind_rows(smr01, smr01e) %>%
    janitor::clean_names() %>%
    dplyr::group_by(.data$link_no) %>%
    dplyr::arrange(.data$admission_date) %>%
    dplyr::mutate(index = c(0, cumsum(dplyr::lead(.data$ch_flag) != .data$ch_flag |
      dplyr::lead(.data$gls_cis_marker) != .data$gls_cis_marker)[-dplyr::n()])) %>%
    dplyr::filter(.data$ch_flag == 0) %>%
    dplyr::group_by(.data$link_no, .data$index) %>%
    dplyr::summarise(
      admission_date = min(.data$admission_date),
      discharge_date = max(.data$discharge_date),
      date_of_death = max(.data$date_of_death)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-"index")

  # Read and organise smr04 data
  smr04 <- odbc::dbGetQuery(smra_connect, glue::glue(readr::read_file("SQL/ni15_smr04.sql"))) %>%
    janitor::clean_names() %>%
    dplyr::group_by(.data$link_no) %>%
    dplyr::arrange(.data$admission_date) %>%
    dplyr::mutate(index = c(0, cumsum(dplyr::lead(.data$ch_flag) != .data$ch_flag |
      dplyr::lead(.data$cis_marker) != .data$cis_marker)[-dplyr::n()])) %>%
    dplyr::filter(.data$ch_flag == 0) %>%
    dplyr::group_by(.data$link_no, .data$index) %>%
    dplyr::summarise(
      admission_date = min(.data$admission_date),
      discharge_date = max(.data$discharge_date),
      date_of_death = max(.data$date_of_death)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-"index")

  # Combine all SMR data
  combined_smr <- dplyr::bind_rows(
    smr01 %>%
      dplyr::mutate(recid = "01"),
    smr04 %>%
      dplyr::mutate(recid = "04")
  ) %>%
    # Aggregate where SMR01 and SMR04 stays overlap
    dplyr::group_by(.data$link_no) %>%
    dplyr::arrange(.data$admission_date) %>%
    dplyr::mutate(index = c(0, cumsum(as.numeric(dplyr::lead(.data$admission_date)) >
      cummax(as.numeric(.data$discharge_date)))[-dplyr::n()])) %>%
    dplyr::group_by(.data$link_no, .data$index) %>%
    dplyr::summarise(
      admission_date = min(.data$admission_date),
      discharge_date = max(.data$discharge_date),
      date_of_death = max(.data$date_of_death)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(-"index")

  return(combined_smr)
}
