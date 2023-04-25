process_ni14_smra_extract_new <- function(min_date = "01-APR-2022") {
  smra_query <- glue::glue(readr::read_file("SQL/ni14_smra.sql"))

  # Read in data
  smra_extract <-
    tibble::as_tibble(
      odbc::dbGetQuery(connect_to_smra(), smra_query)
    ) %>%
    janitor::clean_names() %>%
    dplyr::mutate(
      admission_date = as.Date(.data$admission_date),
      discharge_date = as.Date(.data$discharge_date),
      discharge_type = as.integer(.data$discharge_type)
    ) %>%
    data.table::as.data.table() %>%
    dplyr::group_by(link_no, cis_marker) %>%
    dplyr::summarise(
      cis_admdate = min(admission_date),
      cis_disdate = max(discharge_date),
      admission_type = dplyr::first(admission_type),
      discharge_type = dplyr::last(discharge_type),
      postcode = dplyr::last(postcode)
    ) %>%
    dplyr::ungroup() %>%
    tibble::as_tibble() %>%
    add_readmission_flag()

  return(smra_extract)
}

process_ni14_gro_extract_new <- function(min_date = "01-APR-2022") {
  nrs_query <- glue::glue(readr::read_file("SQL/ni14_gro.sql"))

  gro_extract <- odbc::dbGetQuery(connect_to_smra(), nrs_query) %>%
    tibble::as_tibble() %>%
    # For R-standard column names
    janitor::clean_names()

  return(gro_extract)
}

match_smra_and_deaths_new <- function(smra_data, nrs_data) {
  matched_extracts <-
    dplyr::left_join(
      smra_data,
      nrs_data,
      by = "link_no",
      relationship = "many-to-one"
    )

  matched_extracts <- matched_extracts %>%
    dplyr::mutate(
      discharged_dead = add_smra_death_flag(.data$discharge_type),
      discharge_to_death = lubridate::int_length(lubridate::interval(cis_disdate, death_date)),
      death_before_discharge = .data$discharge_to_death <= 0 & death_date >= cis_admdate,
      discharged_dead_both = death_before_discharge | discharged_dead,
      discharged_dead_both = tidyr::replace_na(discharged_dead_both, FALSE),
      # Set up a flag to keep records where patient is not dead at discharge date
      stay = !.data$discharged_dead_both,
    )
}

match_on_geographies_new <- function(data) {
  return_data <- dplyr::left_join(
    data,
    readr::read_rds(get_spd_path()),
    by = dplyr::join_by("postcode" == "pc7")
  ) %>%
    dplyr::filter(!is_missing(.data$datazone2011)) %>%
    dplyr::left_join(
      readr::read_rds(get_locality_path()) %>%
        dplyr::select("datazone2011", "hscp_locality"),
      by = "datazone2011"
    )

  return(return_data)
}

calculate_locality_totals_new <- function(data) {
  # Aggregate to locality-level at the lowest
  return_data <- data %>%
    dplyr::mutate(
      fin_month = calculate_financial_month(cis_disdate),
      year = phsmethods::extract_fin_year(cis_disdate)
    ) %>%
    dplyr::select(
      "year",
      "fin_month",
      "ca2018",
      "hscp_locality",
      "datazone2011",
      "emergency_readm_28",
      "stay"
    ) %>%
    dplyr::group_by(year, fin_month, ca2018, hscp_locality) %>%
    dplyr::summarise(
      dplyr::across(c("emergency_readm_28", "stay"), ~ sum(.x, na.rm = TRUE))
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(partnership = phsmethods::match_area(ca2018))

  return(return_data)
}
