prepare_slf_episode_file <- function(year, ni_version = FALSE) {
  cost_names <- tolower(paste0(month.abb, "_cost"))
  bedday_names <- tolower(paste0(month.abb, "_beddays"))

 slf_dir <- fs::path("/", "conf", "sourcedev", "Source_Linkage_File_Updates", year)
 
 file_name <- stringr::str_glue("source-episode-file-{year}{ifelse(ni_version, '_ni_version', '')}.parquet")
 
 slf_path <- fs::path(slf_dir, file_name)

  slf <- arrow::read_parquet(slf_path,
      col_select = c(
        "year", "chi", "cij_marker", "cij_pattype", "cij_admtype", "age", "recid", "smrtype",
        "record_keydate1", "record_keydate2",
        "lca", "location", "datazone2011",
        "yearstay", cost_names, bedday_names
      )
    ) %>%
    dplyr::mutate(cij_pattype = dplyr::if_else(.data$cij_admtype == 18, "Non-Elective", .data$cij_pattype)) %>%
    dplyr::filter(!is_missing(.data$chi) &
      !is_missing(.data$datazone2011) &
      .data$recid %in% c("01B", "04B", "GLS") &
      .data$age >= 18 &
      !(.data$location %in% c("T113H", "S206H", "G106H")) &
      (.data$cij_pattype == "Non-Elective" & .data$smrtype %in% c("Acute-IP", "Psych-IP", "GLS-IP"))) %>%
    data.table::as.data.table() %>%
    # Aggregate to CIJ level
    dplyr::group_by(year, chi, cij_marker) %>%
    dplyr::summarise(
      record_keydate1 = min(record_keydate1),
      record_keydate2 = max(record_keydate2),
      dplyr::across(c("lca", "datazone2011"), dplyr::last),
      dplyr::across(c(cost_names, bedday_names), ~sum(.x, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    dplyr::collect()

  return(slf)
}

flag_admission_records <- function(record_keydate1,
                                   fin_year_start,
                                   fin_year_end) {
  fin_year_interval <- lubridate::interval(fin_year_start, fin_year_end)
  admission_record_flag <- lubridate::`%within%`(record_keydate1, fin_year_interval)
  return(admission_record_flag)
}

calculate_ni12 <- function(data,
                           fin_year_start,
                           fin_year_end) {
  dates_and_locality <- slf_aggregated %>%
    dplyr::mutate(admission_record_flag = flag_admission_records(record_keydate1, fin_year_start, fin_year_end)) %>%
    dplyr::mutate(
      record_keydate1 =
        dplyr::if_else(record_keydate1 < fin_year_start,
          fin_year_start,
          record_keydate1
        )
    ) %>%
    dplyr::left_join(readr::read_rds(get_locality_path()),
      by = "datazone2011"
    ) %>%
    dplyr::mutate(
      fin_month = calculate_financial_month(record_keydate1),
      fin_year = phsmethods::extract_fin_year(record_keydate1),
      cal_month = as.character(lubridate::month(record_keydate1)),
      cal_year = as.character(lubridate::year(record_keydate1)),
      partnership = phsmethods::match_area(ca2018)
    )

  date_level_totals <- list(
    "fin_year_totals" = dates_and_locality %>%
      dplyr::group_by(fin_year, partnership, hscp_locality, fin_month) %>%
      dplyr::summarise(admissions = sum(admission_record_flag)) %>%
      dplyr::ungroup() %>%
      dplyr::rename(
        month = fin_month,
        year = fin_year
      ) %>%
      dplyr::mutate(pop_year = stringr::str_sub(year, 1, 4)),
    "cal_year_totals" = dates_and_locality %>%
      dplyr::group_by(cal_year, partnership, hscp_locality, cal_month) %>%
      dplyr::summarise(admissions = sum(admission_record_flag)) %>%
      dplyr::ungroup() %>%
      dplyr::rename(
        month = cal_month,
        year = cal_year
      ) %>%
      dplyr::mutate(pop_year = as.character(lubridate::year(fin_year_start)))
  )

  final_values <- date_level_totals %>%
    purrr::map(~ add_all_groupings_ni12(.x)) %>%
    purrr::map(~ dplyr::left_join(.x, read_population_lookup(min_year = 2017, ages_required = "over18"),
      by = c("hscp_locality" = "locality", "pop_year", "partnership")
    ) %>%
      dplyr::mutate(value = .data$admissions / .data$over18_pop * 100000))

  return(final_values)
}

add_all_groupings_ni12 <- function(data) {
  all_groups <- data %>%
    # Annual totals
    dplyr::mutate(temp_month = "Annual") %>%
    tidyr::pivot_longer(
      cols = c("month", "temp_month"),
      values_to = "month"
    ) %>%
    dplyr::select(-name) %>%
    # Clacks & Stirling totals
    dplyr::mutate(temp_part = dplyr::if_else(partnership %in% c("Clackmannanshire", "Stirling"),
      "Clackmannanshire and Stirling", NA_character_
    )) %>%
    tidyr::pivot_longer(
      cols = c("partnership", "temp_part"),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    # All localities totals
    dplyr::mutate(temp_loc = "All") %>%
    tidyr::pivot_longer(
      cols = c("hscp_locality", "temp_loc"),
      values_to = "hscp_locality"
    ) %>%
    dplyr::select(-name) %>%
    # Scotland totals
    dplyr::mutate(temp_part_2 = dplyr::if_else(partnership != "Clackmannanshire and Stirling", "Scotland", NA_character_)) %>%
    tidyr::pivot_longer(
      cols = c("partnership", "temp_part_2"),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    dplyr::group_by(year, partnership, hscp_locality, month, pop_year) %>%
    dplyr::summarise(admissions = sum(.data$admissions)) %>%
    dplyr::ungroup() %>%
    dplyr::filter((partnership != "Scotland" | hscp_locality == "All") & !is.na(partnership))

  return(all_groups)
}
