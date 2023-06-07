#' Define and return the intervals for each month of the financial year
#'
#' @param year A year in format "XXYY"
#'
#' @return A vector of [lubridate::interval()] objects
get_month_intervals <- function(year) {
  first_year <- paste0("20", stringr::str_sub(year, 1, 2))
  second_year <- paste0("20", stringr::str_sub(year, 3, 4))

  month_intervals <-
    c(
      "apr" = lubridate::interval(glue::glue("{first_year}-04-01"), glue::glue("{first_year}-05-01")),
      "may" = lubridate::interval(glue::glue("{first_year}-05-01"), glue::glue("{first_year}-06-01")),
      "jun" = lubridate::interval(glue::glue("{first_year}-06-01"), glue::glue("{first_year}-07-01")),
      "jul" = lubridate::interval(glue::glue("{first_year}-07-01"), glue::glue("{first_year}-08-01")),
      "aug" = lubridate::interval(glue::glue("{first_year}-08-01"), glue::glue("{first_year}-09-01")),
      "sep" = lubridate::interval(glue::glue("{first_year}-09-01"), glue::glue("{first_year}-10-01")),
      "oct" = lubridate::interval(glue::glue("{first_year}-10-01"), glue::glue("{first_year}-11-01")),
      "nov" = lubridate::interval(glue::glue("{first_year}-11-01"), glue::glue("{first_year}-12-01")),
      "dec" = lubridate::interval(glue::glue("{first_year}-12-01"), glue::glue("{second_year}-01-01")),
      "jan" = lubridate::interval(glue::glue("{second_year}-01-01"), glue::glue("{second_year}-02-01")),
      "feb" = lubridate::interval(glue::glue("{second_year}-02-01"), glue::glue("{second_year}-03-01")),
      "mar" = lubridate::interval(glue::glue("{second_year}-03-01"), glue::glue("{second_year}-04-01"))
    )

  return(month_intervals)
}


#' Calculate monthly beddays for NI12
#'
#' @param data The SLF after having been aggregated from [prepare_slf_episode_file()]
#' @param year The year in "XXYY" format
#'
#' @return The data with monthly beddays as variables
calculate_monthly_beddays <- function(data, year) {
  # Make sure February has the right number of days
  feb_length <- dplyr::case_when(
    year %in% c("1516", "1920", "2324", "2728") ~ 86400 * 29,
    TRUE ~ 86400 * 28
  )

  # Read in the month intervals from function
  month_intervals <- get_month_intervals(year)

  return_data <-
    data %>%
    dplyr::mutate(
      # Calculate the length of the given record
      record_interval = interval(record_keydate1, record_keydate2),
      april_flag = int_overlaps(record_interval, month_intervals[[1]]),
      april_beddays = dplyr::case_when(
        # Record is only one day long
        april_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before April and ends in April
        april_flag == TRUE & record_keydate1 < int_start(month_intervals[[1]]) & record_keydate2 %within% month_intervals[[1]] ~ int_length(interval(int_start(month_intervals[[1]]), record_keydate2)),
        # Record begins after April and ends after April
        april_flag == TRUE & record_keydate1 %within% month_intervals[[1]] & record_keydate2 > int_end(month_intervals[[1]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[1]]))),
        # Record exists entirely within April
        april_flag == TRUE & record_interval %within% month_intervals[[1]] ~ int_length(record_interval),
        # Record starts before April and ends after April
        april_flag == TRUE & record_keydate1 < int_start(month_intervals[[1]]) & record_keydate2 > int_end(month_intervals[[1]]) ~ 2592000,
        # Record doesn't touch April
        april_flag == FALSE ~ 0
      ),
      may_flag = int_overlaps(record_interval, month_intervals[[2]]),
      may_beddays = dplyr::case_when(
        # Record is only one day long
        may_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before may and ends in may
        may_flag == TRUE & record_keydate1 < int_start(month_intervals[[2]]) & record_keydate2 %within% month_intervals[[2]] ~ int_length(interval(int_start(month_intervals[[2]]), record_keydate2)),
        # Record begins after may and ends after may
        may_flag == TRUE & record_keydate1 %within% month_intervals[[2]] & record_keydate2 > int_end(month_intervals[[2]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[2]]))),
        # Record exists entirely within may
        may_flag == TRUE & record_interval %within% month_intervals[[2]] ~ int_length(record_interval),
        # Record starts before may and ends after may
        may_flag == TRUE & record_keydate1 < int_start(month_intervals[[2]]) & record_keydate2 > int_end(month_intervals[[2]]) ~ 2678400,
        # Record doesn't touch may
        may_flag == FALSE ~ 0
      ),
      june_flag = int_overlaps(record_interval, month_intervals[[3]]),
      june_beddays = dplyr::case_when(
        # Record is only one day long
        june_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before june and ends in june
        june_flag == TRUE & record_keydate1 < int_start(month_intervals[[3]]) & record_keydate2 %within% month_intervals[[3]] ~ int_length(interval(int_start(month_intervals[[3]]), record_keydate2)),
        # Record begins after june and ends after june
        june_flag == TRUE & record_keydate1 %within% month_intervals[[3]] & record_keydate2 > int_end(month_intervals[[3]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[3]]))),
        # Record exists entirely within june
        june_flag == TRUE & record_interval %within% month_intervals[[3]] ~ int_length(record_interval),
        # Record starts before june and ends after june
        june_flag == TRUE & record_keydate1 < int_start(month_intervals[[3]]) & record_keydate2 > int_end(month_intervals[[3]]) ~ 2592000,
        # Record doesn't touch june
        june_flag == FALSE ~ 0
      ),
      july_flag = int_overlaps(record_interval, month_intervals[[4]]),
      july_beddays = dplyr::case_when(
        # Record is only one day long
        july_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before july and ends in july
        july_flag == TRUE & record_keydate1 < int_start(month_intervals[[4]]) & record_keydate2 %within% month_intervals[[4]] ~ int_length(interval(int_start(month_intervals[[4]]), record_keydate2)),
        # Record begins after july and ends after july
        july_flag == TRUE & record_keydate1 %within% month_intervals[[4]] & record_keydate2 > int_end(month_intervals[[4]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[4]]))),
        # Record exists entirely within july
        july_flag == TRUE & record_interval %within% month_intervals[[4]] ~ int_length(record_interval),
        # Record starts before july and ends after july
        july_flag == TRUE & record_keydate1 < int_start(month_intervals[[4]]) & record_keydate2 > int_end(month_intervals[[4]]) ~ 2678400,
        # Record doesn't touch july
        july_flag == FALSE ~ 0
      ),
      august_flag = int_overlaps(record_interval, month_intervals[[5]]),
      august_beddays = dplyr::case_when(
        # Record is only one day long
        august_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before august and ends in august
        august_flag == TRUE & record_keydate1 < int_start(month_intervals[[5]]) & record_keydate2 %within% month_intervals[[5]] ~ int_length(interval(int_start(month_intervals[[5]]), record_keydate2)),
        # Record begins after august and ends after august
        august_flag == TRUE & record_keydate1 %within% month_intervals[[5]] & record_keydate2 > int_end(month_intervals[[5]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[5]]))),
        # Record exists entirely within august
        august_flag == TRUE & record_interval %within% month_intervals[[5]] ~ int_length(record_interval),
        # Record starts before august and ends after august
        august_flag == TRUE & record_keydate1 < int_start(month_intervals[[5]]) & record_keydate2 > int_end(month_intervals[[5]]) ~ 2678400,
        # Record doesn't touch august
        august_flag == FALSE ~ 0
      ),
      september_flag = int_overlaps(record_interval, month_intervals[[6]]),
      september_beddays = dplyr::case_when(
        # Record is only one day long
        september_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before september and ends in september
        september_flag == TRUE & record_keydate1 < int_start(month_intervals[[6]]) & record_keydate2 %within% month_intervals[[6]] ~ int_length(interval(int_start(month_intervals[[6]]), record_keydate2)),
        # Record begins after september and ends after september
        september_flag == TRUE & record_keydate1 %within% month_intervals[[6]] & record_keydate2 > int_end(month_intervals[[6]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[6]]))),
        # Record exists entirely within september
        september_flag == TRUE & record_interval %within% month_intervals[[6]] ~ int_length(record_interval),
        # Record starts before september and ends after september
        september_flag == TRUE & record_keydate1 < int_start(month_intervals[[6]]) & record_keydate2 > int_end(month_intervals[[6]]) ~ 2592000,
        # Record doesn't touch september
        september_flag == FALSE ~ 0
      ),
      october_flag = int_overlaps(record_interval, month_intervals[[7]]),
      october_beddays = dplyr::case_when(
        # Record is only one day long
        october_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before october and ends in october
        october_flag == TRUE & record_keydate1 < int_start(month_intervals[[7]]) & record_keydate2 %within% month_intervals[[7]] ~ int_length(interval(int_start(month_intervals[[7]]), record_keydate2)),
        # Record begins after october and ends after october
        october_flag == TRUE & record_keydate1 %within% month_intervals[[7]] & record_keydate2 > int_end(month_intervals[[7]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[7]]))),
        # Record exists entirely within october
        october_flag == TRUE & record_interval %within% month_intervals[[7]] ~ int_length(record_interval),
        # Record starts before october and ends after october
        october_flag == TRUE & record_keydate1 < int_start(month_intervals[[7]]) & record_keydate2 > int_end(month_intervals[[7]]) ~ 2678400,
        # Record doesn't touch october
        october_flag == FALSE ~ 0
      ),
      november_flag = int_overlaps(record_interval, month_intervals[[8]]),
      november_beddays = dplyr::case_when(
        # Record is only one day long
        november_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before november and ends in november
        november_flag == TRUE & record_keydate1 < int_start(month_intervals[[8]]) & record_keydate2 %within% month_intervals[[8]] ~ int_length(interval(int_start(month_intervals[[8]]), record_keydate2)),
        # Record begins after november and ends after november
        november_flag == TRUE & record_keydate1 %within% month_intervals[[8]] & record_keydate2 > int_end(month_intervals[[8]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[8]]))),
        # Record exists entirely within november
        november_flag == TRUE & record_interval %within% month_intervals[[8]] ~ int_length(record_interval),
        # Record starts before november and ends after november
        november_flag == TRUE & record_keydate1 < int_start(month_intervals[[8]]) & record_keydate2 > int_end(month_intervals[[8]]) ~ 2592000,
        # Record doesn't touch november
        november_flag == FALSE ~ 0
      ),
      december_flag = int_overlaps(record_interval, month_intervals[[9]]),
      december_beddays = dplyr::case_when(
        # Record is only one day long
        december_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before december and ends in december
        december_flag == TRUE & record_keydate1 < int_start(month_intervals[[9]]) & record_keydate2 %within% month_intervals[[9]] ~ int_length(interval(int_start(month_intervals[[9]]), record_keydate2)),
        # Record begins after december and ends after december
        december_flag == TRUE & record_keydate1 %within% month_intervals[[9]] & record_keydate2 > int_end(month_intervals[[9]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[9]]))),
        # Record exists entirely within december
        december_flag == TRUE & record_interval %within% month_intervals[[9]] ~ int_length(record_interval),
        # Record starts before december and ends after december
        december_flag == TRUE & record_keydate1 < int_start(month_intervals[[9]]) & record_keydate2 > int_end(month_intervals[[9]]) ~ 2678400,
        # Record doesn't touch december
        december_flag == FALSE ~ 0
      ),
      january_flag = int_overlaps(record_interval, month_intervals[[10]]),
      january_beddays = dplyr::case_when(
        # Record is only one day long
        january_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before january and ends in january
        january_flag == TRUE & record_keydate1 < int_start(month_intervals[[10]]) & record_keydate2 %within% month_intervals[[10]] ~ int_length(interval(int_start(month_intervals[[10]]), record_keydate2)),
        # Record begins after january and ends after january
        january_flag == TRUE & record_keydate1 %within% month_intervals[[10]] & record_keydate2 > int_end(month_intervals[[10]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[10]]))),
        # Record exists entirely within january
        january_flag == TRUE & record_interval %within% month_intervals[[10]] ~ int_length(record_interval),
        # Record starts before january and ends after january
        january_flag == TRUE & record_keydate1 < int_start(month_intervals[[10]]) & record_keydate2 > int_end(month_intervals[[10]]) ~ 2678400,
        # Record doesn't touch january
        january_flag == FALSE ~ 0
      ),
      february_flag = int_overlaps(record_interval, month_intervals[[11]]),
      february_beddays = dplyr::case_when(
        # Record is only one day long
        february_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before february and ends in february
        february_flag == TRUE & record_keydate1 < int_start(month_intervals[[11]]) & record_keydate2 %within% month_intervals[[11]] ~ int_length(interval(int_start(month_intervals[[11]]), record_keydate2)),
        # Record begins after february and ends after february
        february_flag == TRUE & record_keydate1 %within% month_intervals[[11]] & record_keydate2 > int_end(month_intervals[[11]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[11]]))),
        # Record exists entirely within february
        february_flag == TRUE & record_interval %within% month_intervals[[11]] ~ int_length(record_interval),
        # Record starts before february and ends after february
        february_flag == TRUE & record_keydate1 < int_start(month_intervals[[11]]) & record_keydate2 > int_end(month_intervals[[11]]) ~ feb_length,
        # Record doesn't touch february
        february_flag == FALSE ~ 0
      ),
      march_flag = int_overlaps(record_interval, month_intervals[[12]]),
      march_beddays = dplyr::case_when(
        # Record is only one day long
        march_flag == TRUE & record_keydate1 == record_keydate2 ~ 86400,
        # Record begins before march and ends in march
        march_flag == TRUE & record_keydate1 < int_start(month_intervals[[12]]) & record_keydate2 %within% month_intervals[[12]] ~ int_length(interval(int_start(month_intervals[[12]]), record_keydate2)),
        # Record begins after march and ends after march
        march_flag == TRUE & record_keydate1 %within% month_intervals[[12]] & record_keydate2 > int_end(month_intervals[[12]]) ~ int_length(interval(record_keydate1, int_end(month_intervals[[12]]))),
        # Record exists entirely within march
        march_flag == TRUE & record_interval %within% month_intervals[[12]] ~ int_length(record_interval),
        # Record starts before march and ends after march
        march_flag == TRUE & record_keydate1 < int_start(month_intervals[[12]]) & record_keydate2 > int_end(month_intervals[[12]]) ~ 2678400,
        # Record doesn't touch march
        march_flag == FALSE ~ 0
      ),
      # Convert to days
      dplyr::across(dplyr::contains("_beddays"), ~ lubridate::int_length(.x) / 86400)
    )

  return(return_data)
}

#' Format the months and year for splitting
#'
#' @param data A data frame
#' @param year The financial year in "XXYY" format
#'
#' @return A data frame with added columns for population year, financial year,
#' calendar year, calendar month, and financial month
format_date_levels_ni13 <- function(data, year) {
  years <- get_different_years(year)

  return_data <- data %>%
    tidyr::pivot_longer(
      cols = c(contains("_beddays")),
      names_to = c("month", ".value"),
      names_sep = "_"
    ) %>%
    dplyr::mutate(
      pop_year = magrittr::extract2(years, "pop_year"),
      fin_year = magrittr::extract2(years, "financial_year"),
      cal_year = dplyr::case_when(
        month %in% c("april", "may", "june", "july", "august", "september", "october", "november", "december") ~ magrittr::extract2(years, "first_cal_year"),
        TRUE ~ magrittr::extract2(years, "second_cal_year")
      ),
      fin_month = calculate_fin_month_from_string(.data$month),
      cal_month = as.character(match(stringr::str_to_title(month), month.name))
    )

  return(return_data)
}

#' Match on geographies to data, select only relevant variables and aggregate for NI13
#'
#' @param data A data frame after [format_date_levels_ni13()]
#'
#' @return An aggregated data frame to Locality level at the lowest
prepare_for_groupings <- function(data) {
  return_data <- data %>%
    dplyr::left_join(., readr::read_rds(get_locality_path()), by = "datazone2011") %>%
    dplyr::mutate(partnership = phsmethods::match_area(ca2018)) %>%
    dplyr::rename(locality = hscp_locality) %>%
    dplyr::select(
      "partnership",
      "hscp_locality",
      "cal_year",
      "cal_month",
      "fin_year",
      "fin_month",
      "pop_year",
      "beddays"
    ) %>%
    dplyr::group_by(
      partnership,
      hscp_locality,
      cal_year,
      cal_month,
      fin_year,
      fin_month,
      pop_year
    ) %>%
    dplyr::summarise(beddays = sum(beddays, na.rm = TRUE)) %>%
    dplyr::ungroup()

  return(return_data)
}

#' Split the data into calendar year and financial year, and add yearly totals,
#' Scotland totals, locality totals, and C&S totals
#'
#' @param data A data frame after [prepare_for_groupings()]
#'
#' @return A data frame with the described groups added and aggregated
add_all_groupings_ni13 <- function(data) {
  pop_year <- min(data$pop_year)

  different_year_levels <-
    list(
      fin_year = data %>% dplyr::select(-cal_year, -cal_month) %>%
        dplyr::rename(year = fin_year, month = fin_month),
      cal_year = data %>% dplyr::select(-fin_year, -fin_month) %>%
        dplyr::rename(year = cal_year, month = cal_month)
    )

  cal_year <- different_year_levels[["cal_year"]] %>%
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
      cols = c("locality", "temp_loc"),
      values_to = "locality"
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
    dplyr::group_by(year, partnership, locality, month, pop_year) %>%
    dplyr::summarise(beddays = sum(.data$beddays)) %>%
    dplyr::ungroup() %>%
    dplyr::filter((partnership != "Scotland" | hscp_locality == "All") & !is.na(partnership)) %>%
    dplyr::left_join(.,
      read_population_lookup(min_year = pop_year, ages_required = "over18"),
      by = c("locality", "pop_year", "partnership")
    ) %>%
    dplyr::mutate(value = beddays / over18_pop * 100000) %>%
    dplyr::rename(time_period = month)

  fin_year <- different_year_levels[["fin_year"]] %>%
    # Annual totals
    dplyr::mutate(temp_month = "Annual") %>%
    tidyr::pivot_longer(
      cols = c("month", "temp_month"),
      values_to = "time_period"
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
      cols = c("locality", "temp_loc"),
      values_to = "locality"
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
    dplyr::group_by(year, partnership, locality, month, pop_year) %>%
    dplyr::summarise(beddays = sum(.data$beddays)) %>%
    dplyr::ungroup() %>%
    dplyr::filter((partnership != "Scotland" | locality == "All") & !is.na(partnership)) %>%
    dplyr::left_join(.,
      read_population_lookup(min_year = pop_year, ages_required = "over18"),
      by = c("locality", "pop_year", "partnership")
    ) %>%
    dplyr::mutate(value = beddays / over18_pop * 100000)

  return(list(
    "fin_year" = fin_year,
    "cal_year" = cal_year
  ))
}

#' Fully calculate NI13 for a given financial year and save out if required
#'
#' @param year Financial year in "XXYY" format
#' @param write_to_disk When TRUE, writes to disk
#' @seealso [get_ni_output_dir()]
#'
#' @return A list of data frames
#' @export
calculate_ni13 <- function(year, write_to_disk = FALSE) {
  return_data <-
    prepare_slf_episode_file(year) %>%
    calculate_monthly_beddays(year) %>%
    format_date_levels_ni13(year) %>%
    add_all_groupings_ni13()

  if (write_to_disk == TRUE) {
    arrow::write_parquet(return_data[["fin_year"]], glue::glue("{get_ni_output_dir()}/NI13_20{year}_financial_year.parquet"))
    arrow::write_parquet(return_data[["cal_year"]], glue::glue("{get_ni_output_dir()}/NI13_20{year}_calendar_year.parquet"))
  }

  return(return_data)
}
