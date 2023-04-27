library(magrittr)

cost_names <- tolower(paste0(month.abb, "_cost"))

slf_aggregated <-
  arrow::read_parquet("/conf/sourcedev/Source_Linkage_File_Updates/1718/source-episode-file-1718_ni_version.parquet",
                      col_select = c(
                        "year", "chi", "cij_marker", "cij_pattype", "cij_admtype", "age", "recid", "smrtype",
                        "record_keydate1", "record_keydate2",
                        "lca", "location", "datazone2011",
                        "yearstay", dplyr::all_of(cost_names)
                      )
  ) %>%
  dplyr::mutate(cij_pattype = dplyr::if_else(.data$cij_admtype == 18, "Non-Elective", .data$cij_pattype)) %>%
                  (.data$cij_pattype == "Non-Elective" & .data$smrtype %in% c("Acute-IP", "Psych-IP", "GLS-IP"))) %>%
  dplyr::filter(!is_missing(.data$chi),
                  .data$age >= 18,
                  !(.data$location %in% c("T113H", "S206H", "G106H")),
                  !is_missing(.data$datazone2011),
                  .data$recid %in% c("01B", "04B", "GLS"),
  dtplyr::lazy_dt() %>%
  # Aggregate to CIJ level
  dplyr::group_by(year, chi, cij_marker) %>%
  dplyr::summarise(
    record_keydate1 = min(record_keydate1),
    record_keydate2 = max(record_keydate2),
    dplyr::across(c("lca", "datazone2011"), dplyr::last),
    dplyr::across(dplyr::all_of(cost_names), sum, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble()

month_intervals <-
  c(
    "april" = lubridate::interval("2017-04-01", "2017-05-01"),
    "may" = lubridate::interval("2017-05-01", "2017-06-01"),
    "june" = lubridate::interval("2017-06-01", "2017-07-01"),
    "july" = lubridate::interval("2017-07-01", "2017-08-01"),
    "august" = lubridate::interval("2017-08-01", "2017-09-01"),
    "september" = lubridate::interval("2017-09-01", "2017-10-01"),
    "october" = lubridate::interval("2017-10-01", "2017-11-01"),
    "november" = lubridate::interval("2017-11-01", "2017-12-01"),
    "december" = lubridate::interval("2017-12-01", "2018-01-01"),
    "january" = lubridate::interval("2018-01-01", "2018-02-01"),
    "february" = lubridate::interval("2018-02-01", "2018-03-01"),
    "march" = lubridate::interval("2018-03-01", "2018-04-01")
  )

library(lubridate)

records_date_ranges <-
  slf_aggregated %>%
  dplyr::mutate(
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
      february_flag == TRUE & record_keydate1 < int_start(month_intervals[[11]]) & record_keydate2 > int_end(month_intervals[[11]]) ~ 2419200,
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
    dplyr::across(dplyr::contains("_beddays"), ~ lubridate::int_length(.x) / 86400))

test <- records_date_ranges %>%
  tidyr::pivot_longer(
    cols = c(contains("_beddays")),
    names_to = c("month", ".value"),
    names_sep = "_"
  ) %>%
  dplyr::group_by(year, lca, datazone2011, month) %>%
  dplyr::summarise(beddays = sum(beddays)) %>%
  dplyr::ungroup()

test2 <- test %>%
  dplyr::left_join(., readr::read_rds(get_locality_path()), by = "datazone2011") %>%
  dplyr::mutate(pop_year = "2017") %>%
  dplyr::left_join(., read_population_lookup(min_year = 2016, ages_required = "over18"), by = c("hscp_locality" = "locality", "pop_year", "ca2019name" = "partnership")) %>%
  dplyr::mutate(month = "Annual", hscp_locality = "All") %>%
  dplyr::group_by(year, ca2019name, hscp_locality, month, over18_pop) %>%
  dplyr::summarise(beddays = sum(beddays)) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(year, ca2019name, hscp_locality, month) %>%
  dplyr::summarise(beddays = sum(beddays), over18_pop = sum(over18_pop)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(value = beddays / over18_pop * 100000)

