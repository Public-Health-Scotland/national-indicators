slf_aggregated <- slf_aggregated %>% dplyr::select(-bedday_names)

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

records_date_ranges <-
  slf_aggregated %>%
  dplyr::mutate(
    record_interval = lubridate::interval(record_keydate1, record_keydate2),
    april_flag = lubridate::`%within%`(record_interval, month_intervals[[1]]) | lubridate::`%within%`(month_intervals[[1]], record_interval),
    april_beddays = dplyr::if_else(april_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[1]]), lubridate::interval("2020-01-01", "2020-01-01")),
    may_flag = lubridate::`%within%`(record_interval, month_intervals[[2]]) | lubridate::`%within%`(month_intervals[[2]], record_interval),
    may_beddays = dplyr::if_else(may_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[2]]), lubridate::interval("2020-01-01", "2020-01-01")),
    june_flag = lubridate::`%within%`(record_interval, month_intervals[[3]]) | lubridate::`%within%`(month_intervals[[3]], record_interval),
    june_beddays = dplyr::if_else(june_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[3]]), lubridate::interval("2020-01-01", "2020-01-01")),
    july_flag = lubridate::`%within%`(record_interval, month_intervals[[4]]) | lubridate::`%within%`(month_intervals[[4]], record_interval),
    july_beddays = dplyr::if_else(july_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[4]]), lubridate::interval("2020-01-01", "2020-01-01")),
    august_flag = lubridate::`%within%`(record_interval, month_intervals[[5]]) | lubridate::`%within%`(month_intervals[[5]], record_interval),
    august_beddays = dplyr::if_else(august_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[5]]), lubridate::interval("2020-01-01", "2020-01-01")),
    september_flag = lubridate::`%within%`(record_interval, month_intervals[[6]]) | lubridate::`%within%`(month_intervals[[6]], record_interval),
    september_beddays = dplyr::if_else(september_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[6]]), lubridate::interval("2020-01-01", "2020-01-01")),
    october_flag = lubridate::`%within%`(record_interval, month_intervals[[7]]) | lubridate::`%within%`(month_intervals[[7]], record_interval),
    october_beddays = dplyr::if_else(october_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[7]]), lubridate::interval("2020-01-01", "2020-01-01")),
    november_flag = lubridate::`%within%`(record_interval, month_intervals[[8]]) | lubridate::`%within%`(month_intervals[[8]], record_interval),
    november_beddays = dplyr::if_else(november_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[8]]), lubridate::interval("2020-01-01", "2020-01-01")),
    december_flag = lubridate::`%within%`(record_interval, month_intervals[[9]]) | lubridate::`%within%`(month_intervals[[9]], record_interval),
    december_beddays = dplyr::if_else(december_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[9]]), lubridate::interval("2020-01-01", "2020-01-01")),
    january_flag = lubridate::`%within%`(record_interval, month_intervals[[10]]) | lubridate::`%within%`(month_intervals[[10]], record_interval),
    january_beddays = dplyr::if_else(january_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[10]]), lubridate::interval("2020-01-01", "2020-01-01")),
    february_flag = lubridate::`%within%`(record_interval, month_intervals[[11]]) | lubridate::`%within%`(month_intervals[[11]], record_interval),
    february_beddays = dplyr::if_else(february_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[11]]), lubridate::interval("2020-01-01", "2020-01-01")),
    march_flag = lubridate::`%within%`(record_interval, month_intervals[[12]]) | lubridate::`%within%`(month_intervals[[12]], record_interval),
    march_beddays = dplyr::if_else(march_flag == TRUE, lubridate::intersect(record_interval, month_intervals[[12]]), lubridate::interval("2020-01-01", "2020-01-01")),
    dplyr::across(dplyr::contains("_beddays"), ~ lubridate::int_length(.x) / 86400)
  ) %>%
  tidyr::pivot_longer(
    cols = c(contains("beddays")),
    names_to = c("month", ".value"),
    names_sep = "_"
  )

test <- records_date_ranges %>%
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
