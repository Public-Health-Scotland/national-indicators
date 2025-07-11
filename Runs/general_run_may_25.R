## Read in packages if required
library(readr)    # Read .RDS files
library(fs)       # Easier file paths
library(dplyr)    # General tidyverse
library(haven)    # Read/write .sav
library(slfhelper) # Open SLFs easier
library(stringr)  # String manipulation
library(tidyr)    # General
library(magrittr) # Pipe function
library(fst)      # Read/write fst
library(purrr)    # Mapping functions over lists
library(phsmethods) # PHS functions
library(styler)   # Code styling
library(rlang)    # General
library(tictoc)   # Timing functions
library(janitor)  # Cleaning data
library(tidylog)  # Log in console
library(here)     # Filepath link
library(glue)     # For saving files in required folder
library(lubridate)
library(dtplyr)

## NI14 calculations
smra_data <- arrow::read_parquet(fs::path(get_ni_output_dir(), "ni14_smra_temp.parquet")) %>% tibble::as_tibble() %>% dplyr::ungroup()
nrs_data <- arrow::read_parquet(fs::path(get_ni_output_dir(), "ni14_nrs_temp.parquet"))

matched_extracts <-
  dplyr::left_join(smra_data, nrs_data, by = "link_no", relationship = "many-to-one") %>%
  dplyr::mutate(
    # Variable for if person is discharged dead in SMRA, any discharge type
    # in the 40s
    discharged_dead = discharge_type %/% 10 == 4,
    # Time between SMRA discharge date and NRS death date
    discharge_to_death = lubridate::int_length(lubridate::interval(cis_disdate, death_date)),
    # Flag if discharge occurs after death or death occurs before admission (data quality)
    death_before_discharge = discharge_to_death <= 0 & death_date >= cis_admdate,
    # Flag to confirm person is declared dead in both extracts
    discharged_dead_both = death_before_discharge | discharged_dead,
    discharged_dead_both = tidyr::replace_na(discharged_dead_both, FALSE),
    # Set up a flag to keep records where patient is not dead at discharge date
    stay = !discharged_dead_both,
  )

final_data <- matched_extracts %>%
  calculate_locality_totals() %>%
  purrr::map(~ add_additional_groups_ni14(.x)) %>%
  purrr::map(~ dplyr::mutate(.x, value = .data$emergency_readm_28 / .data$stay * 1000))

arrow::write_parquet(final_data[["fin_year"]], fs::path(get_ni_output_dir(), "ni14_financial_year.parquet"))
arrow::write_parquet(final_data[["cal_year"]], fs::path(get_ni_output_dir(), "ni14_calendar_year.parquet"))

ni14 <- arrow::read_parquet(fs::path(get_ni_output_dir(), "ni14_financial_year.parquet"))

ni14 <- ni14 %>%
  dplyr::mutate(indicator = "NI14") %>%
  dplyr::rename(
    time_period = month,
    numerator = emergency_readm_28 ,
    denominator = stay)

ni14_quarterly <- ni14 %>% dplyr::mutate(time_period = convert_fin_month_to_quarter(time_period)) %>%
  dtplyr::lazy_dt() %>%
  dplyr::group_by(indicator, year, time_period, partnership, locality) %>%
  dplyr::summarise(dplyr::across(c("numerator", "denominator"), sum, na.rm = T)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::mutate(value = (numerator / denominator) * 1000)

ni14_scot <- ni14_quarterly %>% dplyr::filter(partnership == "Scotland" & locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni14_with_scot <- dplyr::left_join(ni14_quarterly, ni14_scot, by = c("year", "time_period")) %>%
  dplyr::filter(partnership != "Scotland")

ni14_cal_annual <- arrow::read_parquet(fs::path(get_ni_output_dir(), "ni14_calendar_year.parquet")) %>%
  dplyr::filter(month == "Annual") %>%
  dplyr::mutate(indicator = "NI14") %>%
  dplyr::rename(
    time_period = month,
    numerator = emergency_readm_28 ,
    denominator = stay)

ni14_cal_scot <- ni14_cal_annual %>% dplyr::filter(partnership == "Scotland" & locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni14_cal_final <- dplyr::left_join(ni14_cal_annual, ni14_cal_scot, by = c("year", "time_period")) %>%
  dplyr::mutate(year = as.character(year))

ni14_final <- dplyr::bind_rows(ni14_with_scot, ni14_cal_final)

arrow::write_parquet(ni14_with_scot, fs::path(get_ni_output_dir(), "ni14_tableau_output_may_25.parquet"))

ni14_quarterly_xl <- ni14_quarterly %>%
  dplyr::filter(!(year %in% c("2015/16", "2025/26")) & locality == "All") %>%
  dplyr::mutate(ind_no = 14L, estimate = "No") %>%
  dplyr::select(year, time_period, partnership, indicator, estimate, numerator, denominator, `rate` = value, ind_no) %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership))

ni14_cal_xl <- ni14_cal_annual %>%
  dplyr::filter(year != "2015" & year != "2025") %>%
  dplyr::filter(locality == "All") %>%
  dplyr::mutate(ind_no = 14L, estimate = "No", year = as.character(year)) %>%
  dplyr::select(year, time_period, partnership, indicator, estimate, numerator, denominator, `rate` = value, ind_no) %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership))

ni14_xl <- dplyr::bind_rows(ni14_quarterly_xl, ni14_cal_xl)

ni14_xl <- filter(ni14_xl, year != "2017" & year != "2018" & year != "2019" & year != "2020" & year != "2021" & year != "2022" & year != "2023")
ni14_xl <- filter(ni14_xl, year != "2017/18")

arrow::write_parquet(ni14_xl, fs::path(get_ni_output_dir(), "ni14_excel_output_jun_25.parquet"))
write_csv(ni14_xl, "/conf/sourcedev/TableauUpdates/Ryan/ni14_excel_output_may_25.csv")

## Make a new population lookup

test <- create_population_lookup()
test <- create_lca_population_lookup()

## NI15

ni15_raw <- calculate_ni15(extract_start = "01-APR-2016", extract_end = "25-APR-2024")

arrow::write_parquet(ni15_raw[["fin_year"]], fs::path(get_ni_output_dir(), "ni15_financial_year.parquet"))
arrow::write_parquet(ni15_raw[["cal_year"]], fs::path(get_ni_output_dir(), "ni15_calendar_year.parquet"))

ni15_fin <- arrow::read_parquet(fs::path(get_ni_output_dir(), "ni15_financial_year.parquet"))
ni15_cal <- arrow::read_parquet(fs::path(get_ni_output_dir(), "ni15_calendar_year.parquet"))

ni15_fin <- ni15_fin %>%
  dplyr::rename(partnership = ca2019name, locality = hscp_locality) %>%
  dplyr::mutate(time_period = convert_fin_month_to_quarter(time_period)) %>%
  dplyr::group_by(year, partnership, locality, time_period, indicator) %>%
  dplyr::summarise(numerator = sum(numerator),
                   denominator = sum(denominator)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(value = 100 - ((numerator / (denominator * 182.5)) * 100))

ni15_cal <- ni15_cal %>%
  dplyr::rename(partnership = ca2019name, locality = hscp_locality) %>%
  dplyr::filter(time_period == "Annual" & !(year %in% c("2015", "2025"))) %>%
  dplyr::mutate(year = as.character(year))

ni15_xl <- dplyr::bind_rows(ni15_cal, ni15_fin) %>%
  dplyr::filter(locality == "All") %>%
  dplyr::mutate(ind_no = 15L, estimate = "No", year = as.character(year)) %>%
  dplyr::select(year, time_period, partnership, indicator, estimate, numerator, denominator, `rate` = value, ind_no) %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership))

ni15_xl <- filter(ni15_xl, year != "2025/26")
ni15_xl <- filter(ni15_xl, year != "2017" & year != "2017/18" & year != "2018" & year != "2019" & year != "2020" & year != "2021" & year != "2022" & year != "2023")

arrow::write_parquet(ni15_xl, fs::path(get_ni_output_dir(), "ni15_excel_output_jun_25.parquet"))

ni15_scot <- ni15_fin %>%
  dplyr::filter(partnership == "Scotland" & locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni15_tableau <- dplyr::left_join(ni15_fin, ni15_scot) %>%
  dplyr::filter(!(partnership == "Scotland" & locality == "All"))

arrow::write_parquet(ni15_tableau, fs::path(get_ni_output_dir(), "ni15_tableau_output_may_25.parquet"))

# NI18
# Nice and easy here
ni18 <- calculate_ni18(fs::path(get_ni_input_dir(), "NI18/2024-04-25-balance-of-care.xlsm"), min_year = 2015)

## NI12

#Run for each year one at a time
year <- "1920"

slf_dir <- fs::path("/", "conf", "sourcedev", "Source_Linkage_File_Updates", year)
file_name <- stringr::str_glue("source-episode-file-{year}.parquet")
slf_path <- fs::path(slf_dir, file_name)

slf <- arrow::read_parquet(slf_path,
                           col_select = c(
                             "year", "anon_chi", "cij_marker", "cij_pattype", "cij_admtype", "age", "recid", "smrtype",
                             "record_keydate1", "record_keydate2",
                             "lca", "location", "datazone2011",
                             "yearstay")
) %>%
  dplyr::mutate(cij_pattype = dplyr::if_else(.data$cij_admtype == 18, "Non-Elective", .data$cij_pattype)) %>%
  dplyr::filter(!is_missing(.data$anon_chi) &
                  !is_missing(.data$datazone2011) &
                  .data$recid %in% c("01B", "04B", "GLS") &
                  .data$age >= 18 &
                  !(.data$location %in% c("T113H", "S206H", "G106H")) &
                  (.data$cij_pattype == "Non-Elective" & .data$smrtype %in% c("Acute-IP", "Psych-IP", "GLS-IP"))) %>%
  dtplyr::lazy_dt() %>%
  # Aggregate to CIJ level
  dplyr::group_by(year, anon_chi, cij_marker) %>%
  dplyr::summarise(
    record_keydate1 = min(record_keydate1),
    record_keydate2 = max(record_keydate2),
    dplyr::across(c("lca", "datazone2011"), dplyr::last),
    # dplyr::across(c(cost_names, bedday_names), ~ sum(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::collect()

ni12 <- calculate_ni12(slf, fin_year_start = ymd("2019-04-01"), fin_year_end = ymd("2020-03-31"))

# arrow::write_parquet(ni12[["fin_year_totals"]], glue::glue("{get_ni_output_dir()}/ni12_20{year}_financial_year.parquet"))
arrow::write_parquet(ni12[["cal_year_totals"]], glue::glue("{get_ni_output_dir()}/ni12_20{year}_calendar_year.parquet"))

## NI13

#Run for each year one at a time
year <- "1920"
slf_dir <- fs::path("/", "conf", "sourcedev", "Source_Linkage_File_Updates", year)
file_name <- stringr::str_glue("source-episode-file-{year}.parquet")
slf_path <- fs::path(slf_dir, file_name)

slf <- arrow::read_parquet(slf_path,
                           col_select = c(
                             "year", "anon_chi", "cij_marker", "cij_pattype", "cij_admtype", "age", "recid", "smrtype",
                             "record_keydate1", "record_keydate2",
                             "lca", "location", "datazone2011",
                             "yearstay")
) %>%
  dplyr::mutate(cij_pattype = dplyr::if_else(.data$cij_admtype == 18, "Non-Elective", .data$cij_pattype)) %>%
  dplyr::filter(!is_missing(.data$anon_chi) &
                  !is_missing(.data$datazone2011) &
                  .data$recid %in% c("01B", "04B", "GLS") &
                  .data$age >= 18 &
                  !(.data$location %in% c("T113H", "S206H", "G106H")) &
                  (.data$cij_pattype == "Non-Elective" & .data$smrtype %in% c("Acute-IP", "Psych-IP", "GLS-IP"))) %>%
  dtplyr::lazy_dt() %>%
  # Aggregate to CIJ level
  dplyr::group_by(year, anon_chi, cij_marker) %>%
  dplyr::summarise(
    record_keydate1 = min(record_keydate1),
    record_keydate2 = max(record_keydate2),
    dplyr::across(c("lca", "datazone2011"), dplyr::last),
    # dplyr::across(c(cost_names, bedday_names), ~ sum(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::collect()

ni13 <- slf %>%
  calculate_monthly_beddays(year) %>%
  format_date_levels_ni13(year) %>%
  prepare_for_groupings() %>%
  add_all_groupings_ni13()

# arrow::write_parquet(ni13[["fin_year"]], glue::glue("{get_ni_output_dir()}/ni13_20{year}_financial_year.parquet"))
arrow::write_parquet(ni13[["cal_year"]], glue::glue("{get_ni_output_dir()}/ni13_20{year}_calendar_year.parquet"))

## NI12 full processing

ni12_add <- dplyr::bind_rows(arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_201819_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_201920_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_202021_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_202122_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_202223_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_202324_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_202425_financial_year.parquet")))

ni12_fin <- ni12_add %>%
  dplyr::mutate(time_period = convert_fin_month_to_quarter(month),
                indicator = "NI12") %>%
  dplyr::group_by(year, partnership, hscp_locality, time_period, indicator) %>%
  dplyr::summarise(numerator = sum(admissions),
                   denominator = max(over18_pop),
                   value = sum(value)) %>%
  dplyr::ungroup()

ni12_add_cal <- dplyr::bind_rows(arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_201819_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_201920_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_202021_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_202122_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_202223_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_202324_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI12_202425_calendar_year.parquet")))

ni12_cal <- ni12_add_cal %>%
  dplyr::filter(month != "Annual" & !(year %in% c("2018", "2025"))) %>%
  dplyr::mutate(indicator = "NI12", time_period = "Annual") %>%
  dplyr::group_by(year, partnership, hscp_locality, time_period, indicator) %>%
  dplyr::summarise(numerator = sum(admissions),
                   denominator = max(over18_pop),
                   value = sum(value)) %>%
  dplyr::ungroup()

ni12_xl <- dplyr::bind_rows(ni12_fin, ni12_cal) %>%
  dplyr::filter(hscp_locality == "All") %>%
  dplyr::mutate(ind_no = 12L, estimate = "No", year = as.character(year)) %>%
  dplyr::select(year, time_period, partnership, indicator, estimate, numerator, denominator, `rate` = value, ind_no) %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership))

arrow::write_parquet(ni12_xl, fs::path(get_ni_output_dir(), "ni12_excel_output_may_24.parquet"))

ni12_scot <- ni12_fin %>%
  dplyr::filter(partnership == "Scotland" & hscp_locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni12_tableau <- dplyr::left_join(ni12_fin, ni12_scot) %>%
  dplyr::filter(!(partnership == "Scotland" & hscp_locality == "All")) %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership))

arrow::write_parquet(ni12_tableau, fs::path(get_ni_output_dir(), "ni12_tableau_output_may_24.parquet"))

## NI13 full processing

ni13_add <- dplyr::bind_rows(arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_201819_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_201920_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_202021_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_202122_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_202223_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_202324_financial_year.parquet")),
                             arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_202425_financial_year.parquet")))

ni13_fin <- ni13_add %>%
  dplyr::mutate(time_period = convert_fin_month_to_quarter(month),
                indicator = "NI13") %>%
  dplyr::group_by(year, partnership, locality, time_period, indicator) %>%
  dplyr::summarise(numerator = sum(beddays),
                   denominator = max(over18_pop),
                   value = sum(value)) %>%
  dplyr::ungroup()

ni13_add_cal <- dplyr::bind_rows(arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_201819_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_201920_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_202021_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_202122_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_202223_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_202324_calendar_year.parquet")),
                                 arrow::read_parquet(fs::path(get_ni_output_dir(), "NI13_202425_calendar_year.parquet")))

ni13_cal <- ni13_add_cal %>%
  dplyr::filter(!(year %in% c("2018", "2025"))) %>%
  dplyr::mutate(indicator = "NI13", time_period = "Annual") %>%
  dplyr::group_by(year, partnership, locality, time_period, indicator) %>%
  dplyr::summarise(numerator = sum(beddays),
                   denominator = max(over18_pop),
                   value = sum(value)) %>%
  dplyr::ungroup()

ni13_xl <- dplyr::bind_rows(ni13_fin, ni13_cal) %>%
  dplyr::filter(locality == "All") %>%
  dplyr::mutate(ind_no = 13L, estimate = "No", year = as.character(year)) %>%
  dplyr::select(year, time_period, partnership, indicator, estimate, numerator, denominator, `rate` = value, ind_no) %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership))

arrow::write_parquet(ni13_xl, fs::path(get_ni_output_dir(), "ni13_excel_output_may_24.parquet"))

ni13_scot <- ni13_fin %>%
  dplyr::filter(partnership == "Scotland" & locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni13_tableau <- dplyr::left_join(ni13_fin, ni13_scot) %>%
  dplyr::filter(!(partnership == "Scotland" & locality == "All")) %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership))

arrow::write_parquet(ni13_tableau, fs::path(get_ni_output_dir(), "ni13_tableau_output_may_24.parquet"))

## NI11

ni11_raw <- readxl::read_excel(fs::path(get_ni_input_dir(), "NI11/age-standard-death-rates-22-tab4.xlsx"), sheet = "data for chartLA", range = "A3:AH21") %>%
  dplyr::rename(year = `...1`) %>%
  dplyr::filter(year != "All Persons") %>%
  dplyr::mutate(year = as.integer(year)) %>%
  dplyr::filter(dplyr::between(year, 2016, 2022)) %>%
  tidyr::pivot_longer(cols = c(2:34),
                      names_to = "partnership",
                      values_to = "rate")

ni11_cs <- readxl::read_excel(fs::path(get_ni_input_dir(), "NI11/age-standard-death-rates-22-cands.xlsx"), sheet = "Under 75s", range = "A5:B22", col_names = c("year", "rate")) %>%
  dplyr::mutate(partnership = "Clackmannanshire and Stirling",
                year = as.integer(year)) %>%
  dplyr::filter(dplyr::between(year, 2016, 2022))

ni11 <- dplyr::bind_rows(ni11_raw, ni11_cs) %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership),
                indicator = "NI11",
                estimate = "No",
                time_period = "Annual",
                ind_no = 11L) %>%
  dplyr::arrange(partnership) %>%
  dplyr::select(year, time_period, partnership, indicator, estimate, rate, ind_no)

ni11_scot <- ni11 %>%
  dplyr::filter(partnership == "Scotland") %>%
  dplyr::select(year, `scotland` = rate)

ni11_tableau <- dplyr::left_join(ni11, ni11_scot) %>%
  dplyr::filter(partnership != "Scotland")

arrow::write_parquet(ni11, fs::path(get_ni_output_dir(), "ni11_excel_output_may_24.parquet"))
arrow::write_parquet(ni11_tableau, fs::path(get_ni_output_dir(), "ni11_tableau_output_may_24.parquet"))

## NI16

ni16_raw <- calculate_ni16_final_output(write_to_disk = TRUE)

arrow::write_parquet(ni16_raw[["calendar"]], fs::path(get_ni_output_dir(), "ni16_calendar_year.parquet"))
arrow::write_parquet(ni16_raw[["financial"]], fs::path(get_ni_output_dir(), "ni16_financial_year.parquet"))

ni16 <- ni16_raw[["financial"]]
ni16 <- arrow::read_parquet(fs::path(get_ni_output_dir(), "ni16_financial_year.parquet"))
ni16_cal_annual <- arrow::read_parquet(fs::path(get_ni_output_dir(), "ni16_calendar_year.parquet")) %>%
  dplyr::filter(time_period == "Annual")

ni16_quarterly <- ni16 %>% dplyr::mutate(time_period = convert_fin_month_to_quarter(time_period)) %>%
  dtplyr::lazy_dt() %>%
  dplyr::group_by(indicator, year, time_period, partnership, locality) %>%
  dplyr::summarise(numerator = sum(numerator, na.rm = T),
                   denominator = max(denominator),
                   value = sum(value)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble()


ni16_scot <- ni16_quarterly %>% dplyr::filter(partnership == "Scotland" & locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni16_with_scot <- dplyr::left_join(ni16_quarterly, ni16_scot, by = c("year", "time_period"))

arrow::write_parquet(ni16_with_scot, fs::path(get_ni_output_dir(), "ni16_tableau_output_may_24.parquet"))

ni16_cal_annual <- ni16_raw[["calendar"]]%>%
  dplyr::filter(time_period == "Annual")

ni16_cal_scot <- ni16_cal_annual %>% dplyr::filter(partnership == "Scotland" & locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni16_cal_final <- dplyr::left_join(ni16_cal_annual, ni16_cal_scot, by = c("year", "time_period")) %>%
  dplyr::mutate(year = as.character(year)) %>%
  dplyr::select(-pop_year)

ni16_final <- dplyr::bind_rows(ni16_with_scot, ni16_cal_final) %>%
  dplyr::select(-scotland) %>%
  dplyr::filter(year != "2025/26") %>%
  dplyr::filter(locality == "All") %>%
  dplyr::select(-locality) %>%
  dplyr::mutate(estimate = "No", ind_no = 16L) %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership))

#ni16_final <- filter(ni16_final, year != "2017" & year != "2018" & year != "2019" & year != "2020" & year != "2021" & year != "2022" & year != "2023")
#ni16_final <- filter(ni16_final, year != "2017/18" & year != "2025")

arrow::write_parquet(ni16_final, fs::path(get_ni_output_dir(), "ni16_excel_output_jun_25.parquet"))

## Make a dataset without NI19

prev_update <- arrow::read_parquet(fs::path(get_ni_output_dir(), "previous_mi_output.parquet"))

## Only required when new survey is released every 2 years
ni1_9 <- arrow::read_parquet(fs::path(get_ni_output_dir(), "NI1_to_NI9_202324_spreadsheet_output.parquet"))
ni1_9 <- rename(ni1_9, rate = value)
ni1_9 <- filter(ni1_9, locality == "All")
ni1_9 <- select(ni1_9, -locality)
ni1_9 <- mutate(ni1_9, estimate = "")
ni1_9 <- mutate(ni1_9, time_period = "")
ni1_9 <- mutate(ni1_9, lookup = stringr::str_c(year, time_period, indicator, partnership, estimate))
prev_update <- bind_rows(prev_update, ni1_9)

current_raw <- dplyr::bind_rows(
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni11_excel_output_may_25.parquet")) %>%
    dplyr::mutate(year = as.character(year),
                  rate = as.numeric(rate)),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni12_excel_output_jun_25.parquet")),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni13_excel_output_jun_25.parquet")),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni14_excel_output_jun_25.parquet")),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni15_excel_output_jun_25.parquet")) %>%
    dplyr::mutate(ind_no = 15L),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni16_excel_output_jun_25.parquet")),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni18_2024_spreadsheet_output.parquet")) %>%
    dplyr::mutate(year = as.character(year))
) %>%
  dplyr::mutate(true_value = dplyr::case_when(
    is.na(value) & !is.na(rate) ~ rate,
    !is.na(value) & is.na(rate) ~ value
  )) %>%
  dplyr::select(-rate, -value) %>%
  dplyr::rename(rate = true_value) %>%
  dplyr::mutate(lookup = stringr::str_c(year, time_period, indicator, partnership, estimate))


prev_filtered <- prev_update %>%
  dplyr::filter(!(lookup %in% current_raw$lookup))


## NI17 - Usually only released in time for July publication, do not run during May release
ni17 <- readxl::read_xlsx(fs::path(get_ni_input_dir(), "/NI17/ni17_combined_years_tableau.xlsx"))
ni17 <- ni17 %>% dplyr::left_join(lookup, by = c("partnership" = "ca2019name"))
ni17 <- rename(ni17, locality = hscp_locality)

current_raw <- bind_rows(current_raw, ni17)
final_output <- current_raw %>% dplyr::select(lookup, year, time_period, partnership, indicator, estimate, numerator, denominator,
                                              rate, lower_ci, upper_ci, ind_no)

writexl::write_xlsx(final_output, fs::path(get_ni_excel_output_dir(), "spreadsheet_output_jun_2025_temp.xlsx"))

## NI19

ni19 <- calculate_ni19(file_from_dd_team = "obd_201516to202425_Q4_data") %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership))

ni19_excel <- ni19 %>%
  dplyr::select(-pop_year, -scotland) %>%
  dplyr::mutate(estimate = "No",
                ind_no = 19L)

ni19_tableau <- ni19 %>% dplyr::select(-pop_year) %>%
  dplyr::filter(partnership != "Scotland")

arrow::write_parquet(ni19_excel, fs::path(get_ni_output_dir(), "ni19_excel_output_may_25.parquet"))
arrow::write_parquet(ni19_tableau, fs::path(get_ni_output_dir(), "ni19_tableau_output_may_25.parquet"))

full_data <- readxl::read_xlsx(fs::path(get_ni_excel_output_dir(), "spreadsheet_output_jun_2025_temp.xlsx"),
                               col_types = c("text", "text", "text", "text", "text", "text",
                                             "numeric", "numeric", "numeric", "numeric", "numeric", "numeric")) %>%
  dplyr::filter(indicator != "NI19")

ni19_match <- ni19_excel %>%
  dplyr::mutate(lookup = stringr::str_c(year, time_period, indicator, partnership, estimate)) %>%
  dplyr::rename(rate = value)

final_data <- dplyr::bind_rows(full_data, ni19_match)

writexl::write_xlsx(final_data, fs::path(get_ni_excel_output_dir(), "spreadsheet_output_jun_2025.xlsx"))

## Tableau output

lookup <- readr::read_rds(get_locality_path()) %>%
  dplyr::group_by(ca2019name) %>%
  dplyr::summarise(LA_Code = dplyr::first(ca2011)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(ca2019name = dplyr::if_else(ca2019name == "City of Edinburgh", "Edinburgh", ca2019name)) %>%
  dplyr::bind_rows(., tibble::tibble(ca2019name = "Clackmannanshire and Stirling", LA_Code = "S12000005"))
lookup <- lookup %>%
  dplyr::mutate(ca2019name = standardise_partnership_names(ca2019name))

lookup_moray <- readr::read_rds(get_locality_path()) %>%
  dplyr::filter(ca2019name == "Moray") %>%
  dplyr::distinct(hscp_locality, .keep_all = T)

new_raw <- dplyr::bind_rows(
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni11_tableau_output_may_25.parquet")) %>%
    dplyr::mutate(year = as.character(year),
                  rate = as.numeric(rate),
                  scotland = as.numeric(scotland),
                  year = format_financial_year(year, type = "single_year")),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni12_tableau_output_may_25.parquet")),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni13_tableau_output_may_25.parquet")),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni14_tableau_output_may_25.parquet")),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni15_tableau_output_may_25.parquet")),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni16_tableau_output_may_25.parquet")),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni18_2024_tableau_output.parquet")) %>%
    dplyr::mutate(year = as.character(year)),
  arrow::read_parquet(fs::path(get_ni_output_dir(), "ni19_tableau_output_may_25.parquet")),
) %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership)) %>%
  dplyr::mutate(true_value = dplyr::case_when(
    is.na(value) & !is.na(rate) ~ rate,
    !is.na(value) & is.na(rate) ~ value
  )) %>%
  dplyr::select(-rate, -value) %>%
  dplyr::rename(rate = true_value) %>%
  dplyr::mutate(true_locality = dplyr::case_when(
    is.na(locality) & !is.na(hscp_locality) ~ hscp_locality,
    !is.na(locality) & is.na(hscp_locality) ~ locality,
    is.na(locality) & is.na(locality) ~ "All"
  )) %>%
  dplyr::select(-locality, -hscp_locality) %>%
  dplyr::rename(locality = true_locality) %>%
  dplyr::left_join(lookup, by = c("partnership" = "ca2019name")) %>%
  dplyr::filter(!(partnership %in% c("Other", "Scotland"))) %>%
  dplyr::mutate(lookup = paste0(indicator, year, time_period, partnership, locality))

old_raw <- read.csv(fs::path(get_ni_tableau_output_dir(), "tableau_output_june_2024.csv")) %>%
  rename(
    partnership = Partnership1,
    indicator = Indicator1,
    time_period = Data1,
    year = Year1,
    locality = Locality,
    rate = Value,
    denominator = Denominator,
    numerator = Numerator,
    scotland= Scotland
  ) %>%
  dplyr::mutate(partnership = standardise_partnership_names(partnership)) %>%
  dplyr::mutate(lookup = paste0(indicator, year, time_period, partnership, locality)) %>%
  dplyr::filter(!(lookup %in% new_raw$lookup))

output <- dplyr::bind_rows(new_raw, old_raw) %>%
  dplyr::select(indicator, year, time_period, partnership, locality, denominator, rate, scotland, numerator, LA_Code) %>%
  dplyr::filter(year != "2025/26" & !(year == "2025/26" & time_period == "Annual" & indicator %in% c("NI12", "NI13", "NI14", "NI15", "NI16")) &
                  year != "2014/15" & !(year == "2015/16" & !(indicator %in% c("NI1", "NI2", "NI3", "NI4", "NI5", "NI6", "NI7", "NI8", "NI9")))) %>%
  dplyr::filter(partnership == "Moray" & locality %in% lookup_moray$hscp_locality | partnership != "Moray" | partnership == "Moray" & locality == "All")

colnames(output) <- c("Indicator1", "Year1", "Data1", "Partnership1", "Locality", "Denominator", "Value", "Scotland", "Numerator", "LA_Code", "UpperCI", "LowerCI")

output1 <- bind_rows(output, old_raw)

haven::write_sav(output1, fs::path(get_ni_tableau_output_dir(), "NI Tableau Final-L3-September-2020.sav"))
haven::write_sav(output, fs::path(get_ni_tableau_output_dir(), "NI Tableau Final-L3-September-2020-NoSecurity.sav"))
haven::write_sav(output1 %>% dplyr::filter(Locality == "All"), fs::path(get_ni_tableau_output_dir(), "NI Tableau Final-L2-September-2020.sav"))

new <- haven::read_sav(fs::path(get_ni_tableau_output_dir(), "NI Tableau Final-L3-September-2020.sav"))
colnames(new) <- c("indicator", "year", "time_period", "partnership", "locality", "denominator", "value", "scotland", "numerator", "LA_Code", "upper_ci", "lower_ci")

arrow::write_parquet(new, fs::path(get_ni_tableau_output_dir(), "tableau_output_jun_2025.parquet"))
writexl::write_xlsx(new, fs::path(get_ni_tableau_output_dir(), "tableau_output_jun_2025.xlsx"))

#Level2 - NI20 has some issues displaying data in csv format, so read in again for Discovery files
NI20 <- read_csv(fs::path(get_ni_tableau_output_dir(), "tableau_output_june_2025.csv"))
NI20 <- filter(NI20, Indicator1 == "NI20" & Locality == "All")
NI20 <- filter(NI20, Year1 == "2016/17" | Year1 == "2017/18" | Year1 == "2018/19" | Year1 == "2019/20")

data <- filter(data, Indicator1 != "NI20")
data <- bind_rows(data, NI20)
haven::write_sav(data, fs::path(get_ni_tableau_output_dir(), "NI Tableau Final-L2-September-2020_may.sav"))
data <- haven::read_sav(fs::path(get_ni_tableau_output_dir(), "NI Tableau Final-L2-September-2020_may.sav"))
write_csv(data, fs::path(get_ni_tableau_output_dir(), "NI L2.csv"), na="")

#Level 3 - NI20 has some issues displaying data in csv format, so read in again for Discovery files
NI20 <- read_csv(fs::path(get_ni_tableau_output_dir(), "tableau_output_june_2025.csv"))
NI20 <- filter(NI20, Indicator1 == "NI20")
NI20 <- filter(NI20, Year1 == "2016/17" | Year1 == "2017/18" | Year1 == "2018/19" | Year1 == "2019/20")

#NI20 <- left_join(NI20, postcode_lookup, by = "Partnership1")
#NI20 <- NI20 %>% mutate(hb2019 = case_when(
#  Partnership1 == "Clackmannanshire and Stirling" ~ "S08000019",
#  Partnership1 == "Scotland" ~ "S92000003",
#  Partnership1 == "Undetermined" ~ "S08200003",
#  TRUE ~ hb2019
# ))

# Modify file for Level 2 of Discovery
# Add HB codes for security filters using postcode lookup
postcode_lookup_path <-
  find_latest_file("/conf/linkage/output/lookups/Unicode/Geography/Scottish Postcode Directory/",
                   regexp = "Scottish_Postcode_Directory_.+?\\.rds")
postcode_lookup <- readRDS(postcode_lookup_path) %>%
  clean_names() %>%
  dplyr::select(ca2019name, hb2019) %>%
  rename(Partnership1 = ca2019name)
postcode_lookup <- distinct(postcode_lookup, Partnership1, hb2019)
postcode_lookup <- postcode_lookup %>% mutate(Partnership1 = case_when(
  Partnership1 == "Na h-Eileanan Siar" ~ "Western Isles",
  Partnership1 == "City of Edinburgh" ~ "Edinburgh",
  TRUE ~ Partnership1
))

data1 <- haven::read_sav(fs::path(get_ni_tableau_output_dir(), "NI Tableau Final-L3-September-2020_may.sav"))
data1 <- filter(data1, Indicator1 != "NI20")
data1 <- bind_rows(data1, NI20)
haven::write_sav(data1, fs::path(get_ni_tableau_output_dir(), "NI Tableau Final-L3-September-2020_may.sav"))

data1 <- left_join(data1, postcode_lookup, by = "Partnership1")
data1 <- data1 %>% mutate(hb2019 = case_when(
  Partnership1 == "Clackmannanshire and Stirling" ~ "S08000019",
  Partnership1 == "Scotland" ~ "S92000003",
  Partnership1 == "Undetermined" ~ "S08200003",
  TRUE ~ hb2019
))

write_csv(data1, fs::path(get_ni_tableau_output_dir(), "NI_LV3.csv"), na="")
