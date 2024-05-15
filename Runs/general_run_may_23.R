ni14 <- arrow::read_parquet("Outputs/NI14_calendar_year.parquet")

ni14_no_annual <- ni14 %>%
  dplyr::filter(data != "Annual")

ni14_annual <- ni14 %>%
  dplyr::mutate(data = "Annual") %>%
  dtplyr::lazy_dt() %>%
  dplyr::group_by(year, partnership, locality, data, indicator) %>%
  dplyr::summarise(dplyr::across(c("numerator", "denominator"), sum, na.rm = T)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::mutate(value = 100 - ((numerator / (denominator * 182.5)) * 100))

ni15_cal_scotland <- ni15_cal_annual %>%
  dplyr::filter(locality == "All" & partnership != "Clackmannanshire and Stirling") %>%
  dplyr::mutate(partnership = "Scotland") %>%
  dtplyr::lazy_dt() %>%
  dplyr::group_by(year, partnership, locality, data, indicator) %>%
  dplyr::summarise(dplyr::across(c("numerator", "denominator"), sum, na.rm = T)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::mutate(scotland = 100 - ((numerator / (denominator * 182.5)) * 100)) %>%
  dplyr::select(year, scotland)



ni15_call_annual_w_scotland <- dplyr::left_join(ni15_cal_annual, ni15_cal_scotland, by = "year")

ni15_cal_annual_and_monthly <- dplyr::bind_rows(ni15_cal, ni15_call_annual_w_scotland)

ni15 <- arrow::read_parquet("Outputs/NI15_financial_year.parquet")

# NI15 fin quarter

ni15 <- haven::read_sav("/conf/irf/18-End-of-Life/Publication/Ryan/data/basefiles/NI15-R.sav")

ni15_fin_months <- ni15 %>%
  dplyr::rename(time_period = data) %>%
  dplyr::filter(stringr::str_detect(time_period, "M")) %>%
  dplyr::mutate(time_period = convert_fin_month_to_quarter(time_period)) %>%
  dplyr::group_by(year, partnership, locality, time_period, indicator) %>%
  dplyr::summarise(numerator = sum(numerator),
                   denominator = sum(denominator)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(value = 100 - ((numerator / (denominator * 182.5)) * 100))

ni15_scot <- ni15_fin_months %>%
  dplyr::filter(locality == "All" & partnership != "Clackmannanshire and Stirling") %>%
  dplyr::mutate(partnership = "Scotland") %>%
    dplyr::group_by(year, partnership, locality, time_period, indicator) %>%
    dplyr::summarise(numerator = sum(numerator),
                     denominator = sum(denominator)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(value = 100 - ((numerator / (denominator * 182.5)) * 100),
                  scotland = value)

bind_scotland <- ni15_scot %>% dplyr::select(-scotland)
match_scotland <- ni15_scot %>% dplyr::select(year, time_period, scotland)

ni15_fin_months_scot <- dplyr::left_join(ni15_fin_months, match_scotland) %>%
  dplyr::bind_rows(bind_scotland)

ni15_not_fin_months <- ni15 %>%
  dplyr::rename(time_period = data) %>%
  dplyr::filter(!stringr::str_detect(time_period, "M"))

ni15_final <- dplyr::bind_rows(ni15_fin_months_scot, ni15_not_fin_months) %>%
  dplyr::mutate(scotland = dplyr::if_else(partnership == "Scotland", value, scotland))

test <- dplyr::bind_rows(ni15_final, ni15_cal_final %>% dplyr::rename(year = cal_year))

arrow::write_parquet(test, "Outputs/NI15_excel_output.parquet")

## NI14

ni14 <- arrow::read_parquet("Outputs/NI14_financial_year.parquet")

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

ni14_cal_annual <- arrow::read_parquet("Outputs/NI14_calendar_year.parquet") %>%
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


arrow::write_parquet(ni14_with_scot, fs::path(get_ni_output_dir(), "NI14_tableau_output_jun_23.parquet"))

## NI15

ni15 <- arrow::read_parquet("Outputs/NI15_financial_year.parquet")
ni15_cal <- arrow::read_parquet("Outputs/NI15_calendar_year.parquet") %>%
  dplyr::filter(data == "Annual")

ni15_final <- dplyr::bind_rows(ni15, ni15_cal) %>%
  dplyr::rename(time_period = data)

arrow::write_parquet(ni15_final, "Outputs/NI15_excel_output.parquet")

## NI16

ni16 <- arrow::read_parquet("Outputs/NI16_financial_year.parquet")

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

arrow::write_parquet(ni16_with_scot, fs::path(get_ni_output_dir(), "NI16_tableau_output_jun_23.parquet"))

ni16_cal_annual <- arrow::read_parquet("Outputs/NI16_calendar_year.parquet") %>%
  dplyr::filter(time_period == "Annual")

ni16_cal_scot <- ni16_cal_annual %>% dplyr::filter(partnership == "Scotland" & locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni16_cal_final <- dplyr::left_join(ni16_cal_annual, ni16_cal_scot, by = c("year", "time_period")) %>%
  dplyr::mutate(year = as.character(year)) %>%
  dplyr::select(-pop_year)

ni16_final <- dplyr::bind_rows(ni16_with_scot, ni16_cal_final)


arrow::write_parquet(ni16_final, "Outputs/NI16_excel_output.parquet")

## NI19

ni19_raw <- readr::read_csv("/conf/irf/03-Integration-Indicators/01-Core-Suite/NI 19/obd_201516to202223_Q4_data.csv",
                            col_names = c("partnership", "year", "age_group", "complex_needs", "Q1", "Q2", "Q3", "Q4", "Annual"),
                            col_types = "cccciiiii",
                            col_select = c(1, 2, 5:9),
                            skip = 1) %>%
  tidyr::pivot_longer(cols = c("Q1", "Q2", "Q3", "Q4", "Annual"),
                      names_to = "time_period",
                      values_to = "numerator") %>%
  dplyr::mutate(pop_year = stringr::str_sub(year, 1, 4),
                partnership = stringr::str_replace(partnership, "&", "and"),
                partnership = dplyr::if_else(stringr::str_detect(partnership, "Comha") == TRUE, "Na h-Eileanan Siar", partnership),
                partnership = dplyr::if_else(partnership == "Orkney" | partnership == "Shetland", stringr::str_c(partnership, " Islands"), partnership)) %>%
  dplyr::mutate(temp_part = dplyr::if_else(partnership %in% c("Clackmannanshire", "Stirling"),
                                           "Clackmannanshire and Stirling", NA_character_
  )) %>%
  tidyr::pivot_longer(
    cols = c("partnership", "temp_part"),
    values_to = "partnership",
    values_drop_na = TRUE
  ) %>%
  dplyr::select(-name) %>%
  dplyr::group_by(year, partnership, time_period, pop_year) %>%
  dplyr::summarise(numerator = sum(numerator, na.rm = T)) %>%
  dplyr::ungroup()

ni19_pops <- dplyr::left_join(ni19_raw,
                              read_population_lookup(min_year = 2015, ages_required = "over75") %>%
                                dplyr::group_by(pop_year, partnership) %>%
                                dplyr::summarise(denominator = sum(over75_pop)), by = c("pop_year", "partnership")) %>%
  dplyr::mutate(value = (numerator / denominator) * 1000)

ni19_scot <- ni19_pops %>% dplyr::filter(partnership == "Scotland") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni19_with_scot <- dplyr::left_join(ni19_pops, ni19_scot)

ni19_final <- ni19_with_scot %>%
  dplyr::mutate(indicator = "NI19")

arrow::write_parquet(ni19_final, "Outputs/NI19_excel_output.parquet")

# NI12 full quarterly

ni12_all_years <- dplyr::bind_rows(
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI12_201819_financial_year.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI12_201920_financial_year.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI12_202021_financial_year.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI12_202122_financial_year.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI12_202223_financial_year.parquet"))
)

ni12_quarterly <- ni12_all_years %>%
  dplyr::rename(time_period = month) %>%
  dplyr::mutate(time_period = convert_fin_month_to_quarter(time_period),
                indicator = "NI12") %>%
  dplyr::group_by(indicator, year, time_period, partnership, hscp_locality) %>%
  dplyr::summarise(numerator = sum(admissions),
                   denominator = max(over18_pop),
                   value = sum(value)) %>%
  dplyr::ungroup()

ni12_scot <- ni12_quarterly %>%
  dplyr::filter(partnership == "Scotland" & hscp_locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni12_final_fin <- dplyr::left_join(ni12_quarterly, ni12_scot)

arrow::write_parquet(ni12_final_fin, glue::glue("{get_ni_output_dir()}/NI12_financial_year_totals.parquet"))

# NI12 calendar

ni12_all_years <- dplyr::bind_rows(
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI12_202122_calendar_year.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI12_202223_calendar_year.parquet"))
) %>%
  dplyr::filter(year == "2022" & month != "Annual") %>%
  dplyr::mutate(indicator = "NI12",
                time_period = "Annual") %>%
  dplyr::group_by(year, partnership, indicator, hscp_locality, time_period) %>%
  dplyr::summarise(numerator = sum(admissions),
                   denominator = max(over18_pop),
                   value = sum(value)) %>%
  dplyr::ungroup()

ni12_scot <- ni12_all_years %>%
  dplyr::filter(partnership == "Scotland" & hscp_locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni12_final_cal <- dplyr::left_join(ni12_all_years, ni12_scot)

ni12_final <- dplyr::bind_rows(ni12_final_fin, ni12_final_cal) %>% dplyr::filter(hscp_locality == "All" & time_period == "Annual")

arrow::write_parquet(ni12_final, glue::glue("{get_ni_output_dir()}/NI12_publication_output.parquet"))

# NI13

ni13_all_years <- dplyr::bind_rows(
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI13_201819_financial_year.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI13_201920_financial_year.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI13_202021_financial_year.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI13_202122_financial_year.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI13_202223_financial_year.parquet"))
)

ni13_quarterly <- ni13_all_years %>%
  dplyr::rename(time_period = month) %>%
  dplyr::mutate(time_period = convert_fin_month_to_quarter(time_period),
                indicator = "NI13") %>%
  dplyr::group_by(indicator, year, time_period, partnership, locality) %>%
  dplyr::summarise(numerator = sum(beddays),
                   denominator = max(over18_pop),
                   value = sum(value)) %>%
  dplyr::ungroup()

ni13_scot <- ni13_quarterly %>%
  dplyr::filter(partnership == "Scotland" & locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni13_final_fin <- dplyr::left_join(ni13_quarterly, ni13_scot)

arrow::write_parquet(ni13_final_fin, glue::glue("{get_ni_output_dir()}/NI13_financial_year_totals.parquet"))

# NI12 calendar

ni13_all_years <- dplyr::bind_rows(
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI13_202122_calendar_year.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI13_202223_calendar_year.parquet"))
)  %>%
  dplyr::filter(year == "2022") %>%
  dplyr::mutate(indicator = "NI13",
                time_period = "Annual") %>%
  dplyr::group_by(year, partnership, indicator, locality, time_period) %>%
  dplyr::summarise(numerator = sum(beddays),
                   denominator = max(over18_pop),
                   value = sum(value)) %>%
  dplyr::ungroup()

ni13_scot <- ni13_all_years %>%
  dplyr::filter(partnership == "Scotland" & locality == "All") %>%
  dplyr::select(year, time_period, value) %>%
  dplyr::rename(scotland = value)

ni13_final_cal <- dplyr::left_join(ni13_all_years, ni13_scot)

ni13_final <- dplyr::bind_rows(ni13_final_fin, ni13_final_cal) %>% dplyr::filter(locality == "All" & time_period == "Annual")

arrow::write_parquet(ni13_final, glue::glue("{get_ni_output_dir()}/NI13_publication_output.parquet"))

# Everything

all_indicators <- list(
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI12_publication_output.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI13_publication_output.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI14_publication_output.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI15_publication_output.parquet")),
  arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI16_publication_output.parquet"))
)

all_indicators[[1]] <- all_indicators[[1]] %>%  dplyr::rename(locality = hscp_locality)
all_indicators[[3]] <- all_indicators[[3]] %>%  dplyr::rename(time_period = month)
all_indicators[[4]] <- all_indicators[[4]] %>%  dplyr::rename(time_period = data)

all_indicators <- dplyr::bind_rows(all_indicators) %>%
  dplyr::select(-pop_year)

all_indicators_excel <- all_indicators %>%
  dplyr::mutate(estimate = "No",
                partnership = standardise_partnership_names(partnership),
                ind_no = as.numeric(stringr::str_sub(indicator, 3, -1))) %>%
  dplyr::filter(locality == "All") %>%
  dplyr::select(year, time_period, partnership, indicator, estimate, numerator, denominator, value, ind_no) %>%
  dplyr::arrange(indicator, year, partnership) %>%
  dplyr::mutate(temp_lookup = glue::glue("{year}{time_period}{indicator}{partnership}{estimate}"))

previous_excel <- readxl::read_excel("/conf/irf/03-Integration-Indicators/01-Core-Suite/management_information_spreadsheets/Core_Suite_Integration_Indicators_Management_Information_May2023_ni_18_fix.xlsx",
                                     sheet = "Data",
                                     col_types = c("text", "text", "text", "text", "text", "text", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"),
                                     col_names = c("lookup", "year", "time_period", "partnership", "indicator", "estimate", "numerator", "denominator", "value", "ind_no", "lower_ci", "upper_ci"),
                                     skip = 1) %>%
  dplyr::select(-lookup) %>%
  dplyr::filter(dplyr::between(ind_no, 1, 9) | (time_period == "Annual" & year %in% c("2016/17", "2017/18", "2018/19", "2019/20", "2020/21", "2021/22", "2022") | indicator %in% c("NI18", "NI17", "NI11"))) %>%
  dplyr::mutate(time_period = dplyr::if_else(is.na(time_period), "", time_period),
                estimate = dplyr::if_else(is.na(estimate), "", estimate),
    temp_lookup = glue::glue("{year}{time_period}{indicator}{partnership}{estimate}"))

data_to_keep <- previous_excel %>% dplyr::filter(!(temp_lookup %in% all_indicators_excel$temp_lookup))

final <- dplyr::bind_rows(all_indicators_excel, data_to_keep) %>%
  dplyr::arrange(indicator, year, time_period, partnership) %>%
  dplyr::select(-temp_lookup)

writexl::write_xlsx(final, glue::glue("{get_ni_excel_output_dir()}/publication_output_june_2023.xlsx"))

file.copy("Outputs/spreadsheet_output_may_2022.xlsx", "/conf/irf/03-Integration-Indicators/01-Core-Suite/Spreadsheet outputs/spreadsheet_output_may_2022.xlsx", overwrite = TRUE)


## NI15 correct

ni15 <- haven::read_sav("/conf/irf/18-End-of-Life/Publication/Ryan/data/basefiles/NI15-R.sav")

ni15_cal_year <- ni15 %>%
  dplyr::filter(data != "Annual") %>%
  dplyr::mutate(time_period = dplyr::case_when(
    data == "M1" ~ "4",
    data == "M2" ~ "5",
    data == "M3" ~ "6",
    data == "M4" ~ "7",
    data == "M5" ~ "8",
    data == "M6" ~ "9",
    data == "M7" ~ "10",
    data == "M8" ~ "11",
    data == "M9" ~ "12",
    data == "M10" ~ "1",
    data == "M11" ~ "2",
    data == "M12" ~ "3",
    TRUE ~ data
  ),
  cal_year = dplyr::case_when(
    data %in% c("M10", "M11", "M12") ~ stringr::str_c("20", stringr::str_sub(year, 6, 8)),
    TRUE ~ stringr::str_sub(year, 1, 4)
  ),
  time_period = "Annual") %>%
  dtplyr::lazy_dt() %>%
  dplyr::group_by(cal_year, time_period, partnership, locality, indicator) %>%
  dplyr::summarise(dplyr::across(c(numerator, denominator), sum, na.rm = T)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::mutate(value = 100 - ((numerator / (denominator * 182.5)) * 100))

ni15_cal_scotland <- ni15_cal_year %>%
  dplyr::filter(partnership != "Clackmannanshire and Stirling" & locality == "All") %>%
  dplyr::mutate(partnership = "Scotland") %>%
  dplyr::group_by(cal_year, time_period, partnership, locality, indicator) %>%
  dplyr::summarise(dplyr::across(c(numerator, denominator), sum, na.rm = T)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::mutate(value = 100 - ((numerator / (denominator * 182.5)) * 100)) %>%
  dplyr::mutate(scotland = value)

bind_scotland <- ni15_cal_scotland %>% dplyr::select(-scotland)
match_scotland <- ni15_cal_scotland %>% dplyr::select(cal_year, time_period, scotland)

ni15_cal_final <- dplyr::left_join(ni15_cal_year, match_scotland) %>%
  dplyr::bind_rows(bind_scotland)

arrow::write_parquet(ni15_cal_final %>% dplyr::rename(year = cal_year), "Outputs/NI15_calendar_year.parquet")

## Tableau

previous_tableau <- haven::read_sav("/conf/irf/03-Integration-Indicators/01-Core-Suite/Tableau Outputs/NI Tableau Final-L3-Dec-2022_v2.sav")

ni16 <- ni16[["financial"]] %>%
  dplyr::rename(month = time_period) %>%
  dplyr::mutate(indicator = "NI16",
                time_period = convert_fin_month_to_quarter(month)) #%>%
  #dplyr::rename(numerator = emergency_readm_28, denominator = stay)

ni16_scotland <- ni16 %>%
  dplyr::filter(partnership == "Scotland" & locality == "All") %>%
  dplyr::select(year, month, value) %>%
  dplyr::rename(scotland = value)

ni16_with_scotland <- dplyr::left_join(ni16, ni16_scotland) %>%
  dplyr::filter(partnership != "Scotland")

ni16_quarterly <- ni16_with_scotland %>%
  # Group by break variables
  dplyr::group_by(indicator, year, time_period, partnership, locality) %>%
  # For indicators 14 and 15, we want to add the denominators together,
  # and for all other indicators we want the maximum
  dplyr::summarise(
    denominator = dplyr::case_when(
      indicator %in% c("NI14", "NI15") ~ sum(denominator),
      TRUE ~ max(denominator)
    ),
    # We take the sum of the indicator values, numerators, and Scotland values
    dplyr::across(c("value", "scotland", "numerator"), sum)
  ) %>%
  dplyr::ungroup() %>%
  # Now we recalculate the indicator values for NI15 and NI15 based
  # on their new denominators, and leave the value as-is for the others
  dplyr::mutate(value = dplyr::case_when(
    indicator %in% c("NI14") ~ (numerator / denominator) * 1000,
    indicator %in% c("NI15") ~ 100 - ((numerator / (denominator * 182.5)) * 100),
    TRUE ~ value
  )) %>%
  # As the above will create three identical rows per quarter for all indicators,
  # representing the three months in each quarter we started with, we remove duplicates
  dplyr::distinct()

arrow::write_parquet(ni16_quarterly, "/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/NI16_tableau_output_jun_23.parquet")

ni19 <- arrow::read_parquet("Outputs/NI19_excel_output.parquet") %>%
  dplyr::mutate(denominator = dplyr::if_else(partnership != "Scotland", denominator/2, denominator),
                value = (numerator/denominator)*1000)

arrow::write_parquet(ni19, "Outputs/NI19_excel_output.parquet")

## Everything for Tableau

output_folder <- "/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/"

lookup <- readr::read_rds(get_locality_path()) %>%
  dplyr::group_by(ca2019name) %>%
  dplyr::summarise(LA_Code = dplyr::first(ca2011)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(ca2019name = dplyr::if_else(ca2019name == "City of Edinburgh", "Edinburgh", ca2019name))

new_data <- dplyr::bind_rows(
  arrow::read_parquet(glue::glue("{output_folder}NI12_tableau_output.parquet")) %>% dplyr::rename(locality = hscp_locality),
  arrow::read_parquet(glue::glue("{output_folder}NI13_tableau_output.parquet")) %>% dplyr::rename(locality = hscp_locality),
  arrow::read_parquet(glue::glue("{output_folder}NI14_tableau_output.parquet")),
  arrow::read_parquet(glue::glue("{output_folder}NI15_tableau_output.parquet")),
  arrow::read_parquet(glue::glue("{output_folder}NI16_tableau_output.parquet")),
  arrow::read_parquet(glue::glue("{output_folder}NI18_tableau_output.parquet")),
  arrow::read_parquet(glue::glue("{output_folder}NI19_excel_output.parquet")) %>% dplyr::mutate(locality = "All") %>% dplyr::select(-pop_year)) %>%
  dplyr::mutate(partnership = stringr::str_replace(partnership, "&", "and"),
                partnership = dplyr::if_else(partnership == "City of Edinburgh", "Edinburgh", partnership)) %>%
  dplyr::filter(partnership != "Undetermined") %>%
  dplyr::left_join(., lookup, by = c("partnership" = "ca2019name")) %>%
  dplyr::mutate(partnership = dplyr::if_else(partnership == "Na h-Eileanan Siar", "Western Isles", partnership),
                LA_Code = dplyr::case_when(
                  partnership == "Western Isles" ~ "S12000013",
                  partnership == "Clackmannanshire and Stirling" ~ "S12000005",
                  TRUE ~ LA_Code
                )) %>%
  dplyr::filter(!(partnership %in% c("Other", "Scotland"))) %>%
  dplyr::mutate(lookup = paste0(indicator, year, time_period, partnership, locality))

previous_tableu <- haven::read_sav("/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/Previous update/NI Tableau Final-L3-Dec-2022_v2.sav") %>%
  dplyr::mutate(Partnership1 = stringr::str_replace(Partnership1, " & ", " and "))

colnames(previous_tableau) <- c("year", "value", "scotland", "partnership", "numerator", "locality", "indicator", "denominator", "time_period", "lower_ci", "upper_ci", "LA_Code")

previous_tableau <- previous_tableau %>%
  dplyr::mutate(lookup = paste0(indicator, year, time_period, partnership, locality))

new_output <- dplyr::bind_rows(new_data, previous_tableau %>% dplyr::filter(!(lookup %in% new_data$lookup))) %>%
  dplyr::select(-lookup)

colnames(new_output) <- c("Indicator1", "Year1", "Data1", "Partnership1", "Locality", "Denominator", "Value", "Scotland", "Numerator", "LA_Code", "UpperCI", "LowerCI")

haven::write_sav(new_output, "/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/NI Tableau Final-L3-September-2020.sav")
haven::write_sav(new_output, "/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/NI Tableau Final-L3-September-2020-NoSecurity.sav")
haven::write_sav(new_output %>% dplyr::filter(Locality == "All"), "/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/NI Tableau Final-L2-September-2020.sav")


# NI15 investigation

# "/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/NI15_tableau_output.parquet" is wrong

ryan_ni15 <- haven::read_sav("/conf/irf/18-End-of-Life/Publication/Ryan/data/basefiles/NI15-R.sav")

ni15 <- ni15_raw %>% dplyr::mutate(time_period = convert_fin_month_to_quarter(data)) %>%
  dplyr::select(-scotland, -data) %>%
  dtplyr::lazy_dt() %>%
  dplyr::group_by(year, time_period, partnership, locality, indicator) %>%
  dplyr::summarise(dplyr::across(c(numerator, denominator), sum, na.rm = T)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::mutate(value = 100 - ((numerator / (denominator * 182.5)) * 100))

ni15_scotland <- ni15 %>%
  dplyr::filter(partnership != "Clackmannanshire and Stirling" & locality == "All") %>%
  dplyr::mutate(partnership = "Scotland") %>%
  dplyr::group_by(year, time_period, partnership, locality, indicator) %>%
  dplyr::summarise(dplyr::across(c(numerator, denominator), sum, na.rm = T)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::mutate(value = 100 - ((numerator / (denominator * 182.5)) * 100)) %>%
  dplyr::mutate(scotland = value) %>%
  dplyr::select(year, time_period, scotland)

ni15_final <- dplyr::left_join(ni15, ni15_scotland) %>%
  dplyr::left_join(., la_code_lookup, by = c("partnership" = "ca2019name")) %>%
  dplyr::mutate(ca2011 = dplyr::if_else(partnership == "Clackmannanshire and Stirling", "S12000005", ca2011))
  # dplyr::rename(
  #   Indicator1 = indicator,
  #   Year1 = year,
  #   Data1 = time_period,
  #   Partnership1 = partnership,
  #   Locality = locality,
  #   Denominator = denominator,
  #   Numerator = numerator,
  #   Value = value,
  #   Scotland = scotland,
  #   LA_Code = ca2011
  # )

arrow::write_parquet(ni15_final, "/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/NI15_tableau_output_jun_23.parquet")

tableau_lv3 <- haven::read_sav("/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/NI Tableau Final-L3-September-2020.sav") %>%
  dplyr::filter(Indicator1 != "NI15")

tableau_fixed <- dplyr::bind_rows(tableau_lv3, ni15_final)

haven::write_sav(tableau_fixed, "/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/NI Tableau Final-L3-September-2020.sav")
haven::write_sav(tableau_fixed, "/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/NI Tableau Final-L3-September-2020-NoSecurity.sav")
haven::write_sav(tableau_fixed %>% dplyr::filter(Locality == "All"), "/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/NI Tableau Final-L2-September-2020.sav")

# NI18

ni18 <- get_new_ni_18_data("/conf/irf/03-Integration-Indicators/01-Core-Suite/data_inputs/NI18/2023-02-28-balance-of-care.xlsm", min_year = "2016")

ni18_mutate <- ni18 %>%
  dplyr::mutate(year = paste0(year, "/", as.character(as.integer(stringr::str_sub(year, 3, 4)) + 1)),
                Locality = "All",
                Data1 = "Annual")

ni18_cs <- ni18_mutate %>% dplyr::filter(partnership %in% c("Clackmannanshire", "Stirling")) %>%
  dplyr::mutate(partnership = "Clackmannanshire and Stirling") %>%
  dplyr::group_by(year, Locality, partnership, Data1) %>%
  dplyr::summarise(numerator = sum(numerator), denominator = sum(denominator)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(rate = (numerator / denominator) * 100)

ni18_scot <- ni18_mutate %>% dplyr::filter(partnership %in% c("Scotland")) %>%
  dplyr::rename(scotland = rate) %>%
  dplyr::select(year, scotland)

ni18_final <- dplyr::bind_rows(ni18_mutate, ni18_cs) %>%
  dplyr::filter(partnership != "Scotland") %>%
  dplyr::left_join(., ni18_scot) %>%
  dplyr::mutate(partnership = dplyr::if_else(partnership == "Eilean Siar", "Western Isles", partnership)) %>%
  dplyr::left_join(., lookup, by = c("partnership" = "ca2019name")) %>%
  dplyr::mutate(LA_Code = dplyr::if_else(partnership == "Clackmannanshire and Stirling", "S12000005", LA_Code)) %>%
  # dplyr::rename(Year1 = year,
  #               Partnership1 = partnership,
  #               Numerator = numerator,
  #               Denominator = denominator,
  #               Value = rate,
  #               Scotland = scotland) %>%
  dplyr::mutate(locality = "All", indicator = "NI18")

ni18_final <- ni18_final %>% dplyr::select(year, partnership, locality, indicator, numerator, denominator, `value`=rate, `time_period`=Data1, scotland)

arrow::write_parquet(ni18_final, "/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/NI18_tableau_output.parquet")

full_tableau <- haven::read_sav("/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/NI Tableau Final-L3-September-2020.sav") %>%
  dplyr::filter(Indicator1 != "NI18")

tableau_fixed <- dplyr::bind_rows(full_tableau, ni18_final)

haven::write_sav(tableau_fixed, "/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/NI Tableau Final-L3-September-2020.sav")
haven::write_sav(tableau_fixed, "/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/NI Tableau Final-L3-September-2020-NoSecurity.sav")
haven::write_sav(tableau_fixed %>% dplyr::filter(Locality == "All"), "/conf/irf/03-Integration-Indicators/01-Core-Suite/tableau_outputs/NI Tableau Final-L2-September-2020.sav")

tableau_check <- tableau_fixed %>% dplyr::filter(Indicator1 == "NI18")

## Publication
# NI16

ni16 <- calculate_ni16_final_output(FALSE)

arrow::write_parquet(ni16[["calendar"]], "Outputs/NI16_calendar_year.parquet")
arrow::write_parquet(ni16[["financial"]], "/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs//NI16_financial_year.parquet")

pub_finyear <- ni16[["financial"]] %>%
  dplyr::filter(
    year %in% c("2016/17", "2017/18", "2018/19", "2019/20", "2020/21", "2021/22") &
      time_period == "Annual" &
      locality == "All"
  )

pub_calyear <- ni16[["calendar"]] %>%
  dplyr::filter(
    year %in% c("2022") &
      time_period == "Annual" &
      locality == "All"
  )

pub_ni16 <- dplyr::bind_rows(pub_finyear, pub_calyear %>% dplyr::mutate(year = as.character(year))) %>%
  dplyr::mutate(indicator = "NI16",
                ind_no = 16L) %>%
  dplyr::mutate(partnership = dplyr::case_when(
    partnership == "City of Edinburgh" ~ "Edinburgh",
    partnership == "Na h-Eileanan Siar" ~ "Western Isles",
    TRUE ~ partnership
  ))

arrow::write_parquet(pub_ni16, "/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/NI16_publication_output.parquet")

## NI15

ni15_raw <- readr::read_rds(fs::path(get_ni_output_dir(), "NI15-R.rds"))

ni15_financial <- ni15_raw %>% dplyr::filter(
  year %in% c("2016/17", "2017/18", "2018/19", "2019/20", "2020/21", "2021/22") &
    data == "Annual" &
    locality == "All"
)

ni15_scotland <- ni15_financial %>%
  dplyr::filter(partnership != "Clackmannanshire and Stirling") %>%
  dplyr::mutate(partnership = "Scotland") %>%
  dplyr::group_by(year, data, partnership, locality, indicator) %>%
  dplyr::summarise(dplyr::across(c(numerator, denominator), sum, na.rm = T)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::mutate(value = 100 - ((numerator / (denominator * 182.5)) * 100))

ni15_financial_final <- dplyr::bind_rows(ni15_financial, ni15_scotland)

ni15_2022 <- ni15_raw %>%
  dplyr::filter(year == "2021/22" & data %in% c("M10", "M11", "M12") |
                  year == "2022/23" & !(data %in% c("M10", "M11", "M12", "Annual"))) %>%
  dplyr::filter(locality == "All") %>%
  dplyr::mutate(year = "2022",
                data = "Annual") %>%
  dplyr::group_by(year, data, partnership, locality, indicator) %>%
  dplyr::summarise(dplyr::across(c(numerator, denominator), sum, na.rm = T)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::mutate(value = 100 - ((numerator / (denominator * 182.5)) * 100))

ni15_2022_scotland <- ni15_raw %>%
  dplyr::filter(year == "2021/22" & data %in% c("M10", "M11", "M12") |
                  year == "2022/23" & !(data %in% c("M10", "M11", "M12", "Annual"))) %>%
  dplyr::filter(locality == "All") %>%
  dplyr::mutate(year = "2022",
                data = "Annual") %>%
  dplyr::filter(partnership != "Clackmannanshire and Stirling") %>%
  dplyr::mutate(partnership = "Scotland") %>%
  dplyr::group_by(year, data, partnership, locality, indicator) %>%
  dplyr::summarise(dplyr::across(c(numerator, denominator), sum, na.rm = T)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::mutate(value = 100 - ((numerator / (denominator * 182.5)) * 100))

ni15_2022_final <- dplyr::bind_rows(ni15_2022, ni15_2022_scotland)

ni15_final <- dplyr::bind_rows(ni15_2022_final, ni15_financial_final) %>% dplyr::select(-scotland)

arrow::write_parquet(ni15_final, "/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/NI15_publication_output.parquet")



# Checks

previous_data <- readxl::read_excel(fs::path(get_ni_excel_output_dir(), "spreadsheet_output_may_2023.xlsx")) %>%
  dplyr::filter(indicator == "NI14" & time_period == "Annual" & year %in% c("2016/17", "2017/18", "2018/19", "2019/20", "2020/21", "2021/22", "2022"))

new_data <- arrow::read_parquet("/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/NI14_publication_output.parquet")

checks14 <- dplyr::left_join(previous_data, new_data, by = c("year", "time_period" = "month", "partnership")) %>%
  dplyr::mutate(num_diff = numerator.y - numerator.x,
                denom_diff = denominator.y - denominator.x,
                value_diff = value.y - value.x,
                value_percent = scales::percent(value_diff/value.x, accuracy = 0.01)) %>%
  dplyr::select(year, partnership, num_diff, denom_diff, value_diff, value_percent) %>%
  dplyr::arrange(desc(value_percent))

previous_data <- readxl::read_excel(fs::path(get_ni_excel_output_dir(), "spreadsheet_output_may_2023.xlsx")) %>%
  dplyr::filter(indicator == "NI16" & time_period == "Annual" & year %in% c("2016/17", "2017/18", "2018/19", "2019/20", "2020/21", "2021/22", "2022"))

new_data <- arrow::read_parquet("/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/NI16_publication_output.parquet")

checks16 <- dplyr::left_join(previous_data, new_data, by = c("year", "time_period", "partnership")) %>%
  dplyr::mutate(num_diff = numerator.y - numerator.x,
                denom_diff = denominator.y - denominator.x,
                value_diff = value.y - value.x,
                value_percent = scales::percent(value_diff/value.x, accuracy = 0.01)) %>%
  dplyr::select(year, partnership, num_diff, denom_diff, value_diff, value_percent) %>%
  dplyr::arrange(desc(value_percent))

output <- openxlsx::createWorkbook()
addWorksheet(output, "NI14")
addWorksheet(output, "NI16")
writeData(output, sheet = "NI14", x = checks14)
writeData(output, sheet = "NI16", x = checks16)
saveWorkbook(workbook, file = "/conf/irf/03-Integration-Indicators/01-Core-Suite/checks/publication_checks_2023_07.xlsx", overwrite = TRUE)

workbook <- loadWorkbook("/conf/irf/03-Integration-Indicators/01-Core-Suite/checks/publication_checks_2023_07.xlsx")
addWorksheet(workbook, "NI15-2")

previous_data <- readxl::read_excel(fs::path(get_ni_excel_output_dir(), "spreadsheet_output_may_2023.xlsx")) %>%
  dplyr::filter(indicator == "NI15" & time_period == "Annual" & year %in% c("2016/17", "2017/18", "2018/19", "2019/20", "2020/21", "2021/22", "2022"))

new_data <- arrow::read_parquet("/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/NI15_publication_output.parquet")

checks15 <- dplyr::left_join(previous_data, new_data, by = c("year", "time_period" = "data", "partnership")) %>%
  dplyr::mutate(num_diff = numerator.y - numerator.x,
                denom_diff = denominator.y - denominator.x,
                value_diff = value.y - value.x,
                value_percent = scales::percent(value_diff/value.x, accuracy = 0.01)) %>%
  dplyr::select(year, partnership, num_diff, denom_diff, value_diff, value_percent) %>%
  dplyr::arrange(desc(value_percent))

writeData(workbook, sheet = "NI15-2", x = checks15)

previous_data <- readxl::read_excel(fs::path(get_ni_excel_output_dir(), "spreadsheet_output_may_2023.xlsx")) %>%
  dplyr::filter(indicator == "NI12" & time_period == "Annual" & year %in% c("2018/19", "2019/20", "2020/21", "2021/22", "2022"))

new_data <- arrow::read_parquet("/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/NI12_publication_output.parquet") %>%
  dplyr::mutate(partnership = dplyr::case_when(
    partnership == "City of Edinburgh" ~ "Edinburgh",
    partnership == "Na h-Eileanan Siar" ~ "Western Isles",
    TRUE ~ partnership
  ))

checks12 <- dplyr::left_join(previous_data, new_data, by = c("year", "time_period", "partnership")) %>%
  dplyr::mutate(num_diff = numerator.y - numerator.x,
                denom_diff = denominator.y - denominator.x,
                value_diff = value.y - value.x,
                value_percent = scales::percent(value_diff/value.x, accuracy = 0.01)) %>%
  dplyr::select(year, partnership, num_diff, denom_diff, value_diff, value_percent) %>%
  dplyr::arrange(desc(value_percent))

workbook <- loadWorkbook("/conf/irf/03-Integration-Indicators/01-Core-Suite/checks/publication_checks_2023_07.xlsx")
addWorksheet(workbook, "NI12")
writeData(workbook, sheet = "NI12", x = checks12)
saveWorkbook(workbook, file = "/conf/irf/03-Integration-Indicators/01-Core-Suite/checks/publication_checks_2023_07.xlsx", overwrite = TRUE)

previous_data <- readxl::read_excel(fs::path(get_ni_excel_output_dir(), "spreadsheet_output_may_2023.xlsx")) %>%
  dplyr::filter(indicator == "NI13" & time_period == "Annual" & year %in% c("2018/19", "2019/20", "2020/21", "2021/22", "2022"))

new_data <- arrow::read_parquet("/conf/irf/03-Integration-Indicators/01-Core-Suite/outputs/NI13_publication_output.parquet") %>%
  dplyr::mutate(partnership = dplyr::case_when(
    partnership == "City of Edinburgh" ~ "Edinburgh",
    partnership == "Na h-Eileanan Siar" ~ "Western Isles",
    TRUE ~ partnership
  ))

checks13 <- dplyr::left_join(previous_data, new_data, by = c("year", "time_period", "partnership")) %>%
  dplyr::mutate(num_diff = numerator.y - numerator.x,
                denom_diff = denominator.y - denominator.x,
                value_diff = value.y - value.x,
                value_percent = scales::percent(value_diff/value.x, accuracy = 0.01)) %>%
  dplyr::select(year, partnership, num_diff, denom_diff, value_diff, value_percent) %>%
  dplyr::arrange(desc(value_percent))

workbook <- loadWorkbook("/conf/irf/03-Integration-Indicators/01-Core-Suite/checks/publication_checks_2023_07.xlsx")
addWorksheet(workbook, "NI13")
writeData(workbook, sheet = "NI13", x = checks13)
saveWorkbook(workbook, file = "/conf/irf/03-Integration-Indicators/01-Core-Suite/checks/publication_checks_2023_07.xlsx", overwrite = TRUE)

all_ind_check <- readxl::read_excel(glue::glue("{get_ni_excel_output_dir()}/spreadsheet_output_may_2023.xlsx")) %>%
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
  dplyr::select(order(colnames(.))) %>%
  dplyr::relocate(indicator)

partnership_check <- all_ind_check %>%
  dplyr::group_by(partnership, indicator) %>%
  dplyr::summarise(count = dplyr::n()) %>%
  tidyr::pivot_wider(id_cols = "partnership",
                     names_from = indicator,
                     values_from = count, names_prefix = "year_count_")

workbook <- loadWorkbook("/conf/irf/03-Integration-Indicators/01-Core-Suite/checks/publication_checks_2023_07.xlsx")
addWorksheet(workbook, "all_partnerships_indicators")
writeData(workbook, sheet = "all_partnerships_indicators", x = partnership_check)
saveWorkbook(workbook, file = "/conf/irf/03-Integration-Indicators/01-Core-Suite/checks/publication_checks_2023_07.xlsx", overwrite = TRUE)

ni14_check <- arrow::read_parquet(glue::glue("{output_folder}NI16_tableau_output_jun_23.parquet"))

test <- check_against_previous_output("spreadsheet_output_may_2023", "NI15", "publication")
ni14_test <- arrow::read_parquet(glue::glue("{get_ni_output_dir()}/NI14_publication_output.parquet"))

columns <- colnames(ni14_test %>% dplyr::select(year, dplyr::matches(c("month", "time_period", "data")), partnership))

final <- readxl::read_excel(glue::glue("{get_ni_excel_output_dir()}/publication_output_june_2023.xlsx")) %>%
  dplyr::filter(!(year == "2022/23" & indicator == "NI12"))

writexl::write_xlsx(final, glue::glue("{get_ni_excel_output_dir()}/publication_output_june_2023.xlsx"))

check <- readxl::read_excel(glue::glue("{get_ni_excel_output_dir()}/publication_output_june_2023.xlsx")) %>%
  dplyr::filter(indicator == "NI17") %>%
  tidyr::pivot_wider(id_cols = "partnership",
                     names_from = "year",
                     names_prefix = "NI17_",
                     values_from = "value") %>%
  dplyr::mutate(diff = scales::percent((`NI17_2022/23` - `NI17_2021/22`) / `NI17_2022/23`, accuracy = 0.01))

workbook <- loadWorkbook("/conf/irf/03-Integration-Indicators/01-Core-Suite/checks/publication_checks_2023_07.xlsx")
addWorksheet(workbook, "NI17")
writeData(workbook, sheet = "NI17", x = check)
saveWorkbook(workbook, file = "/conf/irf/03-Integration-Indicators/01-Core-Suite/checks/publication_checks_2023_07.xlsx", overwrite = TRUE)


