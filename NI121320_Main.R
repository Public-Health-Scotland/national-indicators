# NI 12, 13 and 20
ni12_13_20 <- function(year_to_run) {
  # SECTION 1 - WRANGLE SLF ----
  # Load in dates of financial year for calculations later
  interval_finyear <- lubridate::interval(
    lubridate::ymd(stringr::str_c("20", stringr::str_sub(year_to_run, 1, 2), "-04-01")),
    lubridate::ymd(stringr::str_c("20", stringr::str_sub(year_to_run, 3, 4), "-03-31"))
  )
  financial_year <- stringr::str_glue(
    "20{stringr::str_sub(year_to_run, 1, 2)}/{stringr::str_sub(year_to_run, -2)}"
  )

  cost_names <- tolower(paste0(month.abb, "_cost"))
  # Read in SLF episode file
  slf <- arrow::read_parquet(
    "/conf/sourcedev/Source_Linkage_File_Updates/1920/source-episode-file-1920_ni_version.parquet",
    col_select = c(
      "year", "chi", "cij_marker", "cij_pattype", "cij_admtype", "age", "recid", "smrtype",
      "record_keydate1", "record_keydate2",
      "lca", "location", "datazone2011",
      "yearstay", cost_names
    )
  ) %>%
    data.table::as.data.table() %>%
    # We only want the emergency admission records.
    dplyr::filter(recid %in% c("01B", "04B", "GLS")) %>%
    # We only want over 18s, and the three locations are dental hospitals which we don't include
    dplyr::filter(
      (chi != "" | is.na(chi)) & age >= 18 & datazone2011 != "" & (location != "T113H" | location != "S206H" | location != "G106H")
    ) %>%
    # Recode patient type 18 to Non-Elective and filter out non-emergency admissions
    dplyr::mutate(cij_pattype = dplyr::if_else(cij_admtype == 18, "Non-Elective", cij_pattype)) %>%
    # Filter to only emergency CIJ stays
    dplyr::filter(cij_pattype == "Non-Elective" & (smrtype %in% c("Acute-IP", "Psych-IP", "GLS-IP"))) %>%
    # Aggregate to CIJ level
    dplyr::group_by(year, chi, cij_marker) %>%
    dplyr::summarise(
      record_keydate1 = min(record_keydate1),
      record_keydate2 = max(record_keydate2),
      dplyr::across(c("lca", "datazone2011"), dplyr::last),
      dplyr::across(cost_names, sum, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::collect()


  # SECTION 2 - HANDLE DATES ----
  # We only want admissions within the financial year to be counted. However, we need to keep the costs for said records so
  # we don't just filter them out
  dates <- slf %>%
    tidylog::mutate(admission_record = record_keydate1 %within% interval_finyear) %>%
    # If the start date of a record is before the start of the financial year, set it to one day before (31st March XXYY)
    # If the end date of a record is after the end of the financial year, set it to the end
    tidylog::mutate(
      record_keydate1 = dplyr::if_else(
        record_keydate1 < lubridate::int_start(interval_finyear),
        lubridate::int_start(interval_finyear) - lubridate::ddays(1),
        record_keydate1
      ),
      record_keydate2 = dplyr::if_else(
        record_keydate2 > lubridate::int_end(interval_finyear),
        lubridate::int_end(interval_finyear),
        record_keydate2
      )
    ) %>%
    tidylog::replace_na(list(record_keydate2 = lubridate::int_end(interval_finyear))) %>%
    tidylog::rename(admission_date = record_keydate1,
                    discharge_date = record_keydate2) %>%
    tidylog::mutate(
      y_los_tot = difftime(discharge_date, admission_date, units = c("days"))
    )

  # SECTION 3 - CALCULATE ADMISSIONS, BEDDAYS, AND COSTS ----
  # Calculate admissions before beddays as the groups don't work properly afterwards
  # due to the data being pivoted longer
  admissions <- dates %>%
    # Recode Month here to match on to bed days and costs
    tidylog::mutate(month = stringr::str_c("M", as.character((lubridate::month(admission_date) - 3) %% 12))) %>%
    tidylog::mutate(month = dplyr::if_else(month == "M0", "M12", month)) %>%
    tidylog::left_join(readr::read_rds(get_locality_path()),
                       by = "datazone2011") %>%
    tidylog::group_by(ca2018, hscp_locality, month) %>%
    tidylog::summarise(admissions = sum(admission_record)) %>%
    tidylog::ungroup()

  bds_costs <- create_monthly_beddays(
    dates,
    year_to_run,
    admission_date = admission_date,
    discharge_date = discharge_date,
    pivot_longer = TRUE
  )

  all_measures <- tidylog::left_join(
    bds_costs,
    admissions,
    by = c("ca2018", "hscp_locality", "month")
  )

  # SECTION 4 - ADDING 'ALL' GROUPS ----

  all_groups <- all_measures %>%
    # Annual totals
    tidylog::mutate(temp_month = "Annual") %>%
    tidylog::pivot_longer(
      cols = c("month", "temp_month"),
      values_to = "month"
    ) %>%
    tidylog::select(-name) %>%
    # Clacks & Stirling totals
    tidylog::mutate(temp_part = dplyr::if_else(ca2019name %in% c("Clackmannanshire", "Stirling"),
      "Clackmannanshire and Stirling", NA_character_
    )) %>%
    tidylog::pivot_longer(
      cols = c("ca2019name", "temp_part"),
      values_to = "ca2019name",
      values_drop_na = TRUE
    ) %>%
    tidylog::select(-name) %>%
    # All localities totals
    tidylog::mutate(temp_loc = "All") %>%
    tidylog::pivot_longer(
      cols = c("hscp_locality", "temp_loc"),
      values_to = "hscp_locality"
    ) %>%
    tidylog::select(-name) %>%
    # Scotland totals
    tidylog::mutate(temp_part_2 = dplyr::if_else(ca2019name != "Clackmannanshire and Stirling", "Scotland", NA_character_)) %>%
    tidylog::pivot_longer(
      cols = c("ca2019name", "temp_part_2"),
      values_to = "ca2019name",
      values_drop_na = TRUE
    ) %>%
    tidylog::select(-name) %>%
    tidylog::group_by(ca2019name, hscp_locality, month) %>%
    tidylog::summarise(dplyr::across(beddays:admissions, sum, na.rm = TRUE), .groups = "keep") %>%
    tidylog::ungroup() %>%
    tidylog::filter((ca2019name != "Scotland" | hscp_locality == "All") & !is.na(ca2019name)) %>%
    tidylog::mutate(year = financial_year) %>%
    tidylog::rename(partnership = ca2019name, locality = hscp_locality)

  ni_values <- all_groups %>%
    tidylog::left_join(get_loc_pops(), by = c("year", "partnership", "locality")) %>%
    tidylog::left_join(
      readr::read_rds(fs::path(
        "/conf/irf/03-Integration-Indicators/01-Core-Suite",
        "Z2 - R Code",
        "Cost Lookup BM.rds"
      )) %>%
        tidylog::filter(year == financial_year),
      by = c("year", "partnership")
    ) %>%
    tidylog::mutate(
      total_cost = dplyr::case_when(
        month == "Annual" ~ acost,
        month != "Annual" ~ qcost
      ),
      NI12_value = admissions / over18_pop * 100000,
      NI13_value = beddays / over18_pop * 100000,
      NI20_value = cost / total_cost * 100
    ) %>%
    tidylog::mutate(NI12_denominator = over18_pop) %>%
    tidylog::rename(
      NI12_numerator = admissions,
      NI13_numerator = beddays,
      NI20_numerator = cost,
      NI13_denominator = over18_pop,
      NI20_denominator = total_cost
    ) %>%
    tidylog::pivot_longer(
      cols = tidyselect::starts_with("NI"),
      names_to = c("indicator", ".value"),
      names_pattern = "(NI\\d{2})_(\\w+)"
    )

  ni_final <- tidylog::left_join(ni_values,
    ni_values %>%
      tidylog::filter(partnership == "Scotland") %>%
      tidylog::rename(scotland = value) %>%
      tidylog::select(year, month, indicator, scotland),
    by = c("year", "month", "indicator")
  ) %>%
    tidylog::rename(data = month) %>%
    tidylog::select(year, value, scotland, partnership, numerator, locality, indicator, denominator, data)

  return(ni_final)
}

ni_final_monthly <- list(
  ni12_13_20("1920"),
  ni12_13_20("2021"),
  ni12_13_20("2122")
)

ni_final_quarterly <- ni_final_monthly %>%
  purrr::map_dfr(~
    dplyr::mutate(.x, data = dplyr::case_when(
      data %in% c("M1", "M2", "M3") ~ "Q1",
      data %in% c("M4", "M5", "M6") ~ "Q2",
      data %in% c("M7", "M8", "M9") ~ "Q3",
      data %in% c("M10", "M11", "M12") ~ "Q4",
      data == "Annual" ~ "Annual"
    )) %>%
      dplyr::group_by(indicator, year, data, partnership, locality) %>%
      dplyr::summarise(
        dplyr::across(c(value, scotland, numerator), sum),
        dplyr::across(denominator, max)
      ))

ni_final_monthly <- dplyr::bind_rows(ni_final_monthly)

readr::write_excel_csv(ni_final_monthly, "/conf/irf/03-Integration-Indicators/01-Core-Suite/NI 12 13 & 20/R Testing/NI-12-13-20-Monthly_jm.csv")
readr::write_excel_csv(ni_final_quarterly, "/conf/irf/03-Integration-Indicators/01-Core-Suite/NI 12 13 & 20/R Testing/NI-12-13-20-Quarterly_jm.csv")

readr::write_rds(ni_final_monthly, "NI 12 13 & 20/R Testing/NI-12-13-20-Monthly_jm.rds", compress = "gz")
readr::write_rds(ni_final_quarterly, "NI 12 13 & 20/R Testing/NI-12-13-20-Quarterly_jm.rds", compress = "gz")
