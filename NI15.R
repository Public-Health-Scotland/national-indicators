smra_connect <- dbConnect(odbc::odbc(),
  dsn = "SMRA",
  uid = "",
  pwd = ""
)

# EoL activity code ----

smr_extracts <- dplyr::union_all(
  # Extract from Geriatric Long Stay
  dplyr::tbl(smra_connect, dbplyr::in_schema("ANALYSIS", "SMR01_1E_PI")) %>%
    dplyr::filter(DISCHARGE_DATE >= To_date("2009-10-01", "YYYY-MM-DD") &
      INPATIENT_DAYCASE_IDENTIFIER == "I") %>%
    dplyr::select(
      LINK_NO, ADMISSION_DATE, DISCHARGE_DATE,
      SEX, COUNCIL_AREA_2019, INPATIENT_DAYCASE_IDENTIFIER,
      MAIN_CONDITION, OTHER_CONDITION_1, OTHER_CONDITION_2,
      OTHER_CONDITION_3, OTHER_CONDITION_4, OTHER_CONDITION_5,
      ADMISSION, DISCHARGE, LOCATION, URI
    ) %>%
    dplyr::rename_with(stringr::str_to_lower, .cols = everything()) %>%
    dplyr::mutate(recid = "50B"),
  # Extract from Acute
  dplyr::tbl(smra_connect, dbplyr::in_schema("ANALYSIS", "SMR01_PI")) %>%
    dplyr::filter(DISCHARGE_DATE >= To_date("2009-10-01", "YYYY-MM-DD") & INPATIENT_DAYCASE_IDENTIFIER == "I") %>%
    dplyr::select(
      LINK_NO, ADMISSION_DATE, DISCHARGE_DATE,
      SEX, COUNCIL_AREA_2019, INPATIENT_DAYCASE_IDENTIFIER,
      MAIN_CONDITION, OTHER_CONDITION_1, OTHER_CONDITION_2,
      OTHER_CONDITION_3, OTHER_CONDITION_4, OTHER_CONDITION_5,
      ADMISSION, DISCHARGE, LOCATION, URI
    ) %>%
    dplyr::rename_with(stringr::str_to_lower, .cols = everything()) %>%
    dplyr::mutate(recid = "01B"),
  # Extract from Mental Health
  dplyr::tbl(smra_connect, dbplyr::in_schema("ANALYSIS", "SMR04_PI")) %>%
    dplyr::filter(DISCHARGE_DATE >= To_date("2009-10-01", "YYYY-MM-DD") | is.na(DISCHARGE_DATE)) %>%
    dplyr::filter(MANAGEMENT_OF_PATIENT %in% c("1", "3", "5", "7", "A")) %>%
    dplyr::select(
      SEX, ADMISSION_DATE, DISCHARGE_DATE, LINK_NO,
      COUNCIL_AREA_2019, MAIN_CONDITION, OTHER_CONDITION_1, OTHER_CONDITION_2,
      OTHER_CONDITION_3, OTHER_CONDITION_4, OTHER_CONDITION_5,
      ADMISSION, DISCHARGE, LOCATION, URI, MANAGEMENT_OF_PATIENT, DATE_LAST_AMENDED
    ) %>%
    dplyr::rename_with(stringr::str_to_lower, .cols = everything()) %>%
    dplyr::mutate(recid = "04B")
) %>%
  # Sort cases for lag function
  dplyr::arrange(link_no, admission_date, discharge_date, admission, discharge, uri) %>%
  # There are some records which are the same inpatient stay.
  # For this analysis we just want to know the number of beddays and this is calculated using the record dates so even
  # though the second record is different and contains different diagnosis info,
  # we can delete it since the only info we need is the date of admission and date of discharge information.
  dplyr::filter(!(admission_date == dplyr::lag(admission_date) & discharge_date == dplyr::lag(discharge_date) & link_no == dplyr::lag(link_no))) %>%
  dplyr::arrange(admission_date, link_no, dplyr::desc(discharge_date)) %>%
  # Investigate any records which may have the same date of admission but different date of discharge,
  # (excluding those where one record admission_date = discharge_date as this will have LOS = 0)
  # Filter out the earliest discharge
  dplyr::filter(!(admission_date == dplyr::lag(admission_date) & link_no == dplyr::lag(link_no) & !(admission_date == discharge_date))) %>%
  # Investigate any records with same discharge_date but different admission_date,
  # (excluding those where one record admission_date = discharge_date as this will have LOS = 0).
  dplyr::arrange(discharge_date, admission_date, link_no) %>%
  dplyr::filter(!(discharge_date == dplyr::lag(discharge_date) & link_no == dplyr::lag(link_no) & !(admission_date == discharge_date))) %>%
  dplyr::arrange(link_no) %>%
  dplyr::rename(council_area_smra = council_area_2019)

smr_extracts_test <- dplyr::collect(smr_extracts)

# Extract death data from GRO_DEATHS_C
deaths <- dplyr::tbl(smra_connect, dbplyr::in_schema("ANALYSIS", "GRO_DEATHS_C")) %>%
  dplyr::select(DATE_OF_DEATH, UNDERLYING_CAUSE_OF_DEATH, CAUSE_OF_DEATH_CODE_0, CAUSE_OF_DEATH_CODE_1,
    CAUSE_OF_DEATH_CODE_2, CAUSE_OF_DEATH_CODE_3, CAUSE_OF_DEATH_CODE_4, CAUSE_OF_DEATH_CODE_5,
    CAUSE_OF_DEATH_CODE_6, CAUSE_OF_DEATH_CODE_7, CAUSE_OF_DEATH_CODE_8, CAUSE_OF_DEATH_CODE_9,
    AGE, SEX,
    `pc7` = POSTCODE, `council_area_smra` = COUNCIL_AREA_2019, LINK_NO
  ) %>%
  # Sort by link_no and filter out duplicates
  # dbplyr::window_order(LINK_NO, DATE_OF_DEATH) %>%
  # dplyr::filter(!(LINK_NO == dplyr::lag(LINK_NO))) %>%
  # Only get deaths past 2010/11 FY
  dplyr::filter(DATE_OF_DEATH >= To_date("2010-04-01", "YYYY-MM-DD")) %>%
  dplyr::collect() %>%
  dplyr::rename_with(stringr::str_to_lower, .cols = everything())

deaths <- deaths %>% dplyr::arrange(link_no, date_of_death)
deaths <- deaths %>% dplyr::mutate(
  flag1 = dplyr::if_else((link_no == dplyr::lag(link_no)), 1, 0, missing = 0)
)
# 20 duplicate CHI records, remove these
table(deaths$flag1)
deaths <- deaths %>% dplyr::filter(flag1 == 0)
deaths <- dplyr::select(deaths, -flag1)

deaths <- deaths %>% dplyr::mutate(
  externalcause = dplyr::case_when(
    substr(underlying_cause_of_death, 1, 3) %in% external_cause_codes |
      substr(cause_of_death_code_0, 1, 3) %in% external_cause_codes |
      substr(cause_of_death_code_1, 1, 3) %in% external_cause_codes |
      substr(cause_of_death_code_2, 1, 3) %in% external_cause_codes |
      substr(cause_of_death_code_3, 1, 3) %in% external_cause_codes |
      substr(cause_of_death_code_4, 1, 3) %in% external_cause_codes |
      substr(cause_of_death_code_5, 1, 3) %in% external_cause_codes |
      substr(cause_of_death_code_6, 1, 3) %in% external_cause_codes |
      substr(cause_of_death_code_7, 1, 3) %in% external_cause_codes |
      substr(cause_of_death_code_8, 1, 3) %in% external_cause_codes |
      substr(cause_of_death_code_9, 1, 3) %in% external_cause_codes ~ 1, T ~ 0
  )
)

deaths <- deaths %>% dplyr::mutate(
  falls = dplyr::case_when(
    substr(underlying_cause_of_death, 1, 3) %in% falls_codes |
      substr(cause_of_death_code_0, 1, 3) %in% falls_codes |
      substr(cause_of_death_code_1, 1, 3) %in% falls_codes |
      substr(cause_of_death_code_2, 1, 3) %in% falls_codes |
      substr(cause_of_death_code_3, 1, 3) %in% falls_codes |
      substr(cause_of_death_code_4, 1, 3) %in% falls_codes |
      substr(cause_of_death_code_5, 1, 3) %in% falls_codes |
      substr(cause_of_death_code_6, 1, 3) %in% falls_codes |
      substr(cause_of_death_code_7, 1, 3) %in% falls_codes |
      substr(cause_of_death_code_8, 1, 3) %in% falls_codes |
      substr(cause_of_death_code_9, 1, 3) %in% falls_codes ~ 1, T ~ 0
  )
)

deaths <- deaths %>% dplyr::mutate(
  flag1 = dplyr::if_else((externalcause == 1 & falls == 1) | (externalcause == 0 & falls == 0 | falls == 1), 1, 0, missing = 1)
)
table(deaths$flag1)

deaths <- dplyr::filter(deaths, flag1 == 1)
deaths <- dplyr::select(deaths, -flag1)

# We want to exclude certain ICD-10 codes but also include falls, so we introduce three scenarios
# deaths <- deaths %>% dplyr::mutate(cause_flag = dplyr::case_when(
# If any of the causes are excluded and none of them are falls, exclude
# dplyr::if_any(contains("cause"), ~ . %in% excluded_codes) & !(dplyr::if_any(contains("cause"), ~ . %in% falls_codes)) ~ 0,
# If any of the causes are excluded but some of them are falls, include
# dplyr::if_any(contains("cause"), ~ . %in% excluded_codes) & dplyr::if_any(contains("cause"), ~ . %in% falls_codes) ~ 1,
# Include everything else as well
# TRUE ~ 2)) %>%
dplyr::filter(cause_flag > 0) %>%
  # Get the month number from date of death
  dplyr::mutate(
    month = lubridate::month(date_of_death),
    # Convert to financial month
    month = dplyr::case_when(
      month %in% c(1:3) ~ stringr::str_c("M", as.character(month + 9)),
      month %in% c(4:12) ~ stringr::str_c("M", as.character(month - 3))
    ),
    # Create financial year of death
    finyear_dth = lubridate::year(date_of_death),
    finyear_dth = dplyr::if_else(month %in% c("M10", "M11", "M12"), finyear_dth - 1, finyear_dth),
    finyear_dth = stringr::str_c(as.character(finyear_dth), "/", stringr::str_sub(as.character(finyear_dth + 1), 3, 4))
  ) %>%
  # See big.lookup(), this adds simd, locality, lca, partnership, hb
  dplyr::left_join(., big_lookup(), by = "pc7") %>%
  dplyr::mutate(
    hbresname = stringr::str_c("NHS", phsmethods::match_area(hb2018)),
    hscp = phsmethods::match_area(hscp2018),
    council_area = phsmethods::match_area(ca2018),
    # Recode ages into groups
    agegroups = cut(age, breaks = c(-1, 54, 64, 74, 84, max(age)), labels = c("0-54", "55-64", "65-74", "75-84", "85+")),
    gender = dplyr::if_else(sex == 1, "Male", "Female"),
    missinglink_no = is.na(link_no)
  ) %>%
  dplyr::select(-sex)

all_activity <- smr_extracts_test %>%
  # Take all the smr extracts and filter out the ones that don't correspond to a death,
  # based in link_no
  dplyr::filter(link_no %in% deaths$link_no) %>%
  # Join on the deaths data
  dplyr::left_join(., deaths) %>%
  # Where SMR04 records have missing death date, recode date of discharge to date of death
  dplyr::mutate(
    discharge_date = dplyr::if_else(recid == "04B" & is.na(discharge_date), date_of_death, discharge_date),
    # Calculate date six months prior to death
    date_6months_prior_death = date_of_death - lubridate::days(183),
    # If six months prior to death falls in a hospital spell, calculate date of admission equal to 6 months prior to death.
    # We are only interested in LOS within last 6 months of life.
    admission_date = dplyr::if_else(admission_date <= date_6months_prior_death & discharge_date >= date_6months_prior_death,
      date_6months_prior_death,
      admission_date
    ),
    # Select CIS's with date of admission that fall within 183 days prior to death
    days_from_adm_to_death = lubridate::int_length(admission_date %--% date_of_death) / 86400,
    # Check for where death is before discharge
    discharge_date = dplyr::if_else(date_of_death < discharge_date, date_of_death, discharge_date),
    # Length of stay in days, computed between admission and discharge
    los = lubridate::int_length(admission_date %--% discharge_date) / 86400
  ) %>%
  # Filter activity within the last 6 months of life, and where date of death is before admission
  dplyr::filter(days_from_adm_to_death <= 183 & admission_date < date_of_death)

patient_level <- all_activity %>%
  # Exclude Flag and remove any inpatient activity which takes place in a care home/nursing home.
  # This includes locations coded as Non-NHS Provider and typically ending in R or V.
  # Although these are technically NHS beds the setting is within a Care Home and so this will be captured in Community.
  dplyr::filter(!(location %in% excluded_locations)) %>%
  # Aggregate to patient level
  dplyr::group_by(finyear_dth, month, link_no) %>%
  dplyr::summarise(total_los = sum(los, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # Make the maximum length of stay 182.5 days
  dplyr::mutate(total_los = dplyr::if_else(total_los > 182.5, 182.5, total_los))

# Join the patient level to the deaths, and flag one death per row
final_extract <- dplyr::left_join(deaths, patient_level, by = c("link_no", "finyear_dth", "month")) %>%
  dplyr::mutate(deaths = 1)

# NI15 code ----

ni15 <- final_extract %>%
  dplyr::filter(finyear_dth >= "2013/14") %>%
  dplyr::group_by(finyear_dth, month, hscp, council_area, locality) %>%
  dplyr::summarise(
    deaths = dplyr::n(),
    los = sum(total_los, na.rm = T)
  ) %>%
  dplyr::ungroup() %>%
  # Create Annual values
  dplyr::mutate(temp_month = "Annual") %>%
  tidyr::pivot_longer(
    cols = c("month", "temp_month"),
    values_to = "month"
  ) %>%
  dplyr::select(-name) %>%
  # Clacks and Stirling
  dplyr::mutate(temp_part = dplyr::case_when(
    council_area == "Clackmannanshire" ~ "Clackmannanshire",
    council_area == "Stirling" ~ "Stirling"
  )) %>%
  tidyr::pivot_longer(
    cols = c("hscp", "temp_part"),
    values_to = "hscp"
  ) %>%
  dplyr::select(-name) %>%
  # All localities
  dplyr::mutate(temp_loc = "All") %>%
  tidyr::pivot_longer(
    cols = c("locality", "temp_loc"),
    values_to = "locality"
  ) %>%
  dplyr::select(-name) %>%
  # Scotland totals
  dplyr::mutate(temp_part = dplyr::if_else(hscp != "Clackmannanshire and Stirling", "Scotland", NA_character_)) %>%
  tidyr::pivot_longer(
    cols = c("hscp", "temp_part"),
    values_to = "hscp"
  ) %>%
  dplyr::select(-name) %>%
  # Remove individual locality totals for Scotland
  dplyr::filter(hscp != "Scotland" | locality == "All") %>%
  # Aggregate for tidiness and get rid of council_area
  dplyr::group_by(finyear_dth, hscp, locality, month) %>%
  dplyr::summarise(dplyr::across(c("deaths", "los"), sum, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # Calculate indicator value
  dplyr::mutate(
    value = 100 - (((los / deaths) / 182.5) * 100),
    indicator = "NI15"
  ) %>%
  dplyr::rename(
    partnership = hscp,
    year = finyear_dth,
    numerator = los,
    denominator = deaths,
    data = month
  )

ni15_final <- dplyr::left_join(ni15 %>% dplyr::filter(partnership != "Scotland"),
  ni15 %>% dplyr::filter(partnership == "Scotland") %>%
    dplyr::select(year, data, `scotland` = value),
  by = c("year", "data")
)

haven::write_sav(ni15_final, "NI 15/NI15-R.sav")
