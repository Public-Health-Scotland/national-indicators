smra_connect <- dbConnect(odbc(),
  dsn = "SMRA",
  uid = "",
  pwd = ""
)

# EoL activity code ----

smr_extracts <- union_all(
  # Extract from Geriatric Long Stay
  tbl(smra_connect, in_schema("ANALYSIS", "SMR01_1E_PI")) %>%
    filter(DISCHARGE_DATE >= To_date("2009-10-01", "YYYY-MM-DD") &
      INPATIENT_DAYCASE_IDENTIFIER == "I") %>%
    select(
      LINK_NO, ADMISSION_DATE, DISCHARGE_DATE,
      SEX, COUNCIL_AREA_2019, INPATIENT_DAYCASE_IDENTIFIER,
      MAIN_CONDITION, OTHER_CONDITION_1, OTHER_CONDITION_2,
      OTHER_CONDITION_3, OTHER_CONDITION_4, OTHER_CONDITION_5,
      ADMISSION, DISCHARGE, LOCATION, URI
    ) %>%
    rename_with(stringr::str_to_lower, .cols = everything()) %>%
    mutate(recid = "50B"),
  # Extract from Acute
  tbl(smra_connect, in_schema("ANALYSIS", "SMR01_PI")) %>%
    filter(DISCHARGE_DATE >= To_date("2009-10-01", "YYYY-MM-DD") & INPATIENT_DAYCASE_IDENTIFIER == "I") %>%
    select(
      LINK_NO, ADMISSION_DATE, DISCHARGE_DATE,
      SEX, COUNCIL_AREA_2019, INPATIENT_DAYCASE_IDENTIFIER,
      MAIN_CONDITION, OTHER_CONDITION_1, OTHER_CONDITION_2,
      OTHER_CONDITION_3, OTHER_CONDITION_4, OTHER_CONDITION_5,
      ADMISSION, DISCHARGE, LOCATION, URI
    ) %>%
    rename_with(stringr::str_to_lower, .cols = everything()) %>%
    mutate(recid = "01B"),
  # Extract from Mental Health
  tbl(smra_connect, in_schema("ANALYSIS", "SMR04_PI")) %>%
    filter(DISCHARGE_DATE >= To_date("2009-10-01", "YYYY-MM-DD") | is.na(DISCHARGE_DATE)) %>%
    filter(MANAGEMENT_OF_PATIENT %in% c("1", "3", "5", "7", "A")) %>%
    select(
      SEX, ADMISSION_DATE, DISCHARGE_DATE, LINK_NO,
      COUNCIL_AREA_2019, MAIN_CONDITION, OTHER_CONDITION_1, OTHER_CONDITION_2,
      OTHER_CONDITION_3, OTHER_CONDITION_4, OTHER_CONDITION_5,
      ADMISSION, DISCHARGE, LOCATION, URI, MANAGEMENT_OF_PATIENT, DATE_LAST_AMENDED
    ) %>%
    rename_with(stringr::str_to_lower, .cols = everything()) %>%
    mutate(recid = "04B")
) %>%
  # Sort cases for lag function
  arrange(link_no, admission_date, discharge_date, admission, discharge, uri) %>%
  # There are some records which are the same inpatient stay.
  # For this analysis we just want to know the number of beddays and this is calculated using the record dates so even
  # though the second record is different and contains different diagnosis info,
  # we can delete it since the only info we need is the date of admission and date of discharge information.
  filter(!(admission_date == lag(admission_date) & discharge_date == lag(discharge_date) & link_no == lag(link_no))) %>%
  arrange(admission_date, link_no, desc(discharge_date)) %>%
  # Investigate any records which may have the same date of admission but different date of discharge,
  # (excluding those where one record admission_date = discharge_date as this will have LOS = 0)
  # Filter out the earliest discharge
  filter(!(admission_date == lag(admission_date) & link_no == lag(link_no) & !(admission_date == discharge_date))) %>%
  # Investigate any records with same discharge_date but different admission_date,
  # (excluding those where one record admission_date = discharge_date as this will have LOS = 0).
  arrange(discharge_date, admission_date, link_no) %>%
  filter(!(discharge_date == lag(discharge_date) & link_no == lag(link_no) & !(admission_date == discharge_date))) %>%
  arrange(link_no) %>%
  rename(council_area_smra = council_area_2019)

smr_extracts_test <- collect(smr_extracts)

# Extract death data from GRO_DEATHS_C
deaths <- tbl(smra_connect, in_schema("ANALYSIS", "GRO_DEATHS_C")) %>%
  select(DATE_OF_DEATH, UNDERLYING_CAUSE_OF_DEATH, CAUSE_OF_DEATH_CODE_0, CAUSE_OF_DEATH_CODE_1,
         CAUSE_OF_DEATH_CODE_2, CAUSE_OF_DEATH_CODE_3, CAUSE_OF_DEATH_CODE_4, CAUSE_OF_DEATH_CODE_5,
         CAUSE_OF_DEATH_CODE_6, CAUSE_OF_DEATH_CODE_7, CAUSE_OF_DEATH_CODE_8, CAUSE_OF_DEATH_CODE_9,
         AGE, SEX, `pc7` = POSTCODE, `council_area_smra` = COUNCIL_AREA_2019, LINK_NO) %>%
  # Sort by link_no and filter out duplicates
  # window_order(LINK_NO, DATE_OF_DEATH) %>%
  # filter(!(LINK_NO == lag(LINK_NO))) %>%
  # Only get deaths past 2010/11 FY
  filter(DATE_OF_DEATH >= To_date("2010-04-01", "YYYY-MM-DD")) %>%
  collect() %>%
  rename_with(stringr::str_to_lower, .cols = everything())

deaths <- deaths %>% arrange(link_no, date_of_death)
deaths <- deaths %>% mutate(
  flag1 = if_else((link_no == lag(link_no)), 1, 0, missing = 0)
)
# 20 duplicate CHI records, remove these
table(deaths$flag1)
deaths <- deaths %>% filter(flag1 == 0)
deaths <- select(deaths, -flag1)

deaths <- deaths %>% mutate(
  externalcause = case_when(
    substr(underlying_cause_of_death,1,3) %in% external_cause_codes |
      substr(cause_of_death_code_0,1,3) %in% external_cause_codes |
      substr(cause_of_death_code_1,1,3) %in% external_cause_codes |
      substr(cause_of_death_code_2,1,3) %in% external_cause_codes |
      substr(cause_of_death_code_3,1,3) %in% external_cause_codes |
      substr(cause_of_death_code_4,1,3) %in% external_cause_codes |
      substr(cause_of_death_code_5,1,3) %in% external_cause_codes |
      substr(cause_of_death_code_6,1,3) %in% external_cause_codes |
      substr(cause_of_death_code_7,1,3) %in% external_cause_codes |
      substr(cause_of_death_code_8,1,3) %in% external_cause_codes |
      substr(cause_of_death_code_9,1,3) %in% external_cause_codes ~ 1, T ~0))

deaths <- deaths %>% mutate(
  falls = case_when(
    substr(underlying_cause_of_death,1,3) %in% falls_codes |
      substr(cause_of_death_code_0,1,3) %in% falls_codes |
      substr(cause_of_death_code_1,1,3) %in% falls_codes |
      substr(cause_of_death_code_2,1,3) %in% falls_codes |
      substr(cause_of_death_code_3,1,3) %in% falls_codes |
      substr(cause_of_death_code_4,1,3) %in% falls_codes |
      substr(cause_of_death_code_5,1,3) %in% falls_codes |
      substr(cause_of_death_code_6,1,3) %in% falls_codes |
      substr(cause_of_death_code_7,1,3) %in% falls_codes |
      substr(cause_of_death_code_8,1,3) %in% falls_codes |
      substr(cause_of_death_code_9,1,3) %in% falls_codes ~ 1, T ~0))

deaths <- deaths %>% mutate(
  flag1 = if_else((externalcause == 1 & falls == 1)|(externalcause == 0 & falls == 0 | falls == 1), 1, 0, missing = 1)
)
table(deaths$flag1)

deaths <- filter(deaths, flag1 == 1)
deaths <- select(deaths, -flag1)

  # We want to exclude certain ICD-10 codes but also include falls, so we introduce three scenarios
  # deaths <- deaths %>% mutate(cause_flag = case_when(
    # If any of the causes are excluded and none of them are falls, exclude
    # if_any(contains("cause"), ~ . %in% excluded_codes) & !(if_any(contains("cause"), ~ . %in% falls_codes)) ~ 0,
    # If any of the causes are excluded but some of them are falls, include
    # if_any(contains("cause"), ~ . %in% excluded_codes) & if_any(contains("cause"), ~ . %in% falls_codes) ~ 1,
    # Include everything else as well
    # TRUE ~ 2)) %>%
  filter(cause_flag > 0) %>%
  # Get the month number from date of death
  mutate(month = lubridate::month(date_of_death),
         # Convert to financial month
         month = case_when(
           month %in% c(1:3) ~ str_c("M", as.character(month + 9)),
           month %in% c(4:12) ~str_c("M", as.character(month - 3))),
         # Create financial year of death
         finyear_dth = lubridate::year(date_of_death),
         finyear_dth = if_else(month %in% c("M10", "M11", "M12"), finyear_dth - 1, finyear_dth),
         finyear_dth = str_c(as.character(finyear_dth), "/", str_sub(as.character(finyear_dth + 1), 3, 4))
  ) %>%
  # See big.lookup(), this adds simd, locality, lca, partnership, hb
  left_join(., big_lookup(), by = "pc7") %>%
  mutate(hbresname = str_c("NHS", match_area(hb2018)),
         hscp = match_area(hscp2018),
         council_area = match_area(ca2018),
         # Recode ages into groups
         agegroups = cut(age, breaks = c(-1, 54, 64, 74, 84, max(age)), labels = c("0-54", "55-64", "65-74", "75-84", "85+")),
         gender = if_else(sex == 1, "Male", "Female"),
         missinglink_no = is.na(link_no)) %>%
  select(-sex)

all_activity <- smr_extracts_test %>%
  # Take all the smr extracts and filter out the ones that don't correspond to a death,
  # based in link_no
  filter(link_no %in% deaths$link_no) %>%
  # Join on the deaths data
  left_join(., deaths) %>%
  # Where SMR04 records have missing death date, recode date of discharge to date of death
  mutate(discharge_date = if_else(recid == '04B' & is.na(discharge_date), date_of_death, discharge_date),
         # Calculate date six months prior to death
         date_6months_prior_death = date_of_death - days(183),
         # If six months prior to death falls in a hospital spell, calculate date of admission equal to 6 months prior to death.
         # We are only interested in LOS within last 6 months of life.
         admission_date = if_else(admission_date <= date_6months_prior_death & discharge_date >= date_6months_prior_death,
                                  date_6months_prior_death,
                                  admission_date),
         # Select CIS's with date of admission that fall within 183 days prior to death
         days_from_adm_to_death = int_length(admission_date %--% date_of_death)/86400,
         # Check for where death is before discharge
         discharge_date = if_else(date_of_death < discharge_date, date_of_death, discharge_date),
         # Length of stay in days, computed between admission and discharge
         los = int_length(admission_date %--% discharge_date)/86400) %>%
  # Filter activity within the last 6 months of life, and where date of death is before admission
  filter(days_from_adm_to_death <= 183 & admission_date < date_of_death)

patient_level <- all_activity %>%
  # Exclude Flag and remove any inpatient activity which takes place in a care home/nursing home.
  # This includes locations coded as Non-NHS Provider and typically ending in R or V.
  # Although these are technically NHS beds the setting is within a Care Home and so this will be captured in Community.
  filter(!(location %in% excluded_locations)) %>%
  # Aggregate to patient level
  group_by(finyear_dth, month, link_no) %>%
  summarise(total_los = sum(los, na.rm = T))  %>%
  ungroup() %>%
  # Make the maximum length of stay 182.5 days
  mutate(total_los = if_else(total_los > 182.5, 182.5, total_los))

# Join the patient level to the deaths, and flag one death per row
final_extract <- left_join(deaths, patient_level, by = c("link_no", "finyear_dth", "month")) %>%
  mutate(deaths = 1)

# NI15 code ----

ni15 <- final_extract %>%
  filter(finyear_dth >= "2013/14") %>%
  group_by(finyear_dth, month, hscp, council_area, locality) %>%
  summarise(deaths = n(),
            los = sum(total_los, na.rm = T)) %>%
  ungroup() %>%
  # Create Annual values
  mutate(temp_month = "Annual") %>%
  pivot_longer(cols = c("month", "temp_month"),
               values_to = "month") %>%
  select(-name) %>%
  # Clacks and Stirling
  mutate(temp_part = case_when(
    council_area == "Clackmannanshire" ~ "Clackmannanshire",
    council_area == "Stirling" ~ "Stirling"
  )) %>%
  pivot_longer(cols = c("hscp", "temp_part"),
               values_to = "hscp") %>%
  select(-name) %>%
  # All localities
  mutate(temp_loc = "All") %>%
  pivot_longer(cols = c("locality", "temp_loc"),
               values_to = "locality") %>%
  select(-name) %>%
  # Scotland totals
  mutate(temp_part = if_else(hscp != "Clackmannanshire and Stirling", "Scotland", NA_character_)) %>%
  pivot_longer(cols = c("hscp", "temp_part"),
               values_to = "hscp") %>%
  select(-name) %>%
  # Remove individual locality totals for Scotland
  filter(hscp != "Scotland" | locality == "All") %>%
  # Aggregate for tidiness and get rid of council_area
  group_by(finyear_dth, hscp, locality, month) %>%
  summarise(across(c("deaths", "los"), sum, na.rm = T)) %>%
  ungroup() %>%
  # Calculate indicator value
  mutate(value = 100 - (((los / deaths) / 182.5) * 100),
         indicator = "NI15") %>%
  rename(partnership = hscp,
         year = finyear_dth,
         numerator = los,
         denominator = deaths,
         data = month)

ni15_final <- left_join(ni15 %>% filter(partnership != "Scotland"),
                        ni15 %>% filter(partnership == "Scotland") %>%
                          select(year, data, `scotland` = value),
                        by = c("year", "data"))

write_sav(ni15_final, "NI 15/NI15-R.sav")




