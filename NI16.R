## NI 16

# Set up connection to SMRA with username and password
ni16_connect <- dbConnect(odbc(),
  dsn = "SMRA",
  uid = .rs.askForPassword("SMRA Username:"),
  pwd = .rs.askForPassword("SMRA Password:")
)

falls_codes <- paste0("W", str_pad(0:19, 2, "left", "0"))

## Read data from SMRA
# Set up SQL queries
ni16_query <- "SELECT DISCHARGE_DATE, DR_POSTCODE as postcode,
  MAIN_CONDITION, OTHER_CONDITION_1, OTHER_CONDITION_2,
  OTHER_CONDITION_3, OTHER_CONDITION_4, OTHER_CONDITION_5
  FROM ANALYSIS.SMR01_PI
  WHERE ((DISCHARGE_DATE >= '01-APR-2013')
  AND (AGE_IN_YEARS >= 65)
  AND (INPATIENT_DAYCASE_IDENTIFIER = 'I')
  AND (ADMISSION_TYPE Between 33 AND 35)
  AND (SEX In ('1', '2'))
  AND (DR_Postcode is not NULL))
  Order By DR_Postcode"

ni16_extract <-
  as_tibble(
    dbGetQuery(ni16_connect, ni16_query)
  ) %>%
  # For R-standard column names
  clean_names() %>%
  # Change to diagnosis codes to first three characters
  mutate(across(main_condition:other_condition_5, str_sub, 1, 3)) %>%
  # Filter out any records where the diagnosis didn't involve a fall
  filter(if_any(
    c(main_condition:other_condition_5),
    ~ . %in% falls_codes
  ))
# Create variable for counting falls
mutate(falls = TRUE) %>%
  # Extract the financial month and year from discharge date
  fin_year_month(discharge_date) %>%
  select(-c(main_condition:other_condition_5))

ni16 <- left_join(ni16_extract,
  get_pc_lookup(c("pc7", "ca2018", "datazone2011")),
  by = c("postcode" = "pc7")
) %>%
  filter(!datazone2011 == "") %>%
  left_join(
    get_locality_lookup(c(
      "datazone2011",
      "ca2019name",
      "hscp_locality"
    )),
    by = "datazone2011"
  ) %>%
  rename(partnership = ca2019name, locality = hscp_locality)

rm(ni16_extract, ni16_connect)

# Aggregate to locality level
ni16 <- ni16 %>%
  group_by(year, month, partnership, ca2018, locality) %>%
  summarise(falls = sum(falls)) %>%
  ungroup()

# Chunk of code for creating various totals
# Create annual totals
test <- ni16 %>%
  mutate(temp_month = "Annual") %>%
  pivot_longer(c("month", "temp_month"),
    values_to = "month"
  ) %>%
  select(-name) %>%
  # Create Clackmannanshire & Stirling totals
  mutate(temp_part = if_else(ca2018 %in% c("S12000005", "S12000030"),
    "Clackmannanshire and Stirling",
    NA_character_
  )) %>%
  pivot_longer(
    cols = c(partnership, temp_part),
    values_to = "partnership",
    values_drop_na = TRUE
  ) %>%
  select(-name) %>%
  # Create all localities
  mutate(temp_loc = "All") %>%
  pivot_longer(c("locality", "temp_loc"), values_to = "locality") %>%
  select(-name) %>%
  # Scotland totals
  mutate(temp_part = if_else(partnership == "Clackmannanshire & Stirling",
    NA_character_,
    "Scotland"
  )) %>%
  pivot_longer(
    cols = c(partnership, temp_part),
    values_to = "partnership",
    values_drop_na = TRUE
  ) %>%
  select(-name) %>%
  # Select out any rows where partnership is Scotland
  # and localities aren't 'all'
  filter(partnership != "Scotland" | locality == "All") %>%
  # Aggregate
  group_by(year, month, partnership, locality) %>%
  summarise(falls = sum(falls)) %>%
  ungroup()

# Add on populations from estimates file
ni16_final <- left_join(test,
  get_loc_pops(),
  by = c("year", "locality", "partnership")
) %>%
  # We only require the over 65 population
  select(-over18_pop, -over75_pop) %>%
  # NI16 value is falls rate per 1000 population
  mutate(
    value = (falls / over65_pop) * 1000,
    indicator1 = "NI16"
  ) %>%
  # Rename to standard variable names
  rename(
    numerator = falls,
    denominator = over65_pop,
    data1 = month,
    year1 = year,
    partnership1 = partnership
  )

# Create a column for Scotland total instead of rows
scot <- ni16_final %>%
  filter(partnership1 == "Scotland") %>%
  rename(scotland = value) %>%
  select(year1, data1, scotland)
ni16_final <- left_join(ni16_final, scot, by = c("year1", "data1"))

# Section for writing out final files
writexl::write_xlsx(ni16_final, "NI16-R-TEST.xlsx")
