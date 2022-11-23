## NI 16

falls_codes <- paste0("W", stringr::str_pad(0:19, 2, "left", "0"))

## Read data from SMRA

ni16_extract <-
  tibble::as_tibble(
    odbc::dbGetQuery(connect_to_smra(), ni16_smra_query())
  ) %>%
  # For R-standard column names
  janitor::clean_names() %>%
  # Change to diagnosis codes to first three characters
  dplyr::mutate(dplyr::across(main_condition:other_condition_5, stringr::str_sub, 1, 3)) %>%
  # Filter out any records where the diagnosis didn't involve a fall
  dplyr::filter(if_any(
    c(main_condition:other_condition_5),
    ~ . %in% falls_codes
  )) %>%
  # Create variable for counting falls
  dplyr::mutate(
         month = calculate_financial_month(discharge_date),
         year = phsmethods::extract_fin_year(discharge_date)) %>%
  dplyr::left_join(.,
      readr::read_rds(get_spd_path()) %>%
        dplyr::select("pc7", "ca2018", "datazone2011"),
      by = c("postcode" = "pc7")
) %>%
  dplyr::filter(!is_missing(datazone2011)) %>%
  dplyr::left_join(.,
      readr::read_rds(get_locality_path()) %>%
        dplyr::select("datazone2011", "ca2019name", "hscp_locality"),
  by = "datazone2011"
  ) %>%
  dplyr::rename(partnership = ca2019name, locality = hscp_locality) %>%
  # Aggregate to locality level
  dplyr::group_by(year, month, partnership, ca2018, locality) %>%
  dplyr::summarise(falls = dplyr::n()) %>%
  dplyr::ungroup()

test <- add_all_grouping(ni16_extract, "month", "Annual")

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
