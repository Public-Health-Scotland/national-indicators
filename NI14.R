## NI 14

# Set up connection to SMRA with username and password
smra_connect <- odbc::dbConnect(odbc::odbc(),
  dsn = "SMRA",
  uid = Sys.getenv("USER"),
  pwd = .rs.askForPassword("SMRA Password:")
)

## Read data from SMRA
# Set up SQL queries
smra_query <- "SELECT LINK_NO, CIS_MARKER, ADMISSION_DATE, DISCHARGE_DATE,
  ADMISSION_TYPE, DISCHARGE_TYPE, DR_POSTCODE as postcode
  FROM ANALYSIS.SMR01_PI
  WHERE ((ANALYSIS.SMR01_PI.RECORD_TYPE = '01B')
  AND (DISCHARGE_DATE >= '01-APR-2013')
  AND (AGE_IN_YEARS >= 18)
  AND LINK_NO is not NULL)"

gro_query <- "SELECT MAX(DATE_OF_DEATH) as death_date, LINK_NO
  FROM ANALYSIS.GRO_DEATHS_C
  WHERE (DATE_OF_DEATH >= '01-APR-2013')
  GROUP BY LINK_NO"

# Read in data
smra <-
  as_tibble(
    dbGetQuery(smra_connect, smra_query)
  ) %>%
  # For R-standard column names
  clean_names()

smra <- smra %>%
  # Make sure these variables are in date format for quicker summarising
  mutate(
    cis_admdate = as.Date(admission_date),
    cis_disdate = as.Date(discharge_date),
    discharge_type = as.integer(discharge_type)
  ) %>%
  # Aggregate to unique CIS level, based on link_no and cis_marker
  group_by(link_no, cis_marker) %>%
  # We want the first admission date and last discharge date
  summarise(
    cis_admdate = min(cis_admdate),
    cis_disdate = max(cis_disdate),
    admission_type = first(admission_type),
    discharge_type = last(discharge_type),
    postcode = last(postcode)
  ) %>%
  ungroup()

adm_code_list <- c(20:22, 30:39)


adm_checks <- smra %>%
  mutate(
    # If link_no is the same as the next link_no,
    # calculate how many days there are between
    # the next admission and the last discharge
    days_between_stays = if_else(link_no == lead(link_no),
      as.integer(lead(cis_admdate) - cis_disdate),
      NA_integer_
    ),
    # Make a flag, emerg_adm, for admission types 20, 21, 22, and 30-39.
    emerg_adm = if_else(admission_type %in% adm_code_list, TRUE, FALSE)
  ) %>%
  # If the next admission was and emergency
  # and it was less than 28 days later, assign TRUE to
  # variable flag28
  mutate(
    flag28 = (between(days_between_stays, 0, 28)) & (lead(emerg_adm)),
    # Flag admissions where a patient died, as these are excluded
    discharged_dead = (discharge_type %/% 10 == 4)
  ) %>%
  replace_na(list(flag28 = FALSE))

gro <-
  as_tibble(
    dbGetQuery(smra_connect, gro_query)
  ) %>%
  # For R-standard column names
  clean_names() %>%
  # Order by link no for matching
  arrange(link_no) %>%
  # Use Lubridate to put into date format
  mutate(death_date = as.Date(death_date))

# Match the death dates onto the main table
with_deaths <- left_join(adm_checks, gro, by = "link_no")

# If the death date is the same as a discharge
# we will discount it, as this cannot result in a readmission
with_deaths %<>% mutate(discharge_to_death = death_date - cis_disdate) %>%
  mutate(
    discharged_dead_both =
      ((discharge_to_death <= 0) &
        (death_date >= cis_admdate)) |
        (discharged_dead)
  ) %>%
  replace_na(list(discharged_dead_both = FALSE)) %>%
  # Set up a flag to keep records where patient is not dead at discharge date
  mutate(stay = !discharged_dead_both)

# Extract year and make it into financial year, assign financial month
with_deaths <- fin_year_month(with_deaths, with_deaths$cis_disdate)

# Match on postcodes to get datazone, and then use datazone to match locality
with_deaths <- left_join(with_deaths,
  get_pc_lookup(c("pc7", "datazone2011", "ca2018")),
  by = c("postcode" = "pc7")
)
with_deaths <- left_join(
  with_deaths,
  get_locality_lookup(c("hscp_locality", "datazone2011"))
)

# Aggregate to locality-level at the lowest
agg_locality <- with_deaths %>%
  select(year, month, ca2018, hscp_locality, datazone2011, flag28, stay) %>%
  group_by(year, month, ca2018, hscp_locality) %>%
  summarise(across(flag28:stay, sum, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(partnership = match_area(ca2018))

# Create a list with different totals for aggregation
totals_list <- agg_locality %>%
  list(
    # Annual totals for each financial year
    annual = mutate(.,
      month = "Annual"
    ),
    # 'All localities' groupings
    all_loc = mutate(.,
      hscp_locality = "All"
    ),
    # Annual and all localities
    all_all = mutate(.,
      month = "Annual",
      hscp_locality = "All"
    ),
    # Scotland totals
    scotland = mutate(.,
      partnership = "Scotland",
      ca2018 = "Scotland",
      hscp_locality = "All"
    ),
    # Scotland annual totals
    scot_annual = mutate(.,
      partnership = "Scotland",
      ca2018 = "Scotland",
      hscp_locality = "All",
      month = "Annual"
    )
  )

# Apply the same aggregate to each member of the list
ni14_final <- totals_list %>%
  map_dfr(~ group_by(.x, year, month, partnership, ca2018, hscp_locality) %>%
    summarise(across(flag28:stay, sum)) %>%
    ungroup())

# Create Clacks & Stirling totals
ni14_final <- ni14_final %>%
  mutate(temp_part = if_else(partnership %in% c("Clackmannanshire", "Stirling"),
    "Clackmannanshire & Stirling",
    NA_character_
  )) %>%
  pivot_longer(
    cols = c(partnership, temp_part),
    values_to = "partnership",
    values_drop_na = TRUE
  ) %>%
  select(-name) %>%
  # Calculate the actual value of NI14
  mutate(value = (flag28 / stay) * 1000)

# Create Scotland column
just_scotland <- ni14_final %>%
  filter(
    partnership == "Scotland" & hscp_locality == "All"
  ) %>%
  rename(scotland = value) %>%
  select(year, month, scotland)
ni14_final <- left_join(ni14_final, just_scotland, by = c("month", "year")) %>%
  mutate(indicator1 = "NI14")
rm(just_scotland)

# Rename variables for Tableau
ni14_final <- ni14_final %>%
  rename(
    year1 = year,
    data1 = month,
    partnership1 = partnership,
    denominator = stay,
    numerator = flag28
  )

# Write out full data frame
write_rds(ni14_final, "ni14-all-data.rds")

write_xlsx(ni14_final, "ni14-test.xlsx")

test <- ni14_final %>% filter(hscp_locality == "All" & data1 == "Annual")

# For Tableau, we don't want Scotland as a partnership
# tableau <- ni14_final %>% filter(partnership1 != "Scotland")
# write_rds(tableau, "monthly-ni14-final.rds")
# rm(tableau)

# For the MI worksheet, we don't want locality-level data
# mi <- ni14_final %>% filter(locality == "All")
# write_sav(mi, "mi-ni14-final.sav")
# rm(mi, ni14_final)
