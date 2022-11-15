library(data.table)

smra_extract <- process_ni14_smra_extract()
gro_extract <- process_ni14_gro_extract()

# Match the death dates onto the main table
smra_and_gro_extract <-
  dplyr::left_join(smra_extract, gro_extract, by = "link_no") %>%
  # If the death date is the same as a discharge
  # we will discount it, as this cannot result in a readmission
  dplyr::mutate(
    discharge_to_death = .data$death_date - .data$cis_disdate,
    discharged_dead_both =
      discharge_to_death <= 0 & death_date >= cis_admdate |
        discharged_dead,
    dplyr::across("discharged_dead_both", ~ replace(., is.na(.), FALSE)),
    # Set up a flag to keep records where patient is not dead at discharge date
    stay = !discharged_dead_both,
    # Extract the financial year and financial month
    fin_month = calculate_financial_month(cis_disdate),
    year = phsmethods::extract_fin_year(cis_disdate)) %>%
  # Match on postcodes to get datazone, and then use datazone to match locality
  dplyr::left_join(.,
                   readr::read_rds(get_spd_path()),
                   by = c("postcode" = "pc7")) %>%
  dplyr::left_join(.,
                   readr::read_rds(get_locality_path()) %>%
                     dplyr::select(datazone2011, hscp_locality),
                   by = "datazone2011") %>%
  # Aggregate to locality-level at the lowest
  dplyr::select(year, fin_month, ca2018, hscp_locality, datazone2011, flag28, stay) %>%
  dtplyr::lazy_dt() %>%
  dplyr::group_by(year, fin_month, ca2018, hscp_locality) %>%
  dplyr::summarise(dplyr::across(flag28:stay, sum, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  tibble::as_tibble() %>%
  dplyr::mutate(partnership = phsmethods::match_area(ca2018))

# Create a list with different totals for aggregation
totals_list <- smra_and_gro_extract %>%
  list(
    # Annual totals for each financial year
    annual = dplyr::mutate(.,
                    fin_month = "Annual"
    ),
    # 'All localities' groupings
    all_loc = dplyr::mutate(.,
                     hscp_locality = "All"
    ),
    # Annual and all localities
    all_all = dplyr::mutate(.,
                     fin_month = "Annual",
                     hscp_locality = "All"
    ),
    # Scotland totals
    scotland = dplyr::mutate(.,
                      partnership = "Scotland",
                      ca2018 = "Scotland",
                      hscp_locality = "All"
    ),
    # Scotland annual totals
    scot_annual = dplyr::mutate(.,
                         partnership = "Scotland",
                         ca2018 = "Scotland",
                         hscp_locality = "All",
                         fin_month = "Annual"
    )
  )

# Apply the same aggregate to each member of the list
ni14_final <- totals_list %>%
  purrr::map_dfr(~ dplyr::group_by(.x, year, fin_month, partnership, ca2018, hscp_locality) %>%
            dplyr::summarise(across(flag28:stay, sum)) %>%
            dplyr::ungroup())

ni14_cs <- add_clacks_and_stirling(ni14_final, partnership)

# Calculate the actual value of NI14
final_test <- ni14_cs %>% dplyr::mutate(value = (flag28 / stay) * 1000)

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
