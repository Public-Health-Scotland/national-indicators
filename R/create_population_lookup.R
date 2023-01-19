create_population_lookup <- function() {
  # Read in the most recent populations file
  dz_pops <- readr::read_rds(get_population_estimate_path()) %>%
    # Filter out anything before 2013 as we don't use this data
    dplyr::filter(year >= 2013) %>%
    # Calculate populations for over 18s, over 65s, and over 75s
    dplyr::mutate(
      over18_pop = rowSums(dplyr::across(age18:age90plus)),
      over65_pop = rowSums(dplyr::across(age65:age90plus)),
      over75_pop = rowSums(dplyr::across(age75:age90plus))
    ) %>%
    # Get rid of the populations by age
    dplyr::select(-c("age0":"age90plus")) %>%
    # Summarise to year and Datazone level
    dplyr::group_by(year, datazone2011) %>%
    dplyr::summarise(dplyr::across(over18_pop:over75_pop, sum), .groups = "keep")

  # Read in the lookup for the Scottish Postcode Directory and aggregate it so we
  # have one partnership name for each datazone
  temp_pc <- readr::read_rds(get_spd_path()) %>%
    dplyr::select(ca2019name, datazone2011) %>%
    dplyr::group_by(datazone2011) %>%
    dplyr::summarise(lca = dplyr::first(ca2019name))

  # Join the Datazone level populations to the lookup above, then join to the
  # locality lookup to get locality names
  loc_pops <- dplyr::left_join(dz_pops, temp_pc, by = "datazone2011") %>%
    dplyr::left_join(., readr::read_rds(get_locality_path()),
              by = "datazone2011"
    ) %>%
    # Aggregate populations to locality level
    dplyr::group_by(year, lca, hscp_locality) %>%
    dplyr::summarise(dplyr::across(over18_pop:over75_pop, sum), .groups = "keep")

  loc_pops <- loc_pops %>%
    # Create dummy years that we do not have estimates for yet
    # TODO Make this automated so we don't have to change it
    dplyr::mutate(
      temp_year1 = dplyr::if_else(year == 2021, 2022, NA_real_),
      temp_year2 = dplyr::if_else(year == 2021, 2023, NA_real_)
    ) %>%
    tidyr::pivot_longer(
      cols = c(year, temp_year1, temp_year2),
      values_to = "year",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    # Make C&S a partnership
    dplyr::mutate(temp_part = dplyr::if_else(lca == "Clackmannanshire" | lca == "Stirling",
                               "Clackmannanshire and Stirling",
                               NA_character_
    )) %>%
    tidyr::pivot_longer(
      cols = c(lca, temp_part),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    # Add an 'all' group for locality
    add_all_grouping("hscp_locality", "All") %>%
    # Create Scotland totals
    dplyr::mutate(temp_part = dplyr::if_else(partnership == "Clackmannanshire and Stirling",
                               NA_character_,
                               "Scotland"
    )) %>%
    tidyr::pivot_longer(
      cols = c(partnership, temp_part),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    # Remove any Scotland/locality pairs that aren't Scotland/All
    dplyr::filter(partnership != "Scotland" | hscp_locality == "All") %>%
    # Final sum of all the populations and extra groups we added
    dplyr::group_by(year, partnership, hscp_locality) %>%
    dplyr::summarise(dplyr::across(over18_pop:over75_pop, sum), .groups = "keep") %>%
    dplyr::ungroup() %>%
    dplyr::mutate(year = as.character(year)) %>%
    dplyr::rename(
      pop_year = year,
      locality = hscp_locality
    )

  return(loc_pops)
}

