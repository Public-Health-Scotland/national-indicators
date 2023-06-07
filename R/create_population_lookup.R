#' Create a population lookup for use with NIs at locality level
#'
#' @param min_year The earliest year to use in the lookup
#' @param pop_est_path See [get_population_estimate_path()] or use custom path
#' @param spd_path See [get_spd_path()] or use custom path
#' @param locality_path See [get_locality_path()] or use custom path
#'
#' @return A data frame to use as a population lookup for: over 18s, over 65s,
#' and over 75s
#' @export
create_population_lookup <- function(
    min_year,
    pop_est_path = get_population_estimate_path(),
    spd_path = get_spd_path(),
    locality_path = get_locality_path()) {
  # Read in the most recent populations file
  dz_pops <- readr::read_rds(pop_est_path) %>%
    # Filter out anything before 2013 as we don't use this data
    dplyr::filter(year >= min_year) %>%
    # Calculate populations for over 18s, over 65s, and over 75s
    dplyr::mutate(
      over18_pop = rowSums(dplyr::pick(age18:age90plus)),
      over65_pop = rowSums(dplyr::pick(age65:age90plus)),
      over75_pop = rowSums(dplyr::pick(age75:age90plus))
    ) %>%
    # Get rid of the populations by age
    dplyr::select(-c("age0":"age90plus")) %>%
    # Summarise to year and Datazone level
    dplyr::group_by(year, datazone2011) %>%
    dplyr::summarise(dplyr::across(over18_pop:over75_pop, sum), .groups = "keep")

  # Read in the lookup for the Scottish Postcode Directory and aggregate it so we
  # have one partnership name for each datazone
  temp_pc <- readr::read_rds(spd_path) %>%
    dplyr::select(ca2019name, datazone2011) %>%
    dplyr::group_by(datazone2011) %>%
    dplyr::summarise(lca = dplyr::first(ca2019name))

  # Join the Datazone level populations to the lookup above, then join to the
  # locality lookup to get locality names
  loc_pops <- dplyr::left_join(dz_pops, temp_pc, by = "datazone2011") %>%
    dplyr::left_join(., readr::read_rds(locality_path),
      by = "datazone2011"
    ) %>%
    # Aggregate populations to locality level
    dplyr::group_by(year, lca, hscp_locality) %>%
    dplyr::summarise(dplyr::across(over18_pop:over75_pop, sum), .groups = "keep")

  latest_pop_year <- loc_pops %>%
    dplyr::pull(year) %>%
    max()

  loc_pops <- loc_pops %>%
    # Create dummy years that we do not have estimates for yet
    dplyr::bind_rows(
      purrr::map_df(1:3, ~
        loc_pops %>%
          dplyr::filter(year == latest_pop_year) %>%
          dplyr::mutate(
            year = year + .x
          ))
    ) %>%
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

  arrow::write_parquet(loc_pops, glue::glue("Lookups/population_lookup_{min_year}_{latest_update()}.parquet"), compression = "zstd")

  return(loc_pops)
}

#' Create a population lookup for use with NIs at LCA level
#'
#' @param min_year The earliest year to use in the lookup
#' @param pop_est_path See [get_population_estimate_path()] or use custom path
#' @param spd_path See [get_spd_path()] or use custom path
#'
#' @return A data frame to use as a population lookup for: over 18s, over 65s,
#' and over 75s
#' @export
create_lca_population_lookup <- function(
    min_year,
    pop_est_path = get_population_estimate_path(),
    spd_path = get_spd_path()) {


  dz_pops <- readr::read_rds(get_population_estimate_path()) %>%
    # Filter out anything before 2013 as we don't use this data
    dplyr::filter(year >= min_year) %>%
    # Calculate populations for over 18s, over 65s, and over 75s
    dplyr::mutate(
      over18_pop = rowSums(dplyr::pick(age18:age90plus)),
      over65_pop = rowSums(dplyr::pick(age65:age90plus)),
      over75_pop = rowSums(dplyr::pick(age75:age90plus))
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

  lca_pops <- dplyr::left_join(dz_pops, temp_pc, by = "datazone2011") %>%
    dplyr::group_by(year, lca) %>%
    dplyr::summarise(dplyr::across(over18_pop:over75_pop, sum), .groups = "keep")

  latest_pop_year <- lca_pops %>%
    dplyr::pull(year) %>%
    max()

  lca_pops <- lca_pops %>%
    # Create dummy years that we do not have estimates for yet
    dplyr::bind_rows(
      purrr::map_df(1:3, ~
        lca_pops %>%
          dplyr::filter(year == latest_pop_year) %>%
          dplyr::mutate(
            year = year + .x
          ))
    ) %>%
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
    # Final sum of all the populations and extra groups we added
    dplyr::group_by(year, partnership) %>%
    dplyr::summarise(dplyr::across(over18_pop:over75_pop, sum), .groups = "keep") %>%
    dplyr::ungroup() %>%
    dplyr::mutate(year = as.character(year)) %>%
    dplyr::rename(
      pop_year = year
    )

  arrow::write_parquet(lca_pops, glue::glue("Lookups/population_lookup_lca_{latest_update()}.parquet"))

  return(lca_pops)
}

#' Read the population lookup if one exists, otherwise create one
#'
#' @param min_year The minimum year to be passed to [create_population_lookup()]
#' @param update_suffix Defaults to [latest_update()], the suffix of the population lookup
#' @param ages_required One of "over18", "over65", "over75", selects ony relevant columns
#' @param type The level you need the populations at, either "locality" or "partnership"
#'
#' @return The population lookup, either read from disk or created
#' @export
read_population_lookup <- function(min_year,
                                   update_suffix = latest_update(),
                                   ages_required = c("over18", "over65", "over75"),
                                   type = c("locality", "partnership")) {
  if (type == "locality") {
    if (file.exists(glue::glue("Lookups/population_lookup_{min_year}_{update_suffix}.parquet")) == TRUE) {
      pops <- arrow::read_parquet(
        glue::glue("Lookups/population_lookup_{min_year}_{update_suffix}.parquet")
      ) %>%
        dplyr::select(
          "pop_year",
          "partnership",
          "locality",
          glue::glue("{ages_required}_pop")
        )
    } else {
      pops <- create_population_lookup(min_year = min_year) %>%
        dplyr::select(
          "pop_year",
          "partnership",
          "locality",
          glue::glue("{ages_required}_pop")
        )
    }
  } else if (type == "partnership") {
    if (file.exists(glue::glue("Lookups/population_lookup_lca_{update_suffix}.parquet")) == TRUE) {
      pops <- arrow::read_parquet(
        glue::glue("Lookups/population_lookup_lca_{update_suffix}.parquet")
      ) %>%
        dplyr::select(
          "pop_year",
          "partnership",
          glue::glue("{ages_required}_pop")
        )
    } else {
      pops <- create_lca_population_lookup(min_year = min_year) %>%
        dplyr::select(
          "pop_year",
          "partnership",
          glue::glue("{ages_required}_pop")
        )
    }
  }

  return(pops)
}
