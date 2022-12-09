hace_year <- "2022"
finyear <- "2021/22"

# Pull out data from a stable previous version
older_data_tableau <- read_sav("Tableau Outputs/NI Tableau Final-L3-Apr-2022.sav") %>%
  filter(Indicator1 %in% c("NI1", "NI2", "NI3", "NI4", "NI5", "NI6", "NI7", "NI8", "NI9")) %>%
  select(-LA_Code)

# Archive old version
write_sav(older_data_tableau, glue("Z1 - Data Archive/NI 1-9-Tableau-pre-{Sys.Date()}"))

# Read in data from two spreadsheets and wrangle ----

# Get the HACE data at Partnership level
raw_data_hscp <- read_excel(glue("NI 1-9/HSCP_{hace_year}_final_results_For_LIST.xlsx"),
                            sheet = 2,
                            col_names = TRUE
) %>%
  clean_names() %>%
  mutate(locality = "All") %>%
  rename(value = glue("hscp_percent_positive_{hace_year}"),
         upper_ci = glue("wgt_percentpositive_upp_{hace_year}"),
         lower_ci = glue("wgt_percentpositive_low_{hace_year}"),
         partnership = hscp_name)

# Read in the HACE data at locality level
raw_data_loc <- read_excel(glue("NI 1-9/Locality_{hace_year}_final_results_For_LIST.xlsx"),
  # Second sheet is where the data is
  sheet = 2,
  col_names = TRUE
) %>%
  clean_names() %>%
  # Column is formatted "hscp_name HSCP - locality_name" so we want to split those
  bind_cols(., colsplit(.$locality_name, pattern = " - ", names = c("partnership", "locality"))) %>%
  # Get rid of the 'HSCP' and 'Local Authority' strings
  mutate(
    partnership =
      case_when(
        str_detect(partnership, "HSCP") ~ str_replace(partnership, " HSCP", ""),
        str_detect(partnership, "Local Authority") ~ str_replace(partnership, " Local Authority", ""),
        TRUE ~ partnership
      )
  ) %>%
  select(-locality_name) %>%
  # Rename to value and make sure it's a number.
  # Coerces NAs when value is '*' (suppressed data)
  rename(value = glue("locality_percent_positive_{hace_year}"),
         upper_ci = glue("wgt_percentpositive_upp_{hace_year}"),
         lower_ci = glue("wgt_percentpositive_low_{hace_year}")) %>%
  mutate(across(c("value", "upper_ci", "lower_ci"), as.numeric)) %>%
  filter(partnership != "Scotland")

# Add locality and partnership data together
one_to_nine <- bind_rows(raw_data_loc, raw_data_hscp) %>%
  mutate(
    indicator = case_when(
      # Find indicator based on content of question, as the question numbers may change in the future
      str_detect(description_2022, "In general,") ~ "NI1",
      str_detect(description_2022, "independently") ~ "NI2",
      str_detect(description_2022, "had a say") ~ "NI3",
      str_detect(description_2022, "coordinated") ~ "NI4",
      str_detect(description_2022, "exclude") ~ "NI5",
      str_detect(description_2022, "GP") ~ "NI6",
      str_detect(description_2022, "maintained") ~ "NI7",
      str_detect(description_2022, "continue") ~ "NI8",
      str_detect(description_2022, "safe") ~ "NI9",
      TRUE ~ "No"
    ),
    data = "Annual",
    year = finyear,
    numerator = NA,
    denominator = NA
  ) %>%
  select(year, value, partnership, numerator, locality, indicator, denominator, data, lower_ci, upper_ci)

rm(raw_data_hscp, raw_data_loc)

# Section for when no reshaping is required ----

# Add Scotland totals as a column
final <- left_join(one_to_nine,
                      one_to_nine %>% filter(partnership == "Scotland") %>%
                        rename(scotland = value) %>%
                        select(data, indicator, year, scotland),
  by = c("year", "indicator", "data")) %>%
  select(year, value, scotland, partnership, numerator, locality, indicator, denominator, data, lower_ci, upper_ci) %>%
  set_colnames(colnames(older_data_tableau))

final <- bind_rows(final, older_data_tableau)

rm(older_data_tableau)

# Final save outs
write_sav(all_data_and_vars, "NI 1-9/NI 1-9-All Data and Vars.zsav", compress = TRUE)
# No Scotland rows in Tableau output
write_sav(final %>% filter(Partnership1 != "Scotland"), "NI 1-9/NI 1-9-Tableau-Format.zsav")
# No individual localities in MI output
write_xlsx(final %>% filter(Locality == "All"), "NI 1-9/NI 1-9-MI-Format.xlsx")


# Section to get older CI files ----
# confidence <- bind_rows(
#   read_sav("NI 1-9/NI 1-9-With-CIs-Reshaped-1920.zsav"),
#   read_sav("NI 1-9/NI 1-9-With-CIs-Reshaped-1314.zsav"),
#   read_sav("NI 1-9/NI 1-9-With-CIs-Reshaped-1516.zsav"),
#   read_sav("NI 1-9/NI 1-9-With-CIs-Reshaped-1718.zsav")) %>% clean_names() %>%
#   select(-hscp_ques_resp_unweighted_2020, -hscp_percent_positive_2020)

# Section for taking messy column names from CI data and cleaning them ----
# column_names_1 <- c(glue("NI{1:9}_number_of_responses"))
# column_names_2 <- c(glue("NI{1:9}_percentage_positive"))
# column_names_3 <- c(glue("NI{1:9}_lower_ci"))
# column_names_4 <- c(glue("NI{1:9}_upper_ci"))
# column_names <- c("hscp")
# i <- 1
# while (i <= 9) {
#   column_names <- append(column_names, c(column_names_1[i], column_names_2[i], column_names_3[i], column_names_4[i]))
#   i <- i + 1
# }

# Reshape the CI info to be matched on to main HACE data
# reshape_ci <- function(year) {
#   terrible_data <-
#     read_excel("NI 1-9/Integration_indicators_1to9_confidence_intervals_Dec_2021.xlsx", sheet = glue("HSCP {year}"), range = "A6:AS38") %>%
#     select(1:5, 7:10, 12:15, 17:20, 22:25, 27:30, 32:35, 37:40, 42:45)
#
#   colnames(terrible_data) <- column_names
#
#   responses <- terrible_data %>%
#     pivot_longer(
#       cols = c(contains("number_of_responses")),
#       names_to = "indicator",
#       values_to = "number_of_responses"
#     ) %>%
#     mutate(indicator = str_sub(indicator, 1, 3)) %>%
#     select(hscp, indicator, number_of_responses)
#
#   percentage <- terrible_data %>%
#     pivot_longer(
#       cols = c(contains("percentage")),
#       names_to = "indicator",
#       values_to = "percentage_positive"
#     ) %>%
#     mutate(indicator = str_sub(indicator, 1, 3)) %>%
#     select(hscp, indicator, percentage_positive)
#
#   lowerci <- terrible_data %>%
#     pivot_longer(
#       cols = c(contains("lower")),
#       names_to = "indicator",
#       values_to = "lower_ci"
#     ) %>%
#     mutate(indicator = str_sub(indicator, 1, 3)) %>%
#     select(hscp, indicator, lower_ci)
#
#   upperci <- terrible_data %>%
#     pivot_longer(
#       cols = c(contains("upper")),
#       names_to = "indicator",
#       values_to = "upper_ci"
#     ) %>%
#     mutate(indicator = str_sub(indicator, 1, 3)) %>%
#     select(hscp, indicator, upper_ci)
#
#   return_df <- left_join(responses, percentage) %>%
#     left_join(., lowerci) %>%
#     left_join(., upperci) %>%
#     mutate(
#       year = str_c("20", str_sub({{ year }}, 1, 2), "/", str_sub({{ year }}, 3, 4)),
#       locality = "All",
#       data = "Annual"
#     )
#
#   return(return_df)
# }

# Get CIs for each year
# ci_values <- bind_rows(
#   reshape_ci("1920"),
#   reshape_ci("1718"),
#   reshape_ci("1516"),
#   reshape_ci("1314")
# )

# all_data_and_vars <- left_join(one_to_nine, ci_values, by = c("locality", "data", "indicator", "year", "partnership" = "hscp")) %>%
#   select(year:data, upper_ci, lower_ci)









help <- read_sav("Z1 - Data Archive/NI 1-9-All Data and Vars-pre-2022-05-26")
