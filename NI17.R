# Code for NI17

ni17 <- dplyr::bind_rows(
  # Get most recent year's worth of data for all ocuncils, and Scotland
  readxl::read_excel(
    path = "NI 17/Grading indicator final tables march2022.xlsx",
    sheet = "table 2 - all services by LA",
    range = "A9:D42",
    col_names = c("partnership", "numerator", "value", "denominator")
  ) %>%
    dplyr::filter(stringr::str_detect(partnership, "Area in which") == FALSE),
  # C&S is on a seperate sheet, so just bring in the values for this and arrange into
  # format of above
  readxl::read_excel(
    path = "NI 17/Grading indicator final tables march2022.xlsx",
    sheet = "Stirling and Clackmannanshire",
    range = "D16:F16",
    col_names = c("numerator", "value", "denominator")
  ) %>%
    dplyr::mutate(partnership = "Clackmannanshire and Stirling") %>%
    dplyr::relocate(partnership, .before = "numerator")
) %>%
  # Make sure councils are named as expected
  standardise_partnerships(partnership) %>%
  # Add the year
  dplyr::mutate(year = "2021/22")

# Add the column of Scotland values
ni17_final <- dplyr::left_join(
  ni17 %>% dplyr::filter(partnership != "Scotland"),
  ni17 %>% dplyr::filter(partnership == "Scotland") %>%
    dplyr::select(year, `scotland` = value),
  by = "year"
)
