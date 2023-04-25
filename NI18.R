# Read and format NI 18 data
# Output for Excel and Tableau

answer <- readline(
  prompt =
    "Do you want to pull old data, or get new data? Valid answers: 'Old', 'New': "
)

if (stringr::str_detect(answer, stringr::regex("old", ignore_case = TRUE))) { # Read the data from the new 'final' spreadsheet
  data <- readxl::read_xlsx(fs::path("/", "conf", "irf", "03-Integration-Indicators", "01-Core-Suite", "NI 18", "MI_Copy_NI18.xlsx"),
    sheet = "Data",
    guess_max = 12000
  ) %>%
    dplyr::filter(Indicator == "NI18") %>%
    dplyr::select(
      Year1 = Year,
      value = Rate,
      numerator = Numerator,
      denominator = Denominator,
      Partnership1 = Partnership,
      indicator1 = Indicator,
      data1 = `Time Period`
    )

  # Take the Scotland row and turn it into a column and join it back on
  data <- data %>%
    dplyr::left_join(
      dplyr::filter(., Partnership1 == "Scotland") %>%
        dplyr::rename(scotland = value) %>%
        dplyr::select(Year1, scotland)
    ) %>%
    dplyr::filter(Partnership1 != "Scotland")

  # Create extra variables which are needed and reorder
  data <- data %>%
    dplyr::mutate(
      locality = "All"
    ) %>%
    dplyr::select(
      Year1,
      value,
      scotland,
      Partnership1,
      numerator,
      locality,
      indicator1,
      denominator,
      data1
    ) %>%
    # Sort the data nicely
    dplyr::arrange(Year1, Partnership1)

  # Turn year into financial year
  # Create a vectorised function
  year_to_fy <- Vectorize(function(year) {
    year <- as.character(year)
    year_2digit <- as.integer(tidyr::str_extract(year, "\\d\\d$"))

    fy <- stringr::str_glue(year, "/", as.character(year_2digit + 1))
    return(fy)
  })

  # Use the funtion
  data <- data %>% dplyr::mutate(Year1 = year_to_fy(Year1))

  # Write out for Tableau
  data %>%
    # Set the correct length for matching to other SPSS files
    dplyr::mutate(locality = stringr::str_pad(locality, 68, side = "right")) %>%
    haven::write_sav(fs::path("/", "conf", "irf", "03-Integration-Indicators", "01-Core-Suite", "NI 18", "NI 18-Final.zsav", compress = "zsav"))
} else if (stringr::str_detect(answer, stringr::regex("new", ignore_case = T)) == T) {
  raw_data <- readxl::read_excel(
    "NI 18/2022-04-26-balance-of-care.xlsm",
    sheet = "T1 Data",
    range = "B1:Q133",
    col_names = TRUE,
    col_types = rep(c("text", "numeric"), times = c(3, 13))
  ) %>%
    dplyr::rename(identifier = ...1, partnership_no = ...2, partnership = ...3) %>%
    dplyr::select(identifier, partnership_no, partnership, contains("2021")) %>%
    tidyr::pivot_wider(
      names_from = identifier,
      values_from = contains("20")
    ) %>%
    janitor::clean_names() %>%
    dplyr::mutate(
      numerator = total_pc,
      denominator = total_pc + ch_res + cc_census,
      value = percentage
    )

  c_and_s <- raw_data %>%
    dplyr::filter(partnership == "Clackmannanshire" | partnership == "Stirling") %>%
    dplyr::mutate(partnership = "Clackmannanshire and Stirling") %>%
    dplyr::group_by(partnership) %>%
    dplyr::summarise(
      numerator = sum(numerator),
      denominator = sum(denominator)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(value = numerator / denominator)

  ni18_new <- dplyr::bind_rows(raw_data, c_and_s) %>%
    dplyr::mutate(
      indicator1 = "NI18",
      year1 = "2021/22",
      locality = "All",
      data1 = "Annual"
    ) %>%
    dplyr::select(year1, value,
      partnership1 = partnership, numerator, locality, indicator1,
      denominator, data1
    )

  ni18_final <- dplyr::left_join(ni18_new %>% dplyr::filter(partnership1 != "Scotland"),
    ni18_new %>% dplyr::filter(partnership1 == "Scotland") %>%
      dplyr::rename(scotland = value) %>%
      dplyr::select(year1, scotland),
    by = "year1"
  ) %>%
    dplyr::relocate(scotland, .after = "value")
}
