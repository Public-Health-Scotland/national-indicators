# Read and format NI 18 data
# Output for Excel and Tableau

answer <- readline(
  prompt =
    "Do you want to pull old data, or get new data? Valid answers: 'Old', 'New': "
)

ni_18_dir <- fs::path(
  "/",
  "conf",
  "irf",
  "03-Integration-Indicators",
  "01-Core-Suite",
  "data_inputs",
  "NI18"
)

if (stringr::str_detect(answer, stringr::regex("old", ignore_case = TRUE))) { # Read the data from the new 'final' spreadsheet
  data <- readxl::read_xlsx(
    path = fs::path(ni_18_dir, "MI_Copy_NI18.xlsx"),
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
  raw_data <- get_new_ni_18_data(
    path = fs::path(ni_18_dir, "2023-02-28-balance-of-care.xlsm"),
    min_year = 2015
  )

  c_and_s <- raw_data %>%
    dplyr::filter(partnership %in% c("Clackmannanshire", "Stirling")) %>%
    dplyr::mutate(partnership = "Clackmannanshire and Stirling") %>%
    dplyr::group_by(year, partnership) %>%
    dplyr::summarise(
      numerator = sum(numerator),
      denominator = sum(denominator)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(rate = numerator / denominator * 100)

  ni_18_data <- dplyr::bind_rows(raw_data, c_and_s)

  spreadsheet_output <- ni_18_data %>%
    dplyr::mutate(time_period = "Annual",
                  indicator = "NI18",
                  estimate = "No",
                  indicator_num = 18L) %>%
    dplyr::select(
      "year",
      "time_period",
      "partnership",
      "indicator",
      "estimate",
      "numerator",
      "denominator",
      "rate",
      "indicator_num"
    )

  max_year <- max(spreadsheet_output$year)

  spreadsheet_output %>%
    readr::write_csv(
      fs::path(ni_18_dir, stringr::str_glue("NI18_data_to_{max_year}.csv"))
    )


  ni18_new <- ni_18_data %>%
    dplyr::mutate(
      indicator1 = "NI18",
      locality = "All",
      data1 = "Annual"
    ) %>%
    dplyr::select(
      "year1",
      "rate",
      partnership1 = "partnership",
      "numerator",
      "locality",
      "indicator1",
      "denominator",
      "data1"
    )

  ni18_final <- dplyr::left_join(
    ni18_new %>%
      dplyr::filter(partnership1 != "Scotland"),
    ni18_new %>% dplyr::filter(partnership1 == "Scotland") %>%
      dplyr::rename(scotland = value) %>%
      dplyr::select(year1, scotland),
    by = "year1"
  ) %>%
    dplyr::relocate(scotland, .after = "value")
}
