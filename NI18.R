# Read and format NI 18 data
# Output for Excel and Tableau

answer <- readline(
  prompt =
    "Do you want to pull old data, or get new data? Valid answers: 'Old', 'New': "
)

if (str_detect(answer, regex("old", ignore_case = TRUE))) { # Read the data from the new 'final' spreadsheet
  data <- read_xlsx("/conf/irf/03-Integration-Indicators/01-Core-Suite/NI 18/MI_Copy_NI18.xlsx",
    sheet = "Data",
    guess_max = 12000
  ) %>%
    filter(Indicator == "NI18") %>%
    select(
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
    left_join(
      filter(., Partnership1 == "Scotland") %>%
        rename(scotland = value) %>%
        select(Year1, scotland)
    ) %>%
    filter(Partnership1 != "Scotland")

  # Create extra variables which are needed and reorder
  data <- data %>%
    mutate(
      locality = "All"
    ) %>%
    select(
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
    arrange(Year1, Partnership1)

  # Turn year into financial year
  # Create a vectorised function
  year_to_fy <- Vectorize(function(year) {
    year <- as.character(year)
    year_2digit <- as.integer(str_extract(year, "\\d\\d$"))

    fy <- str_glue(year, "/", as.character(year_2digit + 1))
    return(fy)
  })

  # Use the funtion
  data <- data %>% mutate(Year1 = year_to_fy(Year1))

  # Write out for Tableau
  data %>%
    # Set the correct length for matching to other SPSS files
    mutate(locality = str_pad(locality, 68, side = "right")) %>%
    write_sav("/conf/irf/03-Integration-Indicators/01-Core-Suite/NI 18/NI 18-Final.zsav", compress = TRUE)
} else if (str_detect(answer, regex("new", ignore_case = T)) == T) {
  raw_data <- read_excel(
    "NI 18/2022-04-26-balance-of-care.xlsm",
    sheet = "T1 Data",
    range = "B1:Q133",
    col_names = TRUE,
    col_types = rep(c("text", "numeric"), times = c(3, 13))
  ) %>%
    rename(identifier = ...1, partnership_no = ...2, partnership = ...3) %>%
    select(identifier, partnership_no, partnership, contains("2021")) %>%
    pivot_wider(
      names_from = identifier,
      values_from = contains("20")
    ) %>%
    clean_names() %>%
    mutate(
      numerator = total_pc,
      denominator = total_pc + ch_res + cc_census,
      value = percentage
    )

  c_and_s <- raw_data %>%
    filter(partnership == "Clackmannanshire" | partnership == "Stirling") %>%
    mutate(partnership = "Clackmannanshire and Stirling") %>%
    group_by(partnership) %>%
    summarise(
      numerator = sum(numerator),
      denominator = sum(denominator)
    ) %>%
    ungroup() %>%
    mutate(value = numerator / denominator)

  ni18_new <- bind_rows(raw_data, c_and_s) %>%
    mutate(
      indicator1 = "NI18",
      year1 = "2021/22",
      locality = "All",
      data1 = "Annual"
    ) %>%
    select(year1, value,
      partnership1 = partnership, numerator, locality, indicator1,
      denominator, data1
    )

  ni18_final <- left_join(ni18_new %>% filter(partnership1 != "Scotland"),
    ni18_new %>% filter(partnership1 == "Scotland") %>%
      rename(scotland = value) %>%
      select(year1, scotland),
    by = "year1"
  ) %>%
    relocate(scotland, .after = "value")
}
