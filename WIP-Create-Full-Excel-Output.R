# Get old file

previous_data <- openxlsx::read.xlsx("/conf/irf/03-Integration-Indicators/01-Core-Suite/Spreadsheet outputs/Core_Suite_Integration_Indicators_Management_Information_December2022 (1).xlsx",
                                     sheet = "Data") %>%
  # Strip out the data being replaced
  dplyr::filter(!(Indicator %in% c("NI14", "NI15", "NI16", "NI19"))) %>%
  dplyr::filter(!(Indicator %in% c("NI12", "NI13") & Year %in% c("2020/21", "2021/22", "2022/23")))

# Get new file

new_data <- openxlsx::read.xlsx("/conf/irf/03-Integration-Indicators/01-Core-Suite/Spreadsheet outputs/SMR-Indicators-MI-Spreadsheet-Output-Dec-2022.xlsx") %>%
  # NI20 isn't changing
  dplyr::filter(!(Indicator %in% c("NI20"))) %>%
  # Rename in line with Excel template
  dplyr::rename(`Time.Period` = Time_Period,
                Year = year,
                Numerator = numerator,
                Denominator = denominator) %>%
  # Create new variables in line with template
  dplyr::mutate(Estimate = "No",
                `Ind.no.` = as.integer(stringr::str_replace(Indicator, "NI", "")),
                Lookup = paste0(Year, `Time.Period`, Indicator, Partnership, Estimate))

# Add them together

ready_for_pasting <- dplyr::bind_rows(previous_data, new_data)

openxlsx::write.xlsx(ready_for_pasting, fs::path(get_ni_excel_output_dir(), "Final-MI-Dec-2022.xlsx"))

checks <- dplyr::left_join(new_data, prev_data_full,
                           by = c("Year", "Time.Period", "Partnership", "Indicator", "Estimate", "Ind.no.", "Lookup"),
                           suffix = c("_new", "_old")) %>%
  dplyr::mutate(Rate_diff = Rate_new - Rate_old,
                Rate_proportion = scales::percent(Rate_diff/Rate_old),
                Rate_issue = !dplyr::between(Rate_diff/Rate_old, -.05, .05),
                Numerator_diff = Numerator_new - Numerator_old,
                Numerator_proportion = scales::percent(Numerator_diff/Numerator_old),
                Numerator_issue = !dplyr::between(Numerator_diff/Numerator_old, -.05, .05),
                Denominator_diff = Denominator_new - Denominator_old,
                Denominator_proportion = scales::percent(Denominator_diff/Denominator_old),
                Denominator_issue = !dplyr::between(Denominator_diff/Denominator_old, -.05, .05)) %>%
  dplyr::select("Indicator",
                "Partnership",
                "Year",
                dplyr::contains("Rate"),
                dplyr::contains("Numerator"),
                dplyr::contains("Denominator")) %>%
  dplyr::arrange(dplyr::desc(Rate_proportion))

openxlsx::write.xlsx(checks, fs::path(get_ni_dir(), "Checks-Dec-2022.xlsx"))
