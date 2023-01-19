library(readxl)
library(scales)
library(skimr)

spreadsheet_output_dir <- fs::path(
  "/",
  "conf",
  "irf",
  "03-Integration-Indicators",
  "01-Core-Suite",
  "Spreadsheet outputs"
)

new_nis <-
  read_xlsx(
    fs::path(
      spreadsheet_output_dir,
      "SMR-Indicators-MI-Spreadsheet-Output-September-2021.xlsx"
    )
  )
old_nis <-
  read_xlsx(
    fs::path(
      spreadsheet_output_dir,
      "SMR-Indicators-MI-Spreadsheet-Output-Jun21.xlsx"
    )
  )


checks <- left_join(new_nis, old_nis,
  by = c("year", "Time_Period", "Partnership", "Indicator"),
  suffix = c("_new", "_old")
)

checks <- checks %>% mutate(
  numdiff = label_percent(accuracy = 0.01)((numerator_new - numerator_old) / numerator_old),
  denomdiff = label_percent(accuracy = 0.01)((denominator_new - denominator_old) / denominator_old),
  valuediff = label_percent(accuracy = 0.01)((Rate_new - Rate_old) / Rate_old)
)

check_list <- checks %>% split(checks[["Indicator"]])

check_list <- check_list %>% purrr::map(~ arrange(.x, valuediff))
