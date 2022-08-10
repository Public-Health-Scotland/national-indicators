library(readxl)
library(scales)
library(skimr)

new_nis <- read_xlsx("/conf/irf/03-Integration-Indicators/01-Core-Suite/Spreadsheet outputs/SMR-Indicators-MI-Spreadsheet-Output-September-2021.xlsx")
old_nis <- read_xlsx("/conf/irf/03-Integration-Indicators/01-Core-Suite/Spreadsheet outputs/SMR-Indicators-MI-Spreadsheet-Output-Jun21.xlsx")


checks <- left_join(new_nis, old_nis, by = c("year", "Time_Period", "Partnership", "Indicator"),
                  suffix = c("new", "old"))

checks <- checks %>% mutate(numdiff = label_percent(accuracy = 0.01)((numeratornew - numeratorold)/numeratorold),
                        denomdiff = label_percent(accuracy = 0.01)((denominatornew - denominatorold)/denominatorold),
                        valuediff = label_percent(accuracy = 0.01)((Ratenew - Rateold)/Rateold))

check_list<- checks %>% split(checks$Indicator)

check_list <- check_list %>% purrr::map(~ arrange(.x, valuediff))



