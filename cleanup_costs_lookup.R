library(haven)
library(dplyr)
library(fs)
library(readxl)
library(stringr)
library(tidylog)
library(janitor)

cost_lookup_path <- path("Y2 - Derived Lookups", "Cost Lookup.sav")

existing_costs <- read_sav(cost_lookup_path)

new_costs <- read_excel(path("Z1 - Data Archive", "Indicator20Totals_Apr22.xlsx")) %>%
  clean_names()

# Rename variables
new_costs <-
  new_costs %>% rename(
    partnership = hscp_name,
    acost = expenditure
  )

# Rename partnerships if needed
new_costs <- new_costs %>%
  mutate(partnership = str_replace(partnership, "^City of Edinburgh$", "Edinburgh") %>%
    str_replace("&", "and") %>%
    str_replace("(Orkney|Shetland)", "\\1 Islands"))

# Uplift costs
new_costs <-
  bind_rows(
    new_costs,
    # Uplift 19/20 to 20/21
    new_costs %>%
      filter(year == "2019/20") %>%
      mutate(
        year = "2020/21",
        # Uplift by 1% for each year following 2019/20
        acost = acost * 1.01
      ),
    # Uplift 19/20 to 21/22
    new_costs %>%
      filter(year == "2019/20") %>%
      mutate(
        year = "2021/22",
        acost = acost * (1.01)^2L
      )
  )

# Create a C & S combined cost
new_costs <-
  new_costs %>%
  bind_rows(
    new_costs %>%
      filter(partnership == "Clackmannanshire" | partnership == "Stirling") %>%
      group_by(year) %>%
      summarise(
        partnership = "Clackmannanshire and Stirling",
        acost = sum(acost)
      ) %>%
      ungroup()
  )

# Create quarterly cost
new_costs <- new_costs %>% mutate(qcost = acost / 4)

# Join old and new costs
# Use anti-join to remove any exisiting costs which we want to replace
costs <- existing_costs %>%
  anti_join(new_costs, by = "year") %>%
  bind_rows(new_costs) %>%
  arrange(year, partnership)

# Archive old costs files
file_copy(cost_lookup_path, path("Z1 - Data Archive", str_glue("cost_lookup_pre-{Sys.Date()}.sav")))

# Write out for usage in NI 12, 13, 20
write_sav(costs, cost_lookup_path)
