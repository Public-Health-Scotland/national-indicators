# Setup ----

# Libraries for ease
library(lubridate) # Easier manipulation of dates

# Fill in with years that are being updated for NI12 and NI13
years_ni12_ni13 <- c("1617", "1718")

# Outcome indicators ----

# NI12 ----

ni12 <- lapply(years_ni12_ni13, function(x) {
  df <- calculate_ni12(x, write_to_disk = FALSE)
})

names(ni12) <- years_ni12_ni13

# NI13 ----

ni13 <- lapply(years_ni12_ni13, function(x) {
  df <- calculate_ni13(x, write_to_disk = FALSE)
})

names(ni13) <- years_ni12_ni13

# NI14 ----

ni14 <- calculate_ni14()

# NI15 ----

# NI16 ----

# NI17 ----

# NI18 ----

# NI19 ----

# NI20 ----

# Create Excel output ----

# Create Tableau output ----
