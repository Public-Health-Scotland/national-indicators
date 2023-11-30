test_that("Partnership names are modified correctly", {
  dummy_data <- tibble::tribble(
    ~partnership,
    "Aberdeen City",
    "Perth & Kinross",
    "Orkney",
    "Shetland",
    "City of Edinburgh",
    "Na h-Eileanan Siar"
  )

  expect_equal(
    standardise_partnership_names(dummy_data$partnership),
    c(
      "Aberdeen City", "Perth and Kinross", "Orkney Islands", "Shetland Islands",
      "Edinburgh", "Western Isles"
    )
  )
})
