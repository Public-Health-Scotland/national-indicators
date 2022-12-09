test_that("Quarters are assigned correctly", {
  expect_equal(
    convert_fin_month_to_quarter(c("M1", "M4", "M8", "M1", "Annual", "M12")),
    c("Q1", "Q2", "Q3", "Q1", "Annual", "Q4")
  )
})

test_that("Error is thrown as expected", {
  expect_error(convert_fin_month_to_quarter(c("M1", "Hello", "World")))

  expect_error(convert_fin_month_to_quarter(c("", NA, NA)))
})
