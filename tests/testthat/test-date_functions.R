test_that("Financial months are assigned as expected", {
  expect_equal(
    calculate_financial_month(lubridate::dmy("11-04-2021")),
    "M1"
  )

  expect_equal(
    calculate_financial_month(lubridate::dmy("31-03-2021")),
    "M12"
  )

  expect_equal(
    calculate_financial_month(c(
      lubridate::dmy("12-12-2015"),
      lubridate::dmy("02-05-2016"),
      lubridate::dmy("28-06-2017")
    )),
    c("M9", "M2", "M3")
  )
})

test_that("Financial years are calculated correctly", {
  expect_equal(format_financial_year("2016"), "2016/17")
  expect_equal(format_financial_year("2019"), "2019/20")
  expect_equal(
    format_financial_year(c("2015", "2018", "2020")),
    c("2015/16", "2018/19", "2020/21")
  )
})
