#' Generate counts of beddays per month for an episode
#' with an admission and discharge date
#' Optionally these months can be between two
#' supplied dates
#'
#' @param data
#' @param earliest_date
#' @param latest_date
#' @param pivot_longer
#'
#' @return
#' @export
#'
#' @examples
monthly_beddays_and_admissions <- function(data,
                                           earliest_date = NA,
                                           latest_date = NA,
                                           pivot_longer = TRUE) {
  # If earliest or latest date wasn't supplied use dates from the data
  earliest_date <- dplyr::if_else(
    is.na(earliest_date),
    min(data$admission_date),
    as.POSIXct(lubridate::ymd(earliest_date))
  )

  latest_date <- dplyr::if_else(
    is.na(latest_date),
    max(data$discharge_date),
    as.POSIXct(lubridate::ymd(latest_date))
  )

  # Create a vector of years from the first to last
  years <- c(lubridate::year(earliest_date):lubridate::year(latest_date))

  # Create a vector of month names
  month_names <- lubridate::month(1:12, label = T)

  # Use purrr to create a list of intervals these will be
  # date1 -> date1 + 1 month
  # for every month in the time period we're looking at
  month_intervals <-
    purrr::map2(
      # The first parameter produces a list of the years
      # The second produces a list of months
      sort(rep(years, 12)), rep(0:11, length(years)),
      function(year, month) {
        # Initialise a date as start_date + x months * (12 * y years)
        earliest_date %m+% months(month + (12 * (year - min(years))))
      }
    ) %>%
    dplyr::map(function(interval_start) {
      # Take the list of months just produced and create a list of
      # one month intervals
      lubridate::interval(interval_start, interval_start %m+% months(1))
    }) %>%
    # Give them names these will be of the form MMM_YYYY
    setNames(stringr::str_c(
      rep(month_names, length(years)),
      "_", sort(rep(years, 12)), "_beddays"
    ))

  # Remove any months which are after the latest_date
  month_intervals <- month_intervals[dplyr::map_lgl(
    month_intervals,
    ~ latest_date > lubridate::int_start(.)
  )]

  # Use the list of intervals to create new varaibles for each month
  # and work out the beddays
  data <- data %>%
    # map_dfc will return a single dataframe with all the others bound by column
    dplyr::bind_cols(purrr::map_dfc(month_intervals, function(month_interval) {
      # Use intersect to find the overlap between the month of interest
      # and the stay, then use time_length to measure the length in days
      lubridate::time_length(
        intersect(
          # use int_shift to move the interval forward by one day
          # This is so we count the last day (and not the first), which is
          # the correct methodology
          lubridate::int_shift(
            lubridate::interval(
              data %>%
                dplyr::pull(admission_date),
              data %>%
                dplyr::pull(discharge_date)
            ),
            by = lubridate::days(1)
          ),
          month_interval
        ),
        unit = "days"
      )
    }))

  names(month_intervals) <- stringr::str_replace(
    names(month_intervals),
    "_beddays", "_admissions"
  )

  data <- data %>%
    # map_dfc will return a single dataframe with all the others bound by column
    dplyr::bind_cols(purrr::map_dfc(month_intervals, function(month_interval) {
      dplyr::if_else(data %>%
        dplyr::pull(discharge_date) %>%
        lubridate::floor_date(unit = "month") == lubridate::int_start(month_interval),
      1L,
      NA_integer_
      )
    }))

  # Default behaviour
  # Turn all of the Mmm_YYYY (e.g. Jan_2019) into a Month and Year variable
  # This means many more rows so we drop any which aren't interesting
  # i.e. all NAs
  if (pivot_longer) {
    data <- data %>%
      # Use pivot longer to create a month, year and beddays column which
      # can be used to aggregate later
      tidyr::pivot_longer(
        cols = contains(c("_19", "_20")),
        names_to = c("month", "year", ".value"),
        names_pattern = "^([A-Z][a-z]{2})_(\\d{4})_([a-z]+)$",
        names_ptypes = list(
          month = factor(
            levels = as.vector(lubridate::month(1:12,
              label = TRUE
            )),
            ordered = TRUE
          ),
          year = factor(
            levels = years,
            ordered = TRUE
          )
        ),
        values_drop_na = TRUE
      ) %>%
      # Create a 'quarter' column
      dplyr::mutate(
        quarter = dplyr::case_when(
          month %in% month_names[1:3] ~ 1,
          month %in% month_names[4:6] ~ 2,
          month %in% month_names[7:9] ~ 3,
          month %in% month_names[10:12] ~ 4
        )
      )
  }
  return(data)
}
