get_new_ni_18_data <- function(path, min_year) {
  readxl::read_excel(
    path = path,
    sheet = "T1 Data"
  ) %>%
    dplyr::rename(identifier = 2, partnership = 4) %>%
    dplyr::select("identifier", "partnership", dplyr::matches("\\d{4}")) %>%
    tidyr::pivot_longer(
      cols = dplyr::matches("\\d{4}"),
      names_to = "year",
      names_transform = as.integer,
      values_ptypes = numeric()
    ) %>%
    tidyr::pivot_wider(
      names_from = "identifier",
      values_from = "value"
    ) %>%
    janitor::clean_names() %>%
    dplyr::filter(year >= min_year) %>%
    dplyr::mutate(partnership = standardise_partnership_names(partnership)) %>%
    dplyr::mutate(
      numerator = .data$total_pc,
      denominator = .data$total_pc + .data$ch_res + .data$cc_census,
      rate = .data$percentage * 100,
      .keep = "unused"
    )
}
