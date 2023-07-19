#' Prepare the SLF Episode File for use with NI12, NI13, and NI20
#'
#' @param fin_year The financial year for extract
#' @param ni_version Boolean, whether to use specially produced NI version of SLF or not
#'
#' @return A filtered SLF aggregated to CIJ level
#' @export
prepare_slf_episode_file <- function(year, ni_version = FALSE) {
  # cost_names <- tolower(paste0(month.abb, "_cost"))
  # bedday_names <- tolower(paste0(month.abb, "_beddays"))

  slf_dir <- fs::path("/", "conf", "sourcedev", "Source_Linkage_File_Updates", year)

  file_name <- stringr::str_glue("source-episode-file-{year}{ifelse(ni_version, '_ni_version', '')}.parquet")

  slf_path <- fs::path(slf_dir, file_name)

  slf <- arrow::read_parquet(slf_path,
    col_select = c(
      "year", "chi", "cij_marker", "cij_pattype", "cij_admtype", "age", "recid", "smrtype",
      "record_keydate1", "record_keydate2",
      "lca", "location", "datazone2011",
      "yearstay"
      # , cost_names, bedday_names
    )
  ) %>%
    dplyr::mutate(cij_pattype = dplyr::if_else(.data$cij_admtype == 18, "Non-Elective", .data$cij_pattype)) %>%
    dplyr::filter(!is_missing(.data$chi) &
      !is_missing(.data$datazone2011) &
      .data$recid %in% c("01B", "04B", "GLS") &
      .data$age >= 18 &
      !(.data$location %in% c("T113H", "S206H", "G106H")) &
      (.data$cij_pattype == "Non-Elective" & .data$smrtype %in% c("Acute-IP", "Psych-IP", "GLS-IP"))) %>%
    data.table::as.data.table() %>%
    # Aggregate to CIJ level
    dplyr::group_by(year, chi, cij_marker) %>%
    dplyr::summarise(
      record_keydate1 = min(record_keydate1),
      record_keydate2 = max(record_keydate2),
      dplyr::across(c("lca", "datazone2011"), dplyr::last),
      # dplyr::across(c(cost_names, bedday_names), ~ sum(.x, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    dplyr::collect()

  return(slf)
}
