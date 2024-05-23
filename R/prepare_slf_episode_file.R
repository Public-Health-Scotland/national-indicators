#' Prepare the SLF Episode File for use with NI12, NI13, and NI20
#'
#' @param year The financial year for extract
#'
#' @return A filtered SLF aggregated to CIJ level
#' @export
prepare_slf_episode_file <- function(year) {
  slf_dir <- fs::path("/", "conf", "sourcedev", "Source_Linkage_File_Updates", year)

  file_name <- stringr::str_glue("source-episode-file-{year}.parquet")

  slf_path <- fs::path(slf_dir, file_name)

  slf <- arrow::read_parquet(slf_path,
    col_select = c(
      "year", "anon_chi", "cij_marker", "cij_pattype", "cij_admtype", "age", "recid", "smrtype",
      "record_keydate1", "record_keydate2",
      "lca", "location", "datazone2011",
      "yearstay"
    )
  ) %>%
    dplyr::mutate(cij_pattype = dplyr::if_else(.data$cij_admtype == 18, "Non-Elective", .data$cij_pattype)) %>%
    dplyr::filter(!is_missing(.data$anon_chi) &
      !is_missing(.data$datazone2011) &
      .data$recid %in% c("01B", "04B", "GLS") &
      .data$age >= 18 &
      !(.data$location %in% c("T113H", "S206H", "G106H")) &
      (.data$cij_pattype == "Non-Elective" & .data$smrtype %in% c("Acute-IP", "Psych-IP", "GLS-IP"))) %>%
    data.table::as.data.table()

  slf <- slf[, .(
    record_keydate1 = min(record_keydate1),
    record_keydate2 = max(record_keydate2),
    lca = last(lca),
    datazone2011 = last(datazone2011)
  ),
  by = .(year, anon_chi, cij_marker)
  ]

  slf <- tibble::as_tibble(slf)

  return(slf)
}
