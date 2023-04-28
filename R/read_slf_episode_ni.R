#' Read the SLF episode file for use with the NIs
#'
#' @param fin_year Financial year in "XXYY" format
#' @param location "production" will return /hscdiip and "development" will return /sourcedev
#' @param file_type Either "fst" or "parquet" based on which is being read in
#'
#' @return The SLF episode file
#' @export
read_slf_episode_ni <- function(fin_year,
                                location = c("production", "development"),
                                file_type = c("fst", "parquet")) {
  if (location == "production") {
    path <- fs::path(glue::glue("/conf/hscdiip/01-Source-linkage-files/source-episode-file-20{fin_year}.{file_type}"))
  } else if (location == "development") {
    path <- fs::path(glue::glue("/conf/sourcedev/Source_Linkage_File_Updates/{fin_year}/source-episode-file-20{fin_year}.{file_type}"))
  }

  if (file_type == "fst") {
    slf <- fst::read_fst(path = path)
  } else if (file_type == "parquet") {
    slf <- arrow::read_parquet(file = path)
  }
}
