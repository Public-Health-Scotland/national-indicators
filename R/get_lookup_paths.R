#' Lookups Directory Path
#'
#' @description Get the path to the lookups directory
#'
#' @return the Lookups directory path as a [fs::path]
#' @export
get_lookups_dir <- function() {
  fs::path("/conf/linkage/output/lookups/Unicode")
}

#' Scottish Postcode Directory File Path
#'
#' @description Get the path to the centrally held Scottish Postcode Directory
#' (SPD) file.
#'
#' @param file_name (optional) the file name of the SPD, if not supplied it will
#' try to return the latest file automatically (using [find_latest_file()])
#' @param ext The extension (type of the file) - optional
#'
#' @return An [fs::path()] to the Scottish Postcode Directory
#' @export
get_spd_path <- function(file_name = NULL, ext = "rds") {
  spd_dir <-
    fs::path(
      get_lookups_dir(),
      "Geography",
      "Scottish Postcode Directory"
    )
  
  spd_path <- get_file_path(
    directory = spd_dir,
    file_name = file_name,
    ext = ext,
    file_name_regexp = glue::glue("Scottish_Postcode_Directory_.+?\\.{ext}")
  )
  
  return(spd_path)
}

#' Locality File Path
#'
#' @description Get the path to the centrally held HSCP Localities file.
#'
#' @param file_name (optional) the file name of the Localities files, if not
#' supplied it will try to return the latest file automatically (using
#' [find_latest_file()])
#' @param ext The extension (type of the file) - optional
#'
#' @return An [fs::path()] to the Scottish Postcode Directory
#' @export
get_locality_path <- function(file_name = NULL, ext = "rds") {
  locality_dir <-
    fs::path(get_lookups_dir(), "Geography", "HSCP Locality")
  
  locality_path <- get_file_path(
    directory = locality_dir,
    file_name = file_name,
    ext = ext,
    file_name_regexp = glue::glue("HSCP Localities_DZ11_Lookup_\\d+?\\.{ext}")
  )
  
  return(locality_path)
}

#' SIMD File Path
#'
#' @description Get the path to the centrally held Scottish Index of Multiple
#' Deprivation (SIMD) file.
#'
#' @param file_name (optional) the file name of the SIMD file, if not supplied
#' it will try to return the latest file automatically (using
#' [find_latest_file()])
#' @param ext The extension (type of the file) - optional
#'
#' @return An [fs::path()] to the SIMD file
#' @export
get_simd_path <- function(file_name = NULL, ext = "rds") {
  simd_dir <-
    fs::path(get_lookups_dir(), "Deprivation")
  
  simd_path <- get_file_path(
    directory = simd_dir,
    file_name = file_name,
    ext = ext,
    file_name_regexp = glue::glue("postcode_\\d\\d\\d\\d_\\d_simd\\d\\d\\d\\d.*?\\.{ext}")
  )
  
  return(simd_path)
}