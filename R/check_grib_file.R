#' Check GRIB file
#'
#' @param file file path; to file to be checked
#' @param hour integer; hour of forecast cycle
#'
#' @return
#' @noRd

check_grib_file <- function(file, hour) {
  if(!file.exists(file)) {
    warning("File ", file, " does not exist.")
    return(FALSE)
  }
  if(file.info(file)$size == 0) {
    unlink(file, force = TRUE)
    return(FALSE)
  } else {

    grib <- rgdal::readGDAL(file, silent = TRUE)
    if(hour == "000") {
      if(length(grib@data) != 5) {
        warning("Bad file: ", file, "\n Should be 5 fields but has ", length(grib@data), " fields")
        unlink(file, force = TRUE)
        return(FALSE)
      } else if(length(grib@data) == 5) {
        return(TRUE)
      }
    } else {
      if(length(grib@data) != 9) {
        warning("Bad file: ", file, "\n Should be 9 fields but has ", length(grib@data), " fields")
        unlink(file, force = TRUE)
        return(FALSE)
      } else if(length(grib@data) == 9) {
        return(TRUE)
      }
    }
  }
}
