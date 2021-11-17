#' Check GRIB file
#'
#' @param file file path; to file to be checked
#' @param hour integer; hour of forecast cycle
#'
#' @importFrom rgdal readGDAL
#'
#' @return
#' @noRd

check_grib_file <- function(file, hour) {

  if(!file.exists(file)) {
    warning("File ", file, " does not exist.")
    return(FALSE)
  }
  if(file.info(file)$size == 0) {
    return(FALSE)
  } else {

    grib <- rgdal::readGDAL(file, silent = TRUE)

    fsz <- file.info(file)$size
    gribf <- file(file, "rb")
    fsz4 <- fsz-4
    seek(gribf,where = fsz4,origin = "start")
    last4 <- readBin(gribf,"raw",4)
    if(!(as.integer(last4[1])==55 & as.integer(last4[2])==55 & as.integer(last4[3])==55 & as.integer(last4[4])==55)) {
           close(gribf)
      return(FALSE)
    } else {
      if(hour == "000") {
        if(length(grib@data) != 5) {
          warning("Bad file: ", file, "\n Should be 5 fields but has ", length(grib@data), " fields")
          close(gribf)
          return(FALSE)
        } else if(length(grib@data) == 5) {
          close(gribf)
          return(TRUE)
        }
      } else {
        if(length(grib@data) != 9) {
          warning("Bad file: ", file, "\n Should be 9 fields but has ", length(grib@data), " fields")
          close(gribf)
          return(FALSE)
        } else if(length(grib@data) == 9) {
          close(gribf)
          return(TRUE)
        }
      }
    }
  }
}
