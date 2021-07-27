
#' Download gridded forecast in the box bounded by the latitude and longitude list
#'
#' @param lat_list
#' @param lon_list
#' @param forecast_time
#' @param forecast_date
#' @param model_name_raw
#' @param num_cores
#' @param output_directory
#'
#' @return
#' @export
#'
#' @examples
noaa_gefs_grid_download <- function(lat_list, lon_list, forecast_time, forecast_date ,model_name_raw, output_directory) {


  download_grid <- function(forecasted_date, cycle, location, directory, vars,working_directory){
    curr_year <- lubridate::year(forecasted_date)
    curr_month <- lubridate::month(forecasted_date)
    if(curr_month < 10) curr_month <- paste0("0",curr_month)
    curr_day <- lubridate::day(forecasted_date)
    if(curr_day < 10) curr_day <- paste0("0",curr_day)
    start_date <- paste0(curr_year,curr_month,curr_day)
    directory <- paste0("&dir=%2Fcfs.",start_date,"%2F",cycle,"%2F6hrly_grib_01")

    date_vector <- seq(forecasted_date, forecasted_date +lubridate::days(196), "1 day")
    forecast_hours <- c("00", "06", "12", "18")

    if(cycle == "00"){
      start_index = 1
      end_index = 4
    }
    if(cycle == "06"){
      start_index = 2
      end_index = 1
    }
    if(cycle == "12"){
      start_index = 3
      end_index = 2
    }
    if(cycle == "18"){
      start_index = 4
      end_index = 3
    }


    for(ii in 1:length(date_vector)){

      curr_year <- lubridate::year(date_vector[ii])
      curr_month <- lubridate::month(date_vector[ii])
      if(curr_month < 10) curr_month <- paste0("0",curr_month)
      curr_day <- lubridate::day(date_vector[ii])
      if(curr_day < 10) curr_day <- paste0("0",curr_day)
      curr_date <- paste0(curr_year,curr_month,curr_day)


      for(jj in 1:length(forecast_hours)){

        if((ii == 1 & jj >= start_index) | (ii > 1 & ii < length(date_vector)) | (ii == length(date_vector) & jj <= end_index)){

          file_name <- paste0("flxf",curr_date, forecast_hours[jj],".01.",start_date, cycle,".grb2")

          #flxf2021072600.01.2021072600.grb2

          destfile <- paste0(working_directory,"/", file_name,".neon.grib")

          if(file.exists(destfile)){

            fsz <- file.info(destfile)$size
            gribf <- file(destfile, "rb")
            fsz4 <- fsz-4
            seek(gribf,where = fsz4,origin = "start")
            last4 <- readBin(gribf,"raw",4)
            if(as.integer(last4[1])==55 & as.integer(last4[2])==55 & as.integer(last4[3])==55 & as.integer(last4[4])==55) {
              download_file <- FALSE
            } else {
              download_file <- TRUE
            }
            close(gribf)
          }else{
            download_file <- TRUE
          }

          if(download_file){
            download_tries <- 1
            download_failed <- TRUE
            while(download_failed & download_tries < 2){
              Sys.sleep(0.5)
              download_failed <- FALSE
              print(paste0(base_filename1, file_name, vars, location, directory))
              out <- tryCatch(download.file(paste0(base_filename1, file_name, vars, location, directory),
                                            destfile = destfile, quiet = TRUE),
                              error = function(e){
                                warning(paste(e$message, "skipping", file_name),
                                        call. = FALSE)
                                return(NA)
                              },
                              finally = NULL)

              if(is.na(out) | file.info(destfile)$size == 0){
                download_failed <- TRUE
              }else{

                #band 1: tmp (surface) - not used but is automatically included
                #band 2: longwave (surface)
                #band 3: shortwave (surface)
                #band 4: rain (PRATE)
                #band 5: uwind (10 m)
                #band 6: v wind (10 m)
                #band 7: Tmp (2m) used
                #band 8: specific humidity (2 m)
                #band 9: pressure (surface)


                grib <- rgdal::readGDAL(destfile, silent = TRUE)
                if(is.null(grib$band1) | is.null(grib$band5) | is.null(grib$band6) | is.null(grib$band7) | is.null(grib$band8) | is.null(grib$band5)){
                  if(ii >= 1 | (ii == 1 & jj > 1) & (is.null(grib$band2) | is.null(grib$band3) | is.null(grib$band4))){
                    unlink(destfile)
                    download_failed <- TRUE
                  }else{
                    unlink(destfile)
                    download_failed <- TRUE
                  }
                }
              }
              download_tries <- download_tries + 1
            }
          }
        }
      }
    }
  }

  if(length(which(lon_list > 180)) > 0){
    stop("longitude values need to be between -180 and 180")
  }

  forecast_date <- lubridate::as_date(forecast_date)

  model_dir <- file.path(output_directory, model_name_raw)

  curr_time <- lubridate::with_tz(Sys.time(), tzone = "UTC")
  curr_date <- lubridate::as_date(curr_time)
  potential_dates <- seq(curr_date - lubridate::days(6), curr_date, by = "1 day")

  potential_dates <- lubridate::as_date(potential_dates)

  #Remove dates before the new GEFS system
  #potential_dates <- potential_dates[which(potential_dates > lubridate::as_date("2020-09-23"))]

  location <- paste0("&subregion=&leftlon=",
                     floor(min(lon_list)),
                     "&rightlon=",
                     ceiling(max(lon_list)),
                     "&toplat=",
                     ceiling(max(lat_list)),
                     "&bottomlat=",
                     floor(min(lat_list)))


  base_filename1 <- "https://nomads.ncep.noaa.gov/cgi-bin/filter_cfs_flx.pl?file="
  vars <- "&lev_10_m_above_ground=on&lev_2_m_above_ground=on&lev_entire_atmosphere_%5C%28considered_as_a_single_layer%5C%29=on&lev_surface=on&var_DLWRF=on&var_DSWRF=on&var_PRATE=on&var_PRES=on&var_SPFH=on&var_TMP=on&var_UGRD=on&var_VGRD=on"

  for(i in 1:length(potential_dates)){

    forecasted_date <- lubridate::as_date(potential_dates[i])
    if(i == length(potential_dates)){
      forecast_hours <- as.numeric(potential_cycle)
    }else{
      forecast_hours <- c(0,6,12,18)
    }

    if(is.na(forecast_date) | forecasted_date %in% forecast_date){

      for(j in 1:length(forecast_hours)){

        if(is.na(forecast_time) | forecast_hours[j] %in% as.numeric(forecast_time)){
          cycle <- forecast_hours[j]

          if(cycle < 10) cycle <- paste0("0",cycle)

          model_date_hour_dir <- file.path(model_dir,forecasted_date,cycle)
          if(!dir.exists(model_date_hour_dir)){
            dir.create(model_date_hour_dir, recursive=TRUE, showWarnings = FALSE)
          }

          new_download <- TRUE

          if(new_download){

            print(paste("Downloading", forecasted_date, cycle))

            download_grid(forecasted_date = forecasted_date, cycle, location, directory, vars,working_directory = model_date_hour_dir)
          }else{
            print(paste("Existing", forecasted_date, cycle))
          }
        }
      }
    }
  }
}
