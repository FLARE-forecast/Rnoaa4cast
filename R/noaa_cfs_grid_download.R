#' @title Download NOAA CFS Gridded Forecast
#'
#' @description Download a NOAA Climate (Coupled) Forecast System (CFS) gridded forecast for a
#' rectangular region with specified latitude and longitude bounds.
#'
#' @param lat_list Vector or range of latitudes to be downloaded (see details).
#' @param lon_list Vector or range of longitudes to be downloaded (see details).
#' @param forecast_time The 'hour' of the requested forecast, one of "00", "06", "12", or "18", see
#' details.  ((If omitted all times will be downloaded.))
#' @param forecast_date The date, or coercible string, of the requested forecast. ((Defaults to the
#' most recent date.))
#' @param model_name_raw A string with the model name used as the root of the downloaded directory
#' tree.
#' @param output_directory Path: directory where the forecast output and logs will be saved.
#' @param grid_name A short grid name used in directory and file name generation.
#' @param s3_mode Logical: save the forecast to a Amazon S3 bucket rather than locally.
#' @param bucket If s3_mode = TRUE, the S3 bucket name to save to.
#' @details
#' @section Coordinates
#' ((Copy from noaa_gefs_download_downscale))
#'
#' @return
#' @export
#'
#' @examples

#JMR_NOTES:
# - CFS is only run once a day so why is forecast_time needed?
# - num_cores has been deprecated.
# - Calling code expects forecast_time and forecast_date to accept NA. This looks OK for
#forecast_time but will break with forecast_date. Fix and make NA default?
# - Need to add checking of parameters.
# - Add a default for model_name_raw?

noaa_cfs_grid_download <- function(lat_list,
                                   lon_list,
                                   forecast_time,
                                   forecast_date,
                                   model_name_raw,
                                   output_directory,
                                   grid_name,
                                   s3_mode = FALSE,
                                   bucket = NULL) {

  if(length(which(lon_list > 180)) > 0){
    stop("longitude values need to be between -180 and 180")
  }

  forecast_date <- lubridate::as_date(forecast_date)

  model_dir <- file.path(output_directory, model_name_raw)

  curr_time <- lubridate::with_tz(Sys.time(), tzone = "UTC")
  curr_date <- lubridate::as_date(curr_time)

  potential_dates <- seq(curr_date - lubridate::days(6), curr_date, by = "1 day")

  potential_dates <- lubridate::as_date(potential_dates)

  potential_cycle <- c("00","06", "12", "18")


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

        if((is.na(forecast_time) | (forecast_hours[j] %in% as.numeric(forecast_time))) &
           (forecasted_date < curr_date | (forecasted_date == curr_date & lubridate::hour(curr_time) < forecast_hours[j]))){
          cycle <- forecast_hours[j]

          if(cycle < 10) cycle <- paste0("0",cycle)

          model_date_hour_dir <- file.path(model_dir,forecasted_date,cycle)
          if(!dir.exists(model_date_hour_dir)){
            dir.create(model_date_hour_dir, recursive=TRUE, showWarnings = FALSE)
          }

          if(s3_mode){
            s3_objects <- aws.s3::get_bucket(bucket = bucket, prefix = model_date_hour_dir, max = Inf)
            s3_list<- vapply(s3_objects, `[[`, "", "Key", USE.NAMES = FALSE)
            empty <- grepl("/$", s3_list)
            s3_list <- s3_list[!empty]
          }else{
            s3_list <- NULL
          }

          new_download <- TRUE

          if(new_download){

            print(paste("Downloading", forecasted_date, cycle))

            curr_year <- lubridate::year(forecasted_date)
            curr_month <- lubridate::month(forecasted_date)
            if(curr_month < 10) curr_month <- paste0("0",curr_month)
            curr_day <- lubridate::day(forecasted_date)
            if(curr_day < 10) curr_day <- paste0("0",curr_day)
            start_date <- paste0(curr_year,curr_month,curr_day)
            directory <- paste0("&dir=%2Fcfs.",start_date,"%2F",cycle,"%2F6hrly_grib_01")

            date_vector <- seq(forecasted_date, forecasted_date + lubridate::days(217), "1 day")
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

                  destfile <- paste0(working_directory,"/", file_name,".",grid_name,".grib")

                  download_file <- FALSE
                  if(!s3_mode){
                    if(!file.exists(destfile)){
                      download_file <- TRUE
                    }
                  }else{
                    s3_file <- file.path(model_date_hour_dir, file_name)
                    if(!(s3_file %in% s3_list)){
                      download_file <- TRUE
                    }
                  }
                  if(download_file){
                    download_tries <- 1
                    download_failed <- TRUE
                    while(download_failed & download_tries <= 1){
                      if(download_tries > 1) {
                        Sys.sleep(5)
                      }
                      download_failed <- FALSE
                      out <- tryCatch(download.file(paste0(base_filename1, noaa_file_name, vars, location, directory),
                                                    destfile = destfile, quiet = TRUE),
                                      error = function(e){
                                        warning(paste(e$message, "skipping", file_name),
                                                call. = FALSE)
                                        return(NA)
                                      },
                                      finally = NULL)

                      download_check <- Rnoaa4cast:::check_grib_file(file = destfile, hour = curr_hours[hr])
                      if(download_check == FALSE){
                        unlink(destfile, force = TRUE)
                      }else{
                        if(s3_mode){
                          success_transfer <- aws.s3::put_object(file = destfile,
                                                                 object = file.path(model_date_hour_dir, file_name),
                                                                 bucket = bucket)
                          if(success_transfer){
                            unlink(destfile, force = TRUE)
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }else{
            print(paste("Existing", forecasted_date, cycle))
          }
        }
      }
    }
  }
}
