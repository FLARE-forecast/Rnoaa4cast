
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
noaa_gefs_grid_download <- function(lat_list,
                                    lon_list,
                                    forecast_time,
                                    forecast_date,
                                    model_name_raw,
                                    output_directory,
                                    grid_name,
                                    s3_mode = FALSE,
                                    bucket = NULL) {

  lon_list[which(lon_list > 180)] <- lon_list[which(lon_list > 180)] - 360


  if(length(which(lon_list > 180)) > 0){
    stop("longitude values need to be between -180 and 180")
  }

  forecast_date <- lubridate::as_date(forecast_date)

  curr_time <- lubridate::with_tz(Sys.time(), tzone = "UTC")
  curr_date <- lubridate::as_date(curr_time)
  #potential_dates <- seq(curr_date - lubridate::days(6), curr_date, by = "1 day")

  noaa_page <- readLines('https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/')

  potential_dates <- NULL
  for(i in 1:length(noaa_page)){
    if(stringr::str_detect(noaa_page[i], ">gefs.")){
      end <- stringr::str_locate(noaa_page[i], ">gefs.")[2]
      dates <- stringr::str_sub(noaa_page[i], start = end+1, end = end+8)
      potential_dates <- c(potential_dates, dates)
    }
  }

  last_cycle_page <- readLines(paste0('https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/gefs.', dplyr::last(potential_dates)))

  potential_cycle <- NULL
  for(i in 1:length(last_cycle_page)){
    if(stringr::str_detect(last_cycle_page[i], 'href=\"')){
      end <- stringr::str_locate(last_cycle_page[i], 'href=\"')[2]
      cycles <- stringr::str_sub(last_cycle_page[i], start = end+1, end = end+2)
      if(cycles %in% c("00","06", "12", "18")){
        potential_cycle <- c(potential_cycle, cycles)
      }
    }
  }

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

  base_filename1 <- "https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p50a.pl?file="
  vars <- "&lev_10_m_above_ground=on&lev_2_m_above_ground=on&lev_surface=on&lev_entire_atmosphere=on&var_APCP=on&var_DLWRF=on&var_DSWRF=on&var_PRES=on&var_RH=on&var_TMP=on&var_UGRD=on&var_VGRD=on&var_TCDC=on"

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

          model_date_hour_dir <- file.path(model_name_raw,forecasted_date,cycle)
          full_dir <- file.path(output_directory, model_date_hour_dir)
          if(!dir.exists(full_dir)){
            dir.create(full_dir, recursive=TRUE, showWarnings = FALSE)
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

            message(paste("Downloading", forecasted_date, cycle))

            if(cycle == "00"){
              hours <- c(seq(0, 240, 6),seq(246, 840 , 6))
            }else{
              hours <- c(seq(0, 240, 6),seq(246, 384 , 6))
            }
            hours_char <- hours
            hours_char[which(hours < 100)] <- paste0("0",hours[which(hours < 100)])
            hours_char[which(hours < 10)] <- paste0("0",hours_char[which(hours < 10)])
            curr_year <- lubridate::year(forecasted_date)
            curr_month <- lubridate::month(forecasted_date)
            if(curr_month < 10) curr_month <- paste0("0",curr_month)
            curr_day <- lubridate::day(forecasted_date)
            if(curr_day < 10) curr_day <- paste0("0",curr_day)
            curr_date <- paste0(curr_year,curr_month,curr_day)
            directory <- paste0("&dir=%2Fgefs.",curr_date,"%2F",cycle,"%2Fatmos%2Fpgrb2ap5")

            for(ens_index in 1:31){
              if(ens_index == 1){
                base_filename2 <- paste0("gec00",".t",cycle,"z.pgrb2a.0p50.f")
                curr_hours <- hours_char[hours <= 384]
              }else{
                if((ens_index-1) < 10){
                  ens_name <- paste0("0",ens_index - 1)
                }else{
                  ens_name <- as.character(ens_index -1)
                }
                base_filename2 <- paste0("gep",ens_name,".t",cycle,"z.pgrb2a.0p50.f")
                curr_hours <- hours_char
              }

              log_file <- file.path(output_directory, "download.log")

              for(hr in 1:length(curr_hours)){
                file_name <- paste0(base_filename2, curr_hours[hr], ".",grid_name,".grib")
                noaa_file_name <- paste0(base_filename2, curr_hours[hr])
                destfile <- file.path(output_directory, model_date_hour_dir, file_name)

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
                        print(success_transfer)
                        if(success_transfer){
                          unlink(destfile, force = TRUE)
                        }
                      }
                    }
                  }
                }
              }
            }
          }else{
            message(paste("Existing", forecasted_date, cycle))
          }
        }
      }
    }
  }
}
