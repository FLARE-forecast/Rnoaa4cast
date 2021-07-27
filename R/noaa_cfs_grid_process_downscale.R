
#' Extract and temporally downscale points from downloaded grid files
#'
#' @param lat_list
#' @param lon_list
#' @param site_list
#' @param downscale
#' @param overwrite
#' @param model_name
#' @param model_name_ds
#' @param model_name_raw
#' @param num_cores
#' @param output_directory
#'
#' @return
#' @export
#'
#' @examples
#'
noaa_gefs_grid_process_downscale <- function(lat_list,
                                             lon_list,
                                             site_list,
                                             downscale,
                                             debias = FALSE,
                                             overwrite,
                                             model_name,
                                             model_name_ds,
                                             model_name_ds_debias,
                                             model_name_raw,
                                             debias_coefficients = NULL,
                                             num_cores,
                                             output_directory,
                                             reprocess = FALSE,
                                             write_intermediate_ncdf = TRUE){

  extract_sites <- function(site_list, lat_list, lon_list, working_directory){

    files_split <- stringr::str_split(list.files(working_directory),pattern = "[.]", simplify = TRUE)

    forecasts <- tibble(file_name = list.files(working_directory, full.names = TRUE),
                        year = stringr::str_sub(files_split[,1], 5, 8),
                        month = stringr::str_sub(files_split[,1], 9, 10),
                        day = stringr::str_sub(files_split[,1], 11, 12),
                        hour = stringr::str_sub(files_split[,1], 13, 14),
                        forecast_timestep = files_split[,1]) %>%
      mutate(time = lubridate::make_datetime(year = as.numeric(year), month = as.numeric(month), day = as.numeric(day), hour = as.numeric(hour),tz = "UTC")) %>%
      arrange(time) %>%
      mutate(hours_in_future = as.numeric((time - min(time))/60))


    site_length <- length(site_list)
    tmp2m <- array(NA, dim = c(site_length, nrow(forecasts)))
    spfc2m <- array(NA, dim = c(site_length, nrow(forecasts)))
    ugrd10m <- array(NA, dim = c(site_length,nrow(forecasts)))
    vgrd10m <- array(NA, dim = c(site_length, nrow(forecasts)))
    pressfc <- array(NA, dim = c(site_length, nrow(forecasts)))
    prate <- array(NA, dim = c(site_length, nrow(forecasts)))
    dlwrfsfc <- array(NA, dim = c(site_length, nrow(forecasts)))
    dswrfsfc <- array(NA, dim = c(site_length, nrow(forecasts)))


    for(d in 1:nrow(forecasts)){


    lats <- round(lat_list)
    lons <- round(lon_list)

      if(file.exists(forecasts$file_name[d])){
        if(file.info(forecasts$file_name[d])$size != 0){
          grib <- rgdal::readGDAL(forecasts$file_name[d], silent = TRUE)

          lat_lon <- sp::coordinates(grib)

          for(s in 1:length(site_list)){

            min_lat <- which.min(lat_lon[,2] - lat_list[s])
            min_lon <- which.min(lat_lon[,1] - lon_list[s])
            grid_lat <- lat_lon[min_lat[1],2]
            grid_lon <- lat_lon[min_lon[1],1]
            index <- which(lat_lon[,2] == grid_lat & lat_lon[,1] == grid_lon)

            if(length(index) > 0){

              pressfc[s, d]  <- grib$band9[index]
              tmp2m[s, d] <- grib$band7[index]
              spfc2m[s, d]  <- grib$band8[index]
              ugrd10m[s, d]  <- grib$band5[index]
              vgrd10m[s, d]  <- grib$band6[index]
              prate[s, d]  <- grib$band4[index]
              dswrfsfc[s, d]  <- grib$band3[index]
              dlwrfsfc[s, d]  <- grib$band2[index]

            }else{
              pressfc[s, d]  <- NA
              tmp2m[s, d] <- NA
              spfc2m[s, d]  <- NA
              ugrd10m[s, d]  <- NA
              vgrd10m[s, d]  <-NA
              prate[s, d]  <- NA
              dswrfsfc[s, d]  <- NA
              dlwrfsfc[s, d]  <- NA
            }
          }
        }
      }
    }

    return(list(tmp2m = tmp2m,
                pressfc = pressfc,
                spfc2m = spfc2m,
                dlwrfsfc = dlwrfsfc,
                dswrfsfc = dswrfsfc,
                ugrd10m = ugrd10m,
                vgrd10m = vgrd10m,
                prate = prate,
                hours_in_future = forecasts$hours_in_future,
                starting_time = forecasts$time[1]
                ))
  }

  noaa_var_names <- c("tmp2m", "pressfc", "spfc2m", "dlwrfsfc",
                      "dswrfsfc", "prate",
                      "ugrd10m", "vgrd10m")


  model_dir <- file.path(output_directory, model_name)
  model_name_raw_dir <- file.path(output_directory, model_name_raw)

  curr_time <- lubridate::with_tz(Sys.time(), tzone = "UTC")
  curr_date <- lubridate::as_date(curr_time)
  potential_dates <- seq(curr_date - lubridate::days(6), curr_date, by = "1 day")

  if(reprocess){
    potential_dates <- lubridate::as_date(list.dirs(model_name_raw_dir, recursive = FALSE, full.names = FALSE))
  }
  #Remove dates before the new GEFS system
  potential_dates <- potential_dates[which(potential_dates > lubridate::as_date("2020-09-23"))]

  for(k in 1:length(potential_dates)){



    forecast_date <- lubridate::as_date(potential_dates[k])
    forecast_hours <- c(0,6,12,18)



    for(j in 1:length(forecast_hours)){
      cycle <- forecast_hours[j]
      curr_forecast_time <- forecast_date + lubridate::hours(cycle)

      message(paste0("Processing forecast time: ", curr_forecast_time))

      raw_files <- list.files(file.path(model_name_raw_dir,forecast_date,cycle))
      hours_present <- length(raw_files)

      all_downloaded <- FALSE

      if(hours_present == 781)
        all_downloaded <- TRUE
    }

      missing_files <- FALSE
      for(site_index in 1:length(site_list)){
        num_files <- length(list.files(file.path(model_dir, site_list[site_index], forecast_date,cycle)))
        if(num_files != 1){
          missing_files <- TRUE
        }
      }

      if(overwrite){
        missing_files <- TRUE
      }

     # if(write_intermediate_ncdf == TRUE & cycle == "00"){
    #    existing_ncfiles <- list.files(file.path(model_dir, site_list[1], forecast_date,cycle))
    #    if(length(existing_ncfiles) == 31){
    #      split_filenames <- stringr::str_split(existing_ncfiles,pattern = "_")
    #      df <- as.data.frame(matrix(unlist(split_filenames), nrow = length(existing_ncfiles), byrow = TRUE))
    #      count_unique <- tibble(x = factor(df[, ncol(df) - 1])) %>% group_by(x) %>% summarize(count = n())
    #      if((nrow(count_unique) == 2 & count_unique[nrow(count_unique),2] != 30) | nrow(count_unique) != 2){
    #        missing_files <- TRUE
    #      }
    #    }
    #  }

      if(all_downloaded & missing_files){

        #Remove existing files and overwrite
        unlink(list.files(file.path(model_dir, site_list[site_index], forecast_date,cycle)))


        output <- extract_sites(site_list, lat_list, lon_list, working_directory = file.path(model_name_raw_dir,forecast_date,cycle))

        forecast_times <- lubridate::as_datetime(forecast_date) + lubridate::hours(as.numeric(cycle)) + lubridate::hours(as.numeric(hours_char))

        for(site_index in 1:length(site_list)){



          message(paste0("Processing site: ", site_list[site_index]))

          #Convert negetive longitudes to degrees east
          if(lon_list[site_index] < 0){
            lon_east <- 360 + lon_list[site_index]
          }else{
            lon_east <- lon_list[site_index]
          }

          model_site_date_hour_dir <- file.path(model_dir, site_list[site_index], forecast_date,cycle)

          if(!dir.exists(model_site_date_hour_dir)){
            dir.create(model_site_date_hour_dir, recursive=TRUE, showWarnings = FALSE)
          }else{
            unlink(list.files(model_site_date_hour_dir, full.names = TRUE))
          }

          if(downscale){
            modelds_site_date_hour_dir <- file.path(output_directory,model_name_ds,site_list[site_index], forecast_date,cycle)
            if(!dir.exists(modelds_site_date_hour_dir)){
              dir.create(modelds_site_date_hour_dir, recursive=TRUE, showWarnings = FALSE)
            }else{
              unlink(list.files(modelds_site_date_hour_dir, full.names = TRUE))
            }
          }

          if(debias){
            modelds_debias_site_date_hour_dir <- file.path(output_directory,model_name_ds_debias,site_list[site_index], forecast_date,cycle)
            if(!dir.exists(modelds_debias_site_date_hour_dir)){
              dir.create(modelds_debias_site_date_hour_dir, recursive=TRUE, showWarnings = FALSE)
            }else{
              unlink(list.files(modelds_debias_site_date_hour_dir, full.names = TRUE))
            }
          }


          noaa_data <- list()

          for(v in 1:length(noaa_var_names)){

            value <- NULL
            ensembles <- NULL
            forecast.date <- NULL

            noaa_data[v] <- NULL



            for(ens in 1:31){
              curr_ens <- output[[ens]]

              #             if(length(which(!is.na(curr_ens[[noaa_var_names[v]]][site_index, ]))) == 0){
              #                message(paste0("skipping site: ",site_list[site_index], "because not in gridded raw data"))
              #                next
              #              }
              value <- tryCatch({
                c(value, curr_ens[[noaa_var_names[v]]][site_index, ])
              },
              error=function(e) {
                message("curr_ens:")
                message(curr_ens)
                message(e)
                stop()
              }
              )

              ensembles <- c(ensembles, rep(ens, length(curr_ens[[noaa_var_names[v]]][site_index, ])))
              forecast.date <- c(forecast.date, forecast_times)
            }
            noaa_data[[v]] <- list(value = value,
                                   ensembles = ensembles,
                                   forecast.date = lubridate::as_datetime(forecast.date))

          }

          #These are the cf standard names
          cf_var_names <- c("air_temperature", "air_pressure", "relative_humidity", "surface_downwelling_longwave_flux_in_air",
                            "surface_downwelling_shortwave_flux_in_air", "precipitation_flux", "eastward_wind", "northward_wind","cloud_area_fraction")

          #Replace "eastward_wind" and "northward_wind" with "wind_speed"
          cf_var_names1 <- c("air_temperature", "air_pressure", "relative_humidity", "surface_downwelling_longwave_flux_in_air",
                             "surface_downwelling_shortwave_flux_in_air", "precipitation_flux","specific_humidity", "cloud_area_fraction","wind_speed")

          cf_var_units1 <- c("K", "Pa", "1", "Wm-2", "Wm-2", "kgm-2s-1", "1", "1", "ms-1")  #Negative numbers indicate negative exponents

          names(noaa_data) <- cf_var_names

          specific_humidity <- rep(NA, length(noaa_data$relative_humidity$value))

          noaa_data$relative_humidity$value <- noaa_data$relative_humidity$value / 100

          noaa_data$air_temperature$value <- noaa_data$air_temperature$value + 273.15

          specific_humidity[which(!is.na(noaa_data$relative_humidity$value))] <- rh2qair(rh = noaa_data$relative_humidity$value[which(!is.na(noaa_data$relative_humidity$value))],
                                                                                         T = noaa_data$air_temperature$value[which(!is.na(noaa_data$relative_humidity$value))],
                                                                                         press = noaa_data$air_pressure$value[which(!is.na(noaa_data$relative_humidity$value))])


          #Calculate wind speed from east and north components
          wind_speed <- sqrt(noaa_data$eastward_wind$value^2 + noaa_data$northward_wind$value^2)

          forecast_noaa <- tibble::tibble(time = noaa_data$air_temperature$forecast.date,
                                          NOAA.member = noaa_data$air_temperature$ensembles,
                                          air_temperature = noaa_data$air_temperature$value,
                                          air_pressure= noaa_data$air_pressure$value,
                                          relative_humidity = noaa_data$relative_humidity$value,
                                          surface_downwelling_longwave_flux_in_air = noaa_data$surface_downwelling_longwave_flux_in_air$value,
                                          surface_downwelling_shortwave_flux_in_air = noaa_data$surface_downwelling_shortwave_flux_in_air$value,
                                          precipitation_flux = noaa_data$precipitation_flux$value,
                                          specific_humidity = specific_humidity,
                                          cloud_area_fraction = noaa_data$cloud_area_fraction$value,
                                          wind_speed = wind_speed)

          forecast_noaa$cloud_area_fraction <- forecast_noaa$cloud_area_fraction / 100 #Convert from % to proportion

          # Convert the 3 hr precip rate to per second.
          forecast_noaa$precipitation_flux <- forecast_noaa$precipitation_flux / (60 * 60 * 6)

          for (ens in 1:31) { # i is the ensemble number


            #Turn the ensemble number into a string
            if(ens-1< 10){
              ens_name <- paste0("0",ens-1)
            }else{
              ens_name <- ens - 1
            }

            forecast_noaa_ens <- forecast_noaa %>%
              dplyr::filter(NOAA.member == ens) %>%
              dplyr::filter(!is.na(air_temperature))

            end_date <- forecast_noaa_ens %>%
              dplyr::summarise(max_time = max(time))

            identifier <- paste(model_name, site_list[site_index], format(forecast_date, "%Y-%m-%dT%H"),
                                format(end_date$max_time, "%Y-%m-%dT%H"), sep="_")

            fname <- paste0(identifier,"_ens",ens_name,".nc")
            output_file <- file.path(model_site_date_hour_dir,fname)

            #Write netCDF
            noaaGEFSpoint::write_noaa_gefs_netcdf(df = forecast_noaa_ens,ens, lat = lat_list[site_index], lon = lon_east, cf_units = cf_var_units1, output_file = output_file, overwrite = TRUE)

            if(downscale){

              identifier_ds <- paste(model_name_ds, site_list[site_index], format(forecast_date, "%Y-%m-%dT%H"),
                                     format(end_date$max_time, "%Y-%m-%dT%H"), sep="_")

              fname_ds <- file.path(modelds_site_date_hour_dir, paste0(identifier_ds,"_ens",ens_name,".nc"))

              #Run downscaling
              noaaGEFSpoint::temporal_downscale(input_file = output_file, output_file = fname_ds, overwrite = TRUE, hr = 1)

              if(debias){


                identifier_ds_debias <- paste(model_name_ds_debias, site_list[site_index], format(forecast_date, "%Y-%m-%dT%H"),
                                              format(end_date$max_time, "%Y-%m-%dT%H"), sep="_")

                fname_ds <- file.path(modelds_debias_site_date_hour_dir, paste0(identifier_ds_debias,"_ens",ens_name,".nc"))

                spatial_downscale_coeff <- list(AirTemp = c(debias_coefficients[site_index]$temp_intercept, debias_coefficients[site_index]$temp_slope),
                                                ShortWave = c(debias_coefficients[site_index]$sw_intercept, debias_coefficients[site_index]$sw_slope),
                                                LongWave = c(debias_ccoefficients[site_index]$lw_intercept, debias_coefficients[site_index]$lw_slope),
                                                WindSpeed = c(debias_coefficients[site_index]$wind_intercept, debias_coefficients[site_index]$wind_slope))

                noaaGEFSpoint::debias_met_forecast(input_file = output_file, output_file = fname_ds, spatial_downscale_coeff, overwrite = TRUE)

              }
            }
          }
        }
      }
    }
  }
}
