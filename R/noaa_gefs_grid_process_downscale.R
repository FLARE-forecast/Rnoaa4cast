
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
                                             write_intermediate_ncdf = TRUE,
                                             process_specific_date = NA,
                                             process_specific_cycle = NA){

  extract_sites <- function(ens_index, hours_char, hours, cycle, site_list, lat_list, lon_list, working_directory, process_specific_date){

    site_length <- length(site_list)
    tmp2m <- array(NA, dim = c(site_length, length(hours_char)))
    rh2m <- array(NA, dim = c(site_length, length(hours_char)))
    ugrd10m <- array(NA, dim = c(site_length,length(hours_char)))
    vgrd10m <- array(NA, dim = c(site_length, length(hours_char)))
    pressfc <- array(NA, dim = c(site_length, length(hours_char)))
    apcpsfc <- array(NA, dim = c(site_length, length(hours_char)))
    tcdcclm <- array(NA, dim = c(site_length, length(hours_char)))
    dlwrfsfc <- array(NA, dim = c(site_length, length(hours_char)))
    dswrfsfc <- array(NA, dim = c(site_length, length(hours_char)))

    if(ens_index == 1){
      base_filename2 <- paste0("gec00",".t",cycle,"z.pgrb2a.0p50.f")
    }else{
      if(ens_index-1 < 10){
        ens_name <- paste0("0",ens_index-1)
      }else{
        ens_name <- as.character(ens_index-1)
      }
      base_filename2 <- paste0("gep",ens_name,".t",cycle,"z.pgrb2a.0p50.f")
    }

    lats <- round(lat_list/.5)*.5
    lons <- round(lon_list/.5)*.5
    curr_hours <- hours_char

    for(hr in 1:length(curr_hours)){
      file_name <- paste0(working_directory,"/", base_filename2, curr_hours[hr],".neon.grib")

      if(file.exists(file_name)){
        if(file.info(file_name)$size != 0){
          grib <- rgdal::readGDAL(file_name, silent = TRUE)
          #download_error <- FALSE
          #if(is.null(grib$band1) | is.null(grib$band2) | is.null(grib$band3) | is.null(grib$band4) | is.null(grib$band5)){
          #  download_error <- TRUE
          #  unlink(file_name)
          #}
          lat_lon <- sp::coordinates(grib)

          for(s in 1:length(site_list)){

            index <- which(lat_lon[,2] == lats[s] & lat_lon[,1] == lons[s])

            if(length(index) > 0){

              if(is.null(grib$band1[index]) | is.null(grib$band2[index]) | is.null(grib$band3[index]) | is.null(grib$band4[index])){
                if(is.na(process_specific_date)){
                  return(list(NULL, file_name))
                }else{
                  pressfc[s, hr]  <- NA
                  tmp2m[s, hr] <- NA
                  rh2m[s, hr]  <- NA
                  ugrd10m[s, hr]  <- NA
                  vgrd10m[s, hr]  <- NA

                  if(curr_hours[hr] != "000"){
                    apcpsfc[s, hr]  <- NA
                    tcdcclm[s, hr]  <-  NA
                    dswrfsfc[s, hr]  <- NA
                    dlwrfsfc[s, hr]  <- NA
                  }
                }
              }else{
                pressfc[s, hr]  <- grib$band1[index]
                tmp2m[s, hr] <- grib$band2[index]
                rh2m[s, hr]  <- grib$band3[index]
                ugrd10m[s, hr]  <- grib$band4[index]
                vgrd10m[s, hr]  <- grib$band5[index]

                if(curr_hours[hr] != "000"){
                  apcpsfc[s, hr]  <- grib$band6[index]
                  tcdcclm[s, hr]  <-  grib$band7[index]
                  dswrfsfc[s, hr]  <- grib$band8[index]
                  dlwrfsfc[s, hr]  <- grib$band9[index]
                }
              }
            }else{
              pressfc[s, hr]  <- NA
              tmp2m[s, hr] <- NA
              rh2m[s, hr]  <- NA
              ugrd10m[s, hr]  <- NA
              vgrd10m[s, hr]  <-NA
              apcpsfc[s, hr]  <- NA
              tcdcclm[s, hr]  <-  NA
              dswrfsfc[s, hr]  <- NA
              dlwrfsfc[s, hr]  <- NA
            }
          }
        }
      }
    }

    return(list(tmp2m = tmp2m,
                pressfc = pressfc,
                rh2m = rh2m,
                dlwrfsfc = dlwrfsfc,
                dswrfsfc = dswrfsfc,
                ugrd10m = ugrd10m,
                vgrd10m = vgrd10m,
                apcpsfc = apcpsfc,
                tcdcclm = tcdcclm))
  }

  noaa_var_names <- c("tmp2m", "pressfc", "rh2m", "dlwrfsfc",
                      "dswrfsfc", "apcpsfc",
                      "ugrd10m", "vgrd10m", "tcdcclm")


  model_dir <- file.path(output_directory, model_name)
  model_name_raw_dir <- file.path(output_directory, model_name_raw)

  curr_time <- lubridate::with_tz(Sys.time(), tzone = "UTC")
  curr_date <- lubridate::as_date(curr_time)
  potential_dates <- seq(curr_date - lubridate::days(3), curr_date, by = "1 day")

  if(reprocess){
    potential_dates <- lubridate::as_date(list.dirs(model_name_raw_dir, recursive = FALSE, full.names = FALSE))
  }
  #Remove dates before the new GEFS system
  if(is.na(process_specific_date)){
    potential_dates <- potential_dates[which(potential_dates > lubridate::as_date("2020-09-23"))]
  }else{
    potential_dates <- lubridate::as_date(process_specific_date)
  }


  for(k in 1:length(potential_dates)){



    forecast_date <- lubridate::as_date(potential_dates[k])
    if(is.na(process_specific_cycle)){
      forecast_hours <- c(0,6,12,18)
    }else{
      forecast_hours <- process_specific_cycle
    }



    for(j in 1:length(forecast_hours)){
      cycle <- forecast_hours[j]
      curr_forecast_time <- forecast_date + lubridate::hours(cycle)
      if(cycle < 10) cycle <- paste0("0",cycle)
      if(cycle == "00"){
        hours <- c(seq(0, 240, 6),seq(246, 840 , 6))
      }else{
        hours <- c(seq(0, 240, 6),seq(246, 384 , 6))
      }
      hours_char <- hours
      hours_char[which(hours < 100)] <- paste0("0",hours[which(hours < 100)])
      hours_char[which(hours < 10)] <- paste0("0",hours_char[which(hours < 10)])

      message(paste0("Processing forecast time: ", curr_forecast_time))

      raw_files <- list.files(file.path(model_name_raw_dir,forecast_date,cycle))
      hours_present <- as.numeric(stringr::str_sub(raw_files, start = 25, end = 27))

      all_downloaded <- FALSE

      write_intermediate_ncdf <- TRUE

      if(cycle == "00"){
        #Sometime the 16-35 day forecast is not competed for some of the forecasts.  If over 24 hrs has passed then they won't show up.
        #Go ahead and create the netcdf files
        if(length(which(hours_present == 840)) == 30 |
           (length(which(hours_present == 384)) == 31 & curr_forecast_time + lubridate::hours(36) < curr_time) |
           (length(which(hours_present == 384)) == 31 & write_intermediate_ncdf == TRUE)){
          all_downloaded <- TRUE
        }
      }else{
        if(length(which(hours_present == 384)) == 31){
          all_downloaded <- TRUE
        }
      }

      missing_files <- FALSE
      for(site_index in 1:length(site_list)){
        num_files <- length(list.files(file.path(model_dir, site_list[site_index], forecast_date,cycle)))
        if(num_files < 31){
          missing_files <- TRUE
        }

      }

      if(overwrite){
        missing_files <- TRUE
      }

      if(write_intermediate_ncdf == TRUE & cycle == "00"){
        existing_ncfiles <- list.files(file.path(model_dir, site_list[1], forecast_date,cycle))
        if(length(existing_ncfiles) == 31){
          split_filenames <- stringr::str_split(existing_ncfiles,pattern = "_")
          df <- as.data.frame(matrix(unlist(split_filenames), nrow = length(existing_ncfiles), byrow = TRUE))
          count_unique <- tibble::tibble(x = factor(df[, ncol(df) - 1])) %>% dplyr::group_by(x) %>% dplyr::summarize(count = dplyr::n())
          if((nrow(count_unique) == 2 & count_unique[nrow(count_unique),2] != 30) | nrow(count_unique) != 2){
            missing_files <- TRUE
          }
        }
      }

      if(!is.na(process_specific_date)){
        all_downloaded <- TRUE
        missing_files <- TRUE
      }

      if(all_downloaded & missing_files){

        #Remove existing files and overwrite
        unlink(list.files(file.path(model_dir, site_list[site_index], forecast_date,cycle)))

        ens_index <- 1:31
        #Run download_downscale_site() over the site_index
        #Run download_downscale_site() over the site_index
        output <- parallel::mclapply(X = ens_index,
                                     FUN = extract_sites,
                                     hours_char = hours_char,
                                     hours = hours,
                                     cycle,
                                     site_list,
                                     lat_list,
                                     lon_list,
                                     working_directory = file.path(model_name_raw_dir,forecast_date,cycle),
                                     mc.cores = num_cores,
                                     process_specific_date = process_specific_date)
        bad_ens_member <- FALSE
        for(ens in 1:31){
          if(is.null(unlist(output[[ens]][1]))){
            bad_ens_member <- TRUE
            #unlink(unlist(output[[ens]][2]))
            message(paste0("Bad file: ", unlist(output[[ens]][2])))
          }
        }

        if(bad_ens_member){
          next()
        }

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

            start_date <- forecast_noaa_ens %>%
              dplyr::summarise(min_time = min(time))

            identifier <- paste(model_name, site_list[site_index], format(start_date$min_time, "%Y-%m-%dT%H"),
                                format(end_date$max_time, "%Y-%m-%dT%H"), sep="_")

            fname <- paste0(identifier,"_ens",ens_name,".nc")
            output_file <- file.path(model_site_date_hour_dir,fname)

            #Write netCDF
            noaaGEFSpoint::write_noaa_gefs_netcdf(df = forecast_noaa_ens,ens, lat = lat_list[site_index], lon = lon_east, cf_units = cf_var_units1, output_file = output_file, overwrite = TRUE)

            if(downscale){

              identifier_ds <- paste(model_name_ds, site_list[site_index], format(start_date$min_time, "%Y-%m-%dT%H"),
                                     format(end_date$max_time, "%Y-%m-%dT%H"), sep="_")

              fname_ds <- file.path(modelds_site_date_hour_dir, paste0(identifier_ds,"_ens",ens_name,".nc"))

              #Run downscaling
              noaaGEFSpoint::temporal_downscale(input_file = output_file, output_file = fname_ds, overwrite = TRUE, hr = 1)

              if(debias){


                identifier_ds_debias <- paste(model_name_ds_debias, site_list[site_index], format(start_date$min_time, "%Y-%m-%dT%H"),
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
