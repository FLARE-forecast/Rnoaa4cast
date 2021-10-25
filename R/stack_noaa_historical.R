#' @title Function to stack together the first six hours of each NOAA forecast cycle and append them to create a stacked data product
#'
#' @param forecast_dates vector; vector of dates for which you have NOAA GEFS forecasts with all four cycles (00, 06, 12, 18)
#' @param site character; four letter site name
#' @param noaa_directory filepath;  directory where you have noaa forecasts stored
#' @param noaa_model character;  name of the noaa model you are using, e.g. "noaa/NOAAGEFS_6hr"
#' @param output_directory filepath; directory where the output files from this function will go
#' @param model_name character; name of the model output from this function
#' @param dates_w_errors list; list of dates that cause errors, e.g. have missing first timestep
#'
#' @return, returns a netCDF file for each NOAA GEFS ensemble (0-31) with the first six hours of each NOAA GEFS forecast cycle stacked to produce a continuous meteorological data product. Returns both 6hr files and temporally downscaled 1hr files for each ensembles
#' @export


stack_noaa_forecasts <- function(forecast_dates,
                                 site,
                                 noaa_directory,
                                 noaa_model,
                                 output_directory,
                                 model_name = "observed-met-noaa", # file name of the output
                                 dates_w_errors = NA,
                                 s3_mode = FALSE,
                                 bucket = NULL
){

  cf_met_vars <- c("air_temperature",
                   "air_pressure",
                   "relative_humidity",
                   "surface_downwelling_longwave_flux_in_air",
                   "surface_downwelling_shortwave_flux_in_air",
                   "precipitation_flux",
                   "specific_humidity",
                   "wind_speed")

  cf_var_units1 <- c("K",
                     "Pa",
                     "1",
                     "Wm-2",
                     "Wm-2",
                     "kgm-2s-1",
                     "1",
                     "ms-1")  #Negative numbers indicate negative exponents

  system_date <- lubridate::as_date(lubridate::with_tz(Sys.time(),"UTC"))

  dates <- lubridate::as_date(forecast_dates)
  dates <- dates[which(dates < system_date)]

  model_name <- model_name
  cf_units <- cf_var_units1
  identifier <- paste(model_name, site,sep="_")
  fname <- paste0(identifier,".nc")
  output_file <- file.path(output_directory, site, fname)
  stacked_directory <- file.path(paste0(noaa_model, "_stacked"), site)
  local_stacked_directory <- file.path(output_directory, stacked_directory)
  stacked_directory_1hr <- file.path("noaa/NOAAGEFS_1hr_stacked", site)
  local_stacked_directory_1hr <- file.path(output_directory, "noaa/NOAAGEFS_1hr_stacked", site)

  noaa6hr_model_directory <- file.path(noaa_model, site)
  local_noaa6hr_model_directory <- file.path(output_directory, noaa_model, site)

  # set up directories
  if(!dir.exists(local_stacked_directory)){
    dir.create(local_stacked_directory, recursive=TRUE, showWarnings = FALSE)
  }

  # check output directory to see if there are already existing files to append to
  if(s3_mode){
    s3_objects <- aws.s3::get_bucket(bucket = bucket,
                                     prefix = stacked_directory,
                                     max = Inf)
    s3_list<- vapply(s3_objects, `[[`, "", "Key", USE.NAMES = FALSE)
    empty <- grepl("/$", s3_list)
    s3_list <- s3_list[!empty]
    hist_files <- s3_list
  }else{
    hist_files <- list.files(local_stacked_directory, full.names = TRUE)
  }

  hist_files <- hist_files[stringr::str_detect(hist_files, model_name)]
  hist_met_all <- NULL
  run_fx <- TRUE
  append_data <- FALSE


  if(length(hist_files) > 1){
    for(i in 1:length(hist_files)){
      if(s3_mode){
        aws.s3::save_object(object = hist_files[i],
                            bucket = bucket,
                            file = file.path(output_directory, hist_files[i]))
        curr_hist_file <- file.path(output_directory, hist_files[i])
      }else{
        curr_hist_file <- hist_files[i]
      }
      ens <- dplyr::last(unlist(stringr::str_split(basename(curr_hist_file),"_")))
      ens <- as.numeric(stringr::str_sub(ens,4,5))
      hist_met_nc <- ncdf4::nc_open(curr_hist_file)
      hist_met_time <- ncdf4::ncvar_get(hist_met_nc, "time")
      origin <- stringr::str_sub(ncdf4::ncatt_get(hist_met_nc, "time")$units, 13, 28)
      origin <- lubridate::ymd_hm(origin)
      hist_met_time <- origin + lubridate::hours(hist_met_time)
      hist_met <- tibble::tibble(time = hist_met_time,
                                 NOAA.member = ens+1)

      for(j in 1:length(cf_met_vars)){
        hist_met <- cbind(hist_met, ncdf4::ncvar_get(hist_met_nc, cf_met_vars[j]))
      }

      ncdf4::nc_close(hist_met_nc)

      names(hist_met) <- c("time","NOAA.member", cf_met_vars) # glm_met_vars


      hist_met_all <- rbind(hist_met_all, hist_met)
    }

    max_datetime <- max(hist_met_all$time) - lubridate::hours(6)

    if(lubridate::as_date(max_datetime) == max(dates)){
      print('Already up to date, cancel the rest of the function')
      run_fx <- FALSE
    }else if(lubridate::as_date(max_datetime) > max(dates)){
      print('Already up to date, cancel the rest of the function')
      run_fx <- FALSE
    }else if(lubridate::as_date(max_datetime) > min(dates)){
      print('Appending existing historical files')
      append_data <- TRUE
      dates <- dates[dates > max_datetime]
    }else{
      append_data <- FALSE
    }
  }

  missing_forecasts <- NULL

  for(k in 1:length(dates)){

    if(s3_mode){
      s3_objects <- aws.s3::get_bucket(bucket = bucket,
                                       prefix = file.path(noaa6hr_model_directory, dates[k]),
                                       max = Inf)
      s3_list<- vapply(s3_objects, `[[`, "", "Key", USE.NAMES = FALSE)
      empty <- grepl("/$", s3_list)
      s3_list <- s3_list[!empty]
      avialable_cycles <- s3_list
    }else{
      avialable_cycles <- list.files(file.path(local_noaa6hr_model_directory, dates[k]))
    }
    if(length(avialable_cycles) < 4 | dates[k] %in% lubridate::as_date(dates_w_errors)){
      missing_forecasts <- c(missing_forecasts, k)
    }
  }

  if(length(missing_forecasts) > 0){

    reference_date <- rep(NA, length(missing_forecasts))
    gap_size <- rep(NA, length(missing_forecasts))
    for(m in 1:length(missing_forecasts)){
      curr_index <- missing_forecasts[m]
      curr_gap_size <- 0
      while(curr_index %in% missing_forecasts){
        curr_index <- curr_index - 1
        curr_gap_size <- curr_gap_size + 1
      }
      reference_date[m] <- curr_index
      gap_size[m] <- curr_gap_size
    }

    missing_dates <- tibble::tibble(date = dates[missing_forecasts],
                                    reference_date = dates[reference_date],
                                    gap_size = gap_size) %>%
      dplyr::filter(!(date == Sys.Date() | date == max(dates)))



    if(max(missing_dates$gap_size) > 16){
      stop("Gap between missing forecasts is too large (> 16 days); Adjust dates included")
    }

    dates <- dates[which(!(dates %in% missing_dates$date))]

  }else{
    missing_dates <- NULL
  }

  # set up dataframe for output_directory
  noaa_obs_out <- NULL
  daily_noaa <- NULL
  dates_with_issues <- NULL

  # loop through each date of forecasts and extract the first day, stack together to create a continuous dataset of day 1 forecasts

  if(run_fx){
    for(k in 1:length(dates)){

      noaa_model_directory <- file.path(output_directory, noaa_model, site, dates[k])
      cycle <- c("00", "06", "12", "18") #list.files(noaa_model_directory)


      for(f in 1:length(cycle)){

        date_time <- lubridate::as_datetime(dates[k]) + lubridate::hours(as.numeric(cycle[f]))
        local_forecast_dir <- file.path(local_noaa6hr_model_directory, dates[k], cycle[f])


        if(s3_mode){
          s3_objects <- aws.s3::get_bucket(bucket = bucket,
                                           prefix = file.path(noaa6hr_model_directory, dates[k], cycle[f]),
                                           max = Inf)
          s3_list<- vapply(s3_objects, `[[`, "", "Key", USE.NAMES = FALSE)
          empty <- grepl("/$", s3_list)
          forecast_files <- s3_list[!empty]
          forecast_files <- forecast_files[stringr::str_detect(forecast_files, ".nc")]
          nfiles <- length(forecast_files)
        }else{
          if(!is.null(local_forecast_dir)){
            forecast_files <- list.files(file.path(local_forecast_dir, dates[k], cycle[f]), pattern = ".nc", full.names = TRUE)
            nfiles <- length(forecast_files)
          }
        }

        if(nfiles > 0){

          for(j in 1:nfiles){

            if(s3_mode){
              local_file <- file.path(output_directory, forecast_files[j])
              aws.s3::save_object(object = forecast_files[j],
                                  bucket = bucket,
                                  file = local_file)
            }else{
             local_file <-  forecast_files[j]
            }
            ens <- dplyr::last(unlist(stringr::str_split(basename(local_file),"_")))
            ens <- stringr::str_sub(ens,1,5)
            noaa_met_nc <- ncdf4::nc_open(local_file)
            noaa_met_time <- ncdf4::ncvar_get(noaa_met_nc, "time")
            origin <- stringr::str_sub(ncdf4::ncatt_get(noaa_met_nc, "time")$units, 13, 28)
            origin <- lubridate::ymd_hm(origin)
            noaa_met_time <- origin + lubridate::hours(noaa_met_time)
            noaa_met <- tibble::tibble(time = noaa_met_time)
            lat <- ncdf4::ncvar_get(noaa_met_nc, "latitude")
            lon <- ncdf4::ncvar_get(noaa_met_nc, "longitude")

            for(i in 1:length(cf_met_vars)){
              noaa_met <- cbind(noaa_met, ncdf4::ncvar_get(noaa_met_nc, cf_met_vars[i]))
            }

            ncdf4::nc_close(noaa_met_nc)
            names(noaa_met) <- c("time", cf_met_vars) # glm_met_vars

            days_to_include <- 0.25
            if(!is.null(missing_dates)){
              if(dates[k] %in% missing_dates$reference_date & f == 4){
                curr_missing_dates <- missing_dates %>%
                  dplyr::filter(reference_date == dates[k])

                days_to_include <- max(curr_missing_dates$gap_size) + 0.25
              }
            }

            focal_times <- seq(lubridate::as_datetime(dates[k]) + lubridate::hours(as.numeric(cycle[f])),
                               lubridate::as_datetime(dates[k]) + lubridate::hours(as.numeric(cycle[f])) + lubridate::hours(24 * days_to_include), by = "6 hours")

            noaa_met <- noaa_met %>%
              dplyr::mutate(ens = j) %>%
              dplyr::filter(time %in% focal_times) %>% # select just the first day
              dplyr::select(time, ens, all_of(cf_met_vars))

            if(length(cycle > 1)){
              fluxes <- noaa_met[2:nrow(noaa_met), c(6,7,8)]
              noaa_met <- noaa_met[1:(nrow(noaa_met)-1),]
              if(nrow(fluxes) != nrow(noaa_met)){
                stop(paste0("the following file can not be used, fix or add date to dates_w_errors: ", forecast_files[j]))
                dates_with_issues <- c(dates_with_issues, forecast_files[j])
              }
              noaa_met[, c(6,7,8)] <-  fluxes
            }

            noaa_obs_out <- dplyr::bind_rows(noaa_obs_out, noaa_met)

          }
        }
      }
    }

    noaa_obs_out <- noaa_obs_out %>%
      dplyr::arrange(time, ens)

    forecast_noaa <- noaa_obs_out %>%
      dplyr::rename(NOAA.member = ens) %>%
      dplyr::select(time, NOAA.member, air_temperature,
                    air_pressure, relative_humidity, surface_downwelling_longwave_flux_in_air,
                    surface_downwelling_shortwave_flux_in_air, precipitation_flux,
                    specific_humidity, wind_speed) %>%
      dplyr::arrange(time, NOAA.member)

    for (ens in 1:31) { # i is the ensemble number

      #Turn the ensemble number into a string
      if((ens-1)< 10){
        ens_name <- paste0("0",ens-1)
      }else{
        ens_name <- ens-1
      }

      forecast_noaa_ens <- forecast_noaa %>%
        dplyr::filter(NOAA.member == ens)

      last_shortwave <- forecast_noaa_ens$surface_downwelling_shortwave_flux_in_air[nrow(forecast_noaa_ens)]
      last_longwave <- forecast_noaa_ens$surface_downwelling_longwave_flux_in_air[nrow(forecast_noaa_ens)]
      last_precip <- forecast_noaa_ens$precipitation_flux[nrow(forecast_noaa_ens)]

      forecast_noaa_ens$surface_downwelling_longwave_flux_in_air[2:nrow(forecast_noaa_ens)] <- forecast_noaa_ens$surface_downwelling_longwave_flux_in_air[1:(nrow(forecast_noaa_ens)-1)]

      forecast_noaa_ens$surface_downwelling_shortwave_flux_in_air[2:nrow(forecast_noaa_ens)] <- forecast_noaa_ens$surface_downwelling_shortwave_flux_in_air[1:(nrow(forecast_noaa_ens)-1)]

      forecast_noaa_ens$precipitation_flux[2:nrow(forecast_noaa_ens)] <- forecast_noaa_ens$precipitation_flux[1:(nrow(forecast_noaa_ens)-1)]

      next_time_step <- tibble::tibble(time = forecast_noaa_ens$time[nrow(forecast_noaa_ens)] + lubridate::hours(6),
                                       NOAA.member = ens,
                                       air_temperature = NA,
                                       air_pressure = NA,
                                       relative_humidity = NA,
                                       surface_downwelling_longwave_flux_in_air = last_longwave,
                                       surface_downwelling_shortwave_flux_in_air = last_shortwave,
                                       precipitation_flux = last_precip,
                                       specific_humidity = NA,
                                       wind_speed = NA)



      forecast_noaa_ens$surface_downwelling_longwave_flux_in_air[1] <- NA
      forecast_noaa_ens$surface_downwelling_shortwave_flux_in_air[1] <- NA
      forecast_noaa_ens$precipitation_flux[1] <- NA

      forecast_noaa_ens <- rbind(forecast_noaa_ens, next_time_step)

      if(append_data==TRUE){
        hist_met_all_ens <- hist_met_all %>%
          dplyr::filter(NOAA.member == ens) %>%
          dplyr::arrange(time, NOAA.member)

        overlapping_index <- which(hist_met_all_ens$time == forecast_noaa_ens$time[1])

        hist_met_all_ens$air_temperature[overlapping_index] <- forecast_noaa_ens$air_temperature[1]
        hist_met_all_ens$air_pressure[overlapping_index] <- forecast_noaa_ens$air_pressure[1]
        hist_met_all_ens$relative_humidity[overlapping_index] <- forecast_noaa_ens$relative_humidity[1]
        hist_met_all_ens$specific_humidity[overlapping_index] <- forecast_noaa_ens$specific_humidity[1]
        hist_met_all_ens$wind_speed[overlapping_index] <- forecast_noaa_ens$wind_speed[1]



        forecast_noaa_ens <- rbind(hist_met_all_ens, forecast_noaa_ens[2:nrow(forecast_noaa_ens),]) %>%
          dplyr::arrange(time)
      }

      end_date <- forecast_noaa_ens %>%
        dplyr::summarise(max_time = max(time))

      model_name_6hr <- paste0(model_name, "-6hr")
      identifier <- paste(model_name_6hr, site, format(dplyr::first(forecast_noaa_ens$time), "%Y-%m-%dT%H"), sep="_")

      if(!dir.exists(local_stacked_directory)){
        dir.create(local_stacked_directory, recursive=TRUE, showWarnings = FALSE)
      }

      fname <- paste0(identifier,"_ens",ens_name,".nc")
      output_file <- file.path(local_stacked_directory,fname)

      #Write netCDF
      Rnoaa4cast::write_noaa_gefs_netcdf(df = forecast_noaa_ens,
                                            ens, lat = lat,
                                            lon = lon,
                                            cf_units = cf_var_units1,
                                            output_file = output_file,
                                            overwrite = TRUE)

      if(!dir.exists(local_stacked_directory_1hr)){
        dir.create(local_stacked_directory_1hr, recursive=TRUE, showWarnings = FALSE)
      }

      model_name_1hr <- paste0(model_name, "-1hr")
      identifier_ds <- paste(model_name_1hr, site, format(dplyr::first(forecast_noaa_ens$time), "%Y-%m-%dT%H"), sep="_")
      fname_ds <- paste0(identifier_ds,"_ens",ens_name,".nc")
      output_file_ds <- file.path(local_stacked_directory_1hr, fname_ds)

      #Run downscaling
      Rnoaa4cast::temporal_downscale(input_file = output_file,
                                        output_file = output_file_ds,
                                        overwrite = TRUE,
                                        hr = 1)

      if(s3_mode){
        unlink(local_noaa6hr_model_directory, recursive = TRUE)
        success_transfer <- aws.s3::put_object(file = file.path(local_stacked_directory,fname),
                                               object = file.path(stacked_directory,fname),
                                               bucket = bucket)
        if(success_transfer){
          unlink(file.path(local_stacked_directory,fname), force = TRUE)
        }
        success_transfer <- aws.s3::put_object(file = file.path(local_stacked_directory_1hr,fname_ds),
                                               object = file.path(stacked_directory_1hr,fname_ds),
                                               bucket = bucket)
        if(success_transfer){
          unlink(file.path(local_stacked_directory_1hr,fname_ds), force = TRUE)
        }
      }
    }
    unlink(local_noaa6hr_model_directory, recursive = TRUE)
  }
}
