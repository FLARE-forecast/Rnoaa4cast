#' Average ensemble from stacked historical noaa forecasts
#'
#' @param forecast_dates
#' @param noaa_stacked_directory
#' @param output_file
#'
#' @return
#' @export
#'
#' @examples
average_stacked_forecasts <- function(forecast_dates, # vector of the date range you'd like to create
                                      noaa_stacked_directory, # file path of the directory where the stacked ensemble files are stored
                                      output_file # file  where you want the output to go
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
  cf_units <- cf_var_units1

  system_date <- lubridate::as_date(lubridate::with_tz(Sys.time(),"UTC"))

  dates <- lubridate::as_date(forecast_dates)
  dates <- dates[which(dates < system_date)]

  # look in output directory for existing file
  hist_file <- output_file
  hist_files <- list.files(output_directory)
  append_data <- FALSE
  run_fx <- TRUE

  if(hist_file %in% hist_files){
    hist_met_nc <- ncdf4::nc_open(file.path(output_file))
    hist_met_time <- ncdf4::ncvar_get(hist_met_nc, "time")
    origin <- stringr::str_sub(ncdf4::ncatt_get(hist_met_nc, "time")$units, 13, 28)
    origin <- lubridate::ymd_hm(origin)
    hist_met_time <- origin + lubridate::hours(hist_met_time)
    hist_met <- tibble::tibble(time = hist_met_time)

    for(i in 1:length(cf_met_vars)){
      hist_met <- cbind(hist_met, ncdf4::ncvar_get(hist_met_nc, cf_met_vars[i]))
    }

    names(hist_met) <- c("time", cf_met_vars) # glm_met_vars

    if(max(hist_met$time) == max(dates)){
      print('Already up to date, cancel the rest of the function')
      run_fx <- FALSE
    }else if(max(hist_met$time) > max(dates)){
      print('Already up to date, cancel the rest of the function')
      run_fx <- FALSE
    }else if(max(hist_met$time) > min(dates)){
      print('Appending existing historical files')
      append_data <- TRUE
      dates <- dates[dates > max(hist_met$time)]
    }else{
      append_data <- FALSE
    }
  }

  if(run_fx){
    # read in stacked 1hr files
    stacked_files <- list.files(noaa_stacked_directory, full.names = TRUE)
    stacked_met_all <- NULL
    #run_fx <- TRUE
    #append_data <- FALSE


    if(length(stacked_files) > 1){
      for(i in 1:length(stacked_files)){
        ens <- dplyr::last(unlist(stringr::str_split(string = basename(stacked_files[i]),pattern = "_")))
        ens <- as.numeric(stringr::str_sub(ens,4,5))
        stacked_met_nc <- ncdf4::nc_open(stacked_files[i])
        stacked_met_time <- ncdf4::ncvar_get(stacked_met_nc, "time")
        origin <- stringr::str_sub(ncdf4::ncatt_get(stacked_met_nc, "time")$units, 13, 28)
        origin <- lubridate::ymd_hm(origin)
        stacked_met_time <- origin + lubridate::hours(stacked_met_time)
        stacked_met <- tibble::tibble(time = stacked_met_time,
                                      NOAA.member = ens+1)

        for(j in 1:length(cf_met_vars)){
          stacked_met <- cbind(stacked_met, ncdf4::ncvar_get(stacked_met_nc, cf_met_vars[j]))
        }

        ncdf4::nc_close(stacked_met_nc)

        names(stacked_met) <- c("time","NOAA.member", cf_met_vars) # glm_met_vars


        stacked_met_all <- rbind(stacked_met_all, stacked_met)
      }

      noaa_met_nc <- ncdf4::nc_open(stacked_files[1])
      lat <- ncdf4::ncvar_get(noaa_met_nc, "latitude")
      lon <- ncdf4::ncvar_get(noaa_met_nc, "longitude")
      ncdf4::nc_close(noaa_met_nc)

    }


    stacked_met_mean <- NULL
    stacked_met_mean <- stacked_met_all %>%
      dplyr::group_by(time) %>%
      dplyr::mutate(air_temperature = mean(air_temperature, na.rm = TRUE),
                    air_pressure = mean(air_pressure, na.rm = TRUE),
                    relative_humidity = mean(relative_humidity, na.rm = TRUE),
                    surface_downwelling_longwave_flux_in_air = mean(surface_downwelling_longwave_flux_in_air, na.rm = TRUE),
                    surface_downwelling_shortwave_flux_in_air = mean(surface_downwelling_shortwave_flux_in_air, na.rm = TRUE),
                    precipitation_flux = mean(precipitation_flux, na.rm = TRUE),
                    specific_humidity = mean(specific_humidity, na.rm = TRUE),
                    wind_speed = mean(wind_speed)) %>%
      dplyr::distinct(time, .keep_all = TRUE) %>%
      dplyr::select(-NOAA.member)  %>%
      dplyr::arrange(time) %>%
      dplyr::ungroup()


    if(append_data==TRUE){
      stacked_met_mean <- rbind(hist_met, stacked_met_mean)
    }

    # write the file
    stacked_met_mean <- na.omit(stacked_met_mean)
    start_time <- min(stacked_met_mean$time)
    end_time <- max(stacked_met_mean$time)

    data <- stacked_met_mean %>%
      dplyr::select(-time)

    diff_time <- as.numeric(difftime(stacked_met_mean$time, stacked_met_mean$time[1], units = "hours"))

    cf_var_names <- names(data)

    time_dim <- ncdf4::ncdim_def(name="time",
                                 units = paste("hours since", format(start_time, "%Y-%m-%d %H:%M")),
                                 diff_time, #GEFS forecast starts 5 hours from start time
                                 create_dimvar = TRUE)
    lat_dim <- ncdf4::ncdim_def("latitude", "degree_north", lat, create_dimvar = TRUE)
    lon_dim <- ncdf4::ncdim_def("longitude", "degree_east", lon, create_dimvar = TRUE)

    dimensions_list <- list(time_dim, lat_dim, lon_dim)

    nc_var_list <- list()
    for (i in 1:length(cf_var_names)) { #Each ensemble member will have data on each variable stored in their respective file.
      nc_var_list[[i]] <- ncdf4::ncvar_def(cf_var_names[i], cf_units[i], dimensions_list, missval=NaN)
    }

    nc_flptr <- ncdf4::nc_create(output_file, nc_var_list, verbose = FALSE, )

    #For each variable associated with that ensemble
    for (j in 1:ncol(data)) {
      # "j" is the variable number.  "i" is the ensemble number. Remember that each row represents an ensemble
      ncdf4::ncvar_put(nc_flptr, nc_var_list[[j]], unlist(data[,j]))
    }

    ncdf4::nc_close(nc_flptr)

  }
}
