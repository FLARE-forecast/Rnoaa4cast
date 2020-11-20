debias_met_forecast <- function(input_file, output_file, spatial_downscale_coeff, overwrite){
  
  # open netcdf
  nc <- ncdf4::nc_open(input_file)
  
  if(stringr::str_detect(input_file, "ens")){
    ens_postion <- stringr::str_locate(input_file, "ens")
    ens_name <- stringr::str_sub(input_file, start = ens_postion[1], end = ens_postion[2] + 2)
    ens <- as.numeric(stringr::str_sub(input_file, start = ens_postion[2] + 1, end = ens_postion[2] + 2))
  }else{
    ens <- 0
    ens_name <- "ens00"
  }
  
  # retrive variable names
  cf_var_names <- names(nc$var)
  
  # generate time vector
  time <- ncdf4::ncvar_get(nc, "time")
  begining_time <- lubridate::ymd_hm(ncdf4::ncatt_get(nc, "time",
                                                      attname = "units")$value)
  time <- begining_time + lubridate::hours(time)
  
  # retrive lat and lon
  lat.in <- ncdf4::ncvar_get(nc, "latitude")
  lon.in <- ncdf4::ncvar_get(nc, "longitude")
  
  # generate data frame from netcdf variables and retrive units
  noaa_data <- tibble::tibble(time = time)
  var_units <- rep(NA, length(cf_var_names))
  for(i in 1:length(cf_var_names)){
    curr_data <- ncdf4::ncvar_get(nc, cf_var_names[i])
    noaa_data <- cbind(noaa_data, curr_data)
    var_units[i] <- ncdf4::ncatt_get(nc, cf_var_names[i], attname = "units")$value
  }
  
  ncdf4::nc_close(nc)
  
  if(is.null(spatial_downscale_coeff$WindSpeed[1])){
    spatial_downscale_coeff$WindSpeed[1] = 0
    spatial_downscale_coeff$WindSpeed[2] = 1
  }
  if(is.null(spatial_downscale_coeff$AirTemp[1])){
    spatial_downscale_coeff$AirTemp[1] = 0
    spatial_downscale_coeff$AirTemp[2] = 1
  }
  if(is.null(spatial_downscale_coeff$ShortWave[1])){
    spatial_downscale_coeff$ShortWave[1] = 0
    spatial_downscale_coeff$ShortWave[2] = 1
  }
  if(is.null(spatial_downscale_coeff$LongWave[1])){
    spatial_downscale_coeff$LongWave[1] = 0
    spatial_downscale_coeff$LongWave[2] = 1
  }
  
  noaa_met_daily <- noaa_met %>%
    mutate(date = lubridate::as_date(time)) %>%
    select(-time) %>%
    group_by(date) %>%
    summarise_all(mean, na.rm = TRUE) %>%
    mutate(AirTemp_debias = spatial_downscale_coeff$AirTemp[1] + air_temperature * spatial_downscale_coeff$AirTemp[2],
           AirTemp_orig = air_temperature,
           ShortWave_debias = spatial_downscale_coeff$ShortWave[1] + surface_downwelling_shortwave_flux_in_air * spatial_downscale_coeff$ShortWave[2],
           ShortWave_debias = ifelse(ShortWave_debias < 0, 0, ShortWave_debias),
           ShortWave_orig = surface_downwelling_shortwave_flux_in_air,
           LongWave_debias = spatial_downscale_coeff$LongWave[1] + surface_downwelling_longwave_flux_in_air * spatial_downscale_coeff$LongWave[2],
           LongWave_debias = ifelse(LongWave_debias < 0, 0, LongWave_debias),
           LongWave_orig = surface_downwelling_longwave_flux_in_air,
           WindSpeed_debias = spatial_downscale_coeff$WindSpeed[1] + wind_speed * spatial_downscale_coeff$WindSpeed[2],
           WindSpeed_debias = ifelse(WindSpeed_debias < 0, 0, WindSpeed_debias),
           WindSpeed_orig = wind_speed) %>%
    select(date, AirTemp_debias, AirTemp_orig, ShortWave_debias, ShortWave_orig, LongWave_debias, LongWave_orig, WindSpeed_debias, WindSpeed_orig)
  
  noaa_met <- noaa_met %>%
    mutate(date = lubridate::as_date(time)) %>%
    left_join(noaa_met_daily, by = "date") %>%
    mutate(air_temperature = (AirTemp/AirTemp_orig) * AirTemp_debias,
           surface_downwelling_shortwave_flux_in_air = (ShortWave/ShortWave_orig) * ShortWave_debias,
           surface_downwelling_longwave_flux_in_air = (LongWave/LongWave_orig) * LongWave_debias,
           wind_speed = (WindSpeed/WindSpeed_orig) * WindSpeed_debias,
           relative_humidity = RelHum,
           precipitation_flux = Rain) %>%
    mutate(surface_downwelling_shortwave_flux_in_air = ifelse(ShortWave < 0, 0, ShortWave),
           surface_downwelling_longwave_flux_in_air = ifelse(LongWave < 0, 0, LongWave),
           wind_speed = ifelse(WindSpeed < 0, 0, WindSpeed)) %>%
    select(time, all_of(cf_var_names))
  
  #Write netCDF
  noaaGEFSpoint::write_noaa_gefs_netcdf(df = forecast_noaa_ds,
                                        ens = ens,
                                        lat = lat.in,
                                        lon = lon.in,
                                        cf_units = var_units,
                                        output_file = output_file,
                                        overwrite = overwrite)
}