#' @title Debias Meteorology for a NOAA GEFS Forecast
#'
#' @description Take a NOAA GEFS forecast (netCDF) file and adjust the meteorology for a systematic
#' bias between the forecast grid and a specific location within that grid cell.
#'
#' @param input_file The path to a NOAA GEFS forecast netCDF file to modify.
#' @param output_file The path and file name for the debiased output forecast.
#' @param spatial_downscale_coeff A list or data frame containing debasing parameter values lists
#' (see details).
#' @param overwrite Logical stating whether to overwrite any existing output files.
#'
#' @return None.
#'
#' @details
#' @section Debiasing Coefficients
#' This function debiases meteorology using a linear transformation based on parameters you provide.
#' The parameters are supplied as a data frame (or tibble or list) with columns / members AirTemp,
#' LongWave, ShortWave, and WindSpeed with intercept and slope parameters as rows 1 and 2. Example:
#'
#' ```
#' Should we give an example?????
#' ```
#'
#' Missing (NA) parameters will result debiasing to be skipped for the corresponding variable.
#'
#' @export
#'
#' @examples

#Notes:
# - spatial_downscale_coeff is called debias_coefficients in noaa_gefs_download_downscale().
# - Is the Debiasing Coefficients data structure description correct?
# - We don't check to see that the slope is present.  spatial_downscale_coeff should probably be
#checked more fully.
# - Move Debiasing Coefficients section into parameter definition?

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

  # retrieve variable names
  cf_var_names <- names(nc$var)

  # generate time vector
  time <- ncdf4::ncvar_get(nc, "time")
  begining_time <- lubridate::ymd_hm(ncdf4::ncatt_get(nc, "time",
                                                      attname = "units")$value)
  time <- begining_time + lubridate::hours(time)

  # retrieve lat and lon
  lat.in <- ncdf4::ncvar_get(nc, "latitude")
  lon.in <- ncdf4::ncvar_get(nc, "longitude")

  # generate data frame from netcdf variables and retrieve units
  noaa_data <- tibble::tibble(time = time)
  var_units <- rep(NA, length(cf_var_names))
  for(i in 1:length(cf_var_names)){
    curr_data <- ncdf4::ncvar_get(nc, cf_var_names[i])
    noaa_data <- cbind(noaa_data, curr_data)
    var_units[i] <- ncdf4::ncatt_get(nc, cf_var_names[i], attname = "units")$value
  }

  ncdf4::nc_close(nc)

  names(noaa_data) <- c("time",cf_var_names )

  #If parameters for a variable are missing don't transform it:
  if(is.na(spatial_downscale_coeff$WindSpeed[1])){
    spatial_downscale_coeff$WindSpeed[1] = 0
    spatial_downscale_coeff$WindSpeed[2] = 1
  }
  if(is.na(spatial_downscale_coeff$AirTemp[1])){
    spatial_downscale_coeff$AirTemp[1] = 0
    spatial_downscale_coeff$AirTemp[2] = 1
  }
  if(is.na(spatial_downscale_coeff$ShortWave[1])){
    spatial_downscale_coeff$ShortWave[1] = 0
    spatial_downscale_coeff$ShortWave[2] = 1
  }
  if(is.na(spatial_downscale_coeff$LongWave[1])){
    spatial_downscale_coeff$LongWave[1] = 0
    spatial_downscale_coeff$LongWave[2] = 1
  }

  #Aggregate the meteorology to daily and apply debiasing storing values before and after:
  noaa_met_daily <- noaa_data %>%
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

  #Normalize the sub-daily forcasts with the daily calculations from the last step:
  noaa_met <- noaa_data %>%
    mutate(date = lubridate::as_date(time)) %>%
    left_join(noaa_met_daily, by = "date") %>%
    mutate(air_temperature = (air_temperature/AirTemp_orig) * AirTemp_debias,
           surface_downwelling_shortwave_flux_in_air = (surface_downwelling_shortwave_flux_in_air/ShortWave_orig) * ShortWave_debias,
           surface_downwelling_longwave_flux_in_air = (surface_downwelling_longwave_flux_in_air/LongWave_orig) * LongWave_debias,
           wind_speed = (wind_speed/WindSpeed_orig) * WindSpeed_debias) %>%
    mutate(surface_downwelling_shortwave_flux_in_air = ifelse(surface_downwelling_shortwave_flux_in_air < 0, 0, surface_downwelling_shortwave_flux_in_air),
           surface_downwelling_longwave_flux_in_air = ifelse(surface_downwelling_longwave_flux_in_air < 0, 0, surface_downwelling_longwave_flux_in_air),
           wind_speed = ifelse(wind_speed < 0, 0, wind_speed),
           NOAA.member = ens) %>%
    select(time, NOAA.member, all_of(cf_var_names))

  #Write netCDF:
  Rnoaa4cast::write_noaa_gefs_netcdf(df = noaa_met,
                                        ens = ens,
                                        lat = lat.in,
                                        lon = lon.in,
                                        cf_units = var_units,
                                        output_file = output_file,
                                        overwrite = overwrite)
}
