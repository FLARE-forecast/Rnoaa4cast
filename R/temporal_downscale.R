#' @title Temporally Downscale a NOAA GEFS Forecast
#'
#' @description Downscale a 6hr NOAA Global Ensemble Forecast System (GEFS) forecast (in netCDF
#' form) to a finer time resolution.
#'
#' @param input_file Full path to The path to a NOAA GEFS 6hr forecast netCDF file to downscale.
#' @param output_file Full path to 1hr file that will be generated.
#' @param overwrite Logical stating whether to overwrite an existing output file.
#' @param hr Time step in hours of temporal downscaling (default = 1).
#'
#' @return None
#'
#' @export
#'
#' @author Quinn Thomas
#'

#JMR_NOTES:
#Only updated temporal_downscale() documentation.

temporal_downscale <- function(input_file, output_file, overwrite = TRUE, hr = 1){

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

  names(noaa_data) <- c("time",cf_var_names)

  # spline-based downscaling
  if(length(which(c("air_temperature", "wind_speed","specific_humidity", "air_pressure", "relative_humidity") %in% cf_var_names) == 5)){
    forecast_noaa_ds <- downscale_spline_to_hrly(df = noaa_data, VarNames = c("air_temperature", "wind_speed","specific_humidity", "air_pressure", "relative_humidity"))
  }else{
    #Add error message
  }

  # convert longwave to hourly (just copy 6 hourly values over past 6-hour time period)
  if("surface_downwelling_longwave_flux_in_air" %in% cf_var_names){
    LW.flux.hrly <- downscale_repeat_6hr_to_hrly(df = noaa_data, varName = "surface_downwelling_longwave_flux_in_air")
    LW.flux.hrly$surface_downwelling_longwave_flux_in_air[nrow(LW.flux.hrly)] <- NA
    forecast_noaa_ds <- dplyr::inner_join(forecast_noaa_ds, LW.flux.hrly, by = "time")
  }else{
    #Add error message
  }

  # convert precipitation to hourly (just copy 6 hourly values over past 6-hour time period)
  if("precipitation_flux" %in% cf_var_names){
    Precip.flux.hrly <- downscale_repeat_6hr_to_hrly(df = noaa_data, varName = "precipitation_flux")
    Precip.flux.hrly$precipitation_flux[nrow(Precip.flux.hrly)] <- NA
    forecast_noaa_ds <- dplyr::inner_join(forecast_noaa_ds, Precip.flux.hrly, by = "time")
  }else{
    #Add error message
  }

  # convert cloud_area_fraction to hourly (just copy 6 hourly values over past 6-hour time period)
  if("cloud_area_fraction" %in% cf_var_names){
    cloud_area_fraction.flux.hrly <- downscale_repeat_6hr_to_hrly(df = noaa_data, varName = "cloud_area_fraction")
    cloud_area_fraction.flux.hrly$cloud_area_fraction[nrow(cloud_area_fraction.flux.hrly)] <- NA
    forecast_noaa_ds <- dplyr::inner_join(forecast_noaa_ds, cloud_area_fraction.flux.hrly, by = "time")
  }else{
    #Add error message
  }

  # use solar geometry to convert shortwave from 6 hr to 1 hr
  if("surface_downwelling_shortwave_flux_in_air" %in% cf_var_names){
    ShortWave.hrly <- downscale_ShortWave_to_hrly(df = noaa_data, lat = lat.in, lon = lon.in)
    ShortWave.hrly$surface_downwelling_shortwave_flux_in_air[nrow(ShortWave.hrly)] <- NA
    forecast_noaa_ds <- dplyr::inner_join(forecast_noaa_ds, ShortWave.hrly, by = "time")
  }else{
    #Add error message
  }

  #Add dummy ensemble number to work with write_noaa_gefs_netcdf()
  forecast_noaa_ds$NOAA.member <- ens

  #Make sure var names are in correct order
  forecast_noaa_ds <- forecast_noaa_ds %>%
    dplyr::select("time", all_of(cf_var_names), "NOAA.member")

  #Write netCDF
  Rnoaa4cast::write_noaa_gefs_netcdf(df = forecast_noaa_ds,
                         ens = ens,
                         lat = lat.in,
                         lon = lon.in,
                         cf_units = var_units,
                         output_file = output_file,
                         overwrite = overwrite)

}

#' @title Downscale spline to hourly
#' @return A dataframe of downscaled state variables
#' @param df, dataframe of data to be downscales
#' @noRd
#' @author Laura Puckett
#'
#'

downscale_spline_to_hrly <- function(df,VarNames, hr = 1){
  # --------------------------------------
  # purpose: interpolates debiased forecasts from 6-hourly to hourly
  # Creator: Laura Puckett, December 16 2018
  # --------------------------------------
  # @param: df, a dataframe of debiased 6-hourly forecasts

  t0 = min(df$time)
  df <- df %>%
    dplyr::mutate(days_since_t0 = difftime(.$time, t0, units = "days"))

  interp.df.days <- seq(min(df$days_since_t0), as.numeric(max(df$days_since_t0)), 1/(24/hr))

  noaa_data_interp <- tibble::tibble(time = lubridate::as_datetime(t0 + interp.df.days, tz = "UTC"))

  for(Var in 1:length(VarNames)){
    curr_data <- spline(x = df$days_since_t0, y = unlist(df[VarNames[Var]]), method = "fmm", xout = interp.df.days)$y
    noaa_data_interp <- cbind(noaa_data_interp, curr_data)
  }

  names(noaa_data_interp) <- c("time",VarNames)

  return(noaa_data_interp)
}

#' @title Downscale shortwave to hourly
#' @return A dataframe of downscaled state variables
#'
#' @param df, data frame of variables
#' @param lat, lat of site
#' @param lon, long of site
#' @return ShortWave.ds
#' @noRd
#' @author Laura Puckett
#'
#'

downscale_ShortWave_to_hrly <- function(df,lat, lon, hr = 1){
  ## downscale shortwave to hourly

  t0 <- min(df$time)
  df <- df %>%
    dplyr::select("time", "surface_downwelling_shortwave_flux_in_air") %>%
    dplyr::mutate(days_since_t0 = difftime(.$time, t0, units = "days")) %>%
    dplyr::mutate(lead_var = dplyr::lead(surface_downwelling_shortwave_flux_in_air, 1))

  interp.df.days <- seq(min(df$days_since_t0), as.numeric(max(df$days_since_t0)), 1/(24/hr))

  noaa_data_interp <- tibble::tibble(time = lubridate::as_datetime(t0 + interp.df.days))

  data.hrly <- noaa_data_interp %>%
    dplyr::left_join(df, by = "time")

  data.hrly$group_6hr <- NA

  group <- 0
  for(i in 1:nrow(data.hrly)){
    if(!is.na(data.hrly$lead_var[i])){
      curr <- data.hrly$lead_var[i]
      data.hrly$surface_downwelling_shortwave_flux_in_air[i] <- curr
      group <- group + 1
      data.hrly$group_6hr[i] <- group
    }else{
      data.hrly$surface_downwelling_shortwave_flux_in_air[i] <- curr
      data.hrly$group_6hr[i] <- group
    }
  }

  ShortWave.ds <- data.hrly %>%
    dplyr::mutate(hour = lubridate::hour(time)) %>%
    dplyr::mutate(doy = lubridate::yday(time) + hour/(24/hr))%>%
    dplyr::mutate(rpot = downscale_solar_geom(doy, as.vector(lon), as.vector(lat))) %>% # hourly sw flux calculated using solar geometry
    dplyr::group_by(group_6hr) %>%
    dplyr::mutate(avg.rpot = mean(rpot, na.rm = TRUE)) %>% # daily sw mean from solar geometry
    dplyr::ungroup() %>%
    dplyr::mutate(surface_downwelling_shortwave_flux_in_air = ifelse(avg.rpot > 0, rpot* (surface_downwelling_shortwave_flux_in_air/avg.rpot),0)) %>%
    dplyr::select(time,surface_downwelling_shortwave_flux_in_air)

  ShortWave.ds$surface_downwelling_shortwave_flux_in_air[nrow(ShortWave.ds)] <- NA

  return(ShortWave.ds)

}

#' Cosine of solar zenith angle
#'
#' For explanations of formulae, see http://www.itacanet.org/the-sun-as-a-source-of-energy/part-3-calculating-solar-angles/
#'
#' @author Alexey Shiklomanov
#' @param doy Day of year
#' @param lat Latitude
#' @param lon Longitude
#' @param dt Timestep
#' @noRd
#' @param hr Hours timestep
#' @return `numeric(1)` of cosine of solar zenith angle
#' @export
cos_solar_zenith_angle <- function(doy, lat, lon, dt, hr) {
  et <- equation_of_time(doy)
  merid  <- floor(lon / 15) * 15
  merid[merid < 0] <- merid[merid < 0] + 15
  lc     <- (lon - merid) * -4/60  ## longitude correction
  tz     <- merid / 360 * 24  ## time zone
  midbin <- 0.5 * dt / 86400 * 24  ## shift calc to middle of bin
  t0   <- 12 + lc - et - tz - midbin  ## solar time
  h    <- pi/12 * (hr - t0)  ## solar hour
  dec  <- -23.45 * pi / 180 * cos(2 * pi * (doy + 10) / 365)  ## declination
  cosz <- sin(lat * pi / 180) * sin(dec) + cos(lat * pi / 180) * cos(dec) * cos(h)
  cosz[cosz < 0] <- 0
  return(cosz)
}

#' Equation of time: Eccentricity and obliquity
#'
#' For description of calculations, see https://en.wikipedia.org/wiki/Equation_of_time#Calculating_the_equation_of_time
#'
#' @author Alexey Shiklomanov
#' @param doy Day of year
#' @noRd
#' @return `numeric(1)` length of the solar day, in hours.

equation_of_time <- function(doy) {
  stopifnot(doy <= 367)
  f      <- pi / 180 * (279.5 + 0.9856 * doy)
  et     <- (-104.7 * sin(f) + 596.2 * sin(2 * f) + 4.3 *
               sin(4 * f) - 429.3 * cos(f) - 2 *
               cos(2 * f) + 19.3 * cos(3 * f)) / 3600  # equation of time -> eccentricity and obliquity
  return(et)
}

#' @title Downscale repeat to hourly
#' @return A dataframe of downscaled data
#' @param df, dataframe of data to be downscaled (Longwave)
#' @noRd
#' @author Laura Puckett
#'
#'

downscale_repeat_6hr_to_hrly <- function(df, varName, hr = 1){

  #Get first time point
  t0 <- min(df$time)

  df <- df %>%
    dplyr::select("time", all_of(varName)) %>%
    #Calculate time difference
    dplyr::mutate(days_since_t0 = difftime(.$time, t0, units = "days")) %>%
    #Shift valued back because the 6hr value represents the average over the
    #previous 6hr period
    dplyr::mutate(lead_var = dplyr::lead(df[,varName], 1))

  #Create new vector with all hours
  interp.df.days <- seq(min(df$days_since_t0),
                        as.numeric(max(df$days_since_t0)),
                        1 / (24 / hr))

  #Create new data frame
  noaa_data_interp <- tibble::tibble(time = lubridate::as_datetime(t0 + interp.df.days))

  #Join 1 hr data frame with 6 hr data frame
  data.hrly <- noaa_data_interp %>%
    dplyr::left_join(df, by = "time")

  #Fill in hours
  for(i in 1:nrow(data.hrly)){
    if(!is.na(data.hrly$lead_var[i])){
      curr <- data.hrly$lead_var[i]
    }else{
      data.hrly$lead_var[i] <- curr
    }
  }

  #Clean up data frame
  data.hrly <- data.hrly %>% dplyr::select("time", lead_var) %>%
    dplyr::arrange(time)

  names(data.hrly) <- c("time", varName)

  return(data.hrly)
}

#' @title Calculate potential shortwave radiation
#' @return vector of potential shortwave radiation for each doy
#'
#' @param doy, day of year in decimal
#' @param lon, longitude
#' @param lat, latitude
#' @return `numeric(1)`
#' @author Quinn Thomas
#' @noRd
#'
#'
downscale_solar_geom <- function(doy, lon, lat) {

  dt <- median(diff(doy)) * 86400 # average number of seconds in time interval
  hr <- (doy - floor(doy)) * 24 # hour of day for each element of doy

  ## calculate potential radiation
  cosz <- cos_solar_zenith_angle(doy, lat, lon, dt, hr)
  rpot <- 1366 * cosz
  return(rpot)
}
