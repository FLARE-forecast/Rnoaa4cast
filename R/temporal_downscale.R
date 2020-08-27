##' @title Downscale NOAA GEFS frin 6hr to 1hr
##' @return None
##'
##' @param input_file, full path to 6hr file
##' @param output_file, full path to 1hr file that will be generated
##' @param overwrite, logical stating to overwrite any existing output_file
##' @export
##'
##' @author Quinn Thomas
##'
##'

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
  noaa_data <- tibble(time = time)
  var_units <- rep(NA, length(cf_var_names))
  for(i in 1:length(cf_var_names)){
    curr_data <- ncdf4::ncvar_get(nc, cf_var_names[i])
    noaa_data <- cbind(noaa_data, curr_data)
    var_units[i] <- ncdf4::ncatt_get(nc, cf_var_names[i], attname = "units")$value
  }
  names(noaa_data) <- c("time",cf_var_names)

  # spline-based downscaling
  if(length(which(c("air_temperature", "wind_speed","specific_humidity", "air_pressure") %in% cf_var_names) == 4)){
    forecast_noaa_ds <- noaaGEFSpoint::downscale_spline_to_hrly(df = noaa_data, VarNames = c("air_temperature", "wind_speed","specific_humidity", "air_pressure"))
  }else{
    #Add error message
  }

  # Convert splined SH, temperature, and presssure to RH
  forecast_noaa_ds <- forecast_noaa_ds %>%
    mutate(relative_humidity = noaaGEFSpoint::qair2rh(qair = forecast_noaa_ds$specific_humidity,
                                       temp = forecast_noaa_ds$air_temperature,
                                       press = forecast_noaa_ds$air_pressure)) %>%

    mutate(relative_humidity = relative_humidity,
           relative_humidity = ifelse(relative_humidity > 1, 0, relative_humidity))

  # convert longwave to hourly (just copy 6 hourly values over past 6-hour time period)
  if("surface_downwelling_longwave_flux_in_air" %in% cf_var_names){
    LW.flux.hrly <- noaaGEFSpoint::downscale_repeat_6hr_to_hrly(df = noaa_data, varName = "surface_downwelling_longwave_flux_in_air")
    forecast_noaa_ds <- dplyr::inner_join(forecast_noaa_ds, LW.flux.hrly, by = "time")
  }else{
    #Add error message
  }

  # convert precipitation to hourly (just copy 6 hourly values over past 6-hour time period)
  if("surface_downwelling_longwave_flux_in_air" %in% cf_var_names){
    Precip.flux.hrly <- noaaGEFSpoint::downscale_repeat_6hr_to_hrly(df = noaa_data, varName = "precipitation_flux")
    forecast_noaa_ds <- dplyr::inner_join(forecast_noaa_ds, Precip.flux.hrly, by = "time")
  }else{
    #Add error message
  }

  # convert cloud_area_fraction to hourly (just copy 6 hourly values over past 6-hour time period)
  if("cloud_area_fraction" %in% cf_var_names){
    cloud_area_fraction.flux.hrly <- noaaGEFSpoint::downscale_repeat_6hr_to_hrly(df = noaa_data, varName = "cloud_area_fraction")
    forecast_noaa_ds <- dplyr::inner_join(forecast_noaa_ds, cloud_area_fraction.flux.hrly, by = "time")
  }else{
    #Add error message
  }

  # use solar geometry to convert shortwave from 6 hr to 1 hr
  if("surface_downwelling_shortwave_flux_in_air" %in% cf_var_names){
    ShortWave.hrly <- noaaGEFSpoint::downscale_ShortWave_to_hrly(df = noaa_data, lat = lat.in, lon = lon.in)
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
  noaaGEFSpoint::write_noaa_gefs_netcdf(df = forecast_noaa_ds,
                         ens = ens,
                         lat = lat.in,
                         lon.in,
                         cf_units = var_units,
                         output_file = output_file,
                         overwrite = overwrite)

}

##' @title Downscale spline to hourly
##' @return A dataframe of downscaled state variables
##'
##' @param df, dataframe of data to be downscales
##' @param VarNames, names of vars that are state variables
##' @export
##'
##' @author Laura Puckett
##'
##'

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

  noaa_data_interp <- tibble(time = lubridate::as_datetime(t0 + interp.df.days, tz = "UTC"))

  for(Var in 1:length(VarNames)){
    curr_data <- spline(x = df$days_since_t0, y = unlist(df[VarNames[Var]]), method = "fmm", xout = interp.df.days)$y
    noaa_data_interp <- cbind(noaa_data_interp, curr_data)
  }

  names(noaa_data_interp) <- c("time",VarNames)

  return(noaa_data_interp)
}

##' @title Downscale shortwave to hourly
##' @return A dataframe of downscaled state variables
##'
##' @param df, data frame of variables
##' @param lat, lat of site
##' @param lon, long of site
##' @return ShortWave.ds
##' @export
##'
##' @author Laura Puckett
##'
##'

downscale_ShortWave_to_hrly <- function(df,lat, lon, hr = 1){
  ## downscale shortwave to hourly

  t0 <- min(df$time)
  df <- df %>%
    dplyr::select("time", "surface_downwelling_shortwave_flux_in_air") %>%
    dplyr::mutate(days_since_t0 = difftime(.$time, t0, units = "days")) %>%
    dplyr::mutate(lead_var = lead(surface_downwelling_shortwave_flux_in_air, 1))

  interp.df.days <- seq(min(df$days_since_t0), as.numeric(max(df$days_since_t0)), 1/(24/hr))

  noaa_data_interp <- tibble(time = lubridate::as_datetime(t0 + interp.df.days))

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
    dplyr::mutate(rpot = noaaGEFSpoint::downscale_solar_geom(doy, as.vector(lon), as.vector(lat))) %>% # hourly sw flux calculated using solar geometry
    dplyr::group_by(group_6hr) %>%
    dplyr::mutate(avg.rpot = mean(rpot, na.rm = TRUE)) %>% # daily sw mean from solar geometry
    dplyr::ungroup() %>%
    dplyr::mutate(surface_downwelling_shortwave_flux_in_air = ifelse(avg.rpot > 0, rpot* (surface_downwelling_shortwave_flux_in_air/avg.rpot),0)) %>%
    dplyr::select(time,surface_downwelling_shortwave_flux_in_air)

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
#' @param hr Hours timestep
#' @return `numeric(1)` of cosine of solar zenith angle
#' @export
cos_solar_zenith_angle <- function(doy, lat, lon, dt, hr) {
  et <- noaaGEFSpoint::equation_of_time(doy)
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
#' @return `numeric(1)` length of the solar day, in hours.
#' @export
equation_of_time <- function(doy) {
  stopifnot(doy <= 366)
  f      <- pi / 180 * (279.5 + 0.9856 * doy)
  et     <- (-104.7 * sin(f) + 596.2 * sin(2 * f) + 4.3 *
               sin(4 * f) - 429.3 * cos(f) - 2 *
               cos(2 * f) + 19.3 * cos(3 * f)) / 3600  # equation of time -> eccentricity and obliquity
  return(et)
}

##' @title Downscale repeat to hourly
##' @return A dataframe of downscaled data
##'
##' @param df, dataframe of data to be downscaled (Longwave)
##' @export
##'
##' @author Laura Puckett
##'
##'

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
  interp.df.days <- seq(min(df$days_since_t0), as.numeric(max(df$days_since_t0)), 1/(24/hr))

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
    arrange(time)
  names(data.hrly) <- c("time", varName)

  return(data.hrly)
}

##' @title Calculate potential shortwave radiation
##' @return vector of potential shortwave radiation for each doy
##'
##' @param doy, day of year in decimal
##' @param lon, longitude
##' @param lat, latitude
##' @return `numeric(1)`
##' @export
##'
##' @author Quinn Thomas
##'
##'
downscale_solar_geom <- function(doy, lon, lat) {

  dt <- median(diff(doy)) * 86400 # average number of seconds in time interval
  hr <- (doy - floor(doy)) * 24 # hour of day for each element of doy

  ## calculate potential radiation
  cosz <- noaaGEFSpoint::cos_solar_zenith_angle(doy, lat, lon, dt, hr)
  rpot <- 1366 * cosz
  return(rpot)
}
