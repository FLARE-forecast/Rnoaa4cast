##' @title Download NOAA GEFS Forecasts
##'
##' @description Downloads a set of NOAA Global Ensemble Forecast System (GEFS)
##' forecasts for the set of point locatins  or ranges, downscaling them in
##' time if requested.
##'
##' @param site_list Vector of site codes, e.g. "NOAA", used in directory and file name generation.
##' @param lat_list Vector or range of latitudes to be downloaded (see details).
##' @param lon_list Vector or range of longitudes to be downloaded (see details).
##' @param output_directory Path: directory where the model output will be save.
##' @param forecast_time The 'hour' of the requested forecast, one of "00",
##' "06", "12", or "18", see details.
##' @param forecast_date The date, or coercible string, of the requested
##' forecast, defaults to ?????.
##' @param downscale Logical specifying whether to downscale from 6-hr to 1-hr
##' data.
##' @param debias Logical specifying whether weather data should be adjusted
##' for a bias verses the nearest forecast point (wording?).
##' @param debias_coefficients If debias = TRUE, a vector of debasing parameter
##' value lists (see details).
##' @param run_parallel Logical: whether to run on multiple cores.
##' @param num_cores Integer: number of cores used if run_parallel = TRUE.
##' @param method Character string indicating the download method, either "point" or "grid"?????.
##' @param overwrite Logical stating wether to overwrite any existing output files.
##' @param read_from_path
##' @param grid_name Grid mode only: a short grid name used in directory and file name generation.
##' @param process_specific_date Grid mode only: ...
##' @param process_specific_cycle Grid mode only: ...
##' @param delete_bad_files Grid mode only: Logical: ...
##' @param write_intermediate_ncdf Grid mode only: ...
##'
##' @details
##' @section Coordinates
##' The coordinates will be interpreted differently depending on the download
##' method. If a point download is requested lat_list and lon_list should
##' provide a vectors of coordinates to be downloaded the same length as
##' site_list. If a grid download is requested lat_list and lon_list should
##' provide a pair of latitude and longitude value defining the range (not
##' corners) of a rectangular grid of points to be downloaded.
##'      Does the order matter????? Can't cross the date line right?
##' Only decimal coordinates are currently accepted.
##' @section Forecast Times
##' NOAA GEFS forecasts are made 4 times daily. The hour 00 (midnight) forecast
##' goes out for 35 days while the other (hour 06, 12, and 18) only go out to 16 days.
##' @section Weather Debiasing
##' ...
##'
##' @return None

##' @export
##'
##' @author Quinn Thomas
##'
##'

noaa_gefs_download_downscale <- function(site_list,
                                         lat_list,
                                         lon_list,
                                         output_directory,
                                         forecast_time = NA,
                                         forecast_date = NA,
                                         downscale = FALSE,
                                         debias = FALSE,
                                         debias_coefficients = NULL,
                                         run_parallel = FALSE,#Not really used!!!!!
                                         num_cores = 1,
                                         method = "point",
                                         overwrite = FALSE,
                                         read_from_path = FALSE,
                                         grid_name = "neon",
                                         process_specific_date = NA,
                                         process_specific_cycle = NA,
                                         delete_bad_files = TRUE,
                                         write_intermediate_ncdf = TRUE){

  model_name <- "NOAAGEFS_6hr"
  model_name_ds <-"NOAAGEFS_1hr" #Downscaled NOAA GEFS
  model_name_ds_debias <-"NOAAGEFS_1hr-debias" #Downscaled NOAA GEFS
  model_name_raw <- "NOAAGEFS_raw"

  #Validate parameters:
  #All pass-through variables will be validated in their respective functions:
  #method
  #Check lengths of site_list, lat_list, and lon_list match here?
  #What is site_list used for in noaa_gefs_grid_process_downscale.  It is not needed for
  #noaa_gefs_grid_download
  #grid_name?


  #The grid download requires longitude to be -180 to 180 so converting here
  lon_list[which(lon_list > 180)] <- lon_list[which(lon_list > 180)] - 360

  #Summary for the user:
  message(paste0("Number of sites: ", length(site_list)))
  message(paste0("Overwrite existing files: ", overwrite))
  message(paste0("Running in parallel: ", run_parallel))
  message(paste0("downscale: ", downscale))
  message(paste0("debias: ", debias))
  message(paste0("Read From Path: ", read_from_path))

  if(method == "point"){

    message("downloading NOAA using single point method.  Note: only the first 16 days of a 35-day forecast are able to be downloading using this method")

    Rnoaa4cast::noaa_gefs_point_download_downscale(read_from_path = read_from_path,
                                                   lat_list = lat_list,
                                                   lon_list = lon_list,
                                                   site_list = site_list,
                                                   forecast_time = forecast_time,
                                                   forecast_date = forecast_date,
                                                   downscale = downscale,
                                                   overwrite = overwrite,
                                                   model_name = model_name,
                                                   model_name_ds = model_name_ds,
                                                   output_directory = output_directory)

  }else{

    if(is.na(process_specific_date)){
      Rnoaa4cast::noaa_gefs_grid_download(lat_list = lat_list,
                                          lon_list = lon_list,
                                          forecast_time = forecast_time,
                                          forecast_date = forecast_date,
                                          model_name_raw = model_name_raw,
                                          output_directory = output_directory,
                                          grid_name = grid_name)
    }

    Rnoaa4cast::noaa_gefs_grid_process_downscale(lat_list = lat_list,
                                                 lon_list = lon_list,
                                                 site_list = site_list,
                                                 downscale = downscale,
                                                 debias = debias,
                                                 overwrite = overwrite,
                                                 model_name = model_name,
                                                 model_name_ds = model_name_ds,
                                                 model_name_ds_debias = model_name_ds_debias,
                                                 model_name_raw = model_name_raw,
                                                 debias_coefficients = debias_coefficients,
                                                 num_cores = num_cores,
                                                 output_directory = output_directory,
                                                 write_intermediate_ncdf = write_intermediate_ncdf,
                                                 process_specific_date = process_specific_date,
                                                 process_specific_cycle = process_specific_cycle,
                                                 delete_bad_files = delete_bad_files,
                                                 grid_name = grid_name)
  }
}
