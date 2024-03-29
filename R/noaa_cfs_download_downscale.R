##' @title Download NOAA CFS Forecasts
##'
##' @description Downloads a set of NOAA Climate (Coupled) Forecast System (CFS) forecasts for a set
##' of point locations or spatial ranges, downscaling them in time if requested.
##'
##' @param site_list Vector of site codes, e.g. "NOAA", used in directory and file name generation.
##' @param lat_list Vector or range of latitudes to be downloaded (see details).
##' @param lon_list Vector or range of longitudes to be downloaded (see details).
##' @param output_directory Path: directory where the model output will be saved.
##' @param forecast_time The 'hour' of the requested forecast, one of "00", "06", "12", or "18" (see
##' details). If omitted all times will be downloaded.
##' @param forecast_date The date, or coercible string, of the requested forecast. ((Defaults to the
#' most recent date.))
##' @param downscale Logical specifying whether to downscale from 6-hr to 1-hr data.
##' @param debias Logical specifying whether weather data should be adjusted
##' for a bias verses the nearest forecast point.
##' @param debias_coefficients If debias = TRUE, a data frame of debasing parameter value lists (see
##' details).
##' @param run_parallel Logical: whether to run on multiple cores.
##' @param num_cores Integer: number of cores used if run_parallel = TRUE.
##' @param method Character string indicating the download method, either "point" or "grid".
##' @param overwrite Logical stating whether to overwrite any existing output files.
##' @param grid_name Grid mode only: a short grid name used in directory and file name generation.
##'
##' @details
##' @section Coordinates
##' The coordinates will be interpreted differently depending on the download
##' method. If a point download is requested lat_list and lon_list should
##' provide a vectors of coordinates to be downloaded with the same length as
##' site_list. If a grid download is requested lat_list and lon_list should at
##' minimum provide a pair of latitude and longitude values defining a range (not
##' corners) of a rectangular grid of points to be downloaded. Providing a list
##' of more than two point locations will result in a extracted region that
##' encompasses them all.
##' Providing a single set of coordinates will work but will (likely) result in
##' a 3x3 region surrounding the point requested. Only decimal coordinates, without cardinal
##' directions, are currently accepted.
##' @section Forecast Times
##' NOAA CFS forecasts are made once daily. ...
#Add frequency and duration!!!!!
##' @section Weather Debiasing
##' NOAA forecasts are made for a fixed grid. The meteorology at actual locations may differ
##' systemically from their nearest forecast location. Debiasing allows adjust the forecast based
##' on linear relationships between the forecast location and your site based on parameters you can
##' determine from your local meteorology. See \code{\link{debias_met_forecast}} for how to pass
##' debiasing parameters.
##'
##' @return None
##'
##' @export
##'
##' @author Quinn Thomas

#JMR_Notes:
# - There is a large amount of repetition in the documentation of this and noaa_gefs_download_downscale.
#The differences in paramters are the only obstacle to documenting them as a family.
# - run_parallel is not actually used.
# - Are the forecast_times relevant here? CFS is only generated once a day.
# - method is not used!  Is this for interface compatibility with noaa_gefs_download_downscale()?
# - Passing forecast_date = NA to noaa_cfs_grid_download() may be a problem.
# - Review wording for debias parameter description.

noaa_cfs_download_downscale <- function(site_list,
                                         lat_list,
                                         lon_list,
                                         output_directory,
                                         forecast_time = NA,
                                         forecast_date = NA,
                                         downscale = FALSE,
                                         debias = FALSE,
                                         debias_coefficients = NULL,
                                         run_parallel = FALSE,
                                         num_cores = 1,
                                         method = "point",
                                         overwrite = FALSE,
                                         grid_name = "neon"){

  model_name <- "NOAACFS_6hr"
  model_name_ds <-"NOAACFS_1hr" #Downscaled NOAA GEFS
  model_name_ds_debias <-"NOAACFS_1hr-debias" #Downscaled NOAA GEFS
  model_name_raw <- "NOAACFS_raw"

  message(paste0("Number of sites: ", length(site_list)))
  message(paste0("Overwrite existing files: ", overwrite))
  message(paste0("Running in parallel: ", run_parallel))
  message(paste0("downscale: ", downscale))
  message(paste0("debias: ", debias))

  #The grid download requires longitude to be -180 to 180 so converting here
  lon_list[which(lon_list > 180)] <- lon_list[which(lon_list > 180)] - 360

    Rnoaa4cast::noaa_cfs_grid_download(lat_list = lat_list,
                       lon_list = lon_list,
                       forecast_time = forecast_time,
                       forecast_date = forecast_date,
                       model_name_raw = model_name_raw,
                       output_directory = output_directory,
                       grid_name = grid_name)

    Rnoaa4cast::noaa_cfs_grid_process_downscale(lat_list = lat_list,
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
                                              num_cores = 1,
                                              output_directory = output_directory,
                                              grid_name = grid_name)
}
