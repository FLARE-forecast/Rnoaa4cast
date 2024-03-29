% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/noaa_gefs_download_downscale.R
\name{noaa_gefs_download_downscale}
\alias{noaa_gefs_download_downscale}
\title{Download NOAA GEFS Forecasts}
\usage{
noaa_gefs_download_downscale(
  site_list,
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
  read_from_path = FALSE,
  grid_name = "neon",
  process_specific_date = NA,
  process_specific_cycle = NA,
  delete_bad_files = TRUE,
  write_intermediate_ncdf = TRUE
)
}
\arguments{
\item{site_list}{Vector of site codes, e.g. "NOAA", used in directory and file name generation.}

\item{lat_list}{Vector or range of latitudes to be downloaded (see details).}

\item{lon_list}{Vector or range of longitudes to be downloaded (see details).}

\item{output_directory}{Path: directory where the model output will be saved.}

\item{forecast_time}{The 'hour' of the requested forecast, one of "00", "06", "12", or "18" (see
details). If omitted all times will be downloaded.}

\item{forecast_date}{The date, or coercible string, of the requested forecast. ((Defaults to the
most recent date.))}

\item{downscale}{Logical specifying whether to downscale from 6-hr to 1-hr data.}

\item{debias}{Logical specifying whether weather data should be adjusted
for a bias verses the nearest forecast point.}

\item{debias_coefficients}{If debias = TRUE, a data frame of debasing parameter value lists (see
details).}

\item{run_parallel}{Logical: whether to run on multiple cores.}

\item{num_cores}{Integer: number of cores used if run_parallel = TRUE.}

\item{method}{Character string indicating the download method, either "point" or "grid".}

\item{overwrite}{Logical stating whether to overwrite any existing output files.}

\item{read_from_path}{Point mode only:}

\item{grid_name}{Grid mode only: a short grid name used in directory and file name generation.}

\item{process_specific_date}{Grid mode only: A date, or coercible string, specifying specific
date(s) within the forecast to extract. If omitted all dates will be downloaded.}

\item{process_specific_cycle}{Grid mode only: A vector of forecast times ("00", "06", "12", or
"18" hours), to extract. If omitted all cycles will be processed.}

\item{delete_bad_files}{Grid mode only: Logical: delete bad files?}

\item{write_intermediate_ncdf}{Grid mode only: Logical: retain the intermediate netCDF files
created during processing?}
}
\value{
None
}
\description{
Downloads a set of NOAA Global Ensemble Forecast System (GEFS)
forecasts for a set of point locations or spatial ranges, downscaling them in
time if requested.
}
\author{
Quinn Thomas
}
