##' @title Script to launch NOAA download and temporal downscaling
##' @return None
## @param site_list, vector of site codes, used in directory and file name generation
##' @param lat_list, vector of latitudes that correspond to site codes
##' @param lon_list, vector of longitudes that correspond to site codes
##' @param output_directory, directory where the model output will be save
##' @param downscale, logical specifying whether to downscale from 6-hr to 1-hr
##' @param run_parallel, logical whether to run on multiple cores
##' @param num_cores, number of cores used if run_parallel == TRUE
##' @param overwrite, logical stating to overwrite any existing output_file

##' @export
##' @export
##'
##' @author Quinn Thomas
##'
##'

noaa_gefs_download_downscale <- function(site_list,
                                         lat_list,
                                         lon_list,
                                         output_directory,
                                         forecast_time = "latest",
                                         forecast_date = "latest",
                                         downscale = FALSE,
                                         debias = FALSE,
                                         debias_coefficients = NULL,
                                         run_parallel = FALSE,
                                         num_cores = 1,
                                         method = "point",
                                         overwrite = FALSE){

  model_name <- "NOAAGEFS_6hr"
  model_name_ds <-"NOAAGEFS_1hr" #Downscaled NOAA GEFS
  model_name_ds_debias <-"NOAAGEFS_1hr-debias" #Downscaled NOAA GEFS
  model_name_raw <- "NOAAGEFS_raw"

  message(paste0("Number of sites: ", length(site_list)))
  message(paste0("Overwrite existing files: ", overwrite))
  message(paste0("Running in parallel: ", run_parallel))
  message(paste0("downscale: ", downscale))
  message(paste0("debias: ", debias))

  if(method == "point"){

    message("downloading NOAA using single point method.  Note: only the first 16 days of a 35-day forecast are able to be downloading using this method")

    #Create cluster
    message(paste0("Number of cores specified: ", num_cores))
    if(num_cores > parallel::detectCores()){
      #Docker sets the max number of cores, if the request is for more, set to
      #what docker allows
      num_cores <- parallel::detectCores()

    }
    print(message("Number of cores allocated: ", num_cores))

    for(i in 1:length(site_list)){
      noaaGEFSpoint::download_downscale_site(
        site_index = i,
        lat_list = lat_list,
        lon_list = lon_list,
        site_list = site_list,
        forecast_time = forecast_time,
        forecast_date = forecast_date,
        downscale = downscale,
        overwrite = overwrite,
        model_name = model_name,
        model_name_ds = model_name_ds,
        output_directory = output_directory
      )
    }

    #site_index <- 1:length(site_list)
    #Run download_downscale_site() over the site_index
    #parallel::mclapply(X = site_index,
    #                   FUN = noaaGEFSpoint::download_downscale_site,
    #                   lat_list = lat_list,
    #                   lon_list = lon_list,
    #                   site_list = site_list,
    #                   forecast_time = forecast_time,
    #                   forecast_date = forecast_date,
    #                   downscale = downscale,
    #                   overwrite = overwrite,
    #                   model_name = model_name,
    #                   model_name_ds = model_name_ds,
    #                   output_directory = output_directory,
    #                   mc.cores = 1)

  }else{

    noaaGEFSpoint::noaa_grid_download(lat_list = lat_list,
                       lon_list = lon_list,
                       forecast_time = forecast_time,
                       forecast_date = forecast_date,
                       model_name_raw = model_name_raw,
                       num_cores = 1,
                       output_directory = output_directory)

    noaaGEFSpoint::process_gridded_noaa_download(lat_list = lat_list,
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
                                              output_directory = output_directory)
  }
}
