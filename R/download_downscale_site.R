##' @title Download and Downscale NOAA GEFS for a single site
##' @return None
##'
##' @param site_index, index of site_list, lat_list, lon_list to be downloaded
##' @param lat_list, vector of latitudes that correspond to site codes
##' @param lon_list, vector of longitudes that correspond to site codes
##' @param site_list, vector of site codes, used in directory and file name generation
##' @param downscale, logical specifying whether to downscale from 6-hr to 1-hr
##' @param overwrite, logical stating to overwrite any existing output_file
##' @param model_name, directory name for the 6-hr forecast, this will be used in directory and file name generation
##' @param model_name_ds, directory name for the 1-hr forecast, this will be used in directory and file name generation
##' @param output_directory, directory where the model output will be save
##' @export
##'
##' @author Quinn Thomas
##'
##'


download_downscale_site <- function(lat_list,
                                    lon_list,
                                    site_list,
                                    forecast_time = NA,
                                    forecast_date = NA,
                                    downscale,
                                    overwrite,
                                    model_name,
                                    model_name_ds,
                                    output_directory){

  model_dir <- file.path(output_directory, model_name)


  #Create dimensions of the noaa forecast
  lon.dom <- seq(0, 359, by = 0.5) #domain of longitudes in model (1 degree resolution)
  lat.dom <- seq(-90, 90, by = 0.5) #domain of latitudes in model (1 degree resolution)



  #urls.out <- tryCatch(rNOMADS::GetDODSDates(abbrev = "gens_bc"),
  #                     error = function(e){
  #                       warning(paste(e$message, "NOAA Server not responsive"),
  #                               call. = FALSE)
  #                       return(NA)
  #                     },
  #                     finally = NULL)

  urls.out <- list()

  urls.out$model <- "gefs"
  #urls.out$date <- urls.out$date[4:7]
  urls.out$date <- format(c(Sys.Date() - 3, Sys.Date() - 2, Sys.Date() - 1, Sys.Date()), format = "%Y%m%d") 
  #urls.out$url <- paste0("https://nomads.ncep.noaa.gov:443/dods/gefs/gefs",urls.out$date)
  urls.out$url <- paste0("/opt/flare/shared/qnoaa/",urls.out$date)

  if(is.na(urls.out[1])) stop()

  urls.out$date_formated <- lubridate::as_date(urls.out$date)

  if(!is.na(forecast_date)){
    url_index <- which(urls.out$date_formated == lubridate::as_date(forecast_date))
  }else{
    url_index <- 1:length(urls.out$url)
  }

  for(i in url_index){

    model.url <- urls.out$url[i]
    start_date <- urls.out$date[i]

    model_list <- c("gefs_pgrb2ap5_all_00z", "gefs_pgrb2ap5_all_06z", "gefs_pgrb2ap5_all_12z", "gefs_pgrb2ap5_all_18z")
    model_hr <- c(0, 6, 12, 18)

#    model.runs <- tryCatch(rNOMADS::GetDODSModelRuns(model.url),
#                           error = function(e){
#                             warning(paste(e$message, "skipping", model.url),
#                                     call. = FALSE)
#                             return(NA)
#                           },
#                           finally = NULL)
    model.runs <- list()
    model.runs$model.run <- model.runs$model.run <- c("gec00_00z_pgrb2a", "gec00_00z_pgrb2b", "gec00_06z_pgrb2a",
                                                      "gec00_06z_pgrb2b", "gec00_12z_pgrb2a", "gec00_12z_pgrb2b",
                                                      "gec00_18z_pgrb2a", "gec00_18z_pgrb2b", "gefs_pgrb2ap5_all_00z",
                                                      "gefs_pgrb2ap5_all_06z", "gefs_pgrb2ap5_all_12z", "gefs_pgrb2ap5_all_18z",
                                                      "gep01_00z_pgrb2a", "gep01_06z_pgrb2a", "gep01_12z_pgrb2a",
                                                      "gep01_18z_pgrb2a", "gep02_00z_pgrb2a", "gep02_06z_pgrb2a",
                                                      "gep02_12z_pgrb2a", "gep02_18z_pgrb2a", "gep03_00z_pgrb2a",
                                                      "gep03_06z_pgrb2a", "gep03_12z_pgrb2a", "gep03_18z_pgrb2a",
                                                      "gep04_00z_pgrb2a", "gep04_06z_pgrb2a", "gep04_12z_pgrb2a",
                                                      "gep04_18z_pgrb2a", "gep05_00z_pgrb2a", "gep05_06z_pgrb2a",
                                                      "gep05_12z_pgrb2a", "gep05_18z_pgrb2a", "gep06_00z_pgrb2a",
                                                      "gep06_00z_pgrb2b", "gep06_06z_pgrb2a", "gep06_06z_pgrb2b",
                                                      "gep06_12z_pgrb2a", "gep06_12z_pgrb2b", "gep06_18z_pgrb2a",
                                                      "gep06_18z_pgrb2b", "gep07_00z_pgrb2a", "gep07_00z_pgrb2b",
                                                      "gep07_06z_pgrb2a", "gep07_06z_pgrb2b", "gep07_12z_pgrb2a",
                                                      "gep07_12z_pgrb2b", "gep07_18z_pgrb2a", "gep07_18z_pgrb2b",
                                                      "gep08_00z_pgrb2a", "gep08_06z_pgrb2a", "gep08_12z_pgrb2a",
                                                      "gep08_18z_pgrb2a", "gep09_00z_pgrb2a", "gep09_06z_pgrb2a",
                                                      "gep09_12z_pgrb2a", "gep09_18z_pgrb2a", "gep10_00z_pgrb2a",
                                                      "gep10_06z_pgrb2a", "gep10_12z_pgrb2a", "gep10_18z_pgrb2a",
                                                      "gep11_00z_pgrb2a", "gep11_06z_pgrb2a", "gep11_12z_pgrb2a",
                                                      "gep11_18z_pgrb2a", "gep12_00z_pgrb2a", "gep12_06z_pgrb2a",
                                                      "gep12_12z_pgrb2a", "gep12_18z_pgrb2a", "gep13_00z_pgrb2a",
                                                      "gep13_06z_pgrb2a", "gep13_12z_pgrb2a", "gep13_18z_pgrb2a",
                                                      "gep14_00z_pgrb2a", "gep14_06z_pgrb2a", "gep14_12z_pgrb2a",
                                                      "gep14_18z_pgrb2a", "gep15_00z_pgrb2a", "gep15_06z_pgrb2a",
                                                      "gep15_12z_pgrb2a", "gep15_18z_pgrb2a", "gep16_00z_pgrb2a",
                                                      "gep16_06z_pgrb2a", "gep16_12z_pgrb2a", "gep16_18z_pgrb2a",
                                                      "gep17_00z_pgrb2a", "gep17_06z_pgrb2a", "gep17_12z_pgrb2a",
                                                      "gep17_18z_pgrb2a", "gep18_00z_pgrb2a", "gep18_06z_pgrb2a",
                                                      "gep18_12z_pgrb2a", "gep18_18z_pgrb2a", "gep19_00z_pgrb2a",
                                                      "gep19_06z_pgrb2a", "gep19_12z_pgrb2a", "gep19_18z_pgrb2a",
                                                      "gep20_00z_pgrb2a", "gep20_06z_pgrb2a", "gep20_12z_pgrb2a",
                                                      "gep20_18z_pgrb2a", "gep21_00z_pgrb2a", "gep21_06z_pgrb2a",
                                                      "gep21_12z_pgrb2a", "gep21_18z_pgrb2a", "gep22_00z_pgrb2a",
                                                      "gep22_06z_pgrb2a", "gep22_12z_pgrb2a", "gep22_18z_pgrb2a",
                                                      "gep23_00z_pgrb2a", "gep23_06z_pgrb2a", "gep23_12z_pgrb2a",
                                                      "gep23_18z_pgrb2a", "gep24_00z_pgrb2a", "gep24_06z_pgrb2a",
                                                      "gep24_12z_pgrb2a", "gep24_18z_pgrb2a", "gep25_00z_pgrb2a",
                                                      "gep25_06z_pgrb2a", "gep25_12z_pgrb2a", "gep25_18z_pgrb2a",
                                                      "gep26_00z_pgrb2a", "gep26_06z_pgrb2a", "gep26_12z_pgrb2a",
                                                      "gep26_18z_pgrb2a", "gep27_00z_pgrb2a", "gep27_06z_pgrb2a",
                                                      "gep27_12z_pgrb2a", "gep27_18z_pgrb2a", "gep28_00z_pgrb2a",
                                                      "gep28_06z_pgrb2a", "gep28_12z_pgrb2a", "gep28_18z_pgrb2a",
                                                      "gep29_00z_pgrb2a", "gep29_00z_pgrb2b", "gep29_06z_pgrb2a",
                                                      "gep29_06z_pgrb2b", "gep29_12z_pgrb2a", "gep29_12z_pgrb2b",
                                                      "gep29_18z_pgrb2a", "gep29_18z_pgrb2b", "gep30_00z_pgrb2a",
                                                      "gep30_06z_pgrb2a", "gep30_12z_pgrb2a", "gep30_18z_pgrb2a")
    if(is.na(model.runs)[1]) next

    avail_runs <- model.runs$model.run[which(model.runs$model.run %in% model_list)]
    avail_runs_index <- which(model_list %in% avail_runs)

    model_list <- model_list[avail_runs_index]
    model_hr <- model_hr[avail_runs_index]
    if(!is.na(forecast_time)){
      hour_index <- which(model_hr %in% as.numeric(forecast_time))
      model_list <- model_list[hour_index]
    }


    for(m in 1:length(model_list)){

      run_hour <- stringr::str_sub(model_list[m], start = 19, end = 20)
      start_time <- lubridate::as_datetime(start_date) + lubridate::hours(as.numeric(run_hour))
      end_time <- start_time + lubridate::days(16)

      for(site_index in 1:length(site_list)){

        #Convert negetive longitudes to degrees east
        if(lon_list[site_index] < 0){
          lon_east <- 360 + lon_list[site_index]
        }else{
          lon_east <- lon_list[site_index]
        }

      model_site_date_hour_dir <- file.path(model_dir, site_list[site_index], lubridate::as_datetime(start_date) ,run_hour)
      if(!dir.exists(model_site_date_hour_dir)){
        dir.create(model_site_date_hour_dir, recursive=TRUE, showWarnings = FALSE)
      }

      identifier <- paste(model_name, site_list[site_index], format(start_time, "%Y-%m-%dT%H"),
                          format(end_time, "%Y-%m-%dT%H"), sep="_")

      #Check if already downloaded
      if(length(list.files(model_site_date_hour_dir)) != 31){

        print(paste("Downloading", site_list[site_index], format(start_time, "%Y-%m-%dT%H")))

        if(is.na(model.runs)[1]) next

        #check if available at NOAA
        if(model_list[m] %in% model.runs$model.run){

          model.run <- model.runs$model.run[which(model.runs$model.run == model_list[m])]

          noaa_var_names <- c("tmp2m", "pressfc", "rh2m", "dlwrfsfc",
                              "dswrfsfc", "apcpsfc",
                              "ugrd10m", "vgrd10m")

          #These are the cf standard names
          cf_var_names <- c("air_temperature", "air_pressure", "relative_humidity", "surface_downwelling_longwave_flux_in_air",
                            "surface_downwelling_shortwave_flux_in_air", "precipitation_flux", "eastward_wind", "northward_wind")

          #Replace "eastward_wind" and "northward_wind" with "wind_speed"
          cf_var_names1 <- c("air_temperature", "air_pressure", "relative_humidity", "surface_downwelling_longwave_flux_in_air",
                             "surface_downwelling_shortwave_flux_in_air", "precipitation_flux","specific_humidity", "wind_speed")

          cf_var_units1 <- c("K", "Pa", "1", "Wm-2", "Wm-2", "kgm-2s-1", "1", "ms-1")  #Negative numbers indicate negative exponents

          noaa_data <- list()

          download_issues <- FALSE

          for(j in 1:length(noaa_var_names)){

            #For some reason rNOMADS::GetDODSDates doesn't return "gens" even
            #though it is there
            curr_model.url <- model.url

            lon <- which.min(abs(lon.dom - lon_east)) - 1 #NOMADS indexes start at 0
            lat <- which.min(abs(lat.dom - lat_list[site_index])) - 1 #NOMADS indexes start at 0

            noaa_data[[j]] <- tryCatch(rNOMADS::DODSGrab(model.url = curr_model.url,
                                                         model.run = model.run,
                                                         variables	= noaa_var_names[j],
                                                         time = c(0, 64),
                                                         lon = lon,
                                                         lat = lat,
                                                         ensembles=c(0, 30)),
                                       error = function(e){
                                         warning(paste(e$message, "skipping", curr_model.url, model.run, noaa_var_names[j]),
                                                 call. = FALSE)
                                         return(NA)
                                       },
                                       finally = NULL)

            if(is.na(noaa_data[[j]][1])){
              download_issues <- TRUE
            }else if(length(unique(noaa_data[[j]]$value)) == 1){
              #ONLY HAS 9.999e+20 which is the missing value
              download_issues <- TRUE
            }else{
              #For some reason it defaults to the computer's time zone, convert to UTC
              noaa_data[[j]]$forecast.date <- lubridate::with_tz(noaa_data[[j]]$forecast.date,
                                                                 tzone = "UTC")
            }
          }

          if(download_issues == TRUE){
            warning(paste("Error downloading one of the variables: ", curr_model.url, model.run))
            next
          }

          names(noaa_data) <- cf_var_names

          specific_humidity <- rh2qair(rh = noaa_data$relative_humidity$value / 100,
                                       T = noaa_data$air_temperature$value,
                                       press = noaa_data$air_pressure$value)

          #Calculate wind speed from east and north components
          wind_speed <- sqrt(noaa_data$eastward_wind$value^2 + noaa_data$northward_wind$value^2)

          forecast_noaa <- tibble::tibble(time = noaa_data$air_temperature$forecast.date,
                                          NOAA.member = noaa_data$air_temperature$ensembles,
                                          air_temperature = noaa_data$air_temperature$value,
                                          air_pressure= noaa_data$air_pressure$value,
                                          relative_humidity = noaa_data$relative_humidity$value,
                                          surface_downwelling_longwave_flux_in_air = noaa_data$surface_downwelling_longwave_flux_in_air$value,
                                          surface_downwelling_shortwave_flux_in_air = noaa_data$surface_downwelling_shortwave_flux_in_air$value,
                                          precipitation_flux = noaa_data$precipitation_flux$value,
                                          specific_humidity = specific_humidity,
                                          wind_speed = wind_speed)

          #9.999e+20 is the missing value so convert to NA
          forecast_noaa$surface_downwelling_longwave_flux_in_air[forecast_noaa$surface_downwelling_longwave_flux_in_air == 9.999e+20] <- NA
          forecast_noaa$surface_downwelling_shortwave_flux_in_air[forecast_noaa$surface_downwelling_shortwave_flux_in_air == 9.999e+20] <- NA
          forecast_noaa$precipitation_flux[forecast_noaa$precipitation_flux == 9.999e+20] <- NA

          forecast_noaa$relative_humidity <- forecast_noaa$relative_humidity / 100 #Convert from % to proportion

          # Convert the 6 hr precip rate to per second.
          forecast_noaa$precipitation_flux <- forecast_noaa$precipitation_flux / (60 * 60 * 6)


          for (ens in 1:31) { # i is the ensemble number

            #Turn the ensemble number into a string
            if((ens-1)< 10){
              ens_name <- paste0("0",ens-1)
            }else{
              ens_name <- ens-1
            }

            forecast_noaa_ens <- forecast_noaa %>%
              dplyr::filter(NOAA.member == ens) %>%
              dplyr::filter(!is.na(air_temperature))

            end_date <- forecast_noaa_ens %>%
              dplyr::summarise(max_time = max(time))

            identifier <- paste(model_name, site_list[site_index], format(dplyr::first(forecast_noaa_ens$time), "%Y-%m-%dT%H"),
                                format(end_date$max_time, "%Y-%m-%dT%H"), sep="_")

            fname <- paste0(identifier,"_ens",ens_name,".nc")
            output_file <- file.path(model_site_date_hour_dir,fname)

            #Write netCDF
            noaaGEFSpoint::write_noaa_gefs_netcdf(df = forecast_noaa_ens,ens, lat = lat_list[site_index], lon = lon_east, cf_units = cf_var_units1, output_file = output_file, overwrite = overwrite)

            if(downscale){
              #Downscale the forecast from 6hr to 1hr
              modelds_site_date_hour_dir <- file.path(output_directory,model_name_ds,site_list[site_index],lubridate::as_date(start_date),run_hour)

              if(!dir.exists(modelds_site_date_hour_dir)){
                dir.create(modelds_site_date_hour_dir, recursive=TRUE, showWarnings = FALSE)
              }

              identifier_ds <- paste(model_name_ds, site_list[site_index], format(dplyr::first(forecast_noaa_ens$time), "%Y-%m-%dT%H"),
                                     format(end_date$max_time, "%Y-%m-%dT%H"), sep="_")

              fname_ds <- file.path(modelds_site_date_hour_dir, paste0(identifier_ds,"_ens",ens_name,".nc"))

              #Run downscaling
              noaaGEFSpoint::temporal_downscale(input_file = output_file, output_file = fname_ds, overwrite = overwrite, hr = 1)
            }
          }
        }
      }else{
        print(paste("Existing", site_list[site_index], start_time))
      }
      }
    }
  }
}
