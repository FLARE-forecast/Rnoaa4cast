
#' Download gridded forecast in the box bounded by the latitude and longitude list
#'
#' @param lat_list
#' @param lon_list
#' @param forecast_time
#' @param forecast_date
#' @param model_name_raw
#' @param num_cores
#' @param output_directory
#'
#' @return
#' @export
#'
#' @examples
noaa_grid_download <- function(lat_list, lon_list, forecast_time, forecast_date ,model_name_raw, num_cores, output_directory) {


  download_neon_grid <- function(ens_index, location, directory, hours_char, cycle, base_filename1, vars, working_directory){
    #for(j in 1:31){
    if(ens_index == 1){
      base_filename2 <- paste0("gec00",".t",cycle,"z.pgrb2a.0p50.f")
      curr_hours <- hours_char[hours <= 384]
    }else{
      if((ens_index-1) < 10){
        ens_name <- paste0("0",ens_index - 1)
      }else{
        ens_name <- as.character(ens_index -1)
      }
      base_filename2 <- paste0("gep",ens_name,".t",cycle,"z.pgrb2a.0p50.f")
      curr_hours <- hours_char
    }


    for(i in 1:length(curr_hours)){
      file_name <- paste0(base_filename2, curr_hours[i])

      destfile <- paste0(working_directory,"/", file_name,".neon.grib")

      if(file.exists(destfile)){
        size <- file.info(destfile)
      }else{
        size <- NA
      }

      if(!file.exists(destfile) | is.na(size) | size == 0){

        out <- tryCatch(download.file(paste0(base_filename1, file_name, vars, location, directory),
                                      destfile = destfile, quiet = TRUE),
                        error = function(e){
                          warning(paste(e$message, "skipping", file_name),
                                  call. = FALSE)
                          return(NA)
                        },
                        finally = NULL)

        if(is.na(out)) next
      }
    }
  }

  model_dir <- file.path(output_directory, model_name_raw)

  curr_time <- lubridate::with_tz(Sys.time(), tzone = "UTC")
  curr_date <- lubridate::as_date(curr_time)
  potential_dates <- seq(curr_date - lubridate::days(6), curr_date, by = "1 day")

  #Remove dates before the new GEFS system
  potential_dates <- potential_dates[which(potential_dates > lubridate::as_date("2020-09-23"))]

  location <- paste0("&subregion=&leftlon=",
                     floor(min(lon_list)),
                     "&rightlon=",
                     ceiling(max(lon_list)),
                     "&toplat=",
                     ceiling(max(lat_list)),
                     "&bottomlat=",
                     floor(min(lat_list)))

  base_filename1 <- "https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p50a.pl?file="
  vars <- "&lev_10_m_above_ground=on&lev_2_m_above_ground=on&lev_surface=on&lev_entire_atmosphere=on&var_APCP=on&var_DLWRF=on&var_DSWRF=on&var_PRES=on&var_RH=on&var_TMP=on&var_UGRD=on&var_VGRD=on&var_TCDC=on"

  for(i in 1:length(potential_dates)){

    forecast_date <- lubridate::as_date(potential_dates[i])
    forecast_hours <- c(0,6,12,18)

    for(j in 1:length(forecast_hours)){
      cycle <- forecast_hours[j]
      if(forecast_date ==  lubridate::as_date(curr_time) & cycle > lubridate::hour(curr_time)){

        next

      }else{

        if(cycle < 10) cycle <- paste0("0",cycle)

        model_date_hour_dir <- file.path(model_dir,forecast_date,cycle)
        if(!dir.exists(model_date_hour_dir)){
          dir.create(model_date_hour_dir, recursive=TRUE, showWarnings = FALSE)
          new_download <- TRUE
        }else{
          new_download <- TRUE
          file <- list.files(model_date_hour_dir)
          present_ensemble <- str_sub(file, start = 4, end = 5)
          present_times <- as.numeric(str_sub(file, start = 25, end = 27))
          if(length(unique(present_ensemble)) == 31){
            if(cycle == "00"){
              if(max(present_times) == 840 & length(which(present_times == 384)) == 31){
                new_download <- FALSE
              }
              else{
                if( length(which(present_times == 384)) == 31){
                  new_download <- FALSE
                }
              }
            }
          }
        }

        new_download <- TRUE

        if(new_download){

          print(paste("Downloading", forecast_date, cycle))

          if(cycle == "00"){
            hours <- c(seq(0, 240, 3),seq(246, 840 , 6))
          }else{
            hours <- c(seq(0, 240, 3),seq(246, 384 , 6))
          }
          hours_char <- hours
          hours_char[which(hours < 100)] <- paste0("0",hours[which(hours < 100)])
          hours_char[which(hours < 10)] <- paste0("0",hours_char[which(hours < 10)])
          curr_year <- lubridate::year(forecast_date)
          curr_month <- lubridate::month(forecast_date)
          if(curr_month < 10) curr_month <- paste0("0",curr_month)
          curr_day <- lubridate::day(forecast_date)
          if(curr_day < 10) curr_day <- paste0("0",curr_day)
          curr_date <- paste0(curr_year,curr_month,curr_day)
          directory <- paste0("&dir=%2Fgefs.",curr_date,"%2F",cycle,"%2Fatmos%2Fpgrb2ap5")

          ens_index <- 1:31

          parallel::mclapply(X = ens_index,
                             FUN = download_neon_grid,
                             location,
                             directory,
                             hours_char,
                             cycle,
                             base_filename1,
                             vars,
                             working_directory = model_date_hour_dir,
                             mc.cores = num_cores)
        }else{
          print(paste("Existing", forecast_date, cycle))
        }
      }
    }
  }
}
