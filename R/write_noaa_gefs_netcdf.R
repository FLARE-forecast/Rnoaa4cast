##' @title Write NOAA GEFS netCDF
##' @param df data frame of meterological variables to be written to netcdf.  Columns
##' must start with time with the following columns in the order of `cf_units`
##' @param ens ensemble index used for subsetting df
##' @param lat latitude in degree north
##' @param lon longitude in degree east
##' @param cf_units vector of variable names in order they appear in df
##' @param output_file name, with full path, of the netcdf file that is generated
##' @param overwrite logical to overwrite existing netcdf file
##' @return NA
##'
##' @export
##'
##' @author Quinn Thomas
##'
##'

write_noaa_gefs_netcdf <- function(df, ens = NA, lat, lon, cf_units, output_file, overwrite){

  if(!is.na(ens)){
    data <- df %>%
      dplyr::filter(NOAA.member == ens)

    max_index <- max(which(!is.na(data$air_temperature)))
    start_time <- min(data$time)
    end_time <- data$time[max_index]

    data <- data %>% dplyr::select(-c("time", "NOAA.member"))
  }else{
    data <- df

    max_index <- max(which(!is.na(data$air_temperature)))
    start_time <- min(data$time)
    end_time <- data$time[max_index]

    data <- df %>%
      dplyr::select(-c("time"))
  }

  diff_time <- as.numeric(difftime(df$time[2], df$time[1]))

  cf_var_names <- names(data)

  time_dim <- ncdf4::ncdim_def(name="time",
                               units = paste("hours since", format(start_time, "%Y-%m-%d %H:%M")),
                               seq(from = 0, length.out = nrow(data), by = diff_time), #GEFS forecast starts 6 hours from start time
                               create_dimvar = TRUE)
  lat_dim <- ncdf4::ncdim_def("latitude", "degree_north", lat, create_dimvar = TRUE)
  lon_dim <- ncdf4::ncdim_def("longitude", "degree_east", lon, create_dimvar = TRUE)

  dimensions_list <- list(time_dim, lat_dim, lon_dim)

  nc_var_list <- list()
  for (i in 1:length(cf_var_names)) { #Each ensemble member will have data on each variable stored in their respective file.
    nc_var_list[[i]] <- ncdf4::ncvar_def(cf_var_names[i], cf_units[i], dimensions_list, missval=NaN)
  }

  if (!file.exists(output_file) | overwrite) {
    nc_flptr <- ncdf4::nc_create(output_file, nc_var_list, verbose = FALSE)

    #For each variable associated with that ensemble
    for (j in 1:ncol(data)) {
      # "j" is the variable number.  "i" is the ensemble number. Remember that each row represents an ensemble
      ncdf4::ncvar_put(nc_flptr, nc_var_list[[j]], unlist(data[,j]))
    }

    ncdf4::nc_close(nc_flptr)  #Write to the disk/storage
  }
}
