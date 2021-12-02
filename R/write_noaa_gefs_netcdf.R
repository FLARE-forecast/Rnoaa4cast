##' @title Write a (NOAA GEFS?) forecast to netCDF
##'
##' @description This function takes a data frame containing meteorological forecast data and
##' related metadata and saves it to a netCDF file.
##'
##' @param df A data frame of meteorological variables to be written to a netCDF file. Columns
##' must start with 'time' with the following columns in the order of `cf_units`.
##' @param ens Optional: An ensemble index used for subsetting df.
##' @param lat Latitude for the forecast location in degree north.
##' @param lon Longitude for the forecast location in degree east.
##' @param cf_units Vector of variable unit descriptor strings in the order they appear in df.
##' @param output_file Name, with full path, of the netcdf file that is generated.
##' @param overwrite Logical stating whether to overwrite an existing output file.
##'
##' @return None
##'
##' @export
##'
##' @author Quinn Thomas
##'

#JMR_NOTES:
# - There is nothing GEFS specific about this function, it is fairly generic, and is used by
#noaa_cfs_grid_process_downscale().  The name should probably be changed to reflect this.
# - Should this be private?
# - Why 'cf' in cf_units?
# - Providing a named vector for cf_units might allow for a more robust implementation.
# - The value of ens is not used, but its presence is. What is it doing?

write_noaa_gefs_netcdf <- function(df, ens = NA, lat, lon, cf_units, output_file, overwrite){

  if(!is.na(ens)){
    data <- df
    max_index <- max(which(!is.na(data$air_temperature)))
    start_time <- min(data$time)
    end_time <- data$time[max_index]

    data <- data %>% dplyr::select(-c("time", "NOAA.member"))
  }else{
    data <- df
    max_index <- max(which(!is.na(data$air_temperature)))
    start_time <- min(data$time)
    end_time <- data$time[max_index]

    #JMR_NOTE: Won't this cause problems with calculating diff_time?
    data <- df %>%
      dplyr::select(-c("time"))
  }

  diff_time <- as.numeric(difftime(df$time, df$time[1])) / (60 * 60)

  cf_var_names <- names(data)

  #Define dimensions:
  time_dim <- ncdf4::ncdim_def(name="time",
                               units = paste("hours since", format(start_time, "%Y-%m-%d %H:%M")),
                               diff_time, #GEFS forecast starts 6 hours from start time
                               create_dimvar = TRUE)
  lat_dim <- ncdf4::ncdim_def("latitude", "degree_north", lat, create_dimvar = TRUE)
  lon_dim <- ncdf4::ncdim_def("longitude", "degree_east", lon, create_dimvar = TRUE)

  dimensions_list <- list(time_dim, lat_dim, lon_dim)

  nc_var_list <- list()
  for (i in 1:length(cf_var_names)) { #Each ensemble member will have data on each variable stored in their respective file.
    nc_var_list[[i]] <- ncdf4::ncvar_def(cf_var_names[i], cf_units[i], dimensions_list, missval=NaN)
  }

  #Write the netCDF:
  #JMR_NOTE: Add reporting for existing file and change | to ||.
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
