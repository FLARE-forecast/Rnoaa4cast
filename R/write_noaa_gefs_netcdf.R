##' @title Write NOAA GEGS netCDF 
##' @return NA
##'
##' @param df, dataframe of data written to netCDF
##' @param VarNames, names of vars that are state variables
##' @export
##' 
##' @author Quinn Thomas
##' 
##' 

write_noaa_gefs_netcdf <- function(df,ens, lat, lon, cf_units, output_file, overwrite){
  
  start_time <- min(df$time)
  end_time <- max(df$time)
  
  data <- df %>% 
    dplyr::filter(NOAA.member == ens) %>% 
    dplyr::select(-c("NOAA.member", "time"))
  
  diff_time <- as.numeric(difftime(df$time[2], df$time[1]))
    
  cf_var_names <- names(data)
  
  time_dim <- ncdf4::ncdim_def(name="time", 
                               units = paste("hours since", format(start_time, "%Y-%m-%dT%H:%M")), 
                               seq(from = 0, length.out = nrow(data), by = diff_time), #GEFS forecast starts 6 hours from start time 
                               create_dimvar = TRUE)
lat_dim <- ncdf4::ncdim_def("latitude", "degree_north", lat, create_dimvar = TRUE)
lon_dim <- ncdf4::ncdim_def("longitude", "degree_east", lon, create_dimvar = TRUE)

dimensions_list <- list(time_dim, lat_dim, lon_dim)

nc_var_list <- list()
for (i in 1:length(cf_var_names)) { #Each ensemble member will have data on each variable stored in their respective file.
  nc_var_list[[i]] <- ncdf4::ncvar_def(cf_var_names[i], cf_units[i], dimensions_list, missval=NaN)
}

  if(ens < 10){
    ens_name <- paste0("0",ens)
  }else{
    ens_name <- ens
  }
  
  if (!file.exists(output_file) | overwrite) {
    nc_flptr <- ncdf4::nc_create(output_file, nc_var_list, verbose = FALSE)
    
    #For each variable associated with that ensemble
    for (j in 1:ncol(data)) {
      # "j" is the variable number.  "i" is the ensemble number. Remember that each row represents an ensemble
      ncdf4::ncvar_put(nc_flptr, nc_var_list[[j]], unlist(data[,j]))
    }
    
    ncdf4::nc_close(nc_flptr)  #Write to the disk/storage
  } else {
    #PEcAn.logger::logger.info(paste0("The file ", flname, " already exists.  It was not overwritten."))
  }
}