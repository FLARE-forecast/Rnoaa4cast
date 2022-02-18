# Rnoaa4cast

The Rnoaa4cast is an R package that helps you to download and process NOAA weather forecasts for use
in ecological forecasting.

It can be used to:
- Obtain NOAA Global Ensemble Forecast System (GEFS) for regions or point locations.
- Obtain NOAA Climate (Coupled) Forecast System (CFS) forecasts.
- Extract points from regional forecasts.
- Temporally downscale forecasts to hourly timescales.
- Spatially downscale (debias) gridded forecasts to more accurately match specific point locations.
- Convert forecasts from their raw formats, that may span thousands of files, to compact netCDF
versions.

## Getting Started

### Package Installation (from GitHub)
```
install.packages("devtools")
devtools::install_github("FLARE-forecast/Rnoaa4cast")
```

### Download your First Forecast
...


<!--- The following is the original example but it won't work if you don't have the tidyverse
installed and the noaa_gefs_download_downscale() function call is out of date.

Install from GitHub
```devtools::install_github("rqthomas/noaaGEFSpoint")```

Set output directory
 
```output_directory <- "/Users/quinn/Downloads/GEFS_test"```

Read list of latitude and longitudes.  The example uses NEON data that is included in the
package

```site_file <- system.file("extdata", "noaa_download_site_list.csv", package = "noaaGEFSpoint")```

```neon_sites <- read_csv(site_file)```

Download and temporally downscale forecasts

```
noaaGEFSpoint::noaa_gefs_download_downscale(site_list = neon_sites$site_id,
                             lat_list = neon_sites$latitude,
                             lon_list = neon_sites$longitude,
                             output_directory,
                             forecast_time = "all",
                             forecast_date = "all",
                             latest = FALSE,
                             downscale = TRUE,
                             run_parallel = FALSE,
                             num_cores = 1,
                             overwrite = FALSE)
```
--->
