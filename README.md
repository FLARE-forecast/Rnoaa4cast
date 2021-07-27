# noaaGEFSpoint

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

