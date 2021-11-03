test_that("point download works", {
  output_directory <- "downloads"
  dir.create(output_directory, showWarnings = FALSE)
  Rnoaa4cast::noaa_gefs_point_download_downscale(read_from_path = FALSE,
                                                    lat_list = 37.31,
                                                    lon_list = 280.16,
                                                    site_list = "FCRE",
                                                    forecast_time = "00",
                                                    forecast_date = Sys.Date() - 1,
                                                    downscale = FALSE,
                                                    overwrite = FALSE,
                                                    model_name = "NOAAGEFS_6hr",
                                                    model_name_ds = "NOAAGEFS_1hr",
                                                    output_directory = output_directory)

  fils <- list.files(file.path(output_directory, "NOAAGEFS_6hr", "FCRE", (Sys.Date() - 1), "00"))

  testthat::expect_equal(length(fils), 31)
s})
