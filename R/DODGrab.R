#' Title
#'
#' @param model.url
#' @param model.run
#' @param variables
#' @param time
#' @param lon
#' @param lat
#' @param levels
#' @param ensembles
#' @param display.url
#' @param verbose
#' @param request.sleep
#'
#' @return
#' @export
#'
#' @examples
DODSGrab <- function (model.url, model.run, variables, time, lon, lat, levels = NULL,
                      ensembles = NULL, display.url = TRUE, verbose = FALSE, request.sleep = 1){
  prev.digits <- options("digits")
  options(digits = 8)
  model.data <- list(model.run.date = NULL, forecast.date = NULL,
                     variables = NULL, levels = NULL, ensembles = NULL, lon = NULL,
                     lat = NULL, value = NULL, request.url = NULL)
  for (variable in variables) {
    preamble <- paste0(model.url, "/", model.run, ".ascii?",
                       variable)
    time.str <- paste0("[", paste0(time, collapse = ":"),
                       "]")
    l.ind <- !is.null(levels)
    if (l.ind) {
      level.str <- paste0("[", paste0(levels, collapse = ":"),
                          "]")
    }
    else {
      level.str <- ""
    }
    e.ind <- !is.null(ensembles)
    if (e.ind) {
      ensembles.str <- paste0("[", paste0(ensembles, collapse = ":"),
                              "]")
    }
    else {
      ensembles.str <- ""
    }
    lat.str <- paste0("[", paste0(lat, collapse = ":"), "]")
    lon.str <- paste0("[", paste0(lon, collapse = ":"), "]")
    data.url <- paste0(preamble, ensembles.str, time.str,
                       level.str, lat.str, lon.str)
    if (display.url) {
      print(data.url)
    }
    data.txt.raw <- httr::GET(url = data.url)
    if (grepl("[eE][rR][rR][oO][rR]", data.txt.raw)) {
      warning(paste0("There may have been an error retrieving data from the NOMADS server.  HTML text is as follows\n",
                     data.txt.raw))
    }
    data.txt <- unlist(strsplit(data.txt.raw, split = "\\n"))
    lats <- as.numeric(unlist(strsplit(data.txt[grep("^lat,",
                                                     data.txt) + 1], split = ",")))
    lons <- as.numeric(unlist(strsplit(data.txt[grep("^lon,",
                                                     data.txt) + 1], split = ",")))
    if (l.ind) {
      levels.out <- as.numeric(unlist(strsplit(data.txt[grep("^lev,",
                                                             data.txt) + 1], split = ",")))
    }
    if (e.ind) {
      ens.out <- as.numeric(unlist(strsplit(data.txt[grep("^ens,",
                                                          data.txt) + 1], split = ",")))
      r.end <- grep("^ens,", data.txt)
    }
    else {
      r.end <- grep("^time,", data.txt)
    }
    t.ind <- grep("^time,", data.txt)
    prev.digits <- options("digits")
    options(digits = 15)
    num.times <- as.numeric(unlist(strsplit(data.txt[t.ind +
                                                       1], split = ",")))
    options(digits = prev.digits$digits)
    times <- as.POSIXlt(as.Date(num.times - 2, origin = "1-1-1"),
                        tz = "GMT") + 3600 * 24 * (num.times - floor(num.times))
    val.dim <- unlist(stringr::str_extract_all(unlist(strsplit(data.txt[1],
                                                               ","))[2], "\\d"))
    val.txt <- data.txt[2:(r.end - 1)]
    val.txt <- val.txt[val.txt != ""]
    val.txt <- stringr::str_replace_all(val.txt, "\\]\\[",
                                        ",")
    val.txt <- stringr::str_replace_all(val.txt, c("\\]|\\["),
                                        "")
    model.run.date <- paste0(stringr::str_extract(model.url,
                                                  "[1-2]\\d{3}[0-1]\\d{1}[0-3]\\d{1}$"), model.run)
    row.num <- (as.numeric(val.dim[2]) + l.ind + e.ind) *
      length(val.txt)
    r.start <- 3 + l.ind + e.ind
    for (k in seq_len(length(val.txt))) {
      val.tmp <- sapply(strsplit(val.txt[k], split = ","),
                        as.numeric)
      r.end <- length(val.tmp)
      get.rows <- r.end - r.start + 1
      model.data$model.run.date <- append(model.data$model.run.date,
                                          rep(model.run.date, get.rows))
      model.data$forecast.date <- append(model.data$forecast.date,
                                         rep(times[val.tmp[1 + e.ind] + 1], get.rows))
      model.data$variables <- append(model.data$variables,
                                     rep(variable, get.rows))
      if (l.ind) {
        model.data$levels <- append(model.data$levels,
                                    rep(levels.out[val.tmp[2 + e.ind] + 1], get.rows))
      }
      if (e.ind) {
        model.data$ensembles <- append(model.data$ensembles,
                                       rep(ens.out[val.tmp[1] + 1], get.rows))
      }
      model.data$lon <- append(model.data$lon, lons)
      model.data$lat <- append(model.data$lat, rep(lats[val.tmp[2 +
                                                                  l.ind + e.ind] + 1], get.rows))
      model.data$value <- append(model.data$value, val.tmp[r.start:r.end])
    }
    if (!l.ind) {
      model.data$levels <- rep("Level not defined", length(model.data$value))
    }
    if (!e.ind) {
      model.data$ensembles <- rep("Ensemble not defined",
                                  length(model.data$value))
    }
    model.data$request.url <- append(model.data$request.url,
                                     data.url)
    if (length(variables) > 1) {
      Sys.sleep(request.sleep)
    }
  }
  return(model.data)
}
