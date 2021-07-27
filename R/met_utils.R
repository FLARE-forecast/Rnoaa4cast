##' Convert specific humidity to relative humidity
##'
##' converting specific humidity into relative humidity
##' NCEP surface flux data does not have RH
##' from Bolton 1980 Teh computation of Equivalent Potential Temperature
##' \url{http://www.eol.ucar.edu/projects/ceop/dm/documents/refdata_report/eqns.html}
##' @title qair2rh
##' @param qair specific humidity, dimensionless (e.g. kg/kg) ratio of water mass / total air mass
##' @param temp degrees K
##' @param press pressure in Pa
##' @return rh relative humidity, ratio of actual water mixing ratio to saturation mixing ratio (0-1)
##' @noRd
##' @author David LeBauer Quinn Thomas
qair2rh <- function(qair, T, press = 101325) {
  press_hPa <- press / 100
  Tc <- T - 273.15
  es <- 6.112 * exp((17.67 * Tc) / (Tc + 243.5))
  e <- qair * press_hPa / (0.378 * qair + 0.622) #Pa
  rh <- e / es
  rh[rh > 1] <- 1
  rh[rh < 0] <- 0
  return(rh)
} # qair2rh

##' converts relative humidity to specific humidity
##' @title RH to SH
##' @param rh relative humidity (proportion, not percent)
##' @param T absolute temperature (Kelvin)
##' @param press air pressure (Pascals)
##' @noRd
##' @author Mike Dietze, Ankur Desai
##' @aliases rh2rv
rh2qair <- function(rh, T, press = 101325) {
  stopifnot(T[!is.na(T)] >= 0)
  Tc <- T - 273.15
  es <- 6.112 * exp((17.67 * Tc) / (Tc + 243.5))
  e <- rh * es
  p_mb <- press / 100
  qair <- (0.622 * e) / (p_mb - (0.378 * e))
  ## qair <- rh * 2.541e6 * exp(-5415.0 / T) * 18/29
  return(qair)
} # rh2qair
