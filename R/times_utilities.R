
char_utc2local_time <- function(dates, format, tz){
    x <- strptime(dates, format, tz = "UTC")
    x <- as.POSIXct(x)
    x <- format(x, format, tz = tz)
    x <- strptime(x, format, tz = tz)
    as.POSIXct(x)
}

time_utc2local_char <- function(dates, format, tz){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = tz)
    x
}

char_local2utc_time <- function(dates, format, tz){
    x <- strptime(dates, format, tz = tz)
    x <- as.POSIXct(x)
    x <- format(x, format, tz = "UTC")
    x <- strptime(x, format, tz = "UTC")
    as.POSIXct(x)
}

time_local2utc_char <- function(dates, format){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = "UTC")
    x
}

time_local2utc_time <- function(dates){
    format <- "%Y-%m-%d %H:%M:%S"
    x <- time_local2utc_char(dates, format)
    x <- strptime(x, format, tz = "UTC")
    as.POSIXct(x)
}

time_utc2time_local <- function(dates, tz){
    format <- "%Y-%m-%d %H:%M:%S"
    x <- time_utc2local_char(dates, format, tz)
    x <- strptime(x, format, tz = tz)
    as.POSIXct(x)
}

round.time.minutes10 <- function(times){
    mn <- format(times, "%M")
    mn <- as.integer(paste0(substr(mn, 1, 1), 0))
    # trunc.POSIXt  
    # times <- round.POSIXt(times, units = "mins")
    times$min[] <- mn
    times$sec[] <- 0
    times
}
