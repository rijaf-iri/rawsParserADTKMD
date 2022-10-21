
get.tahmo.data <- function(aws_dir){
    tz <- "Africa/Nairobi"
    Sys.setenv(TZ = tz)
    origin <- "1970-01-01"
    dataset <- 'controlled'
    tahmo_time <- "%Y-%m-%dT%H:%M:%SZ"
    awsNET <- 1

    dirOUT <- file.path(aws_dir, "AWS_DATA", "DATA", "minutes", "TAHMO")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "TAHMO")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    awsLOG <- file.path(dirLOG, "AWS_LOG.txt")

    varFile <- file.path(aws_dir, "AWS_DATA", "CSV", "tahmo_pars.csv")
    varTable <- utils::read.table(varFile, sep = ',', header = TRUE,
                                 stringsAsFactors = FALSE,
                                 colClasses = "character", quote = "\"")

    awsFile <- file.path(aws_dir, "AWS_DATA", "CSV", "tahmo_lastDates.csv")
    awsInfo <- utils::read.table(awsFile, sep = ',', header = TRUE,
                                 stringsAsFactors = FALSE, quote = "\"")

    api <- tahmo.api(aws_dir)$connection
    now <- Sys.time()

    for(j in seq_along(awsInfo$id)){
        awsID <- awsInfo$id[j]
        awsVAR <- varTable[varTable$id == awsID, , drop = FALSE]

        endpoint <- paste('services/measurements/v2/stations',
                           awsID, 'measurements', dataset, sep = '/')
        api_url <- paste0(api$url, "/", endpoint)

        if(is.na(awsInfo$last[j])){
            last <- as.POSIXct("2015-01-01T00:00:00Z", format = tahmo_time, tz = tz)
        }else{
            last <- as.POSIXct(as.integer(awsInfo$last[j]), origin = origin, tz = tz) + 60
        }
        daty <- seq(last, now, 'day')
        daty <- time_local2utc_time(daty)

        for(s in 1:(length(daty) - 1)){
            ss <- if(s == 1) 0 else 1
            start <- format(daty[s] + ss, tahmo_time)
            end <- format(daty[s + 1], tahmo_time)

            qres <- httr::GET(api_url, httr::accept_json(),
                    httr::authenticate(api$id, api$secret),
                    httr::timeout(20),
                    query = list(start = start, end = end))
            if(httr::status_code(qres) != 200) next

            resc <- httr::content(qres)
            if(is.null(resc$results[[1]]$series)) next
            if(resc$results[[1]]$statement_id != 0) next

            hdr <- unlist(resc$results[[1]]$series[[1]]$columns)
            val <- lapply(resc$results[[1]]$series[[1]]$values, function(x){
                sapply(x, function(v) if(is.null(v)) NA else v)
            })
            val <- do.call(rbind, val)
            val <- as.data.frame(val)
            names(val) <- hdr
            ic <- c('time', 'value', 'variable')
            val <- val[val$quality == "1", ic, drop = FALSE]
            if(nrow(val) == 0) next

            out <- try(parse.tahmo.data(val, awsVAR, awsID, awsNET), silent = TRUE)
            if(inherits(out, "try-error")){
                mserr <- gsub('[\r\n]', '', out[1])
                msg <- paste("Unable to parse data for", awsID)
                format.out.msg(paste(mserr, '\n', msg), awsLOG)
                next
            }

            if(is.null(out)) next
            awsInfo$last[j] <- max(out$obs_time)

            locFile <- paste(range(out$obs_time), collapse = "_")
            locFile <- paste0(awsID, "_", locFile, '.rds')
            locFile <- file.path(dirOUT, locFile)
            saveRDS(out, locFile)
            ## utils::write.table(awsInfo, ) can be here too
        }

        ## redondant, mais utile en cas de coupure
        utils::write.table(awsInfo, awsFile, sep = ",", na = "", col.names = TRUE,
                           row.names = FALSE, quote = FALSE)
    }

    ## Issue: the last dates of downloaded
    ## aws data are not saved in case of interruption
    # utils::write.table(awsInfo, awsFile, sep = ",", na = "", col.names = TRUE,
    #                    row.names = FALSE, quote = FALSE)

    return(0)
}

parse.tahmo.data <- function(qres, awsVAR, awsID, awsNET){
    tz <- "Africa/Nairobi"
    tahmo_time <- "%Y-%m-%dT%H:%M:%SZ"
    Sys.setenv(TZ = tz)

    temps <- strptime(qres$time, tahmo_time, tz = "UTC")
    ina <- is.na(temps)
    qres <- qres[!ina, , drop = FALSE]

    if(nrow(qres) == 0) return(NULL)

    temps <- temps[!ina]
    temps <- time_utc2time_local(temps, tz)

    ivar <- qres$variable %in% awsVAR$tahmo_code
    tmp <- qres[ivar, , drop = FALSE]
    tmp$time <- as.POSIXct(temps[ivar])
    tmp <- tmp[!is.na(tmp$value), , drop = FALSE]
    tmp$value <- as.numeric(tmp$value)

    if(nrow(tmp) == 0) return(NULL)

    ix <- match(tmp$variable, awsVAR$tahmo_code)

    var_nm <- c("var_height", "var_code", "stat_code")
    var_dat <- awsVAR[ix, var_nm, drop = FALSE]
    tmp <- cbind(tmp, var_dat)

    ## convert soil moisture m^3/m^3 to %
    sm <- tmp$var_code == 7
    tmp$value[sm] <- tmp$value[sm] * 100

    ## convert relative humidity fraction to %
    rh <- tmp$var_code == 6
    tmp$value[rh] <- tmp$value[rh] * 100

    ## convert pressure kPa to hPa
    ap <- tmp$var_code == 1
    tmp$value[ap] <- tmp$value[ap] * 10

    ######

    tmp$network <- awsNET
    tmp$id <- awsID
    tmp$limit_check <- NA

    ######
    tmp <- tmp[, c("network", "id", "var_height",
                   "var_code", "stat_code",
                   "time", "value", "limit_check")]

    fun_format <- list(as.integer, as.character, as.numeric, as.integer,
                       as.integer, as.integer, as.numeric, as.integer)

    tmp <- lapply(seq_along(fun_format), function(j) fun_format[[j]](tmp[[j]]))
    tmp <- as.data.frame(tmp)
    names(tmp) <- c("network", "id", "height", "var_code",
                    "stat_code", "obs_time", "value", "limit_check")

    return(tmp)
}

