
get.kcsap.data <- function(aws_dir, kcsap_dir, adt_dir){
    tz <- "Africa/Nairobi"
    Sys.setenv(TZ = tz)
    origin <- "1970-01-01"
    kcsap_time <- "%Y%m%d%H%M"
    awsNET <- 7

    dirOUT <- file.path(aws_dir, "DATA", "KCSAP")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(aws_dir, "LOG", "KCSAP")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    awsLOG <- file.path(dirLOG, "AWS_LOG.txt")

    uploadFile <- file.path(dirLOG, 'upload_failed.txt')

    session <- connect.ssh(aws_dir)
    if(is.null(session)){
        logPROC <- file.path(dirLOG, "processing_kcsap.txt")
        msg <- paste(session, "Unable to connect to ADT server\n")
        format.out.msg(msg, logPROC)
        upload <- FALSE
    }else{
        dirUP <- file.path(adt_dir, "AWS_DATA", "DATA", "minutes", "KCSAP")
        ssh::ssh_exec_wait(session, command = c(
            paste0('if [ ! -d ', dirUP, ' ] ; then mkdir -p ', dirUP, ' ; fi')
        ))
        upload <- TRUE
    }

    varFile <- file.path(aws_dir, "CSV", "kcsap_pars.csv")
    varTable <- utils::read.table(varFile, sep = ',', header = TRUE,
                                 stringsAsFactors = FALSE, quote = "\"")
    awsFile <- file.path(aws_dir, "CSV", "kcsap_lastDates.csv")
    awsInfo <- utils::read.table(awsFile, sep = ',', header = TRUE,
                                 stringsAsFactors = FALSE, quote = "\"")

    for(j in seq_along(awsInfo$id)){
        awsID <- awsInfo$id[j]
        awsVAR <- varTable[varTable$id == awsID, , drop = FALSE]
        stnID <- sprintf("%03d", awsVAR$kcsap_id[1])
        aws_pars <- awsVAR[, c('id', 'var_height', 'var_code', 'stat_code')]

        if(is.na(awsInfo$last[j])){
            last <- as.POSIXct("201501010000", format = kcsap_time, tz = tz)
        }else{
            last <- as.POSIXct(as.integer(awsInfo$last[j]), origin = origin, tz = tz) + 1
        }
        daty <- seq(last, Sys.time(), 'day')
        daty <- time_local2utc_time(daty)
        if(length(daty) == 1){
            daty <- c(daty, time_local2utc_time(Sys.time()))
        }

        for(s in 1:(length(daty) - 1)){
            ss <- if(s == 1) 0 else 1
            start <- daty[s] + ss
            end <- daty[s + 1]
            tt <- seq(start, end, 'mins')
            tt <- unique(format(tt, '%Y%m%d'))
            pattern <- sapply(tt, function(p){
                paste0("^KMD_", stnID, "_", p, ".+\\.csv$")
            })
            pattern <- paste0(pattern, collapse = "|")
            csvFiles <- list.files(kcsap_dir, pattern)
            if(length(csvFiles) == 0) next

            csvDates <- sapply(strsplit(csvFiles, '_'), '[[', 3)
            csvDates <- gsub("\\.csv", "", csvDates)
            csvDates <- strptime(csvDates, kcsap_time, tz = "UTC")

            it <- csvDates >= start & csvDates <= end

            tmp <- lapply(csvFiles[it], function(csv_f){
                obs_file <- file.path(kcsap_dir, csv_f)
                x <- readLines(obs_file, warn = FALSE)
                x <- strsplit(x[2], ",")
                x <- trimws(x[[1]])
                x <- gsub('^/.+', NA, x)
                if(length(x) != 32) NULL else x
            })

            inull <- sapply(tmp, is.null)
            if(all(inull)){
                msg1 <- paste("Ambiguous column number for:", awsID, '\n')
                msg2 <- paste('Between', start, 'and', end)
                format.out.msg(paste0(msg1, msg2), awsLOG)
                next
            }
            if(any(inull)){
                msg1 <- paste("Ambiguous column number for:", awsID, '\n')
                msg2 <- paste('Files:', paste0(csvFiles[inull], collapse = ', '))
                format.out.msg(paste0(msg1, msg2), awsLOG)
            }

            tmp <- tmp[!inull]
            tmp <- do.call(rbind, tmp)

            out <- try(parse.kcsap.data(tmp, awsVAR, awsID, awsNET), silent = TRUE)
            if(inherits(out, "try-error")){
                mserr <- gsub('[\r\n]', '', out[1])
                msg <- paste("Unable to parse data for", awsID)
                format.out.msg(paste(mserr, '\n', msg), awsLOG)
                next
            }

            awsInfo$last[j] <- max(out$obs_time)

            locFile <- paste(range(out$obs_time), collapse = "_")
            locFile <- paste0(awsID, "_", locFile, '.rds')
            locFile <- file.path(dirOUT, locFile)
            saveRDS(out, locFile)

            if(upload){
                upFile <- file.path(dirUP, basename(locFile))
                ret <- try(ssh::scp_upload(session, locFile, to = upFile, verbose = FALSE), silent = TRUE)

                if(inherits(ret, "try-error")){
                    if(grepl('disconnected', ret[1])){
                        session <- connect.ssh(aws_dir)
                        upload <- if(is.null(session)) FALSE else TRUE
                    }
                    cat(basename(locFile), file = uploadFile, append = TRUE)
                }
            }else{
                cat(basename(locFile), file = uploadFile, append = TRUE)
            }

            utils::write.table(awsInfo, awsFile, sep = ",", na = "", col.names = TRUE,
                               row.names = FALSE, quote = FALSE)
        }
    }

    return(0)
}

parse.kcsap.data <- function(tmp, awsVAR, awsID, awsNET){
    tz <- "Africa/Nairobi"
    kcsap_time <- "%Y%m%d%H%M"
    Sys.setenv(TZ = tz)

    wgt <- paste('20220101', tmp[, 29])
    wgt <- char_utc2local_time(wgt, '%Y%m%d %H:%M:%S', tz)
    tmp[, 29] <- format(wgt, '%H%M%S')

    temps <- strptime(tmp[, 3], kcsap_time, tz = "UTC")
    temps <- time_utc2time_local(temps, tz)

    tmp <- tmp[, awsVAR$col_index, drop = FALSE]

    aws_pars <- awsVAR[, c('id', 'var_height', 'var_code', 'stat_code')]

    tmp <- lapply(seq_along(temps), function(l){
        data.frame(network = awsNET, aws_pars, 
                   obs_time = as.numeric(temps[l]),
                   value = as.numeric(tmp[l, ]),
                   limit_check = NA)
    })

    tmp <- do.call(rbind, tmp)
    tmp <- tmp[!is.na(tmp$value), , drop = FALSE]

    ## convert wind speed knots to m/s
    ws <- tmp$var_code == 10
    tmp$value[ws] <- round(tmp$value[ws] * 0.514444, 2)

    ## convert wind gust speed knots to m/s
    wg <- tmp$var_code == 11
    tmp$value[wg] <- round(tmp$value[wg] * 0.514444, 2)

    fun_format <- list(as.integer, as.character, as.numeric, as.integer,
                       as.integer, as.integer, as.numeric, as.integer)

    tmp <- lapply(seq_along(fun_format), function(j) fun_format[[j]](tmp[[j]]))
    tmp <- as.data.frame(tmp)
    names(tmp) <- c("network", "id", "height", "var_code",
                    "stat_code", "obs_time", "value", "limit_check")

    return(tmp)
}
