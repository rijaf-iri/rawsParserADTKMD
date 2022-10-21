
#' Process TAHMO data from API.
#'
#' Get the data from TAHMO API, parse and convert into ADT format.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

process.tahmo <- function(aws_dir){
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "TAHMO")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_tahmo.txt")

    ret <- try(get.tahmo.data(aws_dir), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- "Getting TAHMO data failed"
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}

#' Process KCSAP data.
#'
#' Get the data from KCSAP, parse and convert into ADT format.
#' 
#' @param aws_dir full path to the directory ADT.
#' @param kcsap_dir full path to the directory containing the KCSAP data.
#' @param adt_dir full path to the directory containing the AWS_DATA folder on ADT server.
#' 
#' @export

process.kcsap <- function(aws_dir, kcsap_dir, adt_dir){
    dirLOG <- file.path(aws_dir, "LOG", "KCSAP")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_kcsap.txt")

    ret <- try(get.kcsap.data(aws_dir, kcsap_dir, adt_dir), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- "Getting KCSAP data failed"
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}
