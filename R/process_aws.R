
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
        msg <- paste(ret, "Getting TAHMO data failed")
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}
