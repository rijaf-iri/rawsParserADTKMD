
connect.DBI <- function(con_args, drv){
    args <- c(list(drv = drv), con_args)
    con <- try(do.call(DBI::dbConnect, args), silent = TRUE)
    if(inherits(con, "try-error")) return(NULL)
    con
}

connect.adt_db <- function(dirAWS){
    ff <- file.path(dirAWS, "AWS_DATA", "AUTH", "adt.con")
    adt <- readRDS(ff)
    conn <- connect.DBI(adt$connection, RMySQL::MySQL())
    if(is.null(conn)){
        Sys.sleep(3)
        conn <- connect.DBI(adt$connection, RMySQL::MySQL())
        if(is.null(conn)) return(NULL)
    }
    DBI::dbExecute(conn, "SET GLOBAL local_infile=1")

    return(conn)
}

tahmo.api <- function(aws_dir){
   ff <- file.path(aws_dir, "AWS_DATA", "AUTH", "tahmo.api")
   readRDS(ff)
}

connect.ssh <- function(adt_dir){
    ff <- file.path(adt_dir, "AUTH", "adt.cred")
    ssh <- readRDS(ff)
    session <- try(do.call(ssh::ssh_connect, ssh$cred), silent = TRUE)
    if(inherits(session, "try-error")) return(NULL)

    return(session)
}

format.out.msg <- function(msg, logfile, append = TRUE){
    ret <- c(paste("Time:", Sys.time(), "\n"), msg, "\n",
             "*********************************\n")
    cat(ret, file = logfile, append = append)
}
