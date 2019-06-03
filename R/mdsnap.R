library(DBI)
library(RPostgreSQL)
library(quantmod)

open_job <- function(conn) {
    # create a new job
    seq_job <- dbGetQuery(conn, "SELECT nextval('seq_job') AS job_id")
    rs <- dbSendStatement(conn, "INSERT INTO t_job (job_id) VALUES ($1)",
                          param = list(seq_job$job_id))
    jobs_inserted <- dbGetRowsAffected(rs)
    dbClearResult(rs)

    stopifnot(jobs_inserted == 1)
    seq_job$job_id
}

complete_job <- function(conn, job_id, job_status) {
    # complete a job
    rs <- dbSendStatement(conn, "UPDATE t_job
                          SET job_status_id=(SELECT job_status_id
                          FROM t_job_status WHERE job_status_name=$1),
                          modified_on=current_timestamp where job_id=$2",
                          param = list(job_status, job_id))
    jobs_updated <- dbGetRowsAffected(rs)
    dbClearResult(rs)

    stopifnot(jobs_updated == 1)
}

mdsnap <- function(host, port, dbname, user, password) {
    conn <- dbConnect(RPostgreSQL::PostgreSQL(), user = user, password = password,
                      host = host, port = port, dbname = dbname)
    on.exit(dbDisconnect(conn))

    secQuery <- "SELECT security_id AS id, security_name AS name FROM t_security"
    securities <- dbGetQuery(conn, secQuery)

    # create a new job
    job_id <- open_job(conn)

    cat(sprintf("Running new job: %d\n", job_id))

    job_status <- tryCatch({
        for (sec_id in securities$id) {
            # load market data for sec
            sec <- securities[securities$id == sec_id, "name"]
            cat(sprintf("Loading symbol '%s'\n", sec))
            secds <- getSymbols(sec, auto.assign = FALSE)

            dbds <- data.frame(security_id = sec_id, job_id = job_id,
                               quote_date = index(secds),
                               quote_open = coredata(secds)[,1],
                               quote_high=coredata(secds)[,2],
                               quote_low=coredata(secds)[,3],
                               quote_close=coredata(secds)[,4],
                               quote_volume=coredata(secds)[,5],
                               quote_adjusted=coredata(secds)[,6])
            cat(sprintf("Writing %d rows for symbol %s into database\n",
                        nrow(dbds), sec))
            dbWriteTable(conn, "t_quote", dbds, row.names=FALSE, append = TRUE)
        }

        return("COMPLETED")
    }, error = function(e) {
        cat(sprintf("Error: %s\n", e))
        return("FAILED")
    })

    complete_job(conn, job_id, job_status)
}

mdload <- function(symbols, asof = NULL,
                   features = c("open", "high", "low", "close", "volume", "adjusted"),
                   host, port, dbname, user, password) {
    conn <- dbConnect(RPostgreSQL::PostgreSQL(), user = user, password = password,
                      host = host, port = port, dbname = dbname)
    on.exit(dbDisconnect(conn))

    # find job
    j <- if (is.null(asof)) {
        sql <- "SELECT job_id FROM t_job
                WHERE created_on=(SELECT MAX(created_on) FROM t_job)"
        dbGetQuery(conn, sql)
    } else {
        sql <- "SELECT job_id FROM t_job WHERE created_on::date = $1"
        dbGetQuery(conn, sql, list(as.character(asof)))
    }

    # find securities
    result <- lapply(symbols, function(sym) {
        sql <- "SELECT security_id FROM t_security WHERE security_name = $1"
        sec <- dbGetQuery(conn, sql, param = list(sym))

        lst_features <- paste(paste0(", quote_", features), collapse = "")
        sql <- paste0("SELECT quote_date", lst_features, " FROM t_quote
                      WHERE security_id = $1 and job_id = $2")
        mdata <- dbGetQuery(conn, sql, list(sec$security_id, j$job_id))
        colnames(mdata) <- c(data, features)
        xts(mdata[, -1], order.by = mdata[, 1])
    })
    names(result) <- symbols
    result
}

mdsnap_test <- function() {
    getSymbols("VGAD.AX", auto.assign = FALSE)
}
