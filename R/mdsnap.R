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

complete_job <- function(conn, job_id) {
    # complete a job
    rs <- dbSendStatement(conn, "UPDATE t_job
                          SET job_status_id=(SELECT job_status_id
                          FROM t_job_status WHERE job_status_name='COMPLETED'),
                          modified_on=current_timestamp where job_id=$1",
                          param = list(job_id))
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

    complete_job(conn, job_id)
}

mdload <- function(symbols, asof, host, port, dbname, user, password) {
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

    # do something with j$job)id

    # find securities
    result <- lapply(symbols, function(sym) {
        sql <- "SELECT security_id FROM t_security WHERE security_name = $1"
        secIds <- dbGetQuery(conn, sql, param = list(sym))

        data.frame(security_id = secIds$security_id)
    })
    names(result) <- symbols
    result
}
