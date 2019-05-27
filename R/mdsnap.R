library(DBI)
library(RPostgreSQL)
library(quantmod)

mdsnap <- function(host, port, dbname, user, password) {
    pg <- dbDriver("PostgreSQL")
    conn <- dbConnect(pg, user = user, password = password,
                      host = host, port = port, dbname = dbname)
    on.exit(dbDisconnect(conn))

    secQuery <- "SELECT security_id AS id, security_name AS name FROM t_security"
    securities <- dbGetQuery(conn, secQuery)

    # create a new job
    seq_job <- dbGetQuery(conn, "SELECT nextval('seq_job') AS job_id")
    rs <- dbSendStatement(conn, "INSERT INTO t_job (job_id) VALUES ($1)",
                          param = list(seq_job$job_id))
    jobs_inserted <- dbGetRowsAffected(rs)
    dbClearResult(rs)

    stopifnot(jobs_inserted == 1)

    cat(sprintf("Running new job: %d\n", seq_job$job_id))

    for (sec_id in securities$id) {
        # load market data for sec
        sec <- securities[securities$id == sec_id, "name"]
        cat(sprintf("Loading symbol '%s'\n", sec))
        secds <- getSymbols(sec, auto.assign = FALSE)
        # print(secds)
        dbds <- data.frame(security_id = sec_id, job_id = seq_job$job_id,
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
}
