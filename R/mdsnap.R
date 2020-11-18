#' Market Data Snap
#'
#' This function snaps current market data snapshot for the symbols universe
#' and puts them into PostgreSQL database. The universe is configured in the
#' database itself and needs to be set up manually.
#' @param ctx Database context
#' @export
#' @importFrom DBI dbGetQuery dbWriteTable
#' @importFrom RPostgreSQL PostgreSQL
#' @importFrom quantmod getSymbols
#' @importFrom zoo coredata index
#' @importFrom xts make.index.unique

mdsnap <- function(ctx) {
    if (!db_connected(ctx)) {
        db_connect(ctx)
        on.exit(db_disconnect(ctx), add = TRUE)
    }

    conn <- ctx$conn

    secQuery <- "SELECT security_id AS id, security_name AS name
                 FROM t_security s inner join t_snap_source ss
                    on s.snap_source_id = ss.snap_source_id
                 WHERE ss.snap_source_name = 'quantmod'"
    securities <- dbGetQuery(conn, secQuery)

    # create a new job
    job_id <- open_job(conn)

    cat(sprintf("Running new job: %d\n", job_id))

    job_status <- tryCatch({
        for (sec_id in securities$id) {
            # load market data for sec
            sec <- securities[securities$id == sec_id, "name"]
            cat(sprintf("Loading symbol '%s'\n", sec))
            # load symbol and suppress warnings about missing values
            secds <- suppressWarnings(getSymbols(sec, auto.assign = FALSE))
            # remove duplicate elements (sometimes happens)
            secds <- make.index.unique(secds, drop = TRUE)

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

        "COMPLETED"
    }, error = function(e) {
        cat(sprintf("Error: %s\n", e))
        "FAILED"
    })

    complete_job(conn, job_id, job_status)
}

#' Market Data Load
#'
#' This function loads the specified set of symbols from the database
#' into an xts object.
#' @param ctx Database context
#' @param symbols A set of symbols to extract, must be specified explicitly.
#' @param asof Allows to specify a snapshot to extract, NULL means to latest.
#' @param features Features to extract.
#' @export
#' @importFrom DBI dbGetQuery
#' @importFrom xts xts
#' @author Alexander Dovzhikov, \email{alexander.dovzhikov@gmail.com}

mdload <- function(ctx, symbols, asof = NULL,
                   features = c("open", "high", "low", "close", "volume", "adjusted")) {
    if (!db_connected(ctx)) {
        db_connect(ctx)
        on.exit(db_disconnect(ctx), add = TRUE)
    }

    conn <- ctx$conn
    j <- find_job(ctx)

    # find securities
    result <- lapply(symbols, function(sym) {
        sql <- "SELECT security_id FROM t_security WHERE security_name = $1"
        sec <- dbGetQuery(conn, sql, param = list(sym))

        lst_features <- paste(paste0(", quote_", features), collapse = "")
        sql <- paste0("SELECT quote_date", lst_features, " FROM t_quote
                      WHERE security_id = $1 and job_id = $2")
        mdata <- dbGetQuery(conn, sql, list(sec$security_id, j$id))
        colnames(mdata) <- c("date", features)
        xts(mdata[, -1], order.by = mdata[, 1])
    })
    names(result) <- symbols
    result
}
