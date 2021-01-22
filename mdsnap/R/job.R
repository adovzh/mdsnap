#' @importFrom DBI dbGetQuery
open_job <- function(conn) {
    seq_id <- dbGetQuery(conn, "SELECT start_job('quantmod') AS job_id")
    seq_id$job_id
}

#' @importFrom DBI dbExecute
complete_job <- function(conn, job_id, job_status) {
    # complete a job
    dbExecute(conn, "CALL complete_job($1, $2)",
              param = list(job_id, job_status))

    cat(sprintf("Job %d is %s\n", job_id, job_status))
}

#' Find Job
#'
#' Locates the job with the specified asof date or the latest if not specified
#' @export
#' @importFrom DBI dbGetQuery
#' @author Alexander Dovzhikov
find_job <- function(ctx, asof = NULL) {
    if (!db_connected(ctx)) {
        db_connect(ctx)
        on.exit(db_disconnect(ctx), add = TRUE)
    }

    conn <- ctx$conn

    sql <- "select
                job_id as id,
                job_status_name as job_status,
                snap_source_name as snap_source,
                created_on,
                modified_on
            from t_job j
                inner join t_job_status js on js.job_status_id = j.job_status_id
                inner join t_snap_source ss on j.snap_status_id = ss.snap_source_id"

    j <- if (is.null(asof)) {
        sql <- paste(sql, "WHERE created_on=(SELECT MAX(created_on) FROM t_job)")
        dbGetQuery(conn, sql)
    } else {
        sql <- paste(sql, "created_on::date = $1")
        dbGetQuery(conn, sql, list(as.character(asof)))
    }

    if (nrow(j) > 0) {
        structure(as.list(j[1,]), class = "job")
    } else NULL
}

#' List Jobs
#'
#' List all the jobs of the specified type.
#' @export
#' @importFrom DBI dbGetQuery
#' @author Alexander Dovzhikov
list_jobs <- function(ctx, type = "completed") {
    if (!db_connected(ctx)) {
        db_connect(ctx)
        on.exit(db_disconnect(ctx), add = TRUE)
    }

    conn <- ctx$conn

    # TODO: check status is one of the supported statuses

    sql <- "select
                job_id as id,
                job_status_name as status,
                snap_source_name as snap_source,
                created_on,
                modified_on
            from t_job j
                inner join t_job_status js on js.job_status_id = j.job_status_id
                inner join t_snap_source ss on j.snap_status_id = ss.snap_source_id
            where
                job_status_name = $1"

    dbGetQuery(conn, sql, list(toupper(type)))
}
