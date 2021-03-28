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
find_job <- function(conn, asof = NULL) {
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
        sql <- paste(sql, "where created_on=(
                     select max(created_on) from t_job j2
                        inner join t_job_status js2
                        on js2.job_status_id = j2.job_status_id
                     where js2.job_status_name = 'COMPLETED')
                            and js.job_status_name = 'COMPLETED'")
        dbGetQuery(conn, sql)
    } else {
        sql <- paste(sql, "where created_on=(
                        select max(created_on) from t_job j2
                            inner join t_job_status js2
                            on js2.job_status_id = j2.job_status_id
                        where js2.job_status_name = 'COMPLETED'
                            and j2.created_on::date <= $1)
                            and js.job_status_name = 'COMPLETED'")
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
list_jobs <- function(conn, type = "completed") {
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
