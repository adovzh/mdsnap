#' Returns a list of currently supported securities
#'
#' Returns a list of currently supported securities (as a data frame).
#' @export
#' @importFrom DBI dbGetQuery
#' @author Alexander Dovzhikov
security_list <- function(conn) {
    sql <- "select
                s.security_id as id,
                s.security_name as name,
                s.security_description as description,
                ss.snap_source_name as source
            from t_security s
                inner join t_snap_source ss on s.snap_source_id = ss.snap_source_id"
    rs <- dbGetQuery(conn, sql)
    class(rs) <- append(class(rs), "sl")
    rs
}

#' @export
#' @author Alexander Dovzhikov
security_names <- function(sl, ...) UseMethod("security_names", sl)

#' @export
#' @author Alexander Dovzhikov
security_names.sl <- function(sl, source = "all") {
    if (source == "non_cash") sl[sl$source != "cash", "name"]
    else if (source == "all") sl$name
    else sl[sl$source == source, "name"]
}

