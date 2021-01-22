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
    dbGetQuery(conn, sql)
}

non_cash_secutity_names <- function(sec) {
    sec %>% filter(source != "cash") %>% select(name) %>% pull()
}

cash_security_name <- function(sec) {
    sec %>% filter(source == "cash") %>% select(name) %>% pull()
}
