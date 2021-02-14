#' @export
#' @importFrom DBI dbGetQuery
#' @author Alexander Dovzhikov
portfolio_list <- function(conn) {
    sql <- "select portfolio_id as id, portfolio_name as name
            from t_portfolio order by portfolio_id"
    dbGetQuery(conn, sql)
}

#' @export
#' @importFrom DBI dbGetQuery
#' @author Alexander Dovzhikov
portfolio_load <- function(conn, portfolio_name) {
    sql <- "select
               trade_date as date,
               security_name as security,
               trade_buy_flag as buy_flag,
               trade_units as units,
               trade_amount as amount
            from t_trade t
                inner join t_security s on t.security_id = s.security_id
                inner join t_portfolio p on t.portfolio_id = p.portfolio_id
            where p.portfolio_name=$1
            order by date;"
    dbGetQuery(conn, sql, list(portfolio_name))
}

buysell <- function(b) as.logical(b) * 2 - 1

#' @export
#' @importFrom magrittr %>%
#' @importFrom dplyr filter mutate group_by summarise
#' @author Alexander Dovzhikov
portfolio_alloc <- function(portfolio, asof = Sys.Date()) {
    alloc <- portfolio %>% filter(date <= asof) %>%
        mutate(signed_units = units * buysell(buy_flag)) %>%
        group_by(security) %>% summarise(total_units = sum(signed_units))

    structure(list(alloc = alloc, asof = asof), class = "portfolio_alloc")
}

is_palloc <- function(palloc) {
  any(class(palloc) == "portfolio_alloc")
}

#' @export
#' @author Alexander Dovzhikov
securities <- function(obj, ...) UseMethod("securities")

#' @export
#' @author Alexander Dovzhikov
securities.portfolio_alloc <- function(palloc, sl, source = "all") {
  intersect(palloc$alloc$security, security_names(sl, source))
}
