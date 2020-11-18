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
#' @importFrom dplyr filter mutate group_by summarise
#' @author Alexander Dovzhikov
portfolio_alloc <- function(portfolio, asof) {
    alloc <- p %>% filter(date < asof) %>%
        mutate(signed_units = units * buysell(buy_flag)) %>%
        group_by(security) %>% summarise(total_units = sum(signed_units))
    structure(list(alloc = alloc, asof = asof), class = "portfolio_alloc")
}

portfolio_eval <- function(ctx, palloc, sec) {
    # non cash security names
    ncsec <- non_cash_secutity_names(sec)
    # security names for evaluations
    evalsec <- ncsec %>% intersect(palloc$alloc$security)
    # vector of security prices
    pvec <- sapply(mdload(ctx, evalsec, features = "close"), function(md) {
        idays <- index(md)
        idays <- idays[idays < palloc$asof]
        as.numeric(coredata(md[idays == max(idays)]))
    })

    # transform named vector into tibble
    pvec_t <- tibble(security = names(pvec), price = pvec)
    pvec_t %>% inner_join(pa$alloc, by = "security") %>%
        mutate(total_price = price * total_units) %>%
        select(total_price) %>% sum()
}
