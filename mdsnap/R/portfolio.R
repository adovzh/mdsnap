#' @export
#' @importFrom DBI dbGetQuery
#' @author Alexander Dovzhikov
portfolio_list <- function(ctx) {
    if (!db_connected(ctx)) {
        db_connect(ctx)
        on.exit(db_disconnect(ctx), add = TRUE)
    }

    sql <- "select portfolio_id as id, portfolio_name as name
            from t_portfolio order by portfolio_id"
    dbGetQuery(ctx$conn, sql)
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

rates_env <- function(ctx, asof = Sys.Date()) {
  if (!db_connected(ctx)) {
    db_connect(ctx)
    on.exit(db_disconnect(ctx), add = TRUE)
  }

  sec <- security_list(ctx$conn)

  # non cash security names
  ncsec <- non_cash_secutity_names(sec)

  # list of security prices with dates + accessor functions
  pvec <- lapply(mdload(ctx, ncsec, features = "close"), function(md) {
    idays <- index(md)
    idays <- idays[idays <= asof]
    last_date <- max(idays)
    list(LastDate = last_date,
         Quote = as.numeric(coredata(md[idays == max(idays)])))

  })

  get_quote <- function(elem) elem$Quote
  get_last_date <- function(elem) elem$LastDate

  # transform named vector into tibble
  security_vec <- names(pvec)
  quote_date_vec <- structure(sapply(pvec, get_last_date), class = "Date")
  quote_vec <- sapply(pvec, get_quote)

  pvec_t <- tibble(security = security_vec,
                   quote_date = quote_date_vec,
                   quote = quote_vec)
  cash_row <- tibble(security = cash_security_name(sec),
                     quote_date = asof,
                     quote = 1)
  pvec_t %>% bind_rows(cash_row)
}

portfolio_eval <- function(ctx, palloc) {
  stopifnot(is_palloc(palloc))

  rates <- rates_env(ctx, palloc$asof)
  rates %>% inner_join(palloc$alloc, by = "security") %>%
    mutate(total_price = quote * total_units) # %>%
    # select(total_price) %>% sum()
}
