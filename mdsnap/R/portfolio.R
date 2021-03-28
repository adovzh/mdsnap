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

#' @export
#' @author Alexander Dovzhikov
portfolio_start_date <- function(portfolio) min(portfolio$date)

#' Security inflow multiplier.
#'
#' @export
#' @author Alexander Dovzhikov
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
#' @importFrom zoo as.Date as.Date.numeric
#' @author Alexander Dovzhikov
portfolio_snapshot <- function(portfolio, mdr, sl, asof = Sys.Date()) {
  pa <- portfolio_alloc(portfolio, asof)
  rsnapshot <- rates_snapshot(mdr, asof)$close
  psyms <- securities(pa, sl, "non_cash")
  csyms <- securities(pa, sl, "cash")

  last_date_rates <- if (nrow(pa$alloc) > 0) {
    lapply(
      c(LastDate = "LastDate", LastRate = "LastRate"),
      function(last_col_name) {
        sapply(pa$alloc$security, function(s) {
          if (s %in% rsnapshot$Symbol)
            rsnapshot[rsnapshot$Symbol == s, last_col_name]
          else if (s %in% csyms) {
            if (last_col_name == "LastDate") asof
            else if (last_col_name == "LastRate") 1
            else NA
          }
          else NA
        })
      })
  } else {
    list(LastDate = as.Date(integer(0)), LastRate = numeric(0))
  }

  psnapshot <- data.frame(Security = pa$alloc$security,
                          LastDate = as.Date(last_date_rates$LastDate),
                          LastRate = last_date_rates$LastRate,
                          TotalUnits = pa$alloc$total_units)
  structure(list(snapshot = psnapshot, asof = asof),
            class = "portfolio_snapshot")
}

#' @export
#' @author Alexander Dovzhikov
mdprice <- function(psnapshot, sl = NULL, source = "all") {
  if (source == "all" || is.null(sl)) {
    sum(psnapshot$snapshot$LastRate * psnapshot$snapshot$TotalUnits)
  } else {
    syms <- securities(psnapshot, sl, source)
    ssnap <- psnapshot$snapshot[match(syms, psnapshot$snapshot$Security), ]
    sum(ssnap$LastRate * ssnap$TotalUnits)
  }
}

#' @export
#' @author Alexander Dovzhikov
securities <- function(obj, ...) UseMethod("securities")

#' @export
#' @author Alexander Dovzhikov
securities.portfolio_alloc <- function(palloc, sl, source = "all") {
  intersect(palloc$alloc$security, security_names(sl, source))
}

#' @export
#' @author Alexander Dovzhikov
securities.portfolio_snapshot <- function(ps, sl, source = "all") {
  intersect(ps$snapshot$Security, security_names(sl, source))
}
