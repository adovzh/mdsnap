#' Returns the list of zoo objects indexed by features.
#' Each zoo object contains observations for the symbols requested.
#'
#' @export
#' @author Alexander Dovzhikov
mdrates <- function(conn, symbols,
                    features = c("open", "high", "low", "close", "volume", "adjusted")) {
    # fetch the list of all the securities in the database
    sl <- security_list(conn)
    # non cash securities
    ncsec <- security_names(sl, source = "non_cash")
    stopifnot(symbols %in% ncsec)

    mds <- mdload(conn, symbols, features = features)
    sapply(features, function(feature) {
        ds <- do.call(merge, sapply(mds, function(ds) ds[, feature],
                                    simplify = FALSE))
        names(ds) <- symbols
        ds
    }, simplify = FALSE)
}

#' @export
#' @importFrom stats na.omit
#' @author Alexander Dovzhikov
rates_snapshot <- function(mdr, asof = Sys.Date()) {
    lapply(mdr, function(frates) {
        syms <- names(frates)
        fr <- frates[index(frates) <= asof, ]
        last_dates <- as.Date(sapply(syms, function(sym) {
            as.character(max(index(na.omit(fr[, sym]))))
        }))
        last_rates <- sapply(seq_along(syms), function(idx) {
            fr[last_dates[idx], syms[idx]]
        })

        data.frame(Symbol = syms, LastDate = last_dates, LastRate = last_rates)
    })
}
