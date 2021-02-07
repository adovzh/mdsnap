#' Returns the list of zoo objects indexed by features.
#' Each zoo object contains observations for the symbols requested.
#'
#' @export
#' @author Alexander Dovzhikov
mdrates <- function(ctx, symbols,
                    features = c("open", "high", "low", "close", "volume", "adjusted")) {
    if (!db_connected(ctx)) {
        db_connect(ctx)
        on.exit(db_disconnect(ctx), add = TRUE)
    }

    # fetch the list of all the securities in the database
    sl <- security_list(ctx$conn)
    # non cash securities
    ncsec <- security_names(sl, source = "non_cash")
    stopifnot(symbols %in% ncsec)

    mds <- mdload(ctx, symbols, features = features)
    sapply(features, function(feature) {
        ds <- do.call(merge, sapply(mds, function(ds) ds[, feature],
                                    simplify = FALSE))
        names(ds) <- symbols
        ds
    }, simplify = FALSE)
}

#' @export
#' @author Alexander Dovzhikov
rates_snapshot <- function(mdr) {
    lapply(mdr, function(frates) {
        syms <- names(frates)
        last_dates <- as.Date(sapply(syms, function(sym) {
            as.character(max(index(na.omit(frates[, sym]))))
        }))
        last_rates <- sapply(seq_along(syms), function(idx) {
            frates[last_dates[idx], syms[idx]]
        })

        data.frame(Symbol = syms, LastDate = last_dates, LastRate = last_rates)
    })
}
