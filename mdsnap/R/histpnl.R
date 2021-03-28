#' @export
#' @importFrom stats aggregate na.omit uniroot
#' @author Alexander Dovzhikov
hist_pnl <- function(p, p_mdr, p_sl, asof = Sys.Date(),
                     p_split = c("annual", "none")) {
    p_split <- match.arg(p_split)
    v_start_date <- portfolio_start_date(p)

    # todo: should really be the last date available in rates
    v_end_date <- Sys.Date()
    v_next_year <- as.integer(format(v_start_date, "%Y")) + 1

    # from the date preceding the start date
    # to the end date
    # separated by year ends
    v_dates <- switch (p_split,
        annual = c(v_start_date - 1,
                   seq(from = as.Date(paste0(v_next_year, "-01-01")),
                       to = Sys.Date(), by = "year") - 1,
                   v_end_date),
        none = c(v_start_date - 1, v_end_date)
    )

    # MV vector
    v_mv <- sapply(v_dates, function(d)  {
        mdprice(portfolio_snapshot(p, p_mdr, p_sl, asof = d))
    })

    # Inflows data
    v_idx_inflows <- findInterval(p[!p$buy_flag, "date"], v_dates,
                                  left.open = TRUE)
    v_ag_inflows <- aggregate(p[!p$buy_flag, "amount", drop=F],
                              by = list(index = v_idx_inflows), sum)

    # Outflows data
    v_idx_outflows <- findInterval(p[p$buy_flag, "date"], v_dates,
                                  left.open = TRUE)
    v_ag_outflows <- aggregate(p[p$buy_flag, "amount", drop=F],
                              by = list(index = v_idx_outflows), sum)

    # Start Date column
    col_start <- v_dates[-length(v_dates)] + 1

    # End Date column
    col_end <- v_dates[-1]

    # Start MV column
    col_mv_start <- v_mv[-length(v_mv)]

    # End MV column
    col_mv_end <- v_mv[-1]

    # Inflows column
    col_inflows <- numeric(length(v_dates) - 1)
    col_inflows[v_ag_inflows$index] <- v_ag_inflows$amount

    # Outflows column
    col_outflows <- numeric(length(v_dates) - 1)
    col_outflows[v_ag_outflows$index] <- v_ag_outflows$amount

    # Yield column
    v_idx_flows <- findInterval(p$date, v_dates, left.open = TRUE)
    col_irr <- sapply(unique(v_idx_flows), function(idx) {
        pds <- p[v_idx_flows == idx,]
        cf_dates <- c(v_dates[idx], pds$date, v_dates[idx+1])
        dt <- datediff("act/365f")(cf_dates, v_dates[idx+1])
        cf_amounts <- -buysell(pds$buy_flag) * pds$amount
        cf_amounts <- c(-v_mv[idx], cf_amounts, v_mv[idx+1])
        uniroot(function(r) sum(exp(r * dt) * cf_amounts), c(-5, 5))$root
    })

    # Resulting table
    v_ds <- data.frame(
        `Start Date` = col_start,
        `End Date` = col_end,
        `Start MV` = col_mv_start,
        `End MV` = col_mv_end,
        Inflows = col_inflows,
        Outflows = col_outflows,
        RealPnL = col_inflows - col_outflows,
        UnrealPnL = col_mv_end - col_mv_start,
        PnL = diff(v_mv) + col_inflows - col_outflows,
        Yield = col_irr
    )

    v_colnames <- c("Start Date", "End Date", "Start MV", "End MV",
                    "Inflows", "Outflows",
                    "Realised PnL", "Unrealised PnL", "PnL", "Yield")
    v_rnd_cols <- c("Start.MV", "End.MV", "Inflows", "Outflows",
                   "RealPnL", "UnrealPnL", "PnL")
    v_pct_cols <- "Yield"

    structure(list(ds = v_ds, colnames = v_colnames, rnd_cols = v_rnd_cols,
                   pct_cols = v_pct_cols), class = "hist_pnl")
}
