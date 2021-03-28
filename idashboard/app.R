library(shinydashboard)
library(DT)
library(mdsnap)
library(logging)
library(ggplot2)
library(DBI)
library(pool)

pool <- dbPool(drv = RPostgreSQL::PostgreSQL(),
               host = Sys.getenv("MDB_HOST"),
               port = Sys.getenv("MDB_PORT"),
               dbname = Sys.getenv("MDB_NAME"),
               user = Sys.getenv("MDB_USER"),
               password = Sys.getenv("MDB_PWD"))

onStop(function() {
    poolClose(pool)
})

# Define UI for application that draws a histogram
ui <- dashboardPage(

    # Application title
    dashboardHeader(title = "I-Dashboard"),
    dashboardSidebar(
        sidebarMenu(
            menuItem("Overview", tabName = "tab_overview"),
            menuItem("History", tabName = "tab_history", selected = TRUE)
        )
    ),
    dashboardBody(
        tabItems(
            tabItem(tabName = "tab_overview",
                    fluidRow(
                        box(title = "Holdings", solidHeader = TRUE,
                            status = "primary", DTOutput("dt_holdings"), br(),
                            textOutput("gear_amount")),
                        box(title = "Summary Chart",
                            solidHeader = TRUE,
                            status = "primary", plotOutput("summary_chart"))
                    ),
                    fluidRow(
                        infoBoxOutput("portfolio_name"),
                        infoBoxOutput("asof"),
                        infoBoxOutput("last_date_available")
                    ),
                    fluidRow(
                        box(title = "Total PnL Table", solidHeader = TRUE,
                            status = "primary", width = 12,
                            DTOutput("total_pnl_table"))
                    ),
                    fluidRow(
                        box(title = "Debug", solidHeader = TRUE,
                            status = "primary",
                            verbatimTextOutput("debug"))
                    )
            ),
            tabItem(tabName = "tab_history",
                    fluidRow(
                        box(title = "Historical MV Chart", solidHeader = TRUE,
                            status = "primary", plotOutput("hist_mv_chart")),
                        box(title = "Historical MV Table", solidHeader = TRUE,
                            status = "primary", DTOutput("hist_mv"))
                    ),
                    fluidRow(
                        box(title = "Historical PnL Table", solidHeader = TRUE,
                            status = "primary", width = 12,
                            DTOutput("hist_pnl_table"))
                    ),
                    fluidRow(
                        box(title = "Debug", solidHeader = TRUE,
                            status = "primary",
                            verbatimTextOutput("hist_debug"))
                    )
            )
        )
    )
)

server <- function(input, output) {
    # session globals
    pname <- portfolio_list(pool)[1, "name"]
    p <- portfolio_load(pool, pname)
    pa <- portfolio_alloc(p)
    sl <- security_list(pool)
    psyms <- securities(pa, sl, "non_cash")
    csyms <- securities(pa, sl, "cash")
    mdr <- mdrates(pool, psyms, "close")
    mdr_snapshot <- rates_snapshot(mdr)$close

    # Holdings table
    # index set of non cash portfolio securities in palloc
    hld_nc_idx <- match(psyms, pa$alloc$security)

    # table columns
    hld_units <- pa$alloc$total_units[hld_nc_idx]
    hld_last_price <- mdr_snapshot[hld_nc_idx, "LastRate"]

    holdings <- data.frame(Code = psyms,
                           `Units` = hld_units,
                           `Last Price` = hld_last_price,
                           `Market Value` = hld_units * hld_last_price)

    # gear and equity
    cash_index <- which(pa$alloc$security == csyms)
    cash <- pa$alloc$total_units[cash_index]
    equity <- sum(holdings$Market.Value) + cash
    lvr_ratio <- -cash / sum(holdings$Market.Value)

    # MVs
    mvs <- sapply(seq(as.Date("2017-01-01"), length = 18, by = "quarter"),
                  function(d) {
                      ps <- portfolio_snapshot(p, mdr, sl, asof = d)
                      mdprice(ps)
                  })
    # historical MVs
    hist_mv_dates <- seq(portfolio_start_date(p), to = Sys.Date(), by = "month")
    # hist_mv_dates <- unique(p$date)
    hist_mv_values <- sapply(hist_mv_dates, function(d) {
        mdprice(portfolio_snapshot(p, mdr, sl, asof = d))
    })
    hist_mv_ds <- data.frame(Date = hist_mv_dates, MV = hist_mv_values)

    # historical PnL
    hist_pnl <- hist_pnl(p, mdr, sl)
    total_pnl <- hist_pnl(p, mdr, sl, p_split = "none")

    output$debug <- renderPrint({
        pa
        # ps <- portfolio_snapshot(p, mdr, sl, asof = as.Date("2016-12-31"))
        # mdprice(ps)
        # cat(paste(pa, 42, 23, sep = "\n"))
        # mvs
    })

    output$asof <- renderInfoBox({
        infoBox("As Of", pa$asof, icon = icon("calendar"))
    })

    output$portfolio_name <- renderInfoBox({
        infoBox("Portfolio", pname, icon = icon("briefcase"))
    })

    output$summary_chart <- renderPlot({
        chart_holdings <- holdings[order(holdings$Code, decreasing = TRUE),
                                   c("Code", "Market.Value")]
        mv <- chart_holdings$Market.Value
        total_mv <- sum(mv)
        # print(chart_holdings)
        chart_holdings <- cbind(chart_holdings,
                                lab = sprintf("%.2f%%", mv / total_mv * 100),
                                lab.pos = cumsum(mv) - 0.5 * mv)
        ggplot(chart_holdings, aes(x = "", y = Market.Value, fill = Code)) +
            geom_bar(width = 1, stat = "identity", color = "white") +
            coord_polar(theta = "y") +
            geom_text(aes(y = lab.pos, label = lab, fontface = 2),
                      color = "white", size = 5) +
            theme_void()
    })

    output$last_date_available <- renderInfoBox({
        last_date <- max(mdr_snapshot$LastDate)
        infoBox("Last Date", last_date, icon = icon("calendar"))
    })

    output$dt_holdings <- renderDT({
        sketch <- htmltools::withTags(table(
            tableHeader(c("Code", "Units", "Last Price", "Market Value")),
            tableFooter(c("Total", "", "", 0))))
        footerJs <- "function( tfoot, data, start, end, display ) {
            var api = this.api()
            total = api.column(3).data().reduce(function(a,b){return a+b})
            $(api.column(3).footer()).html('$' + total.toFixed(2).replace(/\\d(?=(\\d{3})+\\.)/g, '$&,'))
        }"
        datatable(holdings,
                  container = sketch,
                  options = list(lengthChange = FALSE,
                                 ordering = FALSE,
                                 paging = FALSE,
                                 searching = FALSE,
                                 info = FALSE,
                                 footerCallback = JS(footerJs)),
                  rownames = FALSE,
                  selection = "none") %>%
            formatRound("Last.Price", digits = 2) %>%
            formatCurrency("Market.Value")
    })

    output$gear_amount <- renderText({
        sprintf("This portfolio is geared by %s. Equity is %s. LVR %.2f%%",
                priceR::format_dollars(-cash, 2),
                priceR::format_dollars(equity, 2),
                lvr_ratio * 100)
    })

    output$hist_debug <- renderPrint({
        mvs
    })

    output$hist_mv <- renderDT({
        datatable(hist_mv_ds)
    })

    output$hist_mv_chart <- renderPlot({
        ggplot(hist_mv_ds, aes(x = Date, y = MV)) + geom_line() +
            scale_y_continuous(labels = scales::dollar_format(prefix = "$"))
    })

    output$hist_pnl_table <- renderDT({
        datatable(hist_pnl$ds,
                  colnames = hist_pnl$colnames,
                  rownames = FALSE,
                  selection = "none",
                  options = list(lengthChange = FALSE,
                                 ordering = FALSE,
                                 paging = FALSE,
                                 searching = FALSE,
                                 info = FALSE)) %>%
            formatRound(hist_pnl$rnd_cols, digits = 2) %>%
            formatPercentage(hist_pnl$pct_cols, digits = 2)
    })

    output$total_pnl_table <- renderDT({
        datatable(total_pnl$ds,
                  colnames = total_pnl$colnames,
                  rownames = FALSE,
                  selection = "none",
                  options = list(lengthChange = FALSE,
                                 ordering = FALSE,
                                 paging = FALSE,
                                 searching = FALSE,
                                 info = FALSE)) %>%
            formatRound(total_pnl$rnd_cols, digits = 2) %>%
            formatPercentage(total_pnl$pct_cols, digits = 2)
    })
}

# Run the application
shinyApp(ui = ui, server = server)
