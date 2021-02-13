library(shinydashboard)
library(DT)
library(mdsnap)
library(logging)
library(ggplot2)

# Define UI for application that draws a histogram
ui <- dashboardPage(

    # Application title
    dashboardHeader(title = "I-Dashboard"),
    dashboardSidebar(),
    dashboardBody(
        fluidRow(
            box(title = "Holdings", solidHeader = TRUE,
                status = "primary", DTOutput("dt_holdings"), br(),
                textOutput("gear_amount")),
            box(title = "Summary Chart",
                solidHeader = TRUE,
                status = "primary", plotOutput("summary_chart"))
        ),
        fluidRow(
            infoBoxOutput("asof"),
            infoBoxOutput("portfolio_name"),
            infoBoxOutput("last_date_available")
        ),
        fluidRow(
            box(title = "Debug", solidHeader = TRUE, status = "primary",
                verbatimTextOutput("debug"))
        )
    )
)

server <- function(input, output) {
    ctx <- defdbcontext(host = Sys.getenv("MDB_HOST"),
                        port = Sys.getenv("MDB_PORT"),
                        dbname = Sys.getenv("MDB_NAME"),
                        user = Sys.getenv("MDB_USER"),
                        password = Sys.getenv("MDB_PWD"))
    loginfo("Connecting to DB")
    db_connect(ctx)

    onStop(function() {
        db_disconnect(ctx)
        conn_status <- if (db_connected(ctx)) "connected" else "disconnected"
        loginfo("I-Dashboard session cleanup, status: %s", conn_status)
    })

    pname <- portfolio_list(ctx)[1, "name"]
    p <- portfolio_load(ctx$conn, pname)
    pa <- portfolio_alloc(p)
    sl <- security_list(ctx$conn)
    psyms <- securities(pa, sl, "non_cash")
    csyms <- securities(pa, sl, "cash")
    mdr <- mdrates(ctx, psyms, "close")
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

    output$debug <- renderPrint({
        pa
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
        mdr <- mdrates(ctx, psyms)
        snap <- rates_snapshot(mdr)$close
        last_date <- max(snap$LastDate)
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
}

# Run the application
shinyApp(ui = ui, server = server)
