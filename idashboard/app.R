library(shinydashboard)
library(DT)
library(mdsnap)
library(logging)

# Define UI for application that draws a histogram
ui <- dashboardPage(

    # Application title
    dashboardHeader(title = "I-Dashboard"),
    dashboardSidebar(),
    dashboardBody(
        fluidRow(
            box(title = "Holdings", solidHeader = TRUE,
                status = "primary", DTOutput("dt_holdings")),
            infoBoxOutput("asof"),
            infoBox("Portfolio", pname, icon = icon("briefcase")),
            infoBoxOutput("last_date_available")
        ),
        fluidRow(
            box(title = "Portfolio Allocation", solidHeader = TRUE,
                status = "primary", tableOutput("alloc_summary")),
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

    output$debug <- renderPrint({
        pa
    })

    output$asof <- renderInfoBox({
        infoBox("As Of", pa$asof, icon = icon("calendar"))
    })

    output$alloc_summary <- renderTable({
        pa$alloc
    })

    output$last_date_available <- renderInfoBox({
        mdr <- mdrates(ctx, psyms)
        snap <- rates_snapshot(mdr)$close
        last_date <- max(snap$LastDate)
        infoBox("Last Date", last_date, icon = icon("calendar"))
    })

    output$dt_holdings <- renderDT({
        holdings <- data.frame(Code = psyms,
                               `Units` = c(85.04, 79.65, 487.92),
                               `Last Price` = c(85.04, 79.65, 487.92),
                               `Market Value` = c(85.04, 79.65, 487.92))
        sketch <- htmltools::withTags(table(
            tableHeader(c("Code", "Units", "Last Price", "Market Value")),
            tableFooter(c("Total", "", "", 0))))
        footerJs <- "function( tfoot, data, start, end, display ) {
            var api = this.api()
            total = api.column(3).data().reduce(function(a,b){return a+b})
            $(api.column(3).footer()).html(total.toFixed(2))
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
                  selection = "none")
    })
}

# Run the application
shinyApp(ui = ui, server = server)
