library(shinydashboard)
library(mdsnap)
library(logging)

ctx <- defdbcontext(host = Sys.getenv("MDB_HOST"),
                    port = Sys.getenv("MDB_PORT"),
                    dbname = Sys.getenv("MDB_NAME"),
                    user = Sys.getenv("MDB_USER"),
                    password = Sys.getenv("MDB_PWD"))
db_connect(ctx)
pname <- portfolio_list(ctx)[1, "name"]
p <- portfolio_load(ctx$conn, pname)

idashboard_cleanup <- function() {
    db_disconnect(ctx)
    conn_status <- if (db_connected(ctx)) "connected" else "disconnected"
    loginfo("I-Dashboard cleanup, status: %s", conn_status)
}

idashboard_onStart <- function() {
    conn_status <- if (db_connected(ctx)) "connected" else "disconnected"
    loginfo("I-Dashboard setup, status: %s", conn_status)
    onStop(idashboard_cleanup)
}

# Define UI for application that draws a histogram
ui <- dashboardPage(

    # Application title
    dashboardHeader(title = "I-Dashboard"),
    dashboardSidebar(),
    dashboardBody(
        fluidRow(
            box(title = "Debug", solidHeader = TRUE, status = "primary",
                verbatimTextOutput("debug")),
            infoBoxOutput("asof"),
            infoBox("Portfolio", pname, icon = icon("briefcase"))
        ),
        fluidRow(
            box(title = "Portfolio Allocation", solidHeader = TRUE,
                status = "primary", tableOutput("alloc_summary"))
        )
    )
)

# Define server logic required to draw a histogram
server <- function(input, output) {
    pa <- portfolio_alloc(p)

    output$debug <- renderPrint({
        pa
    })

    output$asof <- renderInfoBox({
        infoBox("As Of", pa$asof, icon = icon("calendar"))
    })

    output$alloc_summary <- renderTable({
        pa$alloc
    })
}

# Run the application
shinyApp(ui = ui, server = server, onStart = idashboard_onStart)
