#' Database context constructor
#'
#' @param host Database host.
#' @param port Database port
#' @param dbname Database name.
#' @param user Database user.
#' @param password Database password.
#' @param auto_disconnect Should it be closed after usage
#' @export
defdbcontext <- function(host, port, dbname, user, password, auto_disconnect = TRUE) {
    structure(rlang::env(host = host, port = port, dbname = dbname, user = user,
                   password = password, auto_disconnect = auto_disconnect),
              class = "dbcontext")
}

is_dbcontext <- function(ctx) any(class(ctx) == "dbcontext")

#' Connect to a database
#'
#' @param ctx Database context
#' @export
#' @importFrom DBI dbConnect
#' @importFrom RPostgreSQL PostgreSQL
#' @importFrom logging loginfo
db_connect <- function(ctx) {
    # ctx must of class dbcontext
    stopifnot(is_dbcontext(ctx))

    if (!db_connected(ctx)) {
        ctx$conn <- dbConnect(RPostgreSQL::PostgreSQL(), user = ctx$user,
                              password = ctx$password, host = ctx$host,
                              port = ctx$port, dbname = ctx$dbname)
        loginfo(paste0("Connected to ", ctx$host, ":", ctx$port))
    } else {
        loginfo("Connection already open")
    }
}

#' Disconnect from a database
#'
#' @param ctx Database context
#' @export
#' @importFrom DBI dbDisconnect
#' @importFrom RPostgreSQL PostgreSQL
#' @importFrom logging loginfo
db_disconnect <- function(ctx) {
    # ctx must of class dbcontext
    stopifnot(is_dbcontext(ctx))

    if (db_connected(ctx)) {
        dbDisconnect(ctx$conn)
        ctx$conn <- NULL
        loginfo(paste0("Disconnected from ", ctx$host, ":", ctx$port))
    } else {
        loginfo("Connection is already closed")
    }
}

#' Check if connected to a database
#'
#' Bug: returns TRUE if database connection is expired
#'
#' @param ctx Database context
#' @export
db_connected <- function(ctx) {
    # ctx must of class dbcontext
    stopifnot(is_dbcontext(ctx))

    !is.null(ctx$conn)
}
