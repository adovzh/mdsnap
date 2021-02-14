#!/usr/bin/env Rscript

library(logging)
library(DBI)
library(mdsnap)

basicConfig("DEBUG")
conn <- dbConnect(
    drv = RPostgreSQL::PostgreSQL(),
    host = Sys.getenv("MDB_HOST"),
    port = Sys.getenv("MDB_PORT"),
    dbname = Sys.getenv("MDB_NAME"),
    user = Sys.getenv("MDB_USER"),
    password = Sys.getenv("MDB_PWD")
)

dbWithTransaction(conn, {
    mdsnap(conn)
})

dbDisconnect(conn)
