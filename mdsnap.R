#!/usr/bin/env Rscript

library(mdsnap)
library(logging)

basicConfig("DEBUG")
ctx <- defdbcontext(host = Sys.getenv("MDB_HOST"),
                    port = Sys.getenv("MDB_PORT"),
                    dbname = Sys.getenv("MDB_NAME"),
                    user = Sys.getenv("MDB_USER"),
                    password = Sys.getenv("MDB_PWD"))
mdsnap(ctx)

