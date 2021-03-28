#' @export
#' @author Alexander Dovzhikov
datediff <- function(dcc = c("act/365f")) {
    dcc <- match.arg(dcc)
    switch(dcc, `act/365f` = act365f)
}

act365f <- function(d1, d2) as.numeric(d2 - d1) / 365
