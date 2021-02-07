#!/bin/bash

. ./db-env.sh

R -e 'shiny::runApp("idashboard", port = 7676, launch.browser = TRUE)'
