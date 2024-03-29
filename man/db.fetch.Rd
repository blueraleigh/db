% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/db-eval.R
\name{db.fetch}
\alias{db.fetch}
\title{Retrieve the rows from the result set of a query}
\usage{
db.fetch(cur, row_factory)
}
\arguments{
\item{cur}{The cursor returned by \code{db.eval}. An S4 object of class
"cursor".}

\item{row_factory}{The name of a function to apply to each row of the result
set before it is returned.}
}
\description{
This is an internal function and not meant to be called outside
the db package
}
\note{
The cursor is invalidated afterwards. Do not attempt to use it
again.
}
