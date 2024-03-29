% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/db-eval.R
\name{db.prepare}
\alias{db.prepare}
\title{Prepare a SQL statement for evaluation}
\usage{
db.prepare(db, stmt, params)
}
\arguments{
\item{db}{The database connection. An S4 object of class "database".}

\item{stmt}{The SQL statement to evaluate. SQL parameters can be
bound to the statement and are indicated by the '?' character.}

\item{params}{A matrix, data.frame, or list of lists with parameters
to bind the SQL stmt. Each row in the matrix or data.frame or each
sublist in the list of lists corresponds to a single parameter set.
The number of parameters in each parameter set should equal the
number of '?' characters in the SQL statement. If multiple parameter
sets are bound to the statement then the statement may not be a
SELECT statement.}
}
\value{
A cursor that can be used by \code{\link{db.fetch}}
}
\description{
This is an internal function and not meant to be
called outside the db package.
}
