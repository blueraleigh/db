% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/db.R
\docType{class}
\name{cursor-class}
\alias{cursor-class}
\alias{Cur}
\title{An S4 class to wrap a prepared SQL statement}
\description{
This is meant for internal use only.
}
\details{
SQLite compiles SQL statements into bytecode which it
executes on a virtual machine. This class maintains a reference to
the compiled SQL program so that it can be used by \code{\link{db.fetch}}.
}
\section{Slots}{

\describe{
\item{\code{stmt}}{An external pointer to the prepared sqlite3_stmt object.}

\item{\code{handle}}{An external pointer to the sqlite3 database connection.}
}}

