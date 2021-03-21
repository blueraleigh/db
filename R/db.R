#' An S4 class to wrap a SQLite database connection
#'
#' @slot name The name of the database file.
#' @slot file The absolute file path of the database file.
#' @slot registered A function to test if the connection is registered.
#' @slot handle An external pointer to the sqlite3 database connection.
#' @details If the database file exists it is opened; otherwise, a new
#' database file is created. An exception is when the database file is
#' either ":memory:" or "". In the former case, a new database is created
#' in computer memory; in the latter, a new database is created in a
#' system specific temporary folder.
Db = setClass(
    "database"
    , slots=list(
        name="character"
        , file="character"
        , registered="function"
        , handle="externalptr")
    )


#' An S4 class to wrap a prepared SQL statement
#'
#' This is meant for internal use only.
#'
#' @slot stmt An external pointer to the prepared sqlite3_stmt object.
#' @slot handle An external pointer to the sqlite3 database connection.
#' @details SQLite compiles SQL statements into bytecode which it
#' executes on a virtual machine. This class maintains a reference to
#' the compiled SQL program so that it can be used by \code{\link{db.fetch}}.
Cur = setClass("cursor", slots=list(stmt="externalptr", handle="externalptr"))


#' An environment to store database connections
#'
#' @details Database connections are registered under their
#' file name. Temporary on-disk and in-memory databases are
#' never registered in this environment.
DB.connections = new.env()
