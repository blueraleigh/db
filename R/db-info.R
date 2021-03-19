#' List the tables in a database
#'
#' @param db The database connection. S4 object of class "database".
#' @return A character vector of table names in the database.
#' @export
db.tables = function(db) {
    stopifnot(is(db, "database"))
    tables = db.eval(db,
        "SELECT name FROM sqlite_master WHERE type='table' OR type='view'")
    if (is.null(tables))
        return (character(0))
    return (unname(unlist(tables[,1])))
}

#' List the schema of database tables
#'
#' @param db The database connection. S4 object of class "database".
#' @param table A character vector of table names in the database.
#' @return A data.frame listing the name and SQL schema for each table.
#' @export
db.schema = function(db, table) {
    stopifnot(is(db, "database"))
    if (missing(table) || is.null(table))
    {
        return (db.eval(db,
            "SELECT name,sql AS schema FROM sqlite_master WHERE type='table'"
            , df=TRUE))
    }
    else
    {
        stopifnot(is.character(table))
        ntbl = length(table)
        stmt = sprintf(
            "SELECT name,sql AS schema FROM sqlite_master WHERE name IN (%s)",
            paste0(rep("?", ntbl), collapse=","))
        return (db.eval(db, stmt, table, TRUE))
    }
}

#' List the fields of a database table
#'
#' @param db The database connection. S4 object of class "database".
#' @param table The name of a table in the database.
#' @return A data.frame listing the name and storage mode of each field
#' in the table.
#' @export
db.fields = function(db, table) {
    stopifnot(is(db, "database"))
    stopifnot(is.character(table))
    return (db.eval(
        db,
        "SELECT name,type FROM pragma_table_info(?)",
        table[1L], TRUE))
}

#' Check for the existence of a database table, view, or index
#'
#' @param db The database connection. S4 object of class "database".
#' @param name The name of the table, view, or index in the database.
#' @return \code{TRUE} if the item exists. Otherwise \code{FALSE}.
#' @export
db.exists = function(db, name) {
    stopifnot(is(db, "database"))
    if (is.null(db.eval(db,
        "SELECT 1 FROM sqlite_master WHERE name=?", name[1L])))
    {
        return (FALSE)
    }
    return (TRUE)
}
