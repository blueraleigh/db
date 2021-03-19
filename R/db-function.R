#' Register functions with a database connection
#'
#' @param db The database connection. S4 object of class "database".
#' @param NAME The name to register the function under.
#' @param FUN The function to register.
#' @details Functions registered with a database connection can be referred to
#' by NAME in SQL statements. To unregister a function NAME, pass NULL as the
#' FUN argument. See examples.
#' @note A current limitation is that arguments to functions inside SQL
#' statements can only be referred to by position. In other words, constructs
#' like argname=argvalue will not work.
#' @return None.
#' @examples
#' db = db.open()
#' db.function(db, "multiply", `*`)
#' db.eval(db, "CREATE TABLE t(a REAL, b REAL)")
#' db.eval(db, "INSERT INTO t VALUES (?,?)", matrix(rnorm(200), 100, 2))
#' db.eval(db, "SELECT multiply(a, b) FROM t", df=TRUE)
#' db.function(db, "multiply", NULL)
#' \dontrun{
#' db.eval(db, "SELECT multiply(a, b) FROM t") # no such function: multiply
#' }
#' @export
db.function = function(db, NAME, FUN) {
    stopifnot(is(db, "database"))
    stopifnot(inherits(NAME, "character"))
    stopifnot(length(NAME) == 1)
    stopifnot(is.null(FUN) || is.function(FUN))
    if (!is.null(FUN))
        FUN = match.fun(FUN)
    invisible(.Call(db_create_function, db@handle, FUN, enc2utf8(NAME)))
}
