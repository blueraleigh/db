.list.params = function(l) {
    if (length(unique(lengths(l))) > 1L)
        stop("parameter sets are not all the same length")
    if (sum(sapply(l, is.list)) != length(l))
        stop("invalid parameter format, expected a list of lists")
    return (l)
}

.mat.params = function(mat) {
    params = vector("list", nrow(mat))
    for (i in 1:nrow(mat))
        params[[i]] = as.list(mat[i, ])
    return (params)
}

#' Evaluate SQL statements
#'
#' @param db The database connection. An S4 object of class "database".
#' @param stmt The SQL statement to evaluate. SQL parameters can be
#' bound to the statement and are indicated by the '?' character.
#' @param params A matrix, data.frame, or list of lists with parameters
#' to bind the SQL stmt. Each row in the matrix or data.frame or each
#' sublist in the list of lists corresponds to a single parameter set.
#' The number of parameters in each parameter set should equal the
#' number of '?' characters in the SQL statement. If multiple parameter
#' sets are bound to the statement then the statement may not be a
#' SELECT statement.
#' @return A cursor to retrieve the results of a SELECT statement or
#' (invisibly) NULL if the statement is not a SELECT.
#' @seealso \code{\link{db.fetch}} \code{\link{db.fetchall}}
#' @examples
#' db = db.open()
#' db.eval(db, "CREATE TABLE foo(f1 TEXT)")
#' db.eval(db, "INSERT INTO foo VALUES (?)", list(list("hello")))
#' db.eval(db, "INSERT INTO foo VALUES (?)", data.frame(letters))
#' db.eval(db, "INSERT INTO foo VALUES (?)", matrix("goodbye", 1, 1))
#' cursor = db.eval(db, "SELECT * FROM foo")
#' db.fetchall(cursor)          # return a matrix
#' db.fetchall(cursor, TRUE)    # return a data.frame
#' db.fetch(cursor)             # return one result row
#' db.fetchall(cursor, TRUE)    # note that the first row is missing!
#' db.fetchall(cursor, TRUE)    # okay, now it is back
#' db.close(db)
#' @export
db.eval = function(db, stmt, params) {
    stopifnot(is(db, "database"))
    nbind = 0
    bind = gregexpr("\\?", stmt)[[1]]
    if (missing(params) || is.null(params)) {
        params = list(list())
    } else {
        if (is.data.frame(params) || is.matrix(params))
            params = .mat.params(params)
        else if (is.list(params))
            params = .list.params(params)
        else
            stop("invalid parameter format")
    }
    if (bind[1] > 0)
        nbind = length(bind)
    if (nbind != length(params[[1]]))
        stop(sprintf(
            "expected %d parameters, found %d", nbind, length(params[[1]])))
    params = lapply(params, function(p) {
        lapply(p, function(e) {
            if (is.character(e))
                return (enc2utf8(gsub("'", "''", e)))
            return (e)
        })
    })

    stmt = .Call(db_eval, db@handle, enc2utf8(stmt), params)

    if (!is.null(stmt)) {
        cursor = new("cursor")
        cursor@handle = db@handle
        cursor@stmt = stmt
        return (cursor)
    }
    invisible (NULL)
}


#' Retrieve a row from the result set of a query
#'
#' @param cur The cursor returned by \code{db.eval}. An S4 object of class
#' "cursor".
#' @param df A boolean. Should the result be returned as a data.frame?
#' @return A named list (df=FALSE) or data.frame (df=TRUE) containing a
#' row from the result set.
#' @export
db.fetch = function(cur, df=FALSE) {
    stopifnot(is(cur, "cursor"))
    if (as.logical(df)) {
        res = .Call(db_fetch, cur@stmt, cur@handle)
        if (is.null(res))
            return (NULL)
        return (as.data.frame.list(res))
    }
    return (.Call(db_fetch, cur@stmt, cur@handle))
}


#' Retrieve all rows from the result set of a query
#'
#' @param cur The cursor returned by \code{db.eval}. An S4 object of class
#' "cursor".
#' @param df A boolean. Should the result be returned as a data.frame?
#' @return A matrix (df=FALSE) or data.frame (df=TRUE) containing the rows
#' from the result set. If \code{df=FALSE} subsets of the matrix will
#' return lists rather than atomic vectors.
#' @export
db.fetchall = function(cur, df=FALSE) {
    stopifnot(is(cur, "cursor"))
    return (.Call(db_fetchall, cur@stmt, cur@handle, as.logical(df)))
}


#' Apply a function to rows in a result set
#'
#' @param db The database connection. An S4 object of class "database".
#' @param stmt The SQL statement to evaluate.
#' @param params A matrix, data.frame, or list of lists with parameters.
#' @param FUN The function to apply.
#' @param ... Additional arguments to FUN.
#' @details Function FUN is called on each row returned by the query stmt.
#' The first argument to FUN will be a named list containing the fields
#' for a row returned by the stmt. Subsequent arguments to FUN are passed
#' via ...
#' @return A list, each element of which is the result of applying FUN to
#' a row in the result set.
#' @seealso \code{\link{db.eval}}
#' @examples
#' db = db.open()
#' db.eval(db, "CREATE TABLE t(a REAL, b REAL)")
#' db.eval(db, "INSERT INTO t VALUES(?,?)", matrix(runif(200), 100, 2))
#' db.lapply(db, "SELECT * FROM t", list(list()), function(r) sum(unlist(r)))
#' @export
db.lapply = function(db, stmt, params, FUN, ...) {
    stopifnot(is(db, "database"))
    stopifnot(is.function(FUN))
    if (missing(params))
        params = list(list())
    cur = db.eval(db, stmt, params)
    if (is.null(cur))
        return (list())
    .Call(db_lapply, cur@handle, cur@stmt, match.fun(FUN), list(...))
}
