#' Convert input parameters to expected format (list of lists)
#'
#' This is an internal function and not meant to be
#' called outside the db package.
db.param = function(params) {
    if (missing(params) || is.null(params) || is.na(params))
        return (list(list()))
    else if (is.atomic(params) && !is.matrix(params))
        params = list(list(params))
    else if (is.matrix(params) || is.data.frame(params))
        params = apply(params, 1L, as.list)
    else if (is.list(params)) {
        # in case of list(a=1,b=2) instead of list(list(a=1,b=2))
        if (!is.recursive(params[[1L]]))
            params = list(params)
        if (length(unique(lengths(params))) > 1L)
            stop("parameter sets are not all the same length")
        if (sum(sapply(params, is.list)) != length(params))
            stop("invalid parameter format, expected a list of lists")
    } else
        stop("invalid parameters")
    lapply(params, function(p) {
        lapply(p, function(e) {
            if (is.character(e))
                return (enc2utf8(e))
            return (e)
        })
    })
}


#' Prepare a SQL statement for evaluation
#'
#' This is an internal function and not meant to be
#' called outside the db package.
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
#' @return A cursor that can be used by \code{\link{db.fetch}}
db.prepare = function(db, stmt, params) {
    stopifnot(is(db, "database"))
    params = db.param(params)
    npars = length(params[[1L]])
    # match all ? that are not between quotes
    # we do so by matching all ? that are followed by
    # an even number of quotes (single or double)
    re="\\?(?=(?:[^'\"]*['\"][^'\"]*['\"])*[^'\"]*\\Z)"
    nbind = if ((nbind <- gregexpr(re, stmt, perl=TRUE)[[1]])[1L] > 0) {
        length(nbind)
    } else {
        0L
    }
    if (nbind != npars)
        stop(gettextf("expected %d parameters, found %d", nbind, npars))
    stmt = .Call(db_prepare, db@handle, enc2utf8(stmt), params)
    if (!is.null(stmt)) {
        cursor = new("cursor")
        cursor@handle = db@handle
        cursor@stmt = stmt
        return (cursor)
    }
    return (NULL)
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
#' SELECT statement. For the common case when there is only a single parameter
#' set it is permitted to pass an atomic vector or a single list (i.e., no
#' sublists) with the parameter values to bind.
#' @param row_factory The name of a function to apply to each row of the result
#' set before it is returned.
#' @return The result of a SELECT statement or NULL if the
#' statement is not a SELECT. Results will be returned as
#' a matrix (row_factory="identity") or data.frame (row_factory="data.frame") 
#' containing the rows from the result set, or a list, each element of 
#' which is the result of applying row_factory to the corresponding row in 
#' the result set. If \code{row_factory="identity"} subsets of the matrix will
#' return lists rather than atomic vectors. This is necessary because
#' elements of the matrix can hold any type of object that are not
#' necessarily the same from one element to the next.
#' @examples
#' db = db.open()
#' db.eval(db, "CREATE TABLE foo(f1 TEXT)")
#' db.eval(db, "INSERT INTO foo VALUES (?)", list("hello"))
#' db.eval(db, "INSERT INTO foo VALUES (?)", c("world"))
#' db.eval(db, "INSERT INTO foo VALUES (?)", data.frame(letters))
#' db.eval(db, "INSERT INTO foo VALUES (?)", matrix("goodbye", 1, 1))
#' db.eval(db, "SELECT * FROM foo")
#' db.close(db)
#' @export
db.eval = function(db, stmt, params
        , row_factory="identity"
        , simplify=getOption("DBPKG_SIMPLIFY_RETURN", FALSE)) {
    stopifnot(is(db, "database"))
    cursor = db.prepare(db, stmt, params)
    ans = db.fetch(cursor, row_factory)
    if (simplify && !is.null(ans) && (row_factory=="identity" ||
        row_factory=="data.frame")) {
        if (nrow(ans) == 1) {
            ans = ans[1,]
            if (is.list(ans) && length(ans) == 1)
                ans = ans[[1]]
        }
    }
    ans
}


#' Execute a SQL script
#'
#' Run one or more semi-colon terminated SQL statements
#'
#' @param db The database connection. An S4 object of class "database".
#' @param sql The SQL script to evaluate.
#' @return NULL
#' @export
db.exec = function(db, script) {
    stopifnot(is(db, "database"))
    .Call(db_exec, db@handle, enc2utf8(script))
}


#' Retrieve the rows from the result set of a query
#'
#' This is an internal function and not meant to be called outside
#' the db package
#'
#' @param cur The cursor returned by \code{db.eval}. An S4 object of class
#' "cursor".
#' @param row_factory The name of a function to apply to each row of the result
#' set before it is returned.
#' @note The cursor is invalidated afterwards. Do not attempt to use it
#' again.
db.fetch = function(cur, row_factory) {
    if (is.null(cur))
        return (NULL)
    stopifnot(is(cur, "cursor"))
    return (.Call(db_fetch, cur@stmt, cur@handle, row_factory))
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
#' db.lapply(db, "SELECT * FROM t", FUN=function(r) sum(unlist(r)))
#' @export
db.lapply = function(db, stmt, params, FUN, ...) {
    stopifnot(is(db, "database"))
    stopifnot(is.function(FUN))
    cur = db.prepare(db, stmt, params)
    if (is.null(cur))
        return (list())
    .Call(db_lapply, cur@handle, cur@stmt, match.fun(FUN), list(...))
}
