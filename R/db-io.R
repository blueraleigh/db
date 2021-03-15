#' Open a new database connection
#'
#' @param file A character string specifying the database file.
#' If it is the special string ':memory:', a new temporary database
#' is created in computer memory. If it is an empty string, a new
#' on-disk database is created in the system's temporary location.
#' Otherwise it should specify a file path. If the path exists, the
#' database is opened. If it does not exist a new on-disk database is
#' created at that location and opened.
#' @param functions A named list of functions. Each element is a
#' user-defined SQL function to register with the database connection.
#' @param modules A list of virtual table module factories created
#' by \code{\link{new.virtualtable}}. Each element should be a function
#' that takes as a single parameter the database connection. When called
#' the function will register the virtual table implementation, with
#' the database connection.
#' @param handlers A named list of functions. Each element is a function
#' that implements a database documentation page. See \code{\link{db.httpd}}
#' for details on how to write these.
#' @details When a database connection is opened the database is checked
#' for the presence of two system tables named dbpkg_vtable and
#' dbpkg_sqlfunc. These two tables can be used to register virtual table
#' implementations and user-defined SQL functions with the database
#' connection. dbpkg_vtable should have the following schema
#' \preformatted{
#' CREATE TABLE dbpkg_vtable (
#'    mod TEXT NOT NULL,
#'    pkg TEXT NOT NULL
#' )
#' }
#' where 'mod' is the name of a virtual table module and 'pkg' is the name
#' of an R package that implements the module methods. This package must
#' export a function named db.register_<mod>, where <mod> is replaced by
#' the value of 'mod' stored in the database table. This should be a function
#' returned by a call to \code{\link{new.virtualtable}}. db will then call
#' that function, passing it the newly created database connection, which will
#' register that module with the database.
#'
#' dbpkg_sqlfunc should have the following schema
#' \preformatted{
#' CREATE TABLE dbpkg_sqlfunc (
#'    name TEXT NOT NULL,
#'    func TEXT NOT NULL,
#'    pkg TEXT NOT NULL
#' )
#' }
#' where 'name' is the name of user-defined SQL function, 'func' is the name
#' of the R function that implements it, and 'pkg' is the name of the package
#' that exports the R function. db will then call \code{\link{db.function}}
#' to register the function with the newly created database connection under
#' the given 'name'.
#'
#' When a database connection is opened the database is also checked for a
#' system table named dbpkg_doc that can be used to add static or dynamic web
#' pages to view and interact with the database.
#'
#' dbpkg_doc should have the following schema
#' \preformatted{
#' CREATE TABLE dbpkg_doc (
#'    page TEXT NOT NULL,
#'    handler TEXT NOT NULL,
#'    pkg TEXT NOT NULL
#' )
#' }
#' where 'page' is the name of web page, 'handler' is the name
#' of the R function that implements it, and 'pkg' is the name of the package
#' that exports the R function. For a description of how handler functions
#' should be written consult the documentation for \code{\link{db.httpd}}.
#' @return An S4 object of class "database". This object has two slots.
#' the \code{handle} slot is an external pointer to the underlying
#' sqlite3 database object. The \code{file} slot is a character string
#' with the database file.
#' @export
db.open = function(file=":memory:", functions=list(), modules=list()
    , handlers=list()) {
    stopifnot(is.character(file))
    stopifnot(is.list(functions))
    stopifnot(is.list(modules))
    stopifnot(is.list(handlers))
    if (!is.null(db <- db.get(basename(enc2utf8(file)))))
        return (db)
    db = Db()
    db@file = basename(enc2utf8(file))
    db@handle = .Call(db_open, db@file)
    db.function(db, "sqlar_compress", sqlar_compress)
    db.function(db, "sqlar_uncompress", sqlar_uncompress)
    if (length(functions) && is.null(names(functions)))
        stop("functions list must be named")
    if (length(handlers) && is.null(names(handlers)))
        stop("handlers list must be named")
    handler = db.httpd
    for (i in seq_along(functions)) {
        FUN = functions[[i]]
        NAME = names(functions)[i]
        if (!is.function(FUN))
            stop("functions list contains non-function")
        db.function(db, NAME, FUN)
    }
    for (i in seq_along(modules)) {
        mod = modules[[i]]
        if (!is.function(mod))
            stop("modules list contains non-function")
        mod(db)
    }
    for (i in seq_along(handlers)) {
        h = handlers[[i]]
        if (!is.function(h))
            stop("handlers list contains non-function")
        attr(handler, sprintf("%s-page", names(handlers)[i])) = h
    }
    if (db.exists(db, "dbpkg_vtable")) {
        db.lapply(db,
            "SELECT * FROM dbpkg_vtable", NULL,
            function(l) {
                msg0 = paste0(
                    "Virtual table module '%s' could not be loaded"
                    , " because package '%s' is missing."
                    , "\nSome functionality may be absent.")
                msg1 = paste0(
                    "Virtual table module '%s' could not be loaded"
                    , " because package '%s' does not export a"
                    , "\n'db.register_%s' function. Some functionality"
                    , " may be absent.")
                if (requireNamespace(l$pkg, quietly=TRUE)) {
                    factory = sprintf("db.register_%s", l$mod)
                    if (!is.null(ffn <- get0(factory, getNamespace(l$pkg))))
                        ffn(db)
                    else
                        message(sprintf(msg1, l$mod, l$pkg, l$mod))
                } else {
                    message(sprintf(msg0, l$mod, l$pkg))
                }
            }
        )
    }
    if (db.exists(db, "dbpkg_sqlfunc")) {
        db.lapply(db,
            "SELECT * FROM dbpkg_sqlfunc", NULL,
            function(l) {
                msg0 = paste0(
                    "User-defined SQL function '%s' could not be loaded"
                    , " because package '%s' is missing."
                    , "\nSome functionality may be absent.")
                msg1 = paste0(
                    "User-defined SQL function '%s' could not be loaded"
                    , " because package '%s' does not export '%s'."
                    , "\nSome functionality may be absent.")
                if (requireNamespace(l$pkg, quietly=TRUE)) {
                    if (!is.null(fn <- get0(l$func, getNamespace(l$pkg))))
                        db.function(db, l$name, fn)
                    else
                        message(sprintf(msg1, l$name, l$pkg, l$func))
                } else {
                    message(sprintf(msg0, l$name, l$pkg))
                }
            }
        )
    }
    if (db.exists(db, "dbpkg_doc")) {
        handlers = db.lapply(db,
            "SELECT * FROM dbpkg_doc", NULL,
            function(l) {
                msg0 = paste0("Package '%s' is missing."
                    , " Some documentation may be unavailable")
                msg1 = paste0("Could not import '%s' from package '%s'."
                    , " Some documentation may be unavailable")
                if (requireNamespace(l$pkg, quietly=TRUE)) {
                    if (!is.null(FUN <- get0(
                        l$handler, getNamespace(l$pkg)))) {
                        NAME = sprintf("%s-page", l$page)
                        return (list(NAME=FUN))
                    } else {
                        message(sprintf(msg1, l$handler, l$pkg))
                    }
                } else {
                    message(sprintf(msg0, l$pkg))
                }
                return (NULL)
            }
        )
        for (i in seq_along(handlers)) {
            if (!is.null(handlers[[i]])) {
                NAME = names(handlers[[i]])[1L]
                FUN = handlers[[i]][[1L]]
                attr(handler, NAME) = FUN
            }
        }
    }
    if (length(attributes(handler)))
        assign(db@file, handler, envir=tools:::.httpd.handlers.env)
    if (file != "" && file != ":memory:")
        db.register(db)
    return (db)
}

#' Close a database connection
#'
#' @param db An S4 object of class "database".
#' @export
db.close = function(db) {
    stopifnot(is(db, "database"))
    db.unregister(db)
    invisible(.Call(db_close, db@handle))
}

#' Get a registered database connection
#'
#' @param name The name of the database.
#' @export
db.get = function(name) {
    return (get0(name, DB.connections))
}

#' Register a database connection
#'
#' This is done automatically by \code{\link{db.open}}.
#' Databases are registered under their file name.
#'
#' @param db An S4 object of class "database".
#' @export
db.register = function(db) {
    stopifnot(db@file != "" && db@file != ":memory:")
    assign(db@file, db, envir=DB.connections)
}

#' Unregister a database connection
#'
#' This is done automatically by \code{\link{db.close}}
#'
#' @param db An S4 object of class "database".
#' @export
db.unregister = function(db) {
    rm(db@file, envir=DB.connections)
}
