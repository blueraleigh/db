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
#' @param modules A named list of virtual table module factories created
#' by \code{\link{db.virtualtable}}. Each element should be a function
#' that takes as a single parameter the database connection. When called
#' the function will register the virtual table implementation, with
#' the database connection.
#' @param views A named list of functions. Each element is a function
#' that implements a database documentation page. See \code{\link{db.httpd}}
#' for details on how to write these.
#' @param register A boolean. Should the database connection be registered
#' with the internal registry? \code{TRUE} by default, in which case a request
#' to open a database that is already opened will return the same connection.
#' To open a second connection to the same database set \code{register=FALSE}.
#' The documentation system only works on registered database connections.
#' @param mode If \code{mode = "r+"} the database is opened for reading and
#' writing and created if it does not already exist. If \code{mode = "r"} the
#' database is opened for reading only. An error will be returned if the
#' database does not exist or if a write attempt is made.
#' @details When a database connection is opened the database is checked
#' for the presence of a system table named dbpkg. This table can be used to
#' register virtual table implementations, user-defined SQL functions, and
#' documentation pages with the database connection. The dbpkg table should
#' have the following schema \preformatted{
#' CREATE TABLE dbpkg (
#'    pkg TEXT NOT NULL PRIMARY KEY
#' )
#' }
#' where 'pkg' is the name of an R package that implements the various
#' extensions. An R package that implements dbpkg extensions must contain
#' three objects in its namespace named 'functions', 'modules', and 'views',
#' each of which is expected to be a named list of functions to register with
#' the newly formed database connection. Note that any functions passed in
#' as arguments to \code{db.open} (via 'functions', 'modules', and 'views')
#' take precedence over functions provided by dbpkg extensions.
#' @return An S4 object of class "database". This object has three slots.
#' the \code{handle} slot is an external pointer to the underlying
#' sqlite3 database object. The \code{file} slot is a character string
#' with the absolute path of the database file. The \code{name} slot is
#' the name of the database file.
#' @export
db.open = function(file=":memory:", functions=list(), modules=list()
    , views=list(), register=TRUE, mode=c("r+", "r"))
{
    stopifnot(is.character(file))
    stopifnot(is.list(functions))
    stopifnot(is.list(modules))
    stopifnot(is.list(views))
    file = enc2utf8(file)
    if (!register && length(views))
        stop("views can only be supplied for registered connections")
    if (register && !is.null(db <- db.get(basename(file))))
        return (db)
    db = Db()
    db@name = basename(file)
    db@handle = .Call(db_open, file, match.arg(mode))
    if (file != "" && file != ":memory:")
        db@file = normalizePath(file)
    else
        db@file = ""
    db@registered = local({
        registered = FALSE
        function(register) {
            if (!missing(register)) {
                ok = FALSE
                for (frame in 1:sys.nframe()) {
                    fun = sys.function(frame)
                    if (environmentName(environment(fun)) == "db")
                        ok = TRUE
                }
                if (!ok)
                    stop("cannot call `registered` from this context")
                registered <<- register
            }
            else
                return (registered)
        }
    })
    db.function(db, "sqlar_compress", sqlar_compress)
    db.function(db, "sqlar_uncompress", sqlar_uncompress)
    if (length(functions) && is.null(names(functions)))
        stop("functions list must be named")
    if (length(modules) && is.null(names(modules)))
        stop("modules list must be named")
    if (length(views) && is.null(names(views)))
        stop("views list must be named")
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
    for (i in seq_along(views)) {
        view = views[[i]]
        if (!is.function(view))
            stop("views list contains non-function")
        attr(handler, names(views)[i]) = view
    }
    if (db.exists(db, "dbpkg")) {
        db.lapply(db
            , "SELECT DISTINCT pkg FROM dbpkg"
            , FUN = function(l) {
                pkg = l$pkg
                if (requireNamespace(pkg, quietly=TRUE)) {
                    ns = getNamespace(pkg)
                    udfs = get0("functions", ns, mode="list")
                    mods = get0("modules", ns, mode="list")
                    docs = NULL
                    if (register)
                        docs = get0("views", ns, mode="list")
                    for (i in seq_along(udfs)) {
                        if (!names(udfs)[i] %in% functions)
                            db.function(db, names(udfs)[i], udfs[[i]])
                    }
                    for (i in seq_along(mods)) {
                        if (!names(mods)[i] %in% modules)
                            mods[[i]](db)
                    }
                    for (i in seq_along(docs)) {
                        if (!names(docs)[i] %in% views)
                            attr(handler, names(docs)[i]) = docs[[i]]
                    }
                } else {
                    message(gettextf(paste0("Package '%s' is missing."
                        , " Some functionality may be unavailable."), pkg))
                }
            }
        )
    }
    if (register && file != "" && file != ":memory:")
        db.register(db)
    if (db@registered() && length(attributes(handler))) {
        if (nchar(db@name) < 64L) {
            assign(db@name, handler, envir=tools:::.httpd.handlers.env)
        } else {
            message(paste0(
                "Database name must be under 64 characters in length"
                , "\n  for help documentation to work."))
        }
    }
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
    stopifnot(db@name != "" && db@name != ":memory:")
    stopifnot(!db@registered())
    db@registered(TRUE)
    assign(db@name, db, envir=DB.connections)
}

#' Unregister a database connection
#'
#' This is done automatically by \code{\link{db.close}}
#'
#' @param db An S4 object of class "database".
#' @export
db.unregister = function(db) {
    if (db@registered()) {
        rm(list=db@name, envir=DB.connections)
        db@registered(FALSE)
        if (exists(db@name, envir=tools:::.httpd.handlers.env))
            rm(list=db@name, envir=tools:::.httpd.handlers.env)
    }
}
