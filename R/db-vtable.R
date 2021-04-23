# currently this implementation is not thread-safe because the
# environment used to share data among methods lives with
# the virtual table module, and a cursor just gets a reference
# to that. so any changes one cursor makes (such as incrementing
# the rowid) will impact all other cursors that are derived
# from the same virtual table module object.
.check.method.signature = function(method, FUN) {
    stopifnot(is.function(FUN))
    check = switch(method,
        .create=,
        .connect=function() {
            identical(names(formals(FUN)), c("db", "table", "..."))
        },
        .bestindex=function() {
            identical(names(formals(FUN)),
                c("db", "table", "env", "constraints", "orderbys"))
        },
        .disconnect=,
        .destroy=,
        .open=,
        .next=,
        .eof=,
        .rowid=,
        .begin=,
        .sync=,
        .commit=,
        .rollback=,
        .rename=,
        .savepoint=,
        .release=,
        .rollbackTo=,
        .close=function() {
            identical(names(formals(FUN)), c("db", "table", "env"))
        },
        .filter=function() {
            identical(names(formals(FUN)),
                c("db", "table", "env", "idxnum", "idxname", "..."))
        },
        .column=function() {
            identical(names(formals(FUN)), c("db", "table", "env", "j"))
        },
        .delete=function() {
            identical(names(formals(FUN)),
                c("db", "table", "env", "rid"))
        },
        .insert=function() {
            identical(names(formals(FUN)),
                c("db", "table", "env", "rid", "..."))
        },
        .update=function() {
            identical(names(formals(FUN)),
                c("db", "table", "env", "rid", "oid", "..."))
        },
        .findmethod=function() {
            identical(names(formals(FUN)), c("db", "table", "env", "name"))
        },
        function() return (TRUE)
    )
    if (!check())
        stop(sprintf("invalid signature for method '%s'", method))
}

#' An S4 class to implement new virtual table modules
#'
#' @slot database The database connection. An S4 object of class "database".
#' @slot name A character string. What to call the virtual table module.
#' @slot method A function to register user-defined virtual table methods.
#' @slot .methodEnv An environment in which virtual table methods are invoked.
#' @seealso \code{\link{new.virtualtable}}
VirtualTable =
setClass(
    "virtualtable",
    slots=list(
        database="database",
        name="character",
        method="function",
        .methodEnv="environment"
    )
)

setMethod(
    "initialize",
    "virtualtable",
    function(
    .Object, database, name, env, ...) {
        .Object@database = database
        .Object@name = name
        .Object@.methodEnv = env
        .Object@method = function(method, FUN) {
            .check.method.signature(method, FUN)
            assign(method, FUN, envir=.Object@.methodEnv)
        }
        .Object@method(".create",
            function(db, table, ...) {return (NULL)})
        .Object@method(".connect",
            function(db, table, ...) {return (NULL)})
        .Object@method(".bestindex",
            function(db, table, env, constraints, orderbys) {return (NULL)})
        .Object@method(".disconnect",
            function(db, table, env) {return (NULL)})
        .Object@method(".destroy",
            function(db, table, env) {return (NULL)})
        .Object@method(".open",
            function(db, table, env) {
                assign("rowid", 1L, envir=env)
                return (NULL)
            })
        .Object@method(".close",
            function(db, table, env) {return (NULL)})
        .Object@method(".filter",
            function(db, table, env, idxnum, idxname, ...) {
                assign("rowid", 1L, envir=env)
                return (NULL)
            })
        .Object@method(".next",
            function(db, table, env) {
                rid = get("rowid", envir=env)
                assign("rowid", rid+1L, envir=env)
                return (NULL)
            })
        .Object@method(".eof",
            function(db, table, env) {return (TRUE)})
        .Object@method(".column",
            function (db, table, env, j) {return (NULL)})
        .Object@method(".rowid",
            function(db, table, env) {
                return (get("rowid", envir=env))
            })
        .Object@method(".insert",
            function(db, table, env, rid, ...) {return (NA)})
        .Object@method(".update",
            function(db, table, env, rid, oid, ...) {return (NA)})
        .Object@method(".delete",
            function(db, table, env, rid) {return (NA)})
        .Object@method(".begin",
            function(db, table, env) {return (NULL)})
        .Object@method(".sync",
            function(db, table, env) {return (NULL)})
        .Object@method(".commit",
            function(db, table, env) {return (NULL)})
        .Object@method(".rollback",
            function(db, table, env) {return (NULL)})
        .Object@method(".findmethod",
            function(db, table, env, name) {return (NULL)})
        .Object@method(".rename",
            function(db, table, env) {return (NULL)})
        .Object@method(".savepoint",
            function(db, table, env) {return (NULL)})
        .Object@method(".release",
            function(db, table, env) {return (NULL)})
        .Object@method(".rollbackto",
            function(db, table, env) {return (NULL)})
        return (.Object)
    }
)

#' Create a virtual table module
#'
#' @param name The name of the virtual table module
#' @param methods A named list of functions implementing the virtual table
#' @param env An environment in which virtuabl table methods will be
#' evaluated.
#' @details A virtual table acts like a regular database table except that
#' SQL statements invoke callback methods defined by the virtual table
#' module. In the current implementation there are 13 methods that can be
#' written. Not all of these methods are required as some have sensible
#' defaults. Each section below starts by showing the formal arguments each
#' method is expected to have, followed by a description of the method. To be
#' clear, these methods are typically not invoked by the user. Rather, they are
#' invoked automatically when an SQL statement is run against a virtual table.
#' The SQLite core will communicate the arguments to the different methods.
#' @section The \code{.create} method:
#' function(db, table, ...)
#'
#' The \code{.create} function is called whenever a new virtual table
#' is created by a CREATE VIRTUAL TABLE statement. The function should return
#' a new environment for the virtual table, which can be used to store data
#' that needs to be communicated among the different callbacks. At a minimum,
#' the returned environment should contain a \code{'schema'} variable, which
#' is a scalar character vector specifying the SQL schema of the
#' virtual table.
#'
#' Argument \code{table} is the name of the virtual table being created.
#' The \code{...} contain additional arguments specified as part of the
#' CREATE VIRTUAL TABLE statement. For example, the statement \preformatted{
#'     CREATE VIRTUAL TABLE tablename USING modulename(arg1, arg2, arg3)
#' }
#' will call the \code{.create} function with argument \code{table} set equal to
#' 'tablename' and with module arguments arg1, arg2, arg3 passed through
#' to \code{...}
#'
#' Note that virtual tables can be associated with regular tables
#' that provide a persistent data store. In this case, regular tables
#' tables should follow a strict naming convention like so \preformatted{
#'     <virtual table name>_<persistent table name>
#' }
#' Furthermore, it is imperative that the \code{.create} and \code{.connect}
#' function call db.register_shadowname with <persistent table name> as the
#' \code{name} argument. Failure to do so will allow ordinary SQL to alter
#' the contents of the data store. Calling db.register_shadowname
#' ensures that SQL can alter the data store contents only if it
#' originates from within one of the virtual table's methods.
#' @section The \code{.connect} method:
#' function(db, table, ...)
#'
#' The \code{.connect} function is called whenever a database connects
#' to or reparses a virtual table schema. It receives the same arguments
#' as the \code{.create} function. The only time the \code{.create} and
#' \code{.connect} functions are different is when the \code{.create} function
#' needs to do some initialization work to set up a persistent data store
#' in the form of real database tables.
#' @section The \code{.disconnect} method:
#' function(db, table, env)
#'
#' The \code{.disconnect} function is called whenever a database connection that
#' uses a virtual table is closed. The default method does nothing.
#' @section The \code{.destroy} method:
#' function(db, table, env)
#'
#' The \code{.destroy} function is called whenever a virtual table is deleted by
#' a DROP TABLE statement. The default method does nothing.
#' @section The \code{.open} method:
#' function(db, table, env)
#'
#' The \code{.open} function is called whenever a cursor is needed to access
#' rows of a virtual table. The default method creates a variable named "rowid"
#' in \code{env} and assigns it the value of 1.
#' @section The \code{.close} method:
#' function(db, table, env)
#'
#' The \code{.close} function is called whenever a previously opened cursor is
#' no longer needed. The default method does nothing.
#' @section The \code{.rowid} method:
#' function(db, table, env)
#'
#' The \code{.rowid} function returns the rowid of the row of data the cursor is
#' currently pointing to. The default method returns the current value bound to
#' "rowid" in \code{env}.
#' @section The \code{.next} method:
#' function(db, table, env)
#'
#' The \code{.next} function advances the cursor to the next row of data.
#' The default method increments the current value bound to "rowid" in
#' \code{env} by 1.
#' @section The \code{.eof} method:
#' function(db, table, env)
#'
#' The \code{.eof} function determines if the cursor has advanced passed the
#' last row of data. The default method returns \code{TRUE}.
#' @section The \code{.column} method:
#' function(db, table, env, j)
#'
#' The \code{.column} function returns the value in the j-th column for the
#' current row of data. The default method does nothing.
#' @section The \code{.insert} method:
#' function(db, table, env, rid, ...)
#'
#' The \code{.insert} function is called in response to an INSERT statement on
#' a virtual table. If it returns NA the table is marked
#' as read only and the operation is prohibited. The default method returns
#' NA.
#'
#' Values in \code{...} contain the data being inserted. The values will be in
#' the same length and order as the table's declared columns. Any columns not
#' supplied in the insert statement will be represented as \code{NA} in \code{...}
#' The rowid for the new row is given by \code{rid}. If \code{rid} is \code{NA}
#' then the \code{.insert} function is responsible for choosing a new rowid.
#'
#' The function should return the value of the newly inserted rowid.
#' @section The \code{.update} method:
#' function(db, table, env, rid, oid, ...)
#'
#' The \code{.update} function is called in response to an
#' UPDATE statement on a virtual table. If it returns NA the table is marked
#' as read only and the operation is prohibited. The default method returns
#' NA.
#'
#' Values in \code{...} contain the data being modified. The values will be in
#' the same length and order as the table's declared columns. Any columns not
#' supplied in the update statement will be represented as \code{NA} in \code{...}
#'
#' \code{oid} indicates the rowid of the data being modified. If \code{rid != oid}
#' then this indicates that the rowid of the updated row is
#' being changed from \code{oid} to \code{rid}.
#'
#' @section The \code{.delete} method:
#' function(db, table, env, rid)
#'
#' The \code{.delete} function is called in response to a DELETE
#' statement on a virtual table. If it returns NA the table is marked
#' as read only and the operation is prohibited. The default method returns
#' NA.
#'
#' \code{rid} indicates the rowid of data to be deleted.
#'
#' @section The \code{.bestindex} method:
#' function(db, table, env, constraints, orderbys)
#'
#' This is a complicated method. For starters, it is best to just leave it as
#' the default, which is to return NULL.
#'
#' The \code{.bestindex} function determines the best index to
#' use to efficiently perform a SQL query.
#'
#' Argument \code{constraints} will be a list of usable constraints that
#' comprise the query. Each element in the list is a vector
#' of length 2. The first element is the column index that
#' the constraint operates on; the second, the type of
#' constraint operator. If the column index is 0, this
#' indicates the constraint operates on the table's rowid.
#'
#' Argument \code{orderbys} is a list of order by terms. Each
#' element in the list is also a vector of length 2.
#' The first element is the column index of the order by term;
#' the second, an integer indicating if the order by term is
#' decreasing (1) or increasing (0).
#'
#' The job of \code{.bestindex} is to use this information to select
#' an index to perform the query and communicate this back
#' to SQLite. The return value should be a list with the following
#' structure.
#'
#' The first element in the list should be an integer vector
#' of the same length as the input \code{constraints} list. If no
#' index is available for a given constraint, then the corresponding
#' value in this vector should be set to \code{NA}, indicating a full
#' table scan is necessary to get the desired result. If an index
#' is available, then the corresponding value in this vector should
#' be set to the position the constraint argument will appear in the \code{...}
#' arguments passed to the \code{.filter} function (see below). For example,
#' consider the following query
#' \preformatted{
#'   SELECT *
#'   FROM books
#'   WHERE author='Pratchett'
#'   AND subject='Discworld'
#'   AND pages >= 250
#' }
#' Now, consider that author is column 3, subject is column 1, and
#' pages is column 6, and that we are able to index on author and
#' subject.
#'
#' The \code{constraints} input to \code{.bestindex} will look like
#' \preformatted{
#' [[1]]
#' [1] 3 SQLITE_INDEX_CONSTRAINT_EQ
#' [[2]]
#' [1] 1 SQLITE_INDEX_CONSTRAINT_EQ
#' [[3]]
#' [1] 6 SQLITE_INDEX_CONSTRAINT_GE
#' }
#' Suppose we want the subject to appear before the author in the \code{.filter} argument
#' list. Then the first item in the \code{.bestindex} list output should look like
#' \preformatted{
#' [1] 2 1 NA
#' }
#' Implying that the \code{...} arguments to filter will be \preformatted{
#' [[1]]
#' [1] 'Discworld'
#' [[2]]
#' [1] 'Pratchett'
#' }
#' If instead we had returned \preformatted{
#' [1] 1 2 NA
#' }
#' then the \code{...} arguments to filter would have been \preformatted{
#' [[1]]
#' [1] 'Pratchett'
#' [[2]]
#' [1] 'Discworld'
#' }
#' The second item in the \code{.bestindex} list output should be an integer
#' number used to identify the chosen index.
#'
#' The third item in the \code{.bestindex} list output should be an character
#' string used to identify the chosen index.
#'
#' The fourth item in the \code{.bestindex} list output should be TRUE if the
#' resulting output is already sorted according to the order by terms.
#' Otherwise it should be FALSE, indicating that SQLite needs to do
#' the sorting.
#'
#' The fifth item in the \code{.bestindex} list output should be the estimated
#' cost of using the returned index to perform the query. SQLite will
#' call \code{.bestindex} multiple times with different inputs and will choose
#' the index that has the lowest estimated cost.
#' @section The \code{.filter} method:
#' function(db, table, env, idxnum, idxname, ...)
#'
#' The \code{.filter} function sets the rowid to the rowid of
#' the first row of data in the result set. For starters, just leave this
#' function set to its default, which resets the "rowid" variable in \code{env}
#' to 1.
#'
#' The arguments are communicated to \code{.filter} by SQLite. Arguments
#' \code{idxnum} and \code{idxname} are set by the \code{.bestindex} function,
#' and the \code{.filter} function is free to interpret them however
#' it likes. If \code{.bestindex} function always returns NULL (the default),
#' \code{idxnum} will be 0 and \code{idxname} will be NULL. The remaining
#' arguments in \code{...} will be the constraint values in the
#' SQL query that \code{.bestindex} selected in the order that \code{.bestindex}
#' selected them. If \code{.bestindex} always returns NULL
#' then the \code{...} list will be empty.
#' @return A function that can be used to register the virtual table
#' module with a database connection.
#' @export
db.virtualtable = function(name, methods, env=new.env()) {

    construct_module = function(db) {
        v = new("virtualtable", database=db, name=name, env=env)
        for (i in seq_along(methods))
            v@method(names(methods)[i], methods[[i]])
        return (v)
    }

    return (function(db) {
        'Call this function with a database connection to
         register the virtual table module.'

        module = construct_module(db)
        db.create_module(module)
        invisible (module)
    })
}


#' Register a virtual table module
#'
#' @param module The virtual table module. An S4 object of class "virtualtable".
#' @return None
#' @export
db.create_module = function(module) {
    stopifnot(is(module, "virtualtable"))
    stopifnot(is(module@database, "database"))
    stopifnot(length(module@name) == 1L)
    stopifnot(nchar(module@name) > 0L)
    invisible(.Call(db_create_module, module@database@handle, module))
}

#' Unregister a virtual table module
#'
#' @param module The virtual table module. An S4 object of class "virtualtable".
#' @return None
#' @export
db.delete_module = function(module) {
    stopifnot(is(module, "virtualtable"))
    stopifnot(is(module@database, "database"))
    invisible(.Call(db_delete_module, module@database@handle, module))
}

#' Register a the name of shadow table for a virtual table
#'
#' For details on shadow tables, including how SQLite recognizes a shadow
#' table, go to \url{https://sqlite.org/vtab.html#the_xshadowname_method}.
#'
#' @param db The database connection. An S4 object of class "database".
#' @param name The shadow name.
#' @details When a shadow name is registered with a database connection
#' the contents of the table bearing the shadow name cannot be altered
#' except by SQL that originates from one of the virtual table methods.
#' @return None
#' @export
db.register_shadowname = function(db, name) {
    stopifnot(is(db, "database"))
    stopifnot(is.character(name))
    invisible(.Call(db_register_shadow_name, db@handle, name))
}

#' Unregister a shadow name
#'
#' @param db The database connection. An S4 object of class "database".
#' @param name The shadow name.
#' @return None
#' @export
db.unregister_shadowname = function(db, name) {
    stopifnot(is(db, "database"))
    stopifnot(is.character(name))
    invisible(.Call(db_unregister_shadow_name, db@handle, name))
}

#' @export
SQLITE_INDEX_CONSTRAINT_EQ        = 2L
#' @export
SQLITE_INDEX_CONSTRAINT_GT        = 4L
#' @export
SQLITE_INDEX_CONSTRAINT_LE        = 8L
#' @export
SQLITE_INDEX_CONSTRAINT_LT        = 16L
#' @export
SQLITE_INDEX_CONSTRAINT_GE        = 32L
#' @export
SQLITE_INDEX_CONSTRAINT_MATCH     = 64L
#' @export
SQLITE_INDEX_CONSTRAINT_LIKE      = 65L  # 3.10.0 and later
#' @export
SQLITE_INDEX_CONSTRAINT_GLOB      = 66L  # 3.10.0 and later
#' @export
SQLITE_INDEX_CONSTRAINT_REGEXP    = 67L  # 3.10.0 and later
#' @export
SQLITE_INDEX_CONSTRAINT_NE        = 68L  # 3.21.0 and later
#' @export
SQLITE_INDEX_CONSTRAINT_ISNOT     = 69L  # 3.21.0 and later
#' @export
SQLITE_INDEX_CONSTRAINT_ISNOTNULL = 70L  # 3.21.0 and later
#' @export
SQLITE_INDEX_CONSTRAINT_ISNULL    = 71L  # 3.21.0 and later
#' @export
SQLITE_INDEX_CONSTRAINT_IS        = 72L  # 3.21.0 and later
#' @export
SQLITE_INDEX_CONSTRAINT_FUNCTION  = 150L # 3.25.0 and later
#' @export
SQLITE_INDEX_SCAN_UNIQUE          = 1L   # Scan visits at most 1 row
