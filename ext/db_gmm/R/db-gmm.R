.parse_data_arg = function(x) {
    if (is.symbol(x))
        x = as.character(x)
    if (substr(x, 1, 1) == "(") {
        # data argument to virtual table module is a
        # sub-query that will fetch the data for the
        # mixture model
        return (sub(";", "", substr(x, 2, nchar(x)-1)))
    } else {
        # data argument to virtual table module is a
        # true database table that will provide the
        # data for mixture model
        return (paste0("SELECT * FROM ", x))
    }
}

xCreate = function(db, table, ...) {
    # the module can be used as
    #
    # CREATE VIRTUAL TABLE mytable USING gmm(
    #    , data=
    #    , k=
    #    , maxiter=
    #    , eps=);
    #
    # where the arguments are appropriately filled
    # in (or omitted)
    #
    # or a single table can be created
    #
    # CREATE VIRTUAL TABLE gmm USING gmm;
    #
    # and then used in queries like
    #
    # SELECT * FROM gmm WHERE data= AND k= ;

    args = substitute(expression(...))

    schema = "
    CREATE TABLE x(
        model BLOB
        , data       HIDDEN TEXT      -- SQL query for input data
        , k          HIDDEN INTEGER   -- number of clusters
        , maxiter    HIDDEN INTEGER   -- max EM steps
        , eps        HIDDEN REAL      -- stopping tolerance
    );
    "
    env = new.env()

    if (length(args) > 1) {
        if (is.null(args$data))
            stop("no data provided to virtual table module")
        data = .parse_data_arg(args$data)
        k = ifelse(is.null(args$k), 2L, as.integer(args$k))
        eps = ifelse(is.null(args$eps), 1e-6, as.double(args$eps))
        maxiter = ifelse(is.null(args$maxiter), 1000L, as.integer(args$maxiter))
        assign(
            "gmm_fn"
            , function() {
                x = db.eval(db, data, df=TRUE)
                gmm(x, k, eps, maxiter)
            }
            , envir=env
        )
    }

    assign("schema", schema, envir=env)

    return (env)
}

xConnect = function(db, table, ...) {
    xCreate(db, table, ...)
}

xEof = function(db, table, env) {
    if (get("rowid", envir=env) > 1L) {
        if (exists(".gmm_fn", envir=env))
            rm(list=".gmm_fn", envir=env)
        return (TRUE)
    }
    return (FALSE)
}

xBestIndex = function(db, table, env, constraints, orderbys) {
    `%not in%` = function(x, table) {
        is.na(match(x, table))
    }
    if (!exists("gmm_fn", envir=env)) {
        if (!length(constraints))
            return (NULL)
        if (!all(sapply(constraints, "[[", 2L) == SQLITE_INDEX_CONSTRAINT_EQ))
            return (NULL)
        ccols = sapply(constraints, "[[", 1L)
        if (length(constraints) && any(2:3 %not in% ccols)) {
            return (NULL)
        } else {
            argv = ccols
            argv[which(ccols %not in% 2:5)] = NA_integer_
            argv[!is.na(argv)] = order(argv[!is.na(argv)])
            return (list(
                argv=argv
                , idxnum=1L
                , idxname=""
                , issorted=FALSE
                , cost=1)
            )
        }
    }
    return (NULL)
}

xFilter = function(db, table, env, idxnum, idxname, ...) {
    assign("rowid", 1L, envir=env)
    if (idxnum == 1L) {
        data = .parse_data_arg(...elt(1L))
        k = ...elt(2L)
        eps = if (...length() > 2L) as.double(...elt(3L)) else 1e-6
        maxiter = if (...length() > 3L) as.double(...elt(4L)) else 1000L
        assign("data", ...elt(1L), envir=env)
        assign("k", ...elt(2L), envir=env)
        if (...length() > 2L)
            assign("maxiter", ...elt(3L), envir=env)
        if (...length() > 3L)
            assign("eps", ...elt(4L), envir=env)
        assign(
            ".gmm_fn"
            , function() {
                x = db.eval(db, data, df=TRUE)
                gmm(x, k, eps, maxiter)
            }
            , envir=env
        )
    } else if (!exists("gmm_fn", envir=env)) {
        assign("rowid", 2L, envir=env)
    }
}

xColumn = function(db, table, env, j) {
    switch(j,
        if (exists("gmm_fn", envir=env))
            get("gmm_fn", envir=env)()
        else get(".gmm_fn", envir=env)(),
        get("data", envir=env),
        get("k", envir=env),
        get("maxiter", envir=env),
        get("eps", envir=env)
    )
}

register_gmm_module = db.virtualtable("gmm", list(
    .create=xCreate,
    .connect=xConnect,
    .eof=xEof,
    .bestindex=xBestIndex,
    .filter=xFilter,
    .column=xColumn
))

modules = list(gmm=register_gmm_module)
