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
    # CREATE VIRTUAL TABLE mypca USING pca(
    #    , formula=
    #    , data=);
    #
    # where the arguments are appropriately filled
    # in (or omitted)
    #
    # or a single table can be created
    #
    # CREATE VIRTUAL TABLE pca USING pca;
    #
    # and then used in queries like
    #
    # SELECT * FROM pca WHERE formula= AND data= ;

    args = substitute(expression(...))

    schema = "
    CREATE TABLE x(
        model      BLOB
        , formula  HIDDEN TEXT   -- pca formula (can be omitted)
        , data     HIDDEN TEXT   -- SQL query for input data
    );
    "
    env = new.env()

    if (length(args) > 1) {
        if (is.null(args$data))
            stop("no data provided to virtual table module")
        form = args$formula
        data = .parse_data_arg(args$data)
        assign(
            "prcomp_fn"
            , function() {
                x = db.eval(db, data, df=TRUE)
                if (is.null(form))
                    prcomp(x)
                else
                    prcomp(as.formula(form), x)
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
        if (exists(".prcomp_fn", envir=env))
            rm(list=".prcomp_fn", envir=env)
        return (TRUE)
    }
    return (FALSE)
}

xBestIndex = function(db, table, env, constraints, orderbys) {
    `%not in%` = function(x, table) {
        is.na(match(x, table))
    }
    if (!exists("prcomp_fn", envir=env)) {
        if (!length(constraints))
            return (NULL)
        if (!all(sapply(constraints, "[[", 2L) == SQLITE_INDEX_CONSTRAINT_EQ))
            return (NULL)
        ccols = sapply(constraints, "[[", 1L)
        if (length(constraints) && all(2:3 %not in% ccols)) {
            return (NULL)
        } else {
            argv = ccols
            argv[which(ccols %not in% 2:3)] = NA_integer_
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
        if (...length() == 2L) {
            form = as.formula(...elt(1L))
            data = .parse_data_arg(...elt(2L))
            assign("formula", ...elt(1L), envir=env)
            assign("data", ...elt(2L), envir=env)
        } else {
            form = NULL
            data = .parse_data_arg(...elt(1L))
            assign("formula", NULL, envir=env)
            assign("data", ...elt(1L), envir=env)
        }
        assign(
            ".prcomp_fn"
            , function() {
                x = db.eval(db, data, df=TRUE)
                if (is.null(form))
                    prcomp(x)
                else
                    prcomp(form, x)
            }
            , envir=env
        )
    } else if (!exists("prcomp_fn", envir=env)) {
        assign("rowid", 2L, envir=env)
    }
}

xColumn = function(db, table, env, j) {
    switch(j,
        if (exists("prcomp_fn", envir=env))
            get("prcomp_fn", envir=env)()
        else get(".prcomp_fn", envir=env)(),
        get("formula", envir=env),
        get("data", envir=env),
    )
}

register_pca_module = db.virtualtable("pca", list(
    .create=xCreate,
    .connect=xConnect,
    .eof=xEof,
    .bestindex=xBestIndex,
    .filter=xFilter,
    .column=xColumn
))

modules = list(pca=register_pca_module)
