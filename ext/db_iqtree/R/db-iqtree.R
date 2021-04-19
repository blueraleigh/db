xCreate = function(db, table, ...) {
    args = substitute(expression(...))
    schema = "
    CREATE TABLE x(
        output TEXT
        , data HIDDEN TEXT -- sqlar table containing sequence data
        , cmd  HIDDEN TEXT -- arguments to the iqtree
        , exe  HIDDEN TEXT -- path to iqtree executable
    );
    "
    db.sqlar_skeleton(db, "iqtree_output")
    db.eval(db, "
        INSERT INTO iqtree_output(name,mode,mtime,sz)
        VALUES('iqtree_output',?,?,0)"
        , list(
            493L
            , unclass(as.POSIXct(Sys.time(), origin="1970-01-01"))
        )
    )
    env = new.env()
    assign("schema", schema, envir=env)
    if (length(args) > 1) {
        if (!is.null(args$data))
            assign("data", args$data, envir=env)
        if (!is.null(args$cmd))
            assign("cmd", args$cmd, envir=env)
        if (!is.null(args$exe))
            assign("exe", args$exe, envir=env)
    }
    return (env)
}


xConnect = function(db, table, ...) {
    schema = "
    CREATE TABLE x(
        output TEXT
        , data HIDDEN TEXT -- sqlar table containing sequence data
        , cmd  HIDDEN TEXT -- arguments to the iqtree
        , exe  HIDDEN TEXT -- path to iqtree executable
    );
    "
    env = new.env()
    assign("schema", schema, envir=env)
    return (env)
}


xEof = function(db, table, env) {
    if (get("rowid", envir=env) > 1L)
        return (TRUE)
    return (FALSE)
}


xBestIndex = function(db, table, env, constraints, orderbys) {
    if (!length(constraints))
        return (NULL)

    ccols = sapply(constraints, "[[", 1L)

    # set a bit for each column that has a constraint
    idxnum = Reduce(function(a, b) {
        bitwOr(a, bitwShiftL(1, b-1))
    }, ccols, 0L)

    # idxnum     # constraint columns
    # 1          1
    # 2          2
    # 4          3
    # 3          1,2
    # 5          1,3
    # 6          2,3
    # 7          1,2,3
    # 0          none

    return (list(
        argv=order(ccols)
        , idxnum=idxnum
        , idxname=""
        , issorted=FALSE
        , cost=1)
    )
}

xFilter = function(db, table, env, idxnum, idxname, ...) {

    sqlar_table = switch(idxnum,
        , `1`=
        , `3`=
        , `5`=
        , `7`=...elt(1L)
        , env$data
    )
    cmd = switch(idxnum,
        , `2`=
        , `6`=...elt(1L)
        , `3`=
        , `7`=...elt(2L)
        , env$cmd
    )
    exe = switch(idxnum,
        , `4`=...elt(1L)
        , `5`=
        , `6`=...elt(2L)
        , `7`=...elt(3L)
        , env$exe
    )

    if (is.null(exe))
        exe = Sys.which("iqtree2")

    if (exe == "")
        stop("unable to find iqtree2")
    if (is.null(sqlar_table))
        stop("no input data specified")
    if (is.null(cmd))
        stop("no arguments supplied to iqtree2")

    iqtree = function() {
        owd = getwd()
        on.exit(setwd(owd))

        tmpdir = tempdir()

        db.unsqlar(db, sqlar_table, tmpdir)
        db.unsqlar(db, "iqtree_output", tmpdir)

        setwd(file.path(tmpdir, "iqtree_output"))

        # if any part of the command string specifies a path in the sqlar table
        # we prepend the temporary working directory to it so that it refers to
        # a valid filesystem path
        args = paste(
            sapply(strsplit(cmd, " ", fixed=TRUE)[[1]], function(s) {
                if (!is.null(
                    db.eval(
                        db
                        , sprintf("SELECT 1 FROM %s WHERE name=?", sqlar_table)
                        , s)
                    )
                )
                {
                    s = file.path(tmpdir, db.sqlar_root(db, sqlar_table), s)
                }
                s
            }), collapse=" ")

        cat("IQ-TREE started at: %s\n\n", Sys.time())
        system2(exe, args)
        cat("\n\nIQ-TREE finished at: %s", Sys.time())

        db.sqlar_update(db, "iqtree_output", getwd())
        cat("\n\nResults written to table iqtree_output")
    }

    assign("iqtree", iqtree, envir=env)

}


xColumn = function(db, table, env, j) {
    iqtree = env$iqtree
    enclos = environment(iqtree)
    switch(j
        , iqtree()
        , enclos$sqlar_table
        , enclos$cmd
        , enclos$exe)
}


register_iqtree_module = db.virtualtable("iqtree", list(
    .create=xCreate,
    .connect=xConnect,
    .eof=xEof,
    .bestindex=xBestIndex,
    .filter=xFilter,
    .column=xColumn
))
