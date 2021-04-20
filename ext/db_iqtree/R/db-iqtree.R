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
    args = substitute(expression(...))
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
    # 2          2
    # 4          3
    # 8          4
    # 6          2,3
    # 10         2,4
    # 12         3,4
    # 14         2,3,4
    # 0          none

    return (list(
        argv=order(ccols)
        , idxnum=idxnum
        , idxname=as.character(idxnum)
        , issorted=FALSE
        , cost=25)
    )
}

xFilter = function(db, table, env, idxnum, idxname, ...) {

    if (idxnum == 0L)
        idxname = "0"
    sqlar_table = switch(idxname
        , "2"=
        , "6"=
        , "10"=
        , "14"=...elt(1L)
        , env$data
    )
    cmd = switch(idxname
        , "4"=
        , "12"=...elt(1L)
        , "6"=
        , "14"=...elt(2L)
        , env$cmd
    )
    exe = switch(idxname
        , "8"=...elt(1L)
        , "10"=
        , "12"=...elt(2L)
        , "14"=...elt(3L)
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

        tmpdir = tempdir()

        db.unsqlar(db, sqlar_table, tmpdir)
        db.unsqlar(db, "iqtree_output", tmpdir)

        # if any part of the command string specifies a path in the sqlar_table
        # we prepend the temporary working directory to it so that it refers to
        # a valid filesystem path
        args = paste(
            sapply(strsplit(cmd, " ", fixed=TRUE)[[1]], function(s) {
                if (!is.null(
                    db.eval(
                        db
                        , sprintf("SELECT 1 FROM \"%s\" WHERE name=?", sqlar_table)
                        , s)
                    )
                )
                {
                    s = file.path(tmpdir, s)
                }
                s
            }), collapse=" ")

        cat(sprintf("IQ-TREE started at: %s\n\n", Sys.time()))
        system2(exe, args)
        cat(sprintf("\n\nIQ-TREE finished at: %s", Sys.time()))

        output_files = setdiff(
            list.files(
                file.path(tmpdir, db.sqlar_root(db, sqlar_table))
                , recursive=TRUE, include.dirs=TRUE, full.names=TRUE)
            , 
            file.path(tmpdir, db.eval(db, 
                sprintf("SELECT name FROM \"%s\"", sqlar_table), df=TRUE)[,1])
        )

        output_dir = file.path(
            tmpdir, "iqtree_output", floor(unclass(Sys.time())))
        dir.create(output_dir)

        file.copy(
            output_files, 
            file.path(output_dir, basename(output_files)))

        db.sqlar_update(db, "iqtree_output", file.path(tmpdir, "iqtree_output"))
        cat("\n\nResults written to table iqtree_output\n")
        return (sub(file.path(tmpdir, ""), "", output_dir))
    }

    assign("iqtree", iqtree, envir=env)
    assign("rowid", 1L, envir=env)

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
