xCreate = function(db, table, ...) {
    args = substitute(expression(...))
    schema = "
    CREATE TABLE x(
        status   INTEGER      -- the exit code
        , input  HIDDEN TEXT  -- sqlar table(s) with input data
        , output HIDDEN TEXT  -- sqlar table to hold output data
        , cmd    HIDDEN TEXT  -- arguments to the executable
        , exe    HIDDEN TEXT  -- path to the executable
    );
    "

    env = new.env()
    assign("schema", schema, envir=env)
    if (length(args) > 1) {
        if (!is.null(args$input))
            assign("input", as.character(args$input), envir=env)
        if (!is.null(args$output)) {
            assign("output", as.character(args$output), envir=env)
            if (!db.exists(db, as.character(args$output))) {
                db.sqlar_skeleton(db, as.character(args$output))
                db.eval(db, sprintf("
                    INSERT INTO \"%1$s\"(name,mode,mtime,sz)
                    VALUES('%1$s',?,?,0)", output)
                    , list(
                        493L
                        , as.integer(unclass(as.POSIXct(Sys.time(), origin="1970-01-01")))
                    )
                )
            }
        }
        if (!is.null(args$cmd))
            assign("cmd", as.character(args$cmd), envir=env)
        if (!is.null(args$exe))
            assign("exe", as.character(args$exe), envir=env)
    }

    return (env)
}


xConnect = function(db, table, ...) {
    xCreate(db, table, ...)
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

    # don't index on the first column
    use = ccols != 1L

    # set a bit for each indexable column that has a constraint
    idxnum = Reduce(function(a, b) {
        bitwOr(a, bitwShiftL(1, b-1))
    }, ccols[use], 0L)

    # constraint columns   # idxnum
    # 2                    2
    # 3                    4
    # 4                    8
    # 5                    16
    # 2,3                  6
    # 2,4                  10
    # 2,5                  18
    # 3,4                  12
    # 3,5                  20
    # 4,5                  24
    # 2,3,4                14
    # 2,3,5                22
    # 2,4,5                26
    # 3,4,5                28
    # 2,3,4,5              30
    # none                 0

    argv = ccols
    argv[!use] = NA_integer_
    argv[use] = order(argv[use])

    return (list(
        argv=argv
        , idxnum=idxnum
        , idxname=as.character(idxnum)
        , issorted=FALSE
        , cost=1)
    )
}

xFilter = function(db, table, env, idxnum, idxname, ...) {

    if (idxnum == 0L)
        idxname = "0"
    input = switch(idxname
        , "2"=
        , "6"=
        , "10"=
        , "14"=
        , "18"=
        , "22"=
        , "26"=
        , "30"=...elt(1L)
        , env$input
    )
    output = switch(idxname
        , "4"=
        , "12"=
        , "20"=
        , "28"=...elt(1L)
        , "6"=
        , "14"=
        , "22"=
        , "30"=...elt(2L)
        , env$output
    )
    cmd = switch(idxname
        , "8"=
        , "24"=...elt(1L)
        , "10"=
        , "12"=
        , "26"=
        , "28"=...elt(2L)
        , "14"=
        , "30"=...elt(3L)
        , env$cmd
    )
    exe = switch(idxname
        , "16"=...elt(1L)
        , "18"=
        , "20"=
        , "24"=...elt(2L)
        , "22"=
        , "26"=
        , "28"=...elt(3L)
        , "30"=...elt(4L)
        , env$exe
    )
    
    .cmd = cmd
    .exe = exe

    if (!is.null(exe) && substr(exe, 1, 9) == "java -jar") {
        cmd = paste(substring(exe, 6), cmd, collapse=" ")
        exe = "java"
    }
    
    exe = unname(Sys.which(exe))
    if (exe == "")
        stop("unable to find executable")
    if (is.null(input))
        stop("no input location specified")
    if (is.null(output))
        stop("no output location specified")
    if (is.null(cmd))
        stop("no arguments supplied to executable")

    if (!db.exists(db, output)) {
        db.sqlar_skeleton(db, output)
        db.eval(db, sprintf("
            INSERT INTO \"%1$s\"(name,mode,mtime,sz)
            VALUES('%1$s',?,?,0)", output)
            , list(
                493L
                , as.integer(unclass(as.POSIXct(Sys.time(), origin="1970-01-01")))
            )
        )
    }

    cli = function() {
        owd = getwd()
        on.exit(setwd(owd))
        tmpdir = tempdir()
        setwd(tmpdir)

        # input maybe a comma separated string of sqlar tables
        inputs = unique(strsplit(input, " *, *")[[1]])

        for (i in inputs)
            db.unsqlar(db, i, tmpdir)

        if (is.na(match(output, inputs)))
            db.unsqlar(db, output, tmpdir)

        output_files = list.files(tmpdir)

        # split cmd on spaces not enclosed between quotes
        re="[\\s](?=(?:[^'\"]*['\"][^'\"]*['\"])*[^'\"]*\\Z)"

        # if any part of the command string specifies a path in the input(s)
        # we prepend the temporary working directory to it so that it refers to
        # a valid filesystem path
        args = paste(
            sapply(strsplit(cmd, re, perl=TRUE)[[1]], function(s) {
                for (i in inputs) {
                    if (!is.null(
                        z <- db.eval(
                            db
                            , sprintf("
                                SELECT
                                    name
                                FROM
                                    \"%s\"
                                WHERE
                                    name
                                LIKE
                                    ?
                                ORDER BY
                                    mtime DESC
                                LIMIT 1"
                                , i
                            )
                            , paste0("%", gsub("['\"]", "", s)))
                        )
                    )
                    {
                        if (!grepl("^['\"]", s))
                            s = file.path(tmpdir, z[[1]])
                        else
                            s = shQuote(file.path(tmpdir, z[[1]]))
                        break
                    }
                }
                s
            }), collapse=" ")

        status = system2(exe, args)

        output_files = setdiff(list.files(), output_files)
        for (i in inputs) {
            output_files = union(setdiff(
                list.files(
                    file.path(tmpdir, db.sqlar_root(db, i))
                    , recursive=TRUE, include.dirs=TRUE, full.names=TRUE)
                ,
                file.path(tmpdir, db.eval(db,
                    sprintf("SELECT name FROM \"%s\"", i), df=TRUE)[,1])
            ), output_files)
        }

        result_dir = format(
            as.POSIXct(unclass(Sys.time()), origin="1970-01-01", tz="UTC")
            , "%Y%m%dT%H%M%S%Z")

        output_dir = file.path(tmpdir, output, result_dir)
        dir.create(output_dir)

        file.copy(
            output_files,
            file.path(output_dir, basename(output_files)))

        db.sqlar_update(db, output, file.path(tmpdir, output))
        cat(sprintf("\n\nResults written to table %s\n", output))

        for (i in inputs)
            unlink(file.path(tmpdir, db.sqlar_root(db, i)), recursive=TRUE)
        if (is.na(match(output, inputs)))
            unlink(file.path(tmpdir, output), recursive=TRUE)

        return (status)
        #return (sub(file.path(tmpdir, ""), "", output_dir))
    }

    assign("cli", cli, envir=env)
    assign("rowid", 1L, envir=env)

}


xColumn = function(db, table, env, j) {
    cli = env$cli
    enclos = environment(cli)
    switch(j
        , cli()
        , enclos$input
        , enclos$output
        , enclos$.cmd
        , enclos$.exe)
}


register_cli_module = db.virtualtable("cli", list(
    .create=xCreate,
    .connect=xConnect,
    .eof=xEof,
    .bestindex=xBestIndex,
    .filter=xFilter,
    .column=xColumn
))


modules = list(cli=register_cli_module)
