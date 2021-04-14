context("virtualtable: pivot")

callbacks = list(
    .create=function(db, table, ...) {
        if (...length() != 4L)
            stop("invalid arguments")

        # query or name of table
        query = ...elt(1L)

        # column of query table for pivot table rows
        row = ...elt(2L)

        # column of query table for pivot table columns
        col = ...elt(3L)

        # column of query table wrapped in an
        # aggregate function to use for pivot table
        # values
        data = ...elt(4L)

        # e.g.,
        #
        # suppose there is a table like so:
        #
        # CREATE TABLE specimen(
        #   specimen  TEXT,
        #   character TEXT,
        #   value     REAL
        # )
        #
        # to view this table in 'wide' format where
        # the different characters are pulled out into
        # separate columns we could do:
        #
        # CREATE VIRTUAL TABLE p USING pivot(
        #    'measurements'
        #    , 'specimen'
        #    , 'character'
        #    , 'avg(value)')

        if (substr(query, 1, 1) != '(') {
            # query is the name of a table
            x = db.eval(db,
                sprintf("
                    SELECT
                        %2$s
                        , %3$s
                        , %4$s AS value
                    FROM
                        %1$s
                    GROUP BY
                        %2$s, %3$s
                ", query, row, col, data)
            , df=TRUE)

        } else {
            # wrap SELECT query in a common table expression
            x = db.eval(db,
                sprintf("
                    WITH cte AS
                        %1$s
                    SELECT
                        %2$s
                        , %3$s
                        , %4$s AS value
                    FROM
                        cte
                    GROUP BY
                        %2$s, %3$s
                ", query, row, col, data)
            , df=TRUE)
        }

        x = reshape(
            x,
            direction="wide",
            idvar=row,
            v.names="value",
            timevar=col)

        colnames(x) = gsub("value.", "", colnames(x), fixed=TRUE)
        rownames(x) = NULL

        schema = sprintf(
            "CREATE TABLE x(%s);", paste0(colnames(x), collapse="\n  , "))

        env = new.env(parent=emptyenv())
        assign("schema", schema, envir=env)
        assign("x", x, envir=env)
        return (env)
    }
    , .connect=function(db, table, ...) {
        .create(db, table, ...)
    }
    , .eof=function(db, table, env) {
        if (get("rowid", envir=env) > nrow(get("x", envir=env)))
            return (TRUE)
        return (FALSE)
    }
    , .column=function(db, table, env, j) {
        get("x", envir=env)[get("rowid", envir=env), j]
    }
)


test1 = function() {

    db = db.open()
    on.exit(db.close(db))

    db.eval(db, 'CREATE TABLE measurements(specimen, character, value)')
    db.eval(db, 'INSERT INTO measurements VALUES(?,?,?)',
        data.frame(
            specimen=rep(letters[1:3], each=5),
            character=rep(paste0("c", 1:5), 3),
            value=1:15
        )
    )

    register_pivot_table = db.virtualtable("pivot", callbacks)
    register_pivot_table(db)

    db.eval(db,
    "CREATE VIRTUAL TABLE p USING pivot(
        'measurements'
        , 'specimen'
        , 'character'
        , 'avg(value)')"
    )
    db.eval(db, 'select * from p', df=TRUE)
}


test2 = function() {

    db = db.open()
    on.exit(db.close(db))

    db.eval(db, 'CREATE TABLE measurements(specimen, character, value)')
    db.eval(db, 'INSERT INTO measurements VALUES(?,?,?)',
        data.frame(
            specimen=rep(letters[1:3], each=5),
            character=rep(paste0("c", 1:5), 3),
            value=1:15
        )
    )

    register_pivot_table = db.virtualtable("pivot", callbacks)
    register_pivot_table(db)

    db.eval(db,
        "CREATE VIRTUAL TABLE p USING pivot(
            \"(
            SELECT
                *
            FROM
                measurements
            WHERE
                character
            IN
                ('c1','c2','c3')
            )\"
            , 'specimen'
            , 'character'
            , 'avg(value)')"
    )
    db.eval(db, 'select * from p', df=TRUE)
}


test_that("vtable_pivot", {
    expect_equal(
        test1(),
        data.frame(
            specimen=letters[1:3]
            , c1=c(1L,6L,11L)
            , c2=c(2L,7L,12L)
            , c3=c(3L,8L,13L)
            , c4=c(4L,9L,14L)
            , c5=c(5L,10L,15L)
        ))
    expect_equal(
        test2(),
        data.frame(
            specimen=letters[1:3]
            , c1=c(1L,6L,11L)
            , c2=c(2L,7L,12L)
            , c3=c(3L,8L,13L)
        ))
})

