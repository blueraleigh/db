library(db)

callbacks = list(
    .create = function(db, table, ...) {
        schema = "CREATE TABLE x(result BLOB, formula TEXT HIDDEN, data TEXT HIDDEN)"
        env = new.env()
        assign("schema", schema, envir=env)
        return (env)
    },

    .connect = function(db, table, ...) {
        .create(db, table, ...)
    },

    .eof = function(db, table, env) {
        if (get("rowid", envir=env) > 1L)
            return (TRUE)
        return (FALSE)
    },

    .bestindex = function(db, table, env, constraints, orderbys) {
        # formula and data are both specified
        if (length(constraints) == 2L
            && all(sapply(constraints, "[", 1L) %in% 2:3)) {
            if (constraints[[1L]][1L] == 2L) {
                # formula is the first constaint arg
                argv.index = c(1L, 2L)
            } else {
                # data is the first constaint arg
                argv.index = c(2L, 1L)
            }
            return (list(
                argv.index = argv.index
                , idxnum = 1L
                , idxname = ""
                , issorted = FALSE
                , cost = 1)
            )
        } else {
            return (list(
                args = rep(as.integer(NA), length(constraints))
                , idxnum = 0L
                , idxname = ""
                , issorted = FALSE
                , cost = 2147483647)
            )
        }
    },

    .filter = function(db, table, env, idxnum, idxname, ...) {

        if (idxnum == 1L) {
            assign("formula", ...elt(1L), envir=env)
            assign(".formula", as.formula(...elt(1L)), envir=env)

            # ...elt(2) could be a table name
            # or it could be a select statement to evaluate

            if (substr(...elt(2), 1, 1) == "(")
                .data = db.eval(db, substr(...elt(2), 2, nchar(...elt(2))-1), df=TRUE)
            else
                .data = db.eval(db, sprintf("SELECT * FROM %s", ...elt(2L)), df=TRUE)

            assign("data", ...elt(2), envir=env)
            assign(".data", .data, envir=env)
        }
        assign("rowid", 1L, envir=env)
        return (NULL)
    },

    .column = function(db, table, env, j) {
        if (j == 1L) {
            return (lm(get(".formula", envir=env), get(".data", envir=env)))
        } else if (j == 2L) {
            return (get("formula", envir=env))
        } else if (j == 3L) {
            return (get("data", envir=env))
        } else if (j == 0L) {
            return (get("rowid", envir=env))
        }
        return (NA)
    }
)

register_lm_virtual_table = new.virtualtable("lm", callbacks)

db = db.open()

register_lm_virtual_table(db)
db.eval(db, "CREATE VIRTUAL TABLE lm USING lm")
db.eval(db, "CREATE TABLE cars(speed REAL, dist REAL)")
db.eval(db, "INSERT INTO cars VALUES(?,?)", cars)


# select * from lm where formula='speed ~ dist' and data='cars'
db.eval(db,
    "select * from lm('speed ~ dist', 'cars')")

db.eval(db,
    "select * from lm('speed ~ dist', '(select * from cars)')")

db.eval(db, "create view foo as select * from lm('speed ~ dist', 'cars')")
db.eval(db, "select * from foo")
