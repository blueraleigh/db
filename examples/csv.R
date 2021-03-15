callbacks = list(

    .create = function(db, table, ...) {
        # ... will contain arguments to read.csv
        x = read.csv(...)
        fields = colnames(x)
        types = character(ncol(x))
        for (i in 1:ncol(x)) {
            types[i] = switch(
                class(x[,i]),
                numeric="REAL",
                factor=,
                integer="INTEGER",
                character="TEXT",
                "SEXP"
            )
        }
        schema = sprintf("CREATE TABLE x(\"%s\" %s", fields[1], types[1])
        for (i in 2:ncol(x))
            schema = sprintf("%s\n  , \"%s\" %s", schema, fields[i], types[i])
        schema = sprintf("%s\n%s", schema, ");")
        env = new.env()
        assign("x", x, envir=env)
        assign("schema", schema, envir=env)
        return (env)
    },

    .connect = function(db, table, ...) {
        x = read.csv(...)
        fields = colnames(x)
        types = character(ncol(x))
        for (i in 1:ncol(x)) {
            types[i] = switch(
                class(x[,i]),
                numeric="REAL",
                factor=,
                integer="INTEGER",
                character="TEXT",
                "SEXP"
            )
        }
        schema = sprintf("CREATE TABLE x(\"%s\" %s", fields[1], types[1])
        for (i in 2:ncol(x))
            schema = sprintf("%s\n  , \"%s\" %s", schema, fields[i], types[i])
        schema = sprintf("%s\n%s", schema, ");")
        env = new.env()
        assign("x", x, envir=env)
        assign("schema", schema, envir=env)
        return (env)
    },

    .eof = function(db, table, env) {
        x = get("x", envir=env)
        rid = get("rowid", envir=env)
        if (rid > nrow(x))
            return (TRUE)
        return (FALSE)
    },

    .column = function(db, table, env, j) {
        x = get("x", envir=env)
        rid = get("rowid", envir=env)
        return (x[rid, j])
    },

    .bestindex = function(db, table, env, constraints, orderbys) {
        print(constraints)
        print(orderbys)
        print(table)
        return (NULL)
    }
)


library(Rdb)
register_csv_virtualtable = new.virtualtable("csv", callbacks)

db = db.open()
mod = register_csv_virtualtable(db)

db.eval(db, "create virtual table mass using csv(file='mass.csv', header=FALSE)")
db.eval(db, "create virtual table repro using csv(file='reprod.csv', header=FALSE)")

cur = db.eval(db, "
    select * from mass
    join repro on
    mass.V1=repro.V1
    where mass.V1 like 'Thamnophis%'
    ")
db.fetchall(cur)


cur = db.eval(db, "
    select * from mass

    order by V2
    ")
db.fetchall(cur, TRUE)
