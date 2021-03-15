callbacks = list(

    .create = function(db, table, ...) {
        # ... will contain arguments to read.csv
        x = ...elt(1)
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
        .create(db, table, ...)
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
    }
)


library(Rdb)
register_df_virtualtable = new.virtualtable("df", callbacks)

db = db.open()
mod = register_df_virtualtable(db)

db.eval(db, "create virtual table mtcars using df(mtcars)")
db.eval(db, "create virtual table iris using df(iris)")

cur = db.eval(db, "
    select * from mtcars
    where disp > 400
    ")
db.fetchall(cur)



db.fetchall(db.eval(db, "select * from iris where Species=?", list(list(iris$Species[1]))))
