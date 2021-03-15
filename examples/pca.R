library(Rdb)

callbacks = list(
    .create = function(db, table, ...) {
        # ... will contain args pass on to read.csv
        # as well as two special args `row.labels`
        # and `fields` specifying columns in the csv
        # file. These are used in the update callback
        # below for subsetting the csv content for
        # running the pca and for adding meaningful row
        # names to the pca output.
        cl = match.call()
        cl[[1]] = quote(read.csv)
        schema = "CREATE TABLE x(data BLOB, pca BLOB, mtime TEXT)"
        db.register_shadowname(db, "results")
        db.eval(db, sprintf(
            "CREATE TABLE %s_results(
                data BLOB,
                pca BLOB,
                mtime TEXT DEFAULT CURRENT_TIMESTAMP)", table))
        env = new.env()
        assign("schema", schema, envir=env)
        assign("call", cl[-(2:3)], envir=env)
        return (env)
    },

    .connect = function(db, table, ...) {
        cl = match.call()
        cl[[1]] = quote(read.csv)
        schema = "CREATE TABLE x(data BLOB, pca BLOB, mtime TEXT)"
        db.register_shadowname(db, "results")
        env = new.env()
        assign("schema", schema, envir=env)
        assign("call", cl[-(2:3)], envir=env)
        return (env)
    },

    .eof = function(db, table, env) {
        stmt = sprintf("SELECT COUNT(*) FROM %s_results", table)
        nr = db.fetch(db.eval(db, stmt))[[1]]
        if (get("rowid", envir=env) > nr)
            return (TRUE)
        return (FALSE)
    },

    .column = function(db, table, env, j) {
        rid = get("rid", envir=env)
        col = switch(j, `1`="data", `2`="pca", `3`="mtime")
        stmt = sprintf(
                "SELECT %s FROM %s_results WHERE rowid = ?", col, table)
        cursor = db.eval(db, stmt, list(list(rid)))
        res = db.fetch(cursor)
        if (!is.null(res))
            res = res[[1]]
        return (res)
    },

    .delete = function(db, table, env, rid) {
        stmt = sprintf(
                "DELETE FROM %s_results WHERE rowid = ?", table)
        db.eval(db, stmt, list(list(rid)))
        return (NA)
    },

    .insert = function(db, table, env, rid, ...) {
        # Called from insert/update trigger on sqlar like so
        # INSERT INTO escallonia_pca(data) VALUES (<data>)
        # where <data> is the raw csv content
        stopifnot(is.na(rid))
        cl = get("call", envir=env)
        # cl[[2]] will be the name of the csv file so we alter
        # it to receive data directly from the raw vector while
        # leaving the other arguments as is.
        cl[[2]] = textConnection(rawToChar(...elt(1)))
        m = match(names(formals(read.table)), names(cl), 0L)
        x = eval(cl[c(1:2, m)])
        if ("row.labels" %in% names(cl))
            rownames(x) = x[, eval(cl$row.labels)]
        if ("fields" %in% names(cl))
            x = x[, eval(cl$fields)]
        mu = colMeans(x, na.rm=TRUE)
        for (i in 1:ncol(x))
            x[which(is.na(x[,i])), i] = mu[i]
        pca = prcomp(x)
        stmt = sprintf(
            "INSERT INTO %s_results(data, pca) VALUES (?,?)", table)
        db.eval(db, stmt, list(list(x, pca)))
        rid = db.fetch(db.eval(
            db, "SELECT last_insert_rowid() AS rid"))$rid
        return (as.integer(rid))

    }
)

register_pca_virtual_table = new.virtualtable("pca", callbacks)

db = db.open()
register_pca_virtual_table(db)
db.eval(db, "
CREATE VIRTUAL TABLE
    escallonia_pca
USING
    pca(
        'morphometrics.csv',
        header=TRUE,
        fields=c('PETILEN','LAMLEN','LAMWID', 'OVALEN')
    )"
)

db.sqlar.skeleton(db)
db.eval(db,
"
CREATE TRIGGER pca_insert_trigger
AFTER INSERT ON sqlar
WHEN NEW.name = 'escallonia_data/Specimen_Data/morphometrics.csv'
BEGIN
    INSERT INTO
        escallonia_pca(data)
    VALUES
        (sqlar_uncompress(NEW.data, NEW.sz));
END;
"
)
db.eval(db,
"
CREATE TRIGGER pca_update_trigger
AFTER UPDATE OF data ON sqlar
WHEN NEW.name = 'escallonia_data/Specimen_Data/morphometrics.csv'
BEGIN
    INSERT INTO
        escallonia_pca(data)
    VALUES
        (sqlar_uncompress(NEW.data, NEW.sz));
END;
"
)

db.sqlar(db, "escallonia_data")
x = db.fetchall(db.eval(db, 'select * from escallonia_pca'))

# change the file and update the sqlar
db.sqlar.update(db, "escallonia_data")
x = db.fetchall(db.eval(db, 'select * from escallonia_pca'))

