#' Dump the contents of a table or database into a JSON array
#'
#' @param db The database connection.
#' @param table The table name.
#' @param file A file to dump the contents to. If NULL the dump is
#' returned as a character string.
#' @details If \code{table} is missing the entire database is dumped
#' into an array of JSON objects. Each object has two fields: a "table"
#' field with the name of the table and a "data" field with the table
#' data. The "data" is an array of JSON objects. Each object represents
#' one row of table data and the JSON object holds a field for each table
#' column. If \code{table} is not missing then only the contents of that
#' table are dumped and the return value consists only of the "data" array.
#' @note This does require that SQLite was compiled with the option
#' -DSQLITE_ENABLE_JSON1.
#' @export
db.dumpJSON = function(db, table, file=NULL) {
    stopifnot(is(db, "database"))
    if (missing(table)) {
        tables = db.tables(db)
        ntables = length(tables)
        if (ntables == 0)
            return ("{}")
        stmt = character(ntables)
        for (i in 1:ntables) {
            spec = paste0(
                sprintf("'%1$s',\"%1$s\"", db.fields(db, tables[i])[,1])
                , collapse=",")
            stmt[i] = sprintf("
                    SELECT
                        '%s' AS \"table\"
                        , json_group_array(json_object(%s)) AS data
                    FROM %s
                ", tables[i], spec, tables[i])
        }
        stmt = paste0(stmt, collapse = "UNION ALL")
        stmt = sprintf("
        WITH db_json AS (
            %s
        )
        SELECT
            json_group_array(json_object('table',\"table\",'data',data))
        FROM db_json
        ", stmt)

    } else {
        spec = paste0(
            sprintf("'%1$s',\"%1$s\"", db.fields(db, table)[,1]), collapse=",")

        stmt = sprintf(
            "SELECT json_group_array(json_object(%s)) FROM %s"
            , spec, table)
    }
    if (is.null(file))
        return (db.eval(db, stmt)[[1]])
    cat(db.eval(db, stmt)[[1]], file=file)
}


#' Unserialize a JSON string to an R object
db.fromJSON = function(db, json) {

    read_json_object = function(db, json) {
        n = db.eval(db, "SELECT COUNT(*) FROM json_each(?)", json)[[1]]
        keys = character(n)
        obj = db.lapply(db, "SELECT rowid,key,value,type FROM json_each(?)", json, 
            FUN=function(l) {
                keys[l$rowid+1L] <<- l$key
                switch(l$type,
                    object=read_json_object(db, l$value),
                    array=read_json_array(db, l$value),
                    integer=as.integer(l$value),
                    real=as.numeric(l$value),
                    text=l$value
                    )
            }
        )
        return (structure(obj, names=keys))
    }

    read_json_array = function(db, json) {
        arr = db.lapply(db, "SELECT key,value,type FROM json_each(?)", json, 
            FUN=function(l) {
                switch(l$type,
                    object=read_json_object(db, l$value),
                    array=read_json_array(db, l$value),
                    integer=as.integer(l$value),
                    real=as.numeric(l$value),
                    text=l$value
                    )
            }
        )
        if (inherits(arr[[1]], c("character", "numeric", "integer")))
            arr = unlist(arr)
        arr
    }

    read_json = function(db, json) {
        switch(
            db.eval(db, "SELECT json_type(?)", json)[[1]],
            object=read_json_object(db, json),
            array=read_json_array(db, json),
            integer=as.integer(json),
            real=as.numeric(json),
            text=json
        )
    }

    obj = try(read_json(db, json), silent=TRUE)
    if (inherits(obj, "try-error"))
        stop(gettextf(attr(obj, "condition")$message))
    obj
}


#' Serialize an R object to a JSON string
db.toJSON = function(db, object) {
    dataframe_to_json = function(db, object) {
        class_to_sql = function(cls) {
            switch(cls,
                numeric="REAL",
                integer="INTEGER",
                factor=,
                character="TEXT",
                "BLOB"
            )
        }
        df_to_schema = function(df, name) {
            schema = sprintf("CREATE TABLE %s(", name)
            col_types = lapply(df, function(v) {
                class_to_sql(class(v))
            })
            col_names = names(col_types)
            schema = sprintf("%s\n \"%s\" %s", schema, col_names[1], col_types[1])
            if (ncol(df) > 1) {
                for (i in 2:ncol(df))
                {
                    schema = sprintf(
                        "%s\n, \"%s\" %s", schema, col_names[i], col_types[i])
                }
            }
            schema = sprintf("%s\n)", schema)
            schema
        }
        if (is.character(attr(object, "row.names")))
            object = cbind(object, `_row`=rownames(object))
        db.eval(db, df_to_schema(object, "temp.df"))
        for (p in which(sapply(object, class) %in% "factor"))
             object[, p] = as.character(object[, p])
        stmt = sprintf(
            "INSERT INTO df VALUES(%s)", 
            paste0(rep("?", ncol(object)), collapse=","))
        db.eval(db, stmt, object)
        json = db.dumpJSON(db, "df")
        db.eval(db, "DROP TABLE df")
        json
    }
    list_to_json = function(db, object) {
        keys = names(object)
        if (!is.null(keys)) {
            json = "{}"
            for (key in keys) {
                json = db.eval(
                    db, 
                    "SELECT json_insert(?, ?, json(?))",
                    list(list(
                        json, 
                        sprintf("$.%s", key), 
                        to_json(db, object[[key]])
                    ))
                )[[1]]
            }
        } else {
            json = "[]"
            if (length(object) == 0)
                return (json)
            for (i in 1:length(object)) {
                json = db.eval(
                    db, 
                    "SELECT json_insert(?, ?, json(?))",
                    list(list(
                        json, 
                        "$[#]", 
                        to_json(db, object[[i]])
                    ))
                )[[1]]
            }
        }
        json
    }
    atomic_to_json = function(db, object) {
        json = "[]"
        if (length(object) == 0)
            return (json)
        for (i in 1:length(object)) {
            json = db.eval(
                db, 
                "SELECT json_insert(?, ?, json(?))",
                list(list(
                    json, 
                    "$[#]", 
                    to_json(db, object[i])
                ))
            )[[1]]
        }
        json
    }
    scalar_to_json = function(db, object) {
        switch(class(object),
            numeric=,
            integer=unname(object),
            factor=,
            character=sprintf("\"%s\"", as.character(object))
        )
    }
    to_json = function(db, object) {
        if (is.data.frame(object))
            return (dataframe_to_json(db, object))
        else if (is.list(object))
            return (list_to_json(db, object))
        else if (is.atomic(object)) {
            if (length(object) > 1 || !length(object))
                return (atomic_to_json(db, object))
            else
                return (scalar_to_json(db, object))
        }
        else
            stop(gettextf("no serialization format for objects of class '%s'",
                class(object)))
    }
    json = try(to_json(db, object), silent=TRUE)
    if (inherits(json, "try-error"))
        stop(gettextf(attr(json, "condition")$message))
    json
}