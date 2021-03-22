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
db.json = function(db, table, file=NULL) {
    stopifnot(is(db, "database"))
    if (missing(table)) {
        tables = db.tables(db)
        ntables = length(tables)
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
