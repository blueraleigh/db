#!/usr/local/bin/Rscript --vanilla

if (is.null(sys.calls())) {
    args = commandArgs(TRUE)
    db = db::db.open(args[1])
    db::db.close(db)
}
