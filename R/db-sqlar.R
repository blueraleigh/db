.sqlar.delete = function(db, files) {
    stmt = sprintf("DELETE FROM sqlar WHERE name IN (%s)",
                paste0(rep("?", length(files)), collapse=","))
    db.eval(db, stmt, as.list(files))
}

.sqlar.insert = function(db, fstat, files, ignore.path) {
    fstat = fstat[file.path(normalizePath(ignore.path), files), ]
    for (i in 1:nrow(fstat)) {
        if (fstat$isdir[i]) {
            db.eval(
                db
                , "INSERT INTO sqlar(name,mode,mtime,sz) VALUES(?,?,?,?)"
                , list(
                    files[i]
                    , unclass(fstat$mode[i])
                    , unclass(fstat$mtime[i])
                    , 0))
        } else {
            f = rownames(fstat)[i]
            raw = readBin(f, "raw", fstat$size[i])
            db.eval(
                db
                , "INSERT INTO sqlar VALUES(?,?,?,?,sqlar_compress(?))",
                , list(
                    files[i]
                    , unclass(fstat$mode[i])
                    , unclass(fstat$mtime[i])
                    , fstat$size[i]
                    , raw))
        }
    }
}

.sqlar.update = function(db, fstat, files.content, files.mode, ignore.path) {
    files = union(files.content, files.mode)
    fstat = fstat[file.path(normalizePath(ignore.path), files), ]
    for (i in 1:nrow(fstat)) {
        if (fstat$isdir[i] || files[i] %in% files.mode) {
            db.eval(
                db
                , "UPDATE sqlar SET mode=?, mtime=? WHERE name=?"
                , list(
                    , unclass(fstat$mode[i])
                    , unclass(fstat$mtime[i])
                    files[i]))
        } else {
            f = rownames(fstat)[i]
            raw = readBin(f, "raw", fstat$size[i])
            db.eval(
                db
                , "UPDATE sqlar SET
                    mode=?
                    , mtime=?
                    , sz=?
                    , data=sqlar_compress(?)
                WHERE name=?"
                , list(
                    , unclass(fstat$mode[i])
                    , unclass(fstat$mtime[i])
                    , fstat$size[i]
                    , raw
                    files[i]))
        }
    }
}

.sqlar.changed.mode = function(db.mode, path.mode) {
    common.files = intersect(names(db.mode), names(path.mode))
    return (common.files[db.mode[common.files] != path.mode[common.files]])
}

.sqlar.changed.content = function(db.mtimes, path.mtimes) {
    common.files = intersect(names(db.mtimes), names(path.mtimes))
    return (common.files[db.mtimes[common.files] < path.mtimes[common.files]])
}

#' Initialize the SQLite archive schema
#'
#' @param db The database connection. S4 object of class "database".
#' @details This has a side effect of creating a table named "sqlar"
#' in the database file pointed to by \code{db}. The sqlar table has
#' the following schema:
#' \preformatted{
#'     CREATE TABLE sqlar(
#'         name  TEXT PRIMARY KEY, -- name of the file
#'         mode  INTEGER,          -- access permissions
#'         mtime INTEGER,          -- last modification time
#'         sz    INTEGER,          -- original file size
#'         data  BLOB              -- compressed content
#'     );
#' }
#' @return None.
#' @export
db.sqlar_skeleton = function(db) {
    stopifnot(is(db, "database"))
    db.eval(db, "
        CREATE TABLE IF NOT EXISTS sqlar(
            name  TEXT PRIMARY KEY, -- name of the file
            mode  INTEGER,          -- access permissions
            mtime INTEGER,          -- last modification time
            sz    INTEGER,          -- original file size
            data  BLOB              -- (un)compressed content
        );")
}

#' Update the content of a SQLite archive
#'
#' @param db The database connection. S4 object of class "database".
#' @param path The path to the top-level directory of the archive.
#' @details This will update the sqlar table in the database to reflect
#' any changes made to the files under \code{path} (including file
#' additions and deletions and changed file permissions).
#' @note The \code{\link{sqlar_compress}} function must be registered
#' with the database connection for this function to work. This is done
#' automatically with \code{\link{db.open}}
#' @return None.
#' @export
db.sqlar_update = function(db, path) {
    stopifnot(is(db, "database"))
    path = normalizePath(path)
    ignore = paste(dirname(path), "/", sep="")
    topdir = basename(path)
    flist = list.files(path, recursive=TRUE, include.dirs=TRUE,
        full.names=TRUE)
    path.names = gsub(ignore, "", flist, fixed=TRUE)
    fstat = file.info(flist)
    sqlar = db.eval(db,
            "SELECT name,mtime,mode FROM sqlar WHERE name != ?",
            list(topdir), TRUE)
    rownames(sqlar) = sqlar$name
    deleted.files = setdiff(sqlar$name, path.names)
    added.files = setdiff(path.names, sqlar$name)
    changed.content = .sqlar.changed.content(
        structure(as.POSIXct(sqlar$mtime, origin="1970-01-01"),
            names=sqlar$name),
        structure(fstat$mtime, names=path.names))
    changed.mode = .sqlar.changed.mode(
        structure(as.octmode(sqlar$mode), names=sqlar$name),
        structure(fstat$mode, names=path.names))
    changed.mode.only = setdiff(changed.mode, changed.content)
    if (length(deleted.files))
        .sqlar.delete(db, deleted.files)
    if (length(added.files))
        .sqlar.insert(db, fstat, added.files, ignore)
    if (length(changed.content) || length(changed.mode.only))
        .sqlar.update(db, fstat, changed.content, changed.mode.only, ignore)
}

#' Create a SQLite archive
#'
#' For additional details goto \url{https://sqlite.org/sqlar.html}.
#'
#' @param db The database connection. S4 object of class "database".
#' @param path The path to the top-level directory of the archive.
#' @details This will create a table named "sqlar" in the database. The
#' table will hold the gzip compressed content of all files and
#' directories under \code{path}.
#' @note The \code{\link{sqlar_compress}} function must be registered
#' with the database connection for this function to work. This is done
#' automatically with \code{\link{db.open}}
#' @return None.
#' @seealso \code{\link{db.sqlar.skeleton}}
#' @export
db.sqlar = function(db, path) {
    stopifnot(is(db, "database"))
    db.sqlar_skeleton(db)
    if (db.eval(db, "SELECT COUNT(*) FROM sqlar")[[1]] > 0L)
        stop("sqlar table already in use")
    path = normalizePath(path)
    ignore = paste(dirname(path), "/", sep="")
    topdir = basename(path)
    flist = list.files(
        path, recursive=TRUE, include.dirs=TRUE, full.names=TRUE)
    fstat = file.info(flist)
    db.eval(db,
        "INSERT INTO sqlar(name,mode,mtime,sz) VALUES(?,?,?,?)",
        list(topdir, unclass(file.mode(path)), unclass(file.mtime(path)), 0))
    .sqlar.insert(db, fstat, gsub(ignore, "", flist, fixed=TRUE), ignore)
}

#' Unpack a SQLite archive.
#'
#' @param db The database connection. S4 object of class "database".
#' @param path The path to unpack the archive under.
#' @details This will read the sqlar table in the database and write out
#' its content into a filesystem hierarchy under \code{path}.
#' @note The \code{\link{sqlar_uncompress}} function must be registered
#' with the database connection for this function to work. This is done
#' automatically with \code{\link{db.open}}
#' @return None.
#' @export
db.unsqlar = function(db, path) {
    stopifnot(is(db, "database"))
    wd = getwd()
    on.exit(setwd(wd))
    setwd(path)
    db.lapply(
        db
        , "
        SELECT
            name,mode,mtime,sqlar_uncompress(data,sz) AS data
        FROM sqlar ORDER BY rowid"
        , FUN=function(f) {
            if (length(f$data) == 1 && is.na(f$data)) {
                dir.create(f$name, mode=as.octmode(f$mode))
                Sys.setFileTime(f$name, as.POSIXct(f$mtime, origin="1970-01-01"))
            } else {
                writeBin(f$data, f$name)
                Sys.chmod(f$name, as.octmode(f$mode))
                Sys.setFileTime(f$name, as.POSIXct(f$mtime, origin="1970-01-01"))
            }
        }
    )
    invisible()
}


#' Compress data using gzip
#'
#' @param raw An uncompressed byte vector.
#' @return The compressed byte vector.
sqlar_compress = function(raw) {
    useraw = raw
    zzfil = tempfile()
    zz = gzfile(zzfil, "wb")
    writeBin(raw, zz)
    close(zz)
    raw2 = readBin(zzfil, "raw", file.size(zzfil))
    if (length(raw2) < length(raw))
        useraw = raw2
    unlink(zzfil)
    return (useraw)
}


#' Uncompress gzip'd data
#'
#' @param raw A compressed byte vector.
#' @param sz The size of the uncompressed byte vector.
#' @return The uncompressed byte vector.
sqlar_uncompress = function(raw, sz) {
    if (sz > length(raw)) {
        # content was compressed
        zzfil = tempfile()
        writeBin(raw, zzfil)
        zz = gzcon(file(zzfil, "rb"))
        raw2 = readBin(zz, "raw", sz)
        close(zz)
        unlink(zzfil)
        return (raw2)
    }
    return (raw)
}
