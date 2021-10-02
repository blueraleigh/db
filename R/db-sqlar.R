.sqlar.delete = function(db, name, files) {
    stmt = sprintf("DELETE FROM \"%s\" WHERE name IN (%s)",
                name, paste0(rep("?", length(files)), collapse=","))
    db.eval(db, stmt, as.list(files))
}

.sqlar.insert = function(db, name, fstat, dir.name, file.names) {
    fstat = fstat[file.path(normalizePath(dir.name), file.names), ]
    for (i in 1:nrow(fstat)) {
        if (fstat$isdir[i]) {
            db.eval(
                db
                , sprintf(
                    "INSERT INTO \"%s\"(name,mode,mtime,sz) VALUES(?,?,?,?)", name)
                , list(
                    file.names[i]
                    , unclass(fstat$mode[i])
                    , as.integer(unclass(fstat$mtime[i]))
                    , 0))
        } else {
            raw = readBin(rownames(fstat)[i], "raw", fstat$size[i])
            db.eval(
                db
                , sprintf(
                    "INSERT INTO \"%s\" VALUES(?,?,?,?,sqlar_compress(?))", name)
                , list(
                    file.names[i]
                    , unclass(fstat$mode[i])
                    , as.integer(unclass(fstat$mtime[i]))
                    , fstat$size[i]
                    , raw))
        }
    }
}

.sqlar.update = function(db, name, fstat, dir.name, 
    files.new.content, files.new.mode) 
{
    files = union(files.new.content, files.new.mode)
    fstat = fstat[file.path(normalizePath(dir.name), files), ]
    for (i in 1:nrow(fstat)) {
        if (fstat$isdir[i] || files[i] %in% files.new.mode) {
            db.eval(
                db
                , sprintf("UPDATE \"%s\" SET mode=?, mtime=? WHERE name=?", name)
                , list(
                    unclass(fstat$mode[i])
                    , as.integer(unclass(fstat$mtime[i]))
                    , files[i]))
        } else {
            raw = readBin(rownames(fstat)[i], "raw", fstat$size[i])
            db.eval(
                db
                , sprintf("UPDATE \"%s\" SET
                    mode=?
                    , mtime=?
                    , sz=?
                    , data=sqlar_compress(?)
                WHERE name=?", name)
                , list(
                    unclass(fstat$mode[i])
                    , as.integer(unclass(fstat$mtime[i]))
                    , fstat$size[i]
                    , raw
                    , files[i]))
        }
    }
}

.sqlar.changed.mode = function(db.mode, path.mode) {
    common.files = intersect(names(db.mode), names(path.mode))
    return (common.files[db.mode[common.files] != path.mode[common.files]])
}

.sqlar.changed.content = function(db.mtimes, path.mtimes) {
    common.files = intersect(names(db.mtimes), names(path.mtimes))
    return (common.files[db.mtimes[common.files] != path.mtimes[common.files]])
}

#' Initialize the SQLite archive schema
#'
#' @param db The database connection. S4 object of class "database".
#' @param name The name of the SQLite archive table.
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
db.sqlar_skeleton = function(db, name) {
    stopifnot(is(db, "database"))
    db.eval(db, sprintf("
        CREATE TABLE IF NOT EXISTS \"%s\"(
            name  TEXT PRIMARY KEY, -- name of the file
            mode  INTEGER,          -- access permissions
            mtime INTEGER,          -- last modification time
            sz    INTEGER,          -- original file size
            data  BLOB              -- (un)compressed content
        );", name))
}

#' Update the content of a SQLite archive
#'
#' @param db The database connection. S4 object of class "database".
#' @param name The name of the SQLite archive table.
#' @param path The path to the top-level directory of the archive.
#' @details This will update the sqlar table in the database to reflect
#' any changes made to the files under \code{path} (including file
#' additions and changed file permissions).
#' @note The \code{\link{sqlar_compress}} function must be registered
#' with the database connection for this function to work. This is done
#' automatically with \code{\link{db.open}}
#' @return None.
#' @export
db.sqlar_update = function(db, name, path) {
    stopifnot(is(db, "database"))
    path = normalizePath(path)
    topdir = basename(path)
    file.list = list.files(path, recursive=TRUE, include.dirs=TRUE,
        full.names=TRUE)
    dir.name = paste(dirname(path), "/", sep="")
    file.names = gsub(dir.name, "", file.list, fixed=TRUE)
    fstat = file.info(file.list)
    sqlar = db.eval(db,
            sprintf("SELECT name,mtime,mode FROM \"%s\" WHERE name != ?", name),
            list(topdir), row_factory="data.frame")
    if (is.null(sqlar)) {
        deleted.files = character()
        added.files = file.names
        changed.files = character()
        changed.files.mode = character()
        changed.files.mode.only = character()
    } else {
        rownames(sqlar) = sqlar$name
        deleted.files = setdiff(sqlar$name, file.names)
        added.files = setdiff(file.names, sqlar$name)
        changed.files = .sqlar.changed.content(
            structure(as.POSIXct(sqlar$mtime, origin="1970-01-01"),
                names=sqlar$name),
            structure(fstat$mtime, names=file.names))
        changed.files.mode = .sqlar.changed.mode(
            structure(as.octmode(sqlar$mode), names=sqlar$name),
            structure(fstat$mode, names=file.names))
        changed.files.mode.only = setdiff(changed.files.mode, changed.files)
    }
    if (length(deleted.files))
        .sqlar.delete(db, name, deleted.files)
    if (length(added.files))
        .sqlar.insert(db, name, fstat, dir.name, added.files)
    if (length(changed.content) || length(changed.mode.only))
        .sqlar.update(db, name, fstat, dir.name, changed.files, 
            changed.files.mode.only)
}

#' Create a SQLite archive
#'
#' For additional details goto \url{https://sqlite.org/sqlar.html}.
#'
#' @param db The database connection. S4 object of class "database".
#' @param name The name of the SQLite archive table.
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
db.sqlar = function(db, name, path) {
    stopifnot(is(db, "database"))
    db.sqlar_skeleton(db, name)
    if (db.eval(db, sprintf("SELECT COUNT(*) FROM \"%s\"", name))[[1]] > 0L)
        stop(gettextf("sqlar table already in use"))
    path = normalizePath(path)
    topdir = basename(path)
    dir.name = paste(dirname(path), "/", sep="")
    file.list = list.files(
        path, recursive=TRUE, include.dirs=TRUE, full.names=TRUE)
    fstat = file.info(file.list)
    file.names = gsub(dir.name, "", file.list, fixed=TRUE)
    db.eval(db,
        sprintf("INSERT INTO \"%s\"(name,mode,mtime,sz) VALUES(?,?,?,?)", name),
        list(topdir
            , unclass(file.mode(path))
            , as.integer(unclass(file.mtime(path)))
            , 0)
    )
    .sqlar.insert(db, name, fstat, dir.name, file.names)
}

#' Return the root directory of a SQLite archive
#'
#' @param db The database connection. S4 object of class "database".
#' @param name The name of the SQLite archive table.
db.sqlar_root = function(db, name) {
    db.eval(db,
        sprintf(
            "SELECT name FROM \"%s\" WHERE sz=0 ORDER BY rowid ASC LIMIT 1"
            , name))[[1]]
}

#' Unpack a SQLite archive.
#'
#' @param db The database connection. S4 object of class "database".
#' @param name The name of the SQLite archive table.
#' @param path The path to unpack the archive under.
#' @param files The files in the archive to extract. These need not be
#' complete names. For example, "file1" will be matched using wildcards
#' on either end. So if "file1" is a directory all files under it will be
#' extracted.
#' @details This will read the sqlar table in the database and write out
#' its content into a filesystem hierarchy under \code{path}.
#' @note The \code{\link{sqlar_uncompress}} function must be registered
#' with the database connection for this function to work. This is done
#' automatically with \code{\link{db.open}}
#' @return None.
#' @export
db.unsqlar = function(db, name, path, files) {
    stopifnot(is(db, "database"))
    wd = getwd()
    on.exit(setwd(wd))
    setwd(path)
    if (missing(files) || is.null(files) || !length(files)) {
        stmt = sprintf("
        SELECT
            name,mode,mtime,sqlar_uncompress(data,sz) AS data
        FROM \"%s\" ORDER BY sz ASC,rowid ASC", name)
        params = list(list())
    } else {
        nfiles = length(files)
        like = paste(
            c("name LIKE ?", rep("OR name LIKE ?", nfiles-1)), collapse=" ")
        stmt = sprintf("
        SELECT
            name,mode,mtime,sqlar_uncompress(data,sz) AS data
        FROM \"%s\" WHERE %s ORDER BY sz ASC,rowid ASC", name, like)
        params = list(as.list(paste0("%", files, "%")))

    }
    db.lapply(
        db
        , stmt
        , params
        , FUN=function(f) {
            if (length(f$data) == 1 && is.na(f$data)) {
                dir.create(f$name, mode=as.octmode(f$mode), recursive=TRUE, 
                    showWarnings=FALSE)
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
