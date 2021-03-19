#include <R.h>
#include <Rinternals.h>
#include "sqlite3.h"

void _db_close(SEXP Db)
{
    #ifndef NDEBUG
        Rprintf("running db finalizer\n");
    #endif
    sqlite3 *db = (sqlite3 *)R_ExternalPtrAddr(Db);
    sqlite3_close_v2(db);
    R_ClearExternalPtr(Db);
}


SEXP db_open(SEXP dbfile, SEXP mode)
{
    int rc;
    SEXP Db;
    sqlite3 *db;
    if (!strcmp(CHAR(STRING_ELT(mode, 0)), "r+"))
        rc = sqlite3_open_v2(CHAR(STRING_ELT(dbfile, 0)), &db,
            SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, 0);
    else
        rc = sqlite3_open_v2(CHAR(STRING_ELT(dbfile, 0)), &db,
            SQLITE_OPEN_READONLY, 0);
    if (rc != SQLITE_OK)
        error("unable to open database");
    rc = sqlite3_db_config(db, SQLITE_DBCONFIG_DEFENSIVE, 1, 0);
    if (rc != SQLITE_OK)
        warning("unable to open database in defensive mode");
    Db = PROTECT(R_MakeExternalPtr(db, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(Db, _db_close, TRUE);
    UNPROTECT(1);
    return Db;
}


SEXP db_close(SEXP Db)
{
    _db_close(Db);
    return Db;
}
