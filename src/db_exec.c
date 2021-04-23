#include <R.h>
#include <Rinternals.h>
#include "sqlite3.h"


SEXP db_exec(SEXP Db, SEXP Sql) {

    sqlite3 *db = (sqlite3 *)R_ExternalPtrAddr(Db);
    const char *sql = CHAR(STRING_ELT(Sql, 0));
    char *errmsg;
    int rc = sqlite3_exec(db, sql, 0, 0, &errmsg);

    // sqlite3_exec sets errmsg to NULL if no errors occurred

    if (errmsg != 0) {
        char errbuf[1024] = {0};
        strncpy(errbuf, errmsg, sizeof(errbuf));
        sqlite3_free(errmsg);
        error(errbuf);
    }

    return R_NilValue;
}
