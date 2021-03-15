#include <R.h>
#include <Rinternals.h>
#include "sqlite3.h"
#include "convert.h"


static void db_r_sql_func(sqlite3_context *context, int argc,
    sqlite3_value **argv)
{
    SEXP fun = sqlite3_user_data(context);
    int i;

    if (argc == 0) {

        convert_sexp_to_result(eval(lang1(fun), R_GlobalEnv), context);

    } else {

        SEXP args = PROTECT(allocList(argc));
        SEXP e = args;

        for (i = 0; i < argc; ++i) {
            SETCAR(e, convert_value_to_sexp(argv[i]));
            e = CDR(e);
        }

        SEXP R_fcall = PROTECT(lcons(fun, args));
        convert_sexp_to_result(eval(R_fcall, R_GlobalEnv), context);
        UNPROTECT(2);
    }
}


static void db_release_function(void *fun)
{
    R_ReleaseObject((SEXP)fun);
}


SEXP db_create_function(SEXP Db, SEXP fun, SEXP funname)
{
    sqlite3 *db = (sqlite3 *)R_ExternalPtrAddr(Db);
    if (fun == R_NilValue) {
        sqlite3_create_function_v2(
            db,
            CHAR(STRING_ELT(funname, 0)),
            -1,
            SQLITE_UTF8,
            fun,
            0, 0, 0, db_release_function
        );
    }
    else {
        R_PreserveObject(fun);
        sqlite3_create_function_v2(
            db,
            CHAR(STRING_ELT(funname, 0)),
            -1,
            SQLITE_UTF8,
            fun,
            db_r_sql_func,
            0, 0, db_release_function
        );
    }
    return R_NilValue;
}
