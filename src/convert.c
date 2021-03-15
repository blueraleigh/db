#include "convert.h"

SEXP convert_null_to_sexp(const char *decltype)
{
    if (sqlite3_stricmp(decltype, "int") == 0
        || sqlite3_stricmp(decltype, "integer") == 0)
        return ScalarInteger(NA_INTEGER);
    if (sqlite3_stricmp(decltype, "double") == 0
        || sqlite3_stricmp(decltype, "real") == 0
        || sqlite3_stricmp(decltype, "float") == 0)
        return ScalarReal(NA_REAL);
    else if (sqlite3_stricmp(decltype, "bool") == 0
        || sqlite3_stricmp(decltype, "boolean") == 0)
        return ScalarLogical(NA_LOGICAL);
    return ScalarString(NA_STRING);
}


SEXP convert_value_to_sexp(sqlite3_value *value)
{
    int n, nprotect = 1;
    SEXP v, bytes, R_fcall;
    switch (sqlite3_value_type(value))
    {
        case SQLITE_INTEGER:
            v = PROTECT(ScalarInteger(sqlite3_value_int(value)));
            break;
        case SQLITE_FLOAT:
            v = PROTECT(ScalarReal(sqlite3_value_double(value)));
            break;
        case SQLITE_TEXT:
            v = PROTECT(mkString((const char *)sqlite3_value_text(value)));
            break;
        case SQLITE_NULL:
            //v = PROTECT(R_NilValue);
            v = PROTECT(ScalarLogical(NA_LOGICAL));
            break;
        default: {
            n = sqlite3_value_bytes(value);
            bytes = PROTECT(allocVector(RAWSXP, n));
            memcpy(RAW(bytes), sqlite3_value_blob(value), n);
            if ((RAW(bytes)[0] == 0x58 || RAW(bytes)[0] == 0x41)
                && RAW(bytes)[1] == 0x0a)
            {
                // byte vector originates from R's serialize function
                // i.e. the first line is either 'X\n' or 'A\n'
                // 'X' is 0x58; 'A' is 0x41; '\n' is 0x0a
                R_fcall = PROTECT(lang2(install("unserialize"), bytes));
                v = PROTECT(eval(R_fcall, R_GlobalEnv));
                nprotect = 3;
            }
            else
                v = bytes;
            break;
        }
    }
    UNPROTECT(nprotect);
    return v;
}


SEXP convert_column_to_sexp(sqlite3_stmt *stmt, int i)
{
    int n, nprotect = 1;
    SEXP v, bytes, R_fcall;
    const char *decltype = sqlite3_column_decltype(stmt, i);
    switch (sqlite3_column_type(stmt, i))
    {
        case SQLITE_INTEGER:
            v = PROTECT(ScalarInteger(sqlite3_column_int(stmt, i)));
            break;
        case SQLITE_FLOAT:
            v = PROTECT(ScalarReal(sqlite3_column_double(stmt, i)));
            break;
        case SQLITE_TEXT:
            v = PROTECT(mkString((const char *)sqlite3_column_text(stmt, i)));
            break;
        case SQLITE_NULL:
            //v = PROTECT(R_NilValue);
            v = PROTECT(convert_null_to_sexp(decltype));
            break;
        default:
            n = sqlite3_column_bytes(stmt, i);
            bytes = PROTECT(allocVector(RAWSXP, n));
            memcpy(RAW(bytes), sqlite3_column_blob(stmt, i), n);
            //if (sqlite3_stricmp(decltype, "sexp") == 0
            //    || sqlite3_stricmp(decltype, "sxp") == 0)
            if ((RAW(bytes)[0] == 0x58 || RAW(bytes)[0] == 0x41)
                && RAW(bytes)[1] == 0x0a)
            {
                R_fcall = PROTECT(lang2(install("unserialize"), bytes));
                v = PROTECT(eval(R_fcall, R_GlobalEnv));
                nprotect = 3;
            }
            else
                v = bytes;
            break;
    }
    UNPROTECT(nprotect);
    return v;
}


void convert_sexp_to_result(SEXP val, sqlite3_context *ctx)
{
    PROTECT(val);
    int done = 0;
    if (val == R_NilValue)
    {
        UNPROTECT(1);
        sqlite3_result_null(ctx);
        return;
    }
    switch (isVectorAtomic(val))
    {
        case TRUE:
            if (LENGTH(val) == 1 && ATTRIB(val) == R_NilValue) {
                switch (TYPEOF(val)) {
                    case LGLSXP:
                        if (LOGICAL(val)[0] == NA_LOGICAL)
                            sqlite3_result_null(ctx);
                        else
                            sqlite3_result_int(ctx, (int)LOGICAL(val)[0]);
                        done = 1;
                        break;
                    case INTSXP:
                        if (INTEGER(val)[0] == NA_INTEGER)
                            sqlite3_result_null(ctx);
                        else
                            sqlite3_result_int(ctx, INTEGER(val)[0]);
                        done = 1;
                        break;
                    case REALSXP:
                        if (ISNAN(REAL(val)[0]))
                            sqlite3_result_null(ctx);
                        else
                            sqlite3_result_double(ctx, REAL(val)[0]);
                        done = 1;
                        break;
                    case STRSXP:
                        if (STRING_ELT(val, 0) == NA_STRING)
                            sqlite3_result_null(ctx);
                        else
                            sqlite3_result_text(ctx, CHAR(STRING_ELT(val, 0)),
                                -1, SQLITE_TRANSIENT);
                        done = 1;
                        break;
                    case RAWSXP:
                        sqlite3_result_blob(ctx, RAW(val), 1, SQLITE_TRANSIENT);
                        done = 1;
                        break;
                    case CPLXSXP: ;
                }
            }
            if (done)
                break;
            // else fall through
        default: {
            if (TYPEOF(val) == RAWSXP)
            {
                sqlite3_result_blob(
                    ctx, RAW(val), LENGTH(val), SQLITE_TRANSIENT);
                break;
            }
            SEXP R_fcall = PROTECT(allocVector(LANGSXP, 3));
            SETCAR(R_fcall, install("serialize"));
            SETCADR(R_fcall, val);
            SETCADDR(R_fcall, R_NilValue);
            SEXP bytes = PROTECT(eval(R_fcall, R_GlobalEnv));
            sqlite3_result_blob(
                ctx, RAW(bytes), LENGTH(bytes), SQLITE_TRANSIENT);
            UNPROTECT(2);
            break;
        }
    }
    UNPROTECT(1);
}


void convert_sexp_to_parameter(SEXP val, sqlite3_stmt *stmt, int i)
{
    PROTECT(val);
    int done = 0;
    if (val == R_NilValue)
    {
        UNPROTECT(1);
        sqlite3_bind_null(stmt, i);
        return;
    }
    switch (isVectorAtomic(val))
    {
        case TRUE:
            if (LENGTH(val) == 1 && ATTRIB(val) == R_NilValue) {
                switch (TYPEOF(val)) {
                    case LGLSXP:
                        if (LOGICAL(val)[0] == NA_LOGICAL)
                            sqlite3_bind_null(stmt, i);
                        else
                            sqlite3_bind_int(stmt, i, (int)LOGICAL(val)[0]);
                        done = 1;
                        break;
                    case INTSXP:
                        if (INTEGER(val)[0] == NA_INTEGER)
                            sqlite3_bind_null(stmt, i);
                        else
                            sqlite3_bind_int(stmt, i, INTEGER(val)[0]);
                        done = 1;
                        break;
                    case REALSXP:
                        if (ISNAN(REAL(val)[0]))
                            sqlite3_bind_null(stmt, i);
                        else
                            sqlite3_bind_double(stmt, i, REAL(val)[0]);
                        done = 1;
                        break;
                    case STRSXP:
                        if (STRING_ELT(val, 0) == NA_STRING)
                            sqlite3_bind_null(stmt, i);
                        else
                            sqlite3_bind_text(stmt, i, CHAR(STRING_ELT(val, 0)),
                                -1, SQLITE_TRANSIENT);
                        done = 1;
                        break;
                    case RAWSXP:
                        sqlite3_bind_blob(stmt, i, RAW(val), 1, SQLITE_TRANSIENT);
                        done = 1;
                        break;
                    case CPLXSXP: ;
                }
            }
            if (done)
                break;
            // else fall through
        default: {
            if (TYPEOF(val) == RAWSXP)
            {
                sqlite3_bind_blob(
                    stmt, i, RAW(val), LENGTH(val), SQLITE_TRANSIENT);
                break;
            }
            SEXP R_fcall = PROTECT(allocVector(LANGSXP, 3));
            SETCAR(R_fcall, install("serialize"));
            SETCADR(R_fcall, val);
            SETCADDR(R_fcall, R_NilValue);
            SEXP bytes = PROTECT(eval(R_fcall, R_GlobalEnv));
            sqlite3_bind_blob(
                stmt, i, RAW(bytes), LENGTH(bytes), SQLITE_TRANSIENT);
            UNPROTECT(2);
            break;
        }
    }
    UNPROTECT(1);
}
