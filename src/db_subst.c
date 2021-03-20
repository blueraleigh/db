#include <R.h>
#include <Rinternals.h>
#include <R_ext/Parse.h>

#include "sqlite3.h"


SEXP db_subst(SEXP db, SEXP txt, SEXP env) {

    char c;
    char *ret;
    char *unused;
    const char *cs = CHAR(STRING_ELT(txt, 0));
    ParseStatus status;

    sqlite3_str *s = sqlite3_str_new((sqlite3 *)R_ExternalPtrAddr(db));
    sqlite3_str *e = sqlite3_str_new((sqlite3 *)R_ExternalPtrAddr(db));

    SEXP p, q;
    SEXP expr;
    SEXP result;

    int i = 0;
    int n = strlen(cs);
    int nprotect = 0;
    int err;
    while (i < n) {
        c = cs[i++];

        switch (c) {
            case '{':
                // get value between delims
                while (i < n && (c = cs[i++]) != '}')
                    sqlite3_str_appendchar(e, 1, c);
                p = PROTECT(mkString(sqlite3_str_value(e)));
                expr = PROTECT(R_ParseVector(p, 1, &status, R_NilValue));
                if (status != PARSE_OK) {
                    UNPROTECT(2);
                    goto badparse;
                }
                q = PROTECT(VECTOR_ELT(expr, 0));
                result = PROTECT(R_tryEval(q, env, &err));
                if (err) {
                    UNPROTECT(4);
                    goto badparse;
                }
                if (TYPEOF(result) == STRSXP)
                    sqlite3_str_appendall(s, CHAR(STRING_ELT(result, 0)));
                sqlite3_str_reset(e);
                UNPROTECT(4);
                break;
            default:
                sqlite3_str_appendchar(s, 1, c);
                break;
        }
    }

    unused = sqlite3_str_finish(e);
    sqlite3_free(unused);
    ret = sqlite3_str_finish(s);
    if (ret)
        result = PROTECT(mkString(ret));
    else
        result = PROTECT(mkString(""));
    sqlite3_free(ret);
    UNPROTECT(1);
    return result;

    badparse:
        unused = sqlite3_str_finish(e);
        ret = sqlite3_str_finish(s);
        sqlite3_free(unused);
        sqlite3_free(ret);
        if (status != PARSE_OK)
            error("could not parse template string");
        return R_NilValue;
}
