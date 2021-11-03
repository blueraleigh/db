#include <R.h>
#include <Rinternals.h>
#include <R_ext/Parse.h>

#include "sqlite3.h"

/*
previous version that used { and } as delimiters

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
*/



static int in_delim(int i, const char *str)
{
    if (str[i++] == '%' && str[i] == '>')
        return 0;
    return 1;
}

/*
// new version that uses <% and %> as delimiters like the R package brew
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
    int j;
    int n = strlen(cs);
    int err;
    while (i < n) {
        c = cs[i++];
        switch (c) {
            case '<':
                if (cs[i] == '%') {
                    // discard initial delim
                    ++i;
                    // get value between delims
                    while (i < n && in_delim(i, cs))
                        sqlite3_str_appendchar(e, 1, cs[i++]);
                    // discard final delims
                    ++i, ++i;

                    p = PROTECT(mkString(sqlite3_str_value(e)));
                    expr = PROTECT(R_ParseVector(p, -1, &status, R_NilValue));
                    if (status != PARSE_OK) {
                        UNPROTECT(2);
                        goto badparse;
                    }
                    for (j = 0; j < (length(expr)-1); ++j) {
                        q = PROTECT(VECTOR_ELT(expr, j));
                        R_tryEval(q, env, &err);
                        if (err) {
                            UNPROTECT(3);
                            goto badparse;
                        }
                        UNPROTECT(1);
                    }
                    // use result of last expression
                    q = PROTECT(VECTOR_ELT(expr, j));
                    result = PROTECT(R_tryEval(q, env, &err));
                    if (TYPEOF(result) == STRSXP)
                        sqlite3_str_appendall(s, CHAR(STRING_ELT(result, 0)));
                    sqlite3_str_reset(e);
                    UNPROTECT(4);
                    break;
                }
                // else intentional fall through
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
*/

/* a vectorized version */
SEXP db_subst(SEXP db, SEXP txt, SEXP env) {

    char c;
    char *ret;
    char *unused;
    const char *cs;
    int i;
    int j;
    int n;
    int err;
    int nstr = length(txt);

    ParseStatus status;

    SEXP p, q;
    SEXP expr;
    SEXP result;
    SEXP ans = PROTECT(allocVector(STRSXP, nstr));

    sqlite3_str *s = sqlite3_str_new((sqlite3 *)R_ExternalPtrAddr(db));
    sqlite3_str *e = sqlite3_str_new((sqlite3 *)R_ExternalPtrAddr(db));

    for (int k = 0; k < nstr; ++k) {
        cs = CHAR(STRING_ELT(txt, k));
        n = strlen(cs);
        i = 0;
        while (i < n) {
            c = cs[i++];
            switch (c) {
                case '<':
                    if (cs[i] == '%') {
                        // discard initial delim
                        ++i;
                        // get value between delims
                        while (i < n && in_delim(i, cs))
                            sqlite3_str_appendchar(e, 1, cs[i++]);
                        // discard final delims
                        ++i, ++i;

                        p = PROTECT(mkString(sqlite3_str_value(e)));
                        expr = PROTECT(R_ParseVector(p, -1, &status, R_NilValue));
                        if (status != PARSE_OK) {
                            UNPROTECT(2);
                            goto badparse;
                        }
                        for (j = 0; j < (length(expr)-1); ++j) {
                            q = PROTECT(VECTOR_ELT(expr, j));
                            R_tryEval(q, env, &err);
                            if (err) {
                                UNPROTECT(3);
                                goto badparse;
                            }
                            UNPROTECT(1);
                        }
                        // use result of last expression
                        q = PROTECT(VECTOR_ELT(expr, j));
                        result = PROTECT(R_tryEval(q, env, &err));
                        if (TYPEOF(result) == STRSXP)
                            sqlite3_str_appendall(s, CHAR(STRING_ELT(result, 0)));
                        sqlite3_str_reset(e);
                        UNPROTECT(4);
                        break;
                    }
                    // else intentional fall through
                default:
                    sqlite3_str_appendchar(s, 1, c);
                    break;
            }
        }

        ret = sqlite3_str_value(s);

        if (ret)
            SET_STRING_ELT(ans, k, mkChar(ret));
        else
            SET_STRING_ELT(ans, k, mkChar(""));

        sqlite3_str_reset(s);
    }

    unused = sqlite3_str_finish(e);
    sqlite3_free(unused);
    ret = sqlite3_str_finish(s);
    sqlite3_free(ret);

    UNPROTECT(1);
    return ans;

    badparse:
        unused = sqlite3_str_finish(e);
        ret = sqlite3_str_finish(s);
        sqlite3_free(unused);
        sqlite3_free(ret);
        if (status != PARSE_OK)
            error("could not parse template string");
        return R_NilValue;
}