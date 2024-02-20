#include <R.h>
#include <Rinternals.h>
#include "sqlite3.h"
#include "convert.h"


static void
db_finalize_stmt(SEXP Cur)
{
    #ifndef NDEBUG
        Rprintf("running pstmt finalizer\n");
    #endif
    sqlite3_stmt *stmt = (sqlite3_stmt *)R_ExternalPtrAddr(Cur);
    sqlite3_finalize(stmt);
    R_ClearExternalPtr(Cur);
}


static SEXP
process_row(sqlite3_stmt *stmt)
{
    int i;
    int ncol = sqlite3_column_count(stmt);
    if (ncol > 0)
    {
        SEXP fields = PROTECT(allocVector(VECSXP, ncol));
        SEXP names = PROTECT(allocVector(STRSXP, ncol));
        for (i = 0; i < ncol; ++i)
        {
            SET_VECTOR_ELT(fields, i, convert_column_to_sexp(stmt, i));
            SET_STRING_ELT(names, i, mkChar(sqlite3_column_name(stmt, i)));
        }
        setAttrib(fields, R_NamesSymbol, names);
        UNPROTECT(2);
        return fields;
    }
    return R_NilValue;
}

/*
SEXP db_prepare(SEXP Db, SEXP Stmt, SEXP params)
{
    int i = 0;
    int j;
    int rc;
    int n = length(params);  // n will always be at least 1 b/c list(list())
    int p = length(VECTOR_ELT(params, 0));
    SEXP par;
    const char *stmt = CHAR(STRING_ELT(Stmt, 0));
    sqlite3 *db = (sqlite3 *)R_ExternalPtrAddr(Db);
    sqlite3_stmt *pStmt;
    sqlite3_prepare_v2(db, stmt, -1, &pStmt, NULL);
    do {
        for (j = 0; j < p; ++j)
        {
            par = PROTECT(VECTOR_ELT(VECTOR_ELT(params, i), j));
            convert_sexp_to_parameter(par, pStmt, j+1);
            UNPROTECT(1);
        }
        rc = sqlite3_step(pStmt);
        if (rc == SQLITE_ROW) {
            if (n > 1) {
                sqlite3_finalize(pStmt);
                error_return(
                    "db.eval with multiple parameter sets can only be used"
                    " with INSERT/UPDATE/DELETE statements");
            }
            sqlite3_reset(pStmt);
            goto cursor;
        }
        else if (rc != SQLITE_DONE && rc != SQLITE_OK)
        {
            sqlite3_finalize(pStmt);
            error_return(sqlite3_errmsg(db));
        }
        sqlite3_reset(pStmt);
        sqlite3_clear_bindings(pStmt);
    } while (++i < n);

    sqlite3_finalize(pStmt);
    return R_NilValue;

    cursor:
        return R_MakeExternalPtr(pStmt, R_NilValue, R_NilValue);
}
*/

SEXP
db_prepare(SEXP Db, SEXP Stmt, SEXP params)
{
    int i = 0;
    int j;
    int rc;
    int n = length(params);  // n will always be at least 1 b/c list(list())
    int p = length(VECTOR_ELT(params, 0));
    SEXP par;
    const char *stmt = CHAR(STRING_ELT(Stmt, 0));
    sqlite3 *db = (sqlite3 *)R_ExternalPtrAddr(Db);
    sqlite3_stmt *pStmt;
    sqlite3_prepare_v2(db, stmt, -1, &pStmt, NULL);
    if (sqlite3_stmt_readonly(pStmt)) {
        if (n > 1) {
            sqlite3_finalize(pStmt);
            error_return(
                "db.eval with multiple parameter sets can only be used"
                " with INSERT/UPDATE/DELETE statements");
        }
        for (j = 0; j < p; ++j)
        {
            par = PROTECT(VECTOR_ELT(VECTOR_ELT(params, 0), j));
            convert_sexp_to_parameter(par, pStmt, j+1);
            UNPROTECT(1);
        }
        return R_MakeExternalPtr(pStmt, R_NilValue, R_NilValue);
    }
    do {
        for (j = 0; j < p; ++j)
        {
            par = PROTECT(VECTOR_ELT(VECTOR_ELT(params, i), j));
            convert_sexp_to_parameter(par, pStmt, j+1);
            UNPROTECT(1);
        }
        rc = sqlite3_step(pStmt);
        if (rc != SQLITE_DONE && rc != SQLITE_OK)
        {
            sqlite3_finalize(pStmt);
            error_return(sqlite3_errmsg(db));
        }
        sqlite3_reset(pStmt);
        sqlite3_clear_bindings(pStmt);
    } while (++i < n);

    sqlite3_finalize(pStmt);
    return R_NilValue;
}

/*
SEXP db_fetch(SEXP Cur, SEXP Db)
{
    int rc;
    sqlite3 *db = (sqlite3 *)R_ExternalPtrAddr(Db);
    sqlite3_stmt *pStmt = (sqlite3_stmt *)R_ExternalPtrAddr(Cur);
    rc = sqlite3_step(pStmt);
    if (rc == SQLITE_DONE)
    {
        sqlite3_reset(pStmt);
        return R_NilValue;
    }
    else if (rc == SQLITE_ROW)
        return process_row(pStmt);
    else
        error(sqlite3_errmsg(db));
    return R_NilValue;
}
*/

SEXP
db_fetch(SEXP Cur, SEXP Db, SEXP Rf)
{
    int i;
    int j;
    int k;
    int rc;
    int size;
    int nelem = 0;
    int end = 0;
    int ncol = 0;
    int nrow = 0;
    int count = 0;
    int nprotect = 1;
    sqlite3 *db = (sqlite3 *)R_ExternalPtrAddr(Db);
    sqlite3_stmt *pStmt = (sqlite3_stmt *)R_ExternalPtrAddr(Cur);

    // https://stackoverflow.com/a/8797232 for method
    SEXP buf;
    SEXP root = PROTECT(list1(buf = allocVector(VECSXP, 5000)));
    SEXP tail = root;
    SEXP colnames;
    SEXP dims;

    const char *rf_name = CHAR(STRING_ELT(Rf, 0));

    rc = sqlite3_step(pStmt);
    if (rc == SQLITE_ROW)
    {
        ncol = sqlite3_column_count(pStmt);
        colnames = PROTECT(allocVector(STRSXP, ncol)); ++nprotect;
        for (j = 0; j < ncol; ++j)
            SET_STRING_ELT(colnames, j, mkChar(sqlite3_column_name(pStmt, j)));
        do {
            for (j = 0; j < ncol; ++j, ++nelem)
            {
                SET_VECTOR_ELT(buf, count++, convert_column_to_sexp(pStmt, j));
                if (count == 5000)
                {
                    tail = SETCDR(tail, list1(buf = allocVector(VECSXP, 5000)));
                    count = 0;
                }
            }        
        } while ((rc = sqlite3_step(pStmt)) == SQLITE_ROW);
    }

    db_finalize_stmt(Cur);

    if (rc != SQLITE_DONE)
    {
        UNPROTECT(nprotect);
        error_return(sqlite3_errmsg(db));
    }

    if (nelem)
    {
        nrow = nelem / ncol;
        end = 0;
        if (strcmp(rf_name, "identity") == 0)
        {
            SEXP res = PROTECT(allocVector(VECSXP, nelem)); ++nprotect;
            while (root != R_NilValue)
            {
                size = (CDR(root) == R_NilValue) ? count : 5000;
                for (i = 0; i < size; ++i, ++end) {
                    // results are currently stored as row-major array
                    // with 'ncol' columns and 'nrow' rows.
                    // 'end' is the index position in that array
                    // here is the column index ...
                    k = end % ncol;
                    // ... and here is the row index
                    j = (end - k) / ncol;
                    // to convert 'end' to an index position in a
                    // column-major array we use
                    //   row + col * nrow = end (col-major)
                    // instead of
                    //   col + row * ncol = end (row-major)
                    SET_VECTOR_ELT(res, j+k*nrow, VECTOR_ELT(CAR(root), i));
                }
                root = CDR(root);
            }
            dims = PROTECT(allocVector(INTSXP, 2)); ++nprotect;
            INTEGER(dims)[0] = nrow;
            INTEGER(dims)[1] = ncol;
            setAttrib(res, R_DimSymbol, dims);
            setAttrib(res, R_DimNamesSymbol, list2(R_NilValue, colnames));
            UNPROTECT(nprotect);
            return res;
        }
        else if (strcmp(rf_name, "data.frame") == 0)
        {
            SEXP res = PROTECT(allocVector(VECSXP, ncol)); ++nprotect;
            SEXP rownames = PROTECT(allocVector(INTSXP, nrow)); ++nprotect;
            for (j = 0; j < ncol; ++j)
                SET_VECTOR_ELT(res, j, allocVector(
                    TYPEOF(VECTOR_ELT(CAR(root), j)), nrow));
            while (root != R_NilValue)
            {
                size = (CDR(root) == R_NilValue) ? count : 5000;
                for (i = 0; i < size; ++i, ++end) {
                    j = end % ncol;
                    k = (end - j) / ncol;
                    INTEGER(rownames)[k] = k + 1;
                    switch (TYPEOF(VECTOR_ELT(res, j))) {
                        case INTSXP:
                            INTEGER(VECTOR_ELT(res, j))[k] =
                                *INTEGER(VECTOR_ELT(CAR(root), i));
                            break;
                        case REALSXP:
                            REAL(VECTOR_ELT(res, j))[k] =
                                *REAL(VECTOR_ELT(CAR(root), i));
                            break;
                        case STRSXP:
                            SET_STRING_ELT(VECTOR_ELT(res, j), k,
                                STRING_ELT(VECTOR_ELT(CAR(root), i), 0));
                            break;
                        case RAWSXP:
                            RAW(VECTOR_ELT(res, j))[k] =
                                *RAW(VECTOR_ELT(CAR(root), i));
                            break;
                        case LGLSXP:
                            LOGICAL(VECTOR_ELT(res, j))[k] =
                                *LOGICAL(VECTOR_ELT(CAR(root), i));
                            break;
                        default:
                            UNPROTECT(nprotect);
                            error_return("unexpected SEXP value in C code");
                    }
                }
                root = CDR(root);
            }
            setAttrib(res, R_NamesSymbol, colnames);
            setAttrib(res, R_ClassSymbol, mkString("data.frame"));
            setAttrib(res, mkString("row.names"), rownames);
            UNPROTECT(nprotect);
            return res;
        }
        else
        {
            SEXP row = PROTECT(allocVector(VECSXP, ncol)); ++nprotect;
            SEXP res = PROTECT(allocVector(VECSXP, nrow)); ++nprotect;
            SEXP row_factory = PROTECT(lcons(install(rf_name), R_NilValue));
            ++nprotect;

            setAttrib(row, R_NamesSymbol, colnames);

            while (root != R_NilValue)
            {
                size = (CDR(root) == R_NilValue) ? count : 5000;
                for (i = 0; i < size; ++i, ++end) {
                    // results are currently stored as row-major array
                    // with 'ncol' columns and 'nrow' rows.
                    // 'end' is the index position in that array
                    // here is the column index ...
                    k = end % ncol;
                    // ... and here is the row index
                    j = (end - k) / ncol;
                    // to convert 'end' to an index position in a
                    // column-major array we use
                    //   row + col * nrow = end (col-major)
                    // instead of
                    //   col + row * ncol = end (row-major)
                    SET_VECTOR_ELT(row, k, VECTOR_ELT(CAR(root), i));
                    if (k == (ncol - 1)) {
                        SETCDR(row_factory, list1(row));
                        SET_VECTOR_ELT(res, j, eval(row_factory, R_GlobalEnv));
                    }
                }
                root = CDR(root);
            }
            UNPROTECT(nprotect);
            return res;
        }
    }
    UNPROTECT(nprotect);
    return R_NilValue;
}


SEXP
db_lapply(SEXP Db, SEXP Cur, SEXP fun, SEXP arglist)
{
    int i;
    int rc;
    int size;
    int nelem = 0;
    int end = 0;
    int count = 0;
    sqlite3 *db = (sqlite3 *)R_ExternalPtrAddr(Db);
    sqlite3_stmt *pStmt = (sqlite3_stmt *)R_ExternalPtrAddr(Cur);

    SEXP buf;
    SEXP root = PROTECT(list1(buf = allocVector(VECSXP, 5000)));
    SEXP tail = root;
    SEXP argnames = PROTECT(getAttrib(arglist, R_NamesSymbol));

    SEXP e;
    SEXP args = PROTECT(allocList(LENGTH(arglist)+1));
    e = CDR(args);
    for (i = 0; i < LENGTH(arglist); ++i) {
        SETCAR(e, VECTOR_ELT(arglist, i));
        if (argnames != R_NilValue
            && strcmp(CHAR(STRING_ELT(argnames, i)), "") != 0) {
            SET_TAG(e, STRING_ELT(argnames, i));
        }
        e = CDR(e);
    }

    SEXP R_fcall = PROTECT(lcons(fun, args));

    while ((rc = sqlite3_step(pStmt)) == SQLITE_ROW)
    {
        SETCAR(args, process_row(pStmt));
        SET_VECTOR_ELT(buf, count++, eval(R_fcall, R_GlobalEnv));
        if (count == 5000)
        {
            tail = SETCDR(tail, list1(buf = allocVector(VECSXP, 5000)));
            count = 0;
        }
        ++nelem;
    }

    db_finalize_stmt(Cur);

    if (rc != SQLITE_DONE)
    {
        UNPROTECT(4);
        error_return(sqlite3_errmsg(db));
    }

    if (nelem)
    {
        end = 0;
        SEXP res = PROTECT(allocVector(VECSXP, nelem));
        while (root != R_NilValue)
        {
            size = (CDR(root) == R_NilValue) ? count : 5000;
            for (i = 0; i < size; ++i)
                SET_VECTOR_ELT(res, end++, VECTOR_ELT(CAR(root), i));
            root = CDR(root);
        }
        UNPROTECT(5);
        return res;
    }
    return R_NilValue;
}
