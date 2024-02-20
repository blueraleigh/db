#include <R.h>
#include <Rinternals.h>
#include <R_ext/Parse.h>
#include "sqlite3.h"
#include "convert.h"

static
int isna(SEXP x)
{
    switch (TYPEOF(x)) {
        case LGLSXP:
            return LOGICAL(x)[0] == NA_LOGICAL;
        case INTSXP:
            return INTEGER(x)[0] == NA_INTEGER;
        case REALSXP:
            return REAL(x)[0] == NA_REAL;
        case STRSXP:
            return STRING_ELT(x, 0) == NA_STRING;
        default:
            return 0;
    }
    return 0;
}


static SEXP
module_slot(SEXP module, const char *slot)
{
    return R_do_slot(module, install(slot));
}


static SEXP
module_db(SEXP module)
{
    return module_slot(module, "database");
}


static SEXP
module_env(SEXP module)
{
    return module_slot(module, ".methodEnv");
}


static const char *
module_name(SEXP module)
{
    return CHAR(STRING_ELT(module_slot(module, "name"), 0));
}


/* Only used by RvtabCreate and RvtabConnect */
static SEXP
module_expression_eval(
    SEXP module, const char *expr, int *err)
{

    SEXP R_fcall = PROTECT(
        eval(lang2(install("str2lang"), mkString(expr)), R_GlobalEnv));
    SETCADR(R_fcall, module_db(module));
    SEXP result = PROTECT(R_tryEvalSilent(R_fcall, module_env(module), err));
    UNPROTECT(2);
    return result;
}


/* Used by all other virtual table methods */
static SEXP
module_method_eval(
    SEXP module, const char *method, SEXP args, int *err)
{
    PROTECT(args);
    SEXP R_fcall;
    SEXP result;
    if (args)
        R_fcall = PROTECT(lcons(install(method), args));
    else
        R_fcall = PROTECT(lang1(install(method)));
    result = PROTECT(R_tryEvalSilent(R_fcall, module_env(module), err));
    UNPROTECT(3);
    return result;
}


/* Initialize argument list for virtual table method callbacks */
static SEXP
allocArgs(SEXP db, SEXP name, SEXP env, int n)
{
    SEXP args = PROTECT(allocList(n));
    SETCAR(args, db);
    SETCADR(args, name);
    SETCADDR(args, env);
    UNPROTECT(1);
    return args;
}


struct Rvtab {
    /* sqlite virtual table base class */
    sqlite3_vtab base;
    /* R S4 object implementing virtual table module methods */
    SEXP mod;
    /* Environment for holding virtual table data needed by module methods */
    SEXP env;
    /* Virtual table name */
    SEXP name;
};


/* Called only for a CREATE VIRTUAL TABLE statement
**
** CREATE VIRTUAL TABLE <argv[2]> USING <argv[0]>(<argv[3]>,<argv[4]>,...)
**   argv[0] name of module being invoked
**   argv[1] name of the database connection
**   argv[2] name of virtual table being created
**   argv[3] ... module arguments
**
** This function will use R to parse the module arguments and then
** will call the R create function with those arguments. Note that the module
** arguments may be other R variables, function calls, etc. so long as they are
** defined in the create function's evaluation environment.
*/
static int
RvtabCreate(
    sqlite3 *db,
    /* R S4 object implementing virtual table module methods */
    void *pAux,
    int argc,
    const char *const *argv,
    sqlite3_vtab **pVtab,
    char **errmsg
)
{
    int i;
    int rc;
    int err = 0;
    struct Rvtab *vtab;
    // pAux is preserved with R_PreserveObject
    // so there is no need to PROTECT
    SEXP mod = (SEXP)pAux;
    sqlite3_str *cmdstr = sqlite3_str_new(db);
    sqlite3_str_appendf(cmdstr, ".create(db,%Q", argv[2]);
    for (i=3; i < argc; ++i)
        sqlite3_str_appendf(cmdstr, ", %s", argv[i]);
    sqlite3_str_appendall(cmdstr, ")");
    char *cmd = sqlite3_str_finish(cmdstr);
    SEXP env = PROTECT(module_expression_eval(mod, cmd, &err));
    sqlite3_free(cmd);
    if (err) {
        *errmsg = sqlite3_mprintf("%s", R_curErrorBuf());
        UNPROTECT(1);
        return SQLITE_ERROR;
    }
    if (!isEnvironment(env)) {
        *errmsg = sqlite3_mprintf("%s",
            "virtual table .create method did not return an environment");
        UNPROTECT(1);
        return SQLITE_ERROR;
    }
    SEXP schema = PROTECT(findVar(install("schema"), env));
    if (TYPEOF(schema) != STRSXP || LENGTH(schema) != 1) {
        *errmsg = sqlite3_mprintf("%s",
            "invalid schema assigned by virtual table .create method");
        UNPROTECT(2);
        return SQLITE_ERROR;
    }
    rc = sqlite3_declare_vtab(db, CHAR(STRING_ELT(schema, 0)));
    if (rc == SQLITE_OK)
    {
        vtab = sqlite3_malloc(sizeof(*vtab));
        memset(vtab, 0, sizeof(*vtab));
        vtab->mod = mod;
        vtab->env = env;
        vtab->name = mkString(argv[2]);
        R_PreserveObject(vtab->env);
        R_PreserveObject(vtab->name);
        *pVtab = (sqlite3_vtab *)vtab;
    }
    UNPROTECT(2);
    return rc;
}


/* Called whenever the database connection attaches to or reparses
** a virtual table schema */
static int
RvtabConnect(
    sqlite3 *db,
    void *pAux,
    int argc,
    const char *const *argv,
    sqlite3_vtab **pVtab,
    char **errmsg
)
{
    int i;
    int rc;
    int err = 0;
    struct Rvtab *vtab;
    SEXP mod = (SEXP)pAux;
    sqlite3_str *cmdstr = sqlite3_str_new(db);
    sqlite3_str_appendf(cmdstr, ".connect(db,%Q", argv[2]);
    for (i=3; i < argc; ++i)
        sqlite3_str_appendf(cmdstr, ", %s", argv[i]);
    sqlite3_str_appendall(cmdstr, ")");
    char *cmd = sqlite3_str_finish(cmdstr);
    SEXP env = PROTECT(module_expression_eval(mod, cmd, &err));
    sqlite3_free(cmd);
    if (err) {
        *errmsg = sqlite3_mprintf("%s", R_curErrorBuf());
        UNPROTECT(1);
        return SQLITE_ERROR;
    }
    if (!isEnvironment(env)) {
        *errmsg = sqlite3_mprintf("%s",
            "virtual table .connect method did not return an environment");
        UNPROTECT(1);
        return SQLITE_ERROR;
    }
    SEXP schema = PROTECT(findVar(install("schema"), env));
    if (TYPEOF(schema) != STRSXP && LENGTH(schema) != 1) {
        *errmsg = sqlite3_mprintf("%s",
            "invalid schema assigned by virtual table .connect method");
        UNPROTECT(2);
        return SQLITE_ERROR;
    }
    rc = sqlite3_declare_vtab(db, CHAR(STRING_ELT(schema, 0)));
    if (rc == SQLITE_OK)
    {
        vtab = sqlite3_malloc(sizeof(*vtab));
        memset(vtab, 0, sizeof(*vtab));
        vtab->mod = mod;
        vtab->env = env;
        vtab->name = mkString(argv[2]);
        R_PreserveObject(vtab->env);
        R_PreserveObject(vtab->name);
        *pVtab = (sqlite3_vtab *)vtab;
    }
    UNPROTECT(2);
    return rc;
}


/* Called whenever the database connection is closed */
static int
RvtabDisconnect(sqlite3_vtab *pVtab)
{
    int err = 0;
    struct Rvtab *vtab = (struct Rvtab *)pVtab;
    SEXP args = PROTECT(
        allocArgs(module_db(vtab->mod), vtab->name, vtab->env, 3));
    module_method_eval(vtab->mod, ".disconnect", args, &err);
    UNPROTECT(1);
    R_ReleaseObject(vtab->env);
    R_ReleaseObject(vtab->name);
    if (pVtab->zErrMsg)
        sqlite3_free(pVtab->zErrMsg);
    sqlite3_free(vtab);
    return SQLITE_OK;
}


/* Called only for a DROP TABLE statement */
static int
RvtabDestroy(sqlite3_vtab *pVtab)
{
    int err = 0;
    struct Rvtab *vtab = (struct Rvtab *)pVtab;
    SEXP args = PROTECT(
        allocArgs(module_db(vtab->mod), vtab->name, vtab->env, 3));
    module_method_eval(vtab->mod, ".destroy", args, &err);
    UNPROTECT(1);
    R_ReleaseObject(vtab->env);
    R_ReleaseObject(vtab->name);
    if (pVtab->zErrMsg)
        sqlite3_free(pVtab->zErrMsg);
    sqlite3_free(vtab);
    return SQLITE_OK;
}


struct Rvtab_cursor {
    /* sqlite virtual table base class */
    sqlite3_vtab_cursor base;
    /* R S4 object implementing virtual table module methods */
    SEXP mod;
    /* Environment for holding virtual table data needed by module methods */
    SEXP env;
    /* Virtual table name */
    SEXP name;
};


/* Creates a new cursor for reading and writing */
static int
RvtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    int err = 0;
    struct Rvtab *vtab = (struct Rvtab *)p;
    struct Rvtab_cursor *cursor;
    SEXP args = PROTECT(
        allocArgs(module_db(vtab->mod), vtab->name, vtab->env, 3));
    module_method_eval(vtab->mod, ".open", args, &err);
    UNPROTECT(1);
    if (err) {
        if (p->zErrMsg)
            sqlite3_free(p->zErrMsg);
        p->zErrMsg = sqlite3_mprintf("%s", R_curErrorBuf());
        return SQLITE_ERROR;
    }
    cursor = sqlite3_malloc(sizeof(*cursor));
    if (cursor == 0) return SQLITE_NOMEM;
    memset(cursor, 0, sizeof(*cursor));
    cursor->mod = vtab->mod;
    cursor->env = vtab->env;
    cursor->name = vtab->name;
    *ppCursor = &cursor->base;
    return SQLITE_OK;
}


/* Destroys the cursor created by RvtabOpen */
static int
RvtabClose(sqlite3_vtab_cursor *cur)
{
    int err = 0;
    struct Rvtab_cursor *cursor = (struct Rvtab_cursor *)cur;
    SEXP args = PROTECT(
        allocArgs(module_db(cursor->mod), cursor->name, cursor->env, 3));
    module_method_eval(cursor->mod, ".close", args, &err);
    UNPROTECT(1);
    sqlite3_free(cursor);
    return SQLITE_OK;
}

/* Called to initiate a search of the virtual table. The simplest
** implementation is just to have this method set the cursor so that
** it points to the first row of data. */
static int
RvtabFilter(
    sqlite3_vtab_cursor *pVtabCursor,
    int idxNum, const char *idxStr,
    int argc, sqlite3_value **argv
)
{
    int i;
    int err = 0;
    int rc = SQLITE_OK;
    struct Rvtab_cursor *cursor = (struct Rvtab_cursor *)pVtabCursor;

    SEXP e;
    SEXP args = PROTECT(
        allocArgs(module_db(cursor->mod), cursor->name, cursor->env, argc+5));
    e = CDR(args); e = CDR(e); e = CDR(e);
    SETCAR(e, ScalarInteger(idxNum)); e = CDR(e);
    if (idxStr) {
        SETCAR(e, mkString(idxStr)); e = CDR(e);
    }
    else {
        SETCAR(e, R_NilValue); e = CDR(e);
    }

    for (i = 0; i < argc; ++i) {
        SETCAR(e, convert_value_to_sexp(argv[i])); e = CDR(e);
    }

    module_method_eval(cursor->mod, ".filter", args, &err);

    if (err) {
        if (pVtabCursor->pVtab->zErrMsg)
            sqlite3_free(pVtabCursor->pVtab->zErrMsg);
        pVtabCursor->pVtab->zErrMsg = sqlite3_mprintf("%s", R_curErrorBuf());
        rc = SQLITE_ERROR;
    }

    UNPROTECT(1);
    return rc;
}

/* Advance the cursor to the next row of data */
static int
RvtabNext(sqlite3_vtab_cursor *cur)
{
    int err = 0;
    struct Rvtab_cursor *cursor = (struct Rvtab_cursor*)cur;
    SEXP args = PROTECT(
        allocArgs(module_db(cursor->mod), cursor->name, cursor->env, 3));
    module_method_eval(cursor->mod, ".next", args, &err);
    UNPROTECT(1);
    if (err) {
        if (cur->pVtab->zErrMsg)
            sqlite3_free(cur->pVtab->zErrMsg);
        cur->pVtab->zErrMsg = sqlite3_mprintf("%s", R_curErrorBuf());
        return SQLITE_ERROR;
    }
    return SQLITE_OK;
}

/* Return data in the i-th column of the current row. The i-index is
** 0 based, so it is incremented by 1 before the hand-off to R. */
static int
RvtabColumn(
    sqlite3_vtab_cursor *cur,   /* The cursor */
    sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
    int i                       /* Which column to return */
)
{
    int err;
    struct Rvtab_cursor *cursor = (struct Rvtab_cursor*)cur;
    SEXP e;
    SEXP args = PROTECT(
        allocArgs(module_db(cursor->mod), cursor->name, cursor->env, 4));
    e = CDR(args); e = CDR(e); e = CDR(e);
    SETCAR(e, ScalarInteger(i+1));
    SEXP val = PROTECT(module_method_eval(
        cursor->mod, ".column", args, &err));
    if (err)
    {
        if (cur->pVtab->zErrMsg)
            sqlite3_free(cur->pVtab->zErrMsg);
        cur->pVtab->zErrMsg = sqlite3_mprintf("%s", R_curErrorBuf());
        UNPROTECT(2);
        return SQLITE_ERROR;
    }
    convert_sexp_to_result(val, ctx);
    UNPROTECT(2);
    return SQLITE_OK;
}


/* Called by SQLite when it is compiling a statement to execute.
** Used to plan the best way to run the query and access rows
** from the virtual table. */
static int
RvtabBestIndex(
    sqlite3_vtab *tab,
    sqlite3_index_info *pIdxInfo
)
{
    int i;
    int j;
    int nconstraint = 0;
    int err = 0;
    SEXP constraints;
    SEXP orderbys;
    SEXP result;
    struct Rvtab *vtab = (struct Rvtab *)tab;

    for (i = 0; i < pIdxInfo->nConstraint; ++i)
    {
        if (pIdxInfo->aConstraint[i].usable)
            ++nconstraint;
    }

    if (pIdxInfo->nConstraint > 0 && nconstraint == 0)
        return SQLITE_CONSTRAINT;

    constraints = PROTECT(allocVector(VECSXP, nconstraint));
    orderbys = PROTECT(allocVector(VECSXP, pIdxInfo->nOrderBy));

    for (i = 0, j = 0; i < pIdxInfo->nConstraint; ++i)
    {
        if (!pIdxInfo->aConstraint[i].usable)
            continue;
        SET_VECTOR_ELT(constraints, j, allocVector(INTSXP, 2));
        INTEGER(VECTOR_ELT(constraints, j))[0] =
            pIdxInfo->aConstraint[i].iColumn + 1; // add 1 for R index
        INTEGER(VECTOR_ELT(constraints, j))[1] = pIdxInfo->aConstraint[i].op;
        ++j;
    }

    for (i = 0; i < pIdxInfo->nOrderBy; ++i)
    {
        SET_VECTOR_ELT(orderbys, i, allocVector(INTSXP, 2));
        INTEGER(VECTOR_ELT(orderbys, i))[0] =
            pIdxInfo->aOrderBy[i].iColumn + 1; // add 1 for R index
        INTEGER(VECTOR_ELT(orderbys, i))[1] = pIdxInfo->aOrderBy[i].desc;
    }
    SEXP e;
    SEXP args = PROTECT(
        allocArgs(module_db(vtab->mod), vtab->name, vtab->env, 5));
    e = CDR(args); e = CDR(e); e = CDR(e);
    SETCAR(e, constraints); e = CDR(e);
    SETCAR(e, orderbys);

    result = PROTECT(module_method_eval(vtab->mod, ".bestindex", args, &err));

    if (err)
    {
        if (tab->zErrMsg)
            sqlite3_free(tab->zErrMsg);
        tab->zErrMsg = sqlite3_mprintf("%s", R_curErrorBuf());
        UNPROTECT(4);
        return SQLITE_ERROR;
    }

    if (result == R_NilValue || LENGTH(result) == 0)
    {
        UNPROTECT(4);
        return SQLITE_OK;
    }

    if (TYPEOF(result) != VECSXP)
    {
        UNPROTECT(4);
        vtab->base.zErrMsg = sqlite3_mprintf(
            "%s", "result from .bestindex is not a list");
        return SQLITE_ERROR;
    }

    SEXP indices = PROTECT(VECTOR_ELT(result, 0));
    if (TYPEOF(indices) != INTSXP)
    {
        UNPROTECT(5);
        vtab->base.zErrMsg = sqlite3_mprintf(
            "%s", ".bestindex did not return an integer vector of indices");
        return SQLITE_ERROR;
    }
    if (LENGTH(indices) != LENGTH(constraints))
    {
        UNPROTECT(5);
        vtab->base.zErrMsg = sqlite3_mprintf(
            "%s", "length of .bestindex output indices do not match input ");
        return SQLITE_ERROR;
    }

    for(i = 0, j = 0; i < pIdxInfo->nConstraint; i++)
    {
        if (!pIdxInfo->aConstraint[i].usable)
            continue;
        if (INTEGER(indices)[j] != NA_INTEGER)
            pIdxInfo->aConstraintUsage[i].argvIndex = INTEGER(indices)[j];
        ++j;
    }
    UNPROTECT(1);

    if (LENGTH(result) > 1)
        pIdxInfo->idxNum = INTEGER(VECTOR_ELT(result, 1))[0];
    if (LENGTH(result) > 2)
    {
        pIdxInfo->idxStr = sqlite3_mprintf(
            "%s", CHAR(STRING_ELT(VECTOR_ELT(result, 2), 0)));
        pIdxInfo->needToFreeIdxStr = 1;
    }
    if (LENGTH(result) > 3)
        pIdxInfo->orderByConsumed = (int)(LOGICAL(VECTOR_ELT(result, 3))[0]);
    if (LENGTH(result) > 4)
        pIdxInfo->estimatedCost = REAL(VECTOR_ELT(result, 4))[0];

    UNPROTECT(4);
    return SQLITE_OK;
}


/* Returns the rowid of the current row of data */
static int
RvtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    int err = 0;
    struct Rvtab_cursor *cursor = (struct Rvtab_cursor*)cur;
    SEXP args = PROTECT(
        allocArgs(module_db(cursor->mod), cursor->name, cursor->env, 3));
    SEXP rid = PROTECT(module_method_eval(cursor->mod, ".rowid", args, &err));
    if (err)
    {
        if (cur->pVtab->zErrMsg)
            sqlite3_free(cur->pVtab->zErrMsg);
        cur->pVtab->zErrMsg = sqlite3_mprintf("%s", R_curErrorBuf());
        UNPROTECT(2);
        return SQLITE_ERROR;
    }
    *pRowid = INTEGER(rid)[0];
    UNPROTECT(2);
    return SQLITE_OK;
}

static int
RvtabUpdate(sqlite3_vtab *pVtab, int argc,
    sqlite3_value **argv, sqlite3_int64 *pRowid)
{
    int i;
    int insert = 0;
    int update = 0;
    int delete = 0;
    int err = 0;
    struct Rvtab *vtab = (struct Rvtab *)pVtab;

    // argv[0] rowid in the virtual table
    // argv[1] rowid for newly inserted row. If SQLITE_NULL, implementation
    //         must choose this value
    // argv[2] and beyond are new values for the fields in the order that
    //         the columns of the virtual table were declared

    SEXP e;
    SEXP args;
    if (argc == 1)
    {
        // delete existing row with rowid = argv[0]
        args = PROTECT(
            allocArgs(module_db(vtab->mod), vtab->name, vtab->env, argc+3));
        e = CDR(args); e = CDR(e); e = CDR(e);
        SETCAR(e, ScalarInteger(sqlite3_value_int(argv[0]))); e = CDR(e);
        delete = 1;
    }
    else if (sqlite3_value_type(argv[0]) == SQLITE_NULL)
    {
        // insert a new row with rowid = argv[1]
        args = PROTECT(
            allocArgs(module_db(vtab->mod), vtab->name, vtab->env, (argc-1)+3));
        e = CDR(args); e = CDR(e); e = CDR(e);
        if (sqlite3_value_type(argv[1]) == SQLITE_NULL) {
            SETCAR(e, ScalarInteger(NA_INTEGER)); e = CDR(e);
        }
        else {
            SETCAR(e, ScalarInteger(sqlite3_value_int(argv[1]))); e = CDR(e);
        }
        insert = 1;
    }
    else
    {
        // update existing row with rowid = argv[0], possibly changing
        // the rowid to argv[1] during the update if argv[0] != argv[1]
        args = PROTECT(
            allocArgs(module_db(vtab->mod), vtab->name, vtab->env, argc+3));
        e = CDR(args); e = CDR(e); e = CDR(e);
        SETCAR(e, ScalarInteger(sqlite3_value_int(argv[1]))); e = CDR(e);
        SETCAR(e, ScalarInteger(sqlite3_value_int(argv[0]))); e = CDR(e);
        update = 1;
    }

    if (insert || update)
    {
        for (i = 2; i < argc; ++i)
        {
            SETCAR(e, convert_value_to_sexp(argv[i]));
            e = CDR(e);
        }
    }

    SEXP res;

    if (delete)
        res = PROTECT(
            module_method_eval(vtab->mod, ".delete", args, &err));
    if (insert)
        res = PROTECT(
            module_method_eval(vtab->mod, ".insert", args, &err));
    if (update)
        res = PROTECT(
            module_method_eval(vtab->mod, ".update", args, &err));

    if (err)
    {
        if (pVtab->zErrMsg)
            sqlite3_free(pVtab->zErrMsg);
        pVtab->zErrMsg = sqlite3_mprintf("%s", R_curErrorBuf());
        UNPROTECT(2);
        return SQLITE_ERROR;
    }

    if (isna(res))
    {
        UNPROTECT(2);
        return SQLITE_READONLY;
    }

    if (insert)
    {
        if (TYPEOF(res) != INTSXP)
        {
            vtab->base.zErrMsg = sqlite3_mprintf(
                "%s", ".insert returned a non-integer value");
            UNPROTECT(2);
            return SQLITE_ERROR;
        }

        if (ISNA(INTEGER(res)[0]))
        {
            vtab->base.zErrMsg = sqlite3_mprintf(
                "%s", ".insert returned an NA integer value");
            UNPROTECT(2);
            return SQLITE_ERROR;
        }

        // an insert was performed, so res must provide the new rowid
        // for sqlite3_last_insert_rowid
        *pRowid = INTEGER(res)[0];
    }

    UNPROTECT(2);
    return SQLITE_OK;
}


/* Returns FALSE if the cursor points to a valid row of data, TRUE otherwise.
** Called immediately after RvtabNext and RvtabFilter. */
static int
RvtabEof(sqlite3_vtab_cursor *cur)
{
    int err = 0;
    struct Rvtab_cursor *cursor = (struct Rvtab_cursor*)cur;
    SEXP args = PROTECT(
        allocArgs(module_db(cursor->mod), cursor->name, cursor->env, 3));
    SEXP eof = PROTECT(module_method_eval(cursor->mod, ".eof", args, &err));
    if (!err)
    {
        UNPROTECT(2);
        return (int)(LOGICAL(eof)[0]);
    }
    UNPROTECT(2);
    return 1;
}


// for registering shadow table names
static int Rvtab_ShadowNameSet_inited = 0;
static SEXP Rvtab_ShadowNameSet = 0;

static int
RvtabShadowName(const char *name)
{
    if (Rvtab_ShadowNameSet == 0)
        return 0;
    if (name == 0)
        return 0;
    if (sqlite3_stricmp(name, "") == 0)
        return 0;
    SEXP store = CAR(Rvtab_ShadowNameSet);
    if (store == R_NilValue)
        return 0;
    int n = INTEGER(CDR(Rvtab_ShadowNameSet))[0];
    for (R_xlen_t i = (n - 1); i >= 0; i--)
    {
        if (sqlite3_stricmp(
            CHAR(STRING_ELT(VECTOR_ELT(store, i), 0)), name) == 0)
        {
            return 1;
        }
    }
    return 0;
}


static sqlite3_module RvtabModule = {
  /* iVersion    */ 3,
  /* xCreate     */ RvtabCreate,
  /* xConnect    */ RvtabConnect,
  /* xBestIndex  */ RvtabBestIndex,
  /* xDisconnect */ RvtabDisconnect,
  /* xDestroy    */ RvtabDestroy,
  /* xOpen       */ RvtabOpen,
  /* xClose      */ RvtabClose,
  /* xFilter     */ RvtabFilter,
  /* xNext       */ RvtabNext,
  /* xEof        */ RvtabEof,
  /* xColumn     */ RvtabColumn,
  /* xRowid      */ RvtabRowid,
  /* xUpdate     */ RvtabUpdate,
  /* xBegin      */ 0,
  /* xSync       */ 0,
  /* xCommit     */ 0,
  /* xRollback   */ 0,
  /* xFindMethod */ 0,
  /* xRename     */ 0,
  /* xSavepoint  */ 0,
  /* xRelease    */ 0,
  /* xRollbackTo */ 0,
  /* xShadowName */ RvtabShadowName
};


void
db_release_module(void *module)
{
    R_ReleaseObject((SEXP)module);
}


SEXP
db_create_module(SEXP Db, SEXP module)
{
    R_PreserveObject(module);
    sqlite3 *db = (sqlite3 *)R_ExternalPtrAddr(Db);
    sqlite3_create_module_v2(db, module_name(module),
        &RvtabModule, (void *)module, db_release_module);
    return R_NilValue;
}


SEXP
db_delete_module(SEXP Db, SEXP module)
{
    sqlite3 *db = (sqlite3 *)R_ExternalPtrAddr(Db);
    sqlite3_create_module_v2(db, module_name(module),
        0, (void *)module, db_release_module);
    return R_NilValue;
}


SEXP
db_register_shadow_name(SEXP Db, SEXP name)
{
    (void)(Db);
    if (!Rvtab_ShadowNameSet_inited)
    {
        Rvtab_ShadowNameSet = R_NewPreciousMSet(50);
        R_PreserveObject(Rvtab_ShadowNameSet);
        Rvtab_ShadowNameSet_inited = 1;
    }
    R_PreserveInMSet(name, Rvtab_ShadowNameSet);
    return R_NilValue;
}


SEXP
db_unregister_shadow_name(SEXP Db, SEXP name)
{
    (void)(Db);
    if (Rvtab_ShadowNameSet_inited)
    {
        R_ReleaseFromMSet(name, Rvtab_ShadowNameSet);
    }
    return R_NilValue;
}
