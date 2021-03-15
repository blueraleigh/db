#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>
#include <R_ext/Visibility.h>

#define CALLDEF(name, n)  {#name, (DL_FUNC) &name, n}

/* db_io.c */
SEXP db_open(SEXP);
SEXP db_close(SEXP);
/* db_eval.c */
SEXP db_eval(SEXP, SEXP, SEXP);
SEXP db_fetch(SEXP, SEXP);
SEXP db_fetchall(SEXP, SEXP, SEXP);
SEXP db_lapply(SEXP, SEXP, SEXP);
/* db_function.c */
SEXP db_create_function(SEXP, SEXP, SEXP);
/* db_vtable.c */
SEXP db_create_module(SEXP, SEXP);
SEXP db_delete_module(SEXP, SEXP);
SEXP db_register_shadow_name(SEXP, SEXP);
SEXP db_unregister_shadow_name(SEXP, SEXP);
