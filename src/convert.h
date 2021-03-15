#ifndef CONVERT_UTILS_H
#define CONVERT_UTILS_H

#include <R.h>
#include <Rinternals.h>
#include "sqlite3.h"

SEXP convert_null_to_sexp(const char *decltype);
SEXP convert_value_to_sexp(sqlite3_value *value);
SEXP convert_column_to_sexp(sqlite3_stmt *stmt, int i);
void convert_sexp_to_result(SEXP val, sqlite3_context *ctx);
void convert_sexp_to_parameter(SEXP val, sqlite3_stmt *stmt, int i);

#endif
