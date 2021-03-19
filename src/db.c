#include "db.h"

static const R_CallMethodDef CallEntries[] = {
    CALLDEF(db_open, 2),
    CALLDEF(db_close, 1),
    CALLDEF(db_prepare, 3),
    CALLDEF(db_fetch, 3),
    CALLDEF(db_lapply, 3),
    CALLDEF(db_create_module, 2),
    CALLDEF(db_delete_module, 2),
    CALLDEF(db_register_shadow_name, 2),
    CALLDEF(db_unregister_shadow_name, 2),
    CALLDEF(db_create_function, 3),
    CALLDEF(db_subst, 3),
    {NULL, NULL, 0}
};

void attribute_visible R_init_db(DllInfo *info)
{
    R_registerRoutines(info, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(info, FALSE);
    R_forceSymbols(info, TRUE);
}
