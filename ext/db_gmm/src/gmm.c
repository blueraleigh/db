#include <R.h>
#include <Rinternals.h>
#include <Rmath.h>


static double compute_logp(
    unsigned int p,
    unsigned int k,
    double *x,
    double *weight,
    double *mu,
    double *var,
    double *membership
)
{
    unsigned int i;
    unsigned int j;
    double norm = 0;
    double p_max = R_NegInf;
    double lnL[k];
    for (i = 0; i < k; ++i) {
        lnL[i] = log(weight[i]);
        for (j = 0; j < p; ++j) {
            if (ISNAN(x[j]))
                continue;
            lnL[i] += dnorm(x[j], mu[i + j*k], sqrt(var[j]), 1);
        }
        if (lnL[i] > p_max)
            p_max = lnL[i];
    }
    for (i = 0; i < k; ++i)
        norm += membership[i] = exp(lnL[i] - p_max);
    for (i = 0; i < k; ++i)
        membership[i] /= norm;
    return logspace_sum(lnL, k);
}


static void e_step(
    unsigned int n,
    unsigned int p,
    unsigned int k,
    double *x,              // p by n
    double *logp,
    double *mu,             // k by p
    double *var,
    double *weight,
    double *membership,     // k by n
    double *delta,
    double *lnLk
)
{
    unsigned int i;

    double lnL_new = 0;
    double lnL_old = *lnLk;

    for (i = 0; i < n; ++i) {
        lnL_new += logp[i] = compute_logp(
            p, k, x + i*p, weight, mu, var, membership + i*k);
    }

    *delta = lnL_new - lnL_old;
    *lnLk = lnL_new;
}


static void m_step(
    unsigned int n,
    unsigned int p,
    unsigned int k,
    double *x,
    double *logp,
    double *mu,
    double *var,
    double *weight,
    double *membership,
    double *delta,
    double *lnLk
)
{
    unsigned int i;
    unsigned int j;
    unsigned int h;
    memset(mu, 0, k*p*sizeof(double));
    memset(weight, 0, k*sizeof(double));
    memset(var, 0, p*sizeof(double));
    for (h = 0; h < k; ++h) {
        for (i = 0; i < n; ++i) {
            for (j = 0; j < p; ++j) {
                mu[h + j*k] += x[j + i*p] * membership[h + i*k];
            }
            weight[h] += membership[h + i*k];
        }
    }

    for (h = 0; h < k; ++h) {
        for (j = 0; j < p; ++j)
            mu[h + j*k] /= weight[h];
        weight[h] /= n;
    }

    for (i = 0; i < n; ++i) {
        for (h = 0; h < k; ++h) {
            for (j = 0; j < p; ++j) {
                var[j] += (x[j + i*p] - mu[h + j*k])
                    * membership[h + i*k] * (x[j + i*p] - mu[h + j*k]) / n;
            }
        }
    }
}


static void step(
    unsigned int n,
    unsigned int p,
    unsigned int k,
    double *x,
    double *logp,
    double *mu,
    double *var,
    double *weight,
    double *membership,
    double *delta,
    double *lnLk
)
{
    e_step(n, p, k, x, logp, mu, var, weight, membership, delta, lnLk);
    m_step(n, p, k, x, logp, mu, var, weight, membership, delta, lnLk);
}


static void run(
    unsigned int n,
    unsigned int p,
    unsigned int k,
    unsigned int maxiter,
    double eps,
    double *x,
    double *logp,
    double *mu,
    double *var,
    double *weight,
    double *membership,
    unsigned int *iter,
    double *delta,
    double *lnLk
)
{
    do {
        step(n, p, k, x, logp, mu, var, weight, membership, delta, lnLk);
    } while (++(*iter) < maxiter && *delta > eps);
}


SEXP C_db_gmm(
    SEXP n,
    SEXP p,
    SEXP k,
    SEXP maxiter,
    SEXP eps,
    SEXP x,
    SEXP logp,
    SEXP mu,
    SEXP var,
    SEXP weight,
    SEXP membership
)
{
    GetRNGstate();

    unsigned int iter = 0;
    double delta = R_PosInf;
    double lnLk = R_NegInf;

    run(
        *INTEGER(n),
        *INTEGER(p),
        *INTEGER(k),
        *INTEGER(maxiter),
        *REAL(eps),
        REAL(x),
        REAL(logp),
        REAL(mu),
        REAL(var),
        REAL(weight),
        REAL(membership),
        &iter,
        &delta,
        &lnLk
    );

    SEXP ans = PROTECT(allocVector(VECSXP, 3));
    SEXP nms = PROTECT(allocVector(STRSXP, 3));
    SET_VECTOR_ELT(ans, 0, ScalarReal(lnLk));
    SET_VECTOR_ELT(ans, 1, ScalarInteger((int)iter));
    SET_VECTOR_ELT(ans, 2, ScalarLogical(delta < *REAL(eps)));
    SET_STRING_ELT(nms, 0, mkChar("lnLk"));
    SET_STRING_ELT(nms, 1, mkChar("iter"));
    SET_STRING_ELT(nms, 2, mkChar("converged"));
    setAttrib(ans, R_NamesSymbol, nms);

    UNPROTECT(2);

    PutRNGstate();

    return ans;
}


#include <R_ext/Rdynload.h>
#include <R_ext/Visibility.h>

#define CALLDEF(name, n)  {#name, (DL_FUNC) &name, n}

static const R_CallMethodDef CallEntries[] = {
    CALLDEF(C_db_gmm, 11),
    {NULL, NULL, 0}
};

void attribute_visible R_init_db_gmm(DllInfo *info)
{
    R_registerRoutines(info, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(info, FALSE);
    R_forceSymbols(info, TRUE);
}

