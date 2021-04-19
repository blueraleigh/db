gmm = function(x, k=2L, eps=1e-6, maxiter=1000L) {
    stopifnot(is.data.frame(x))

    keep_cols = sapply(x, is.numeric)
    keep_rows = Reduce(`&`, lapply(x[, keep_cols], Negate(is.na)))

    y = as.matrix(x[keep_rows, keep_cols])

    n = nrow(y)
    p = ncol(y)

    # initial cluster memberships
    id = sample(1:k, n, replace=TRUE)

    # current mixture weights. dim: [k,1]
    weights = tabulate(id, k) / n

    # current global covariance matrix.
    # dim: [p, p] but since it is diagonal
    # we only store [p, 1]
    variance = diag(var(y))

    # current cluster means. dim: [k, p]
    centers = apply(y, 2, function(d) {
        tapply(d, id, mean)
    })

    # log likelihood of each datum. dim: [n, 1]
    logp = numeric(n)

    # current membership coefficients. dim: [k,n]
    membership = matrix(0, k, n)

    model = .Call(
        C_db_gmm
        , n
        , p
        , as.integer(k)
        , as.integer(maxiter)
        , as.numeric(eps)
        , t(y)
        , logp
        , centers
        , variance
        , weights
        , membership)

    model$centers = centers
    model$variance = variance
    model$weights = weights
    model$membership = t(membership)
    model$logp = logp
    model$data = x[keep_rows, ]
    model$.data = y
    model$call = match.call()

    class(model) = "gmm"

    return (model)
}


logLik.gmm = function(object, x, ...) {
    if (missing(x)) {
        return (object$lnLk)
    } else {
        logspace_add = function(a, b) {
            x = max(a, b)
            y = min(a, b)
            return (x + log1p(exp(y-x)))
        }

        logspace_sum = function(x) {
            Reduce(logspace_add, x)
        }

        logp = function(x) {
            log(object$weights) +
                apply(
                    object$centers
                    , 1
                    , function(mu) {
                        sum(dnorm(x, mu, sqrt(object$variance), log=TRUE))
                    })
        }

        if (NCOL(x) != length(object$variance))
            stop("data have wrong dimension")

        if (NCOL(x) > 1) {
            logprob = apply(x, 1L, logp)
            return (apply(logprob, 2L, logspace_sum))

        } else {
            return (logspace_sum(logp(x)))
        }
    }
}


predict.gmm = function(object, x, alpha=0, ...) {
    if (NCOL(x) != length(object$variance))
        stop("new data have wrong dimension")
    if (alpha < 0 || alpha > 1)
        stop("invalid significance threshold")
    crit = qchisq(alpha, length(object$variance), lower.tail=FALSE)
    ma = apply(object$centers, 1, function(mu) {
        mahalanobis(x, mu, diag(object$variance))
    })
    if (NCOL(ma) > 1) {
        cls = unname(apply(ma, 1, which.min))
        attr(cls, "score") = unname(ma[(1:nrow(ma)-1) + nrow(ma)*(cls-1) + 1])
    } else {
        cls = unname(which.min(ma))
        attr(cls, "score") = unname(ma[cls])
    }
    cls[which(attr(cls, "score") > crit)] = NA_integer_
    return (cls)
}


simulate.gmm = function(object, nsim=1, seed=NULL, ...) {
    p = ncol(object$centers)
    m = rmultinom(1, nsim, object$weights)[,1]
    ans = matrix(0, p, nsim)
    ofs = 1L
    for (i in seq_along(m)) {
        if (m[i] > 0)
            ans[ofs:(ofs+m[i]*p-1L)] = rnorm(
                m[i]*p, object$centers[i, ], sqrt(object$variance))
        ofs = ofs + m[i]*p
    }
    t(ans)
}


print.gmm = function(x, digits = max(3L, getOption("digits") - 3L), ...) {
    cat("\nCall:\n", paste(deparse(x$call), sep = "\n", collapse = "\n"),
        "\n\n", sep = "")
    cat("Mixture weights:\n")
    print.default(format(x$weights, digits = digits), print.gap = 2L,
        quote = FALSE)
    cat("\n")
    cat("Component means:\n")
    print.default(format(x$centers, digits = digits), print.gap = 2L,
        quote = FALSE)
    cat("\n")
    cat("Variances:\n")
    print.default(format(x$variance, digits = digits), print.gap = 2L,
        quote = FALSE)
    cat("\n")
    cat("Converged:\n")
    print.default(x$converged, print.gap = 2L,
        quote = FALSE)
    cat("\n")
    cat("LnLk:\n")
    print.default(format(logLik(x), digits = digits), print.gap = 2L,
        quote = FALSE)
    cat("\n")
    invisible(x)
}


summary.gmm = function(object, ...) {
    print(object, ...)
}
