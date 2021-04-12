# Returns a function which implements an EM algorithm
# for fitting a Gaussian mixture model to a set of
# continuous measurements. The model assumes that the
# different mixtures have distinct centers but that
# they share a common, diagonal covariance matrix.
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
    sdev = sqrt(variance)

    # current cluster means. dim: [k, p]
    centers = apply(y, 2, function(d) {
        tapply(d, id, mean)
    })

    # current membership coefficients. dim: [n,k]
    membership = matrix(0, n, k)

    # current log likelihood
    lnL = -Inf
    # current iteration
    iter = 0L
    # improvement in likelihood score over previous iteration
    delta = 0

    logspace_add = function(a, b) {
        x = max(a, b)
        y = min(a, b)
        return (x + log1p(exp(y-x)))
    }

    logspace_sum = function(x) {
        Reduce(logspace_add, x)
    }

    logp = function(x) {
        # x is a single datum
        # p(x) = sum p(x|k)p(k) over k, where k is cluster index
        # here we a return vector of log p(x)'s and later sum the
        # with logspace_sum

        log(weights) +
            apply(
                centers
                , 1
                , function(mu) {
                    sum(dnorm(x, mu, sdev, log=TRUE))
                })
    }

    e_step = function() {
        logprob = t(apply(y, 1L, logp))

        tmp = exp(sweep(logprob, 1, apply(logprob, 1, max), "-"))

        membership <<- sweep(tmp, 1, rowSums(tmp), "/")

        oLnL = lnL
        lnL <<- sum(apply(logprob, 1, logspace_sum))
        delta <<- lnL - oLnL
    }

    m_step = function() {

        # new cluster means are a weighted average of data
        # using membership probabilities as weights
        # i.e. center[k, p] = sum ( m[, k] * y[, p] ) / sum( m[, k] )
        # tmp = t(membership) %*% y
        tmp = crossprod(membership, y)
        centers <<- sweep(tmp, 1, colSums(membership), "/")
        # new mixture weights are just the expected probability
        # that a data point is assigned to each mixture
        weights <<- colMeans(membership)
        variance <<- rowSums(sapply(1:k, function(i) {
            crossprod(sweep(y, 2, centers[i, ], "-")^2, membership[,i])
        })) / n
        sdev <<- sqrt(variance)

    }

    step = function() {
        e_step()
        m_step()
        iter <<- iter + 1L
        result[[iter]] <<- list(
            lnL=lnL
            , centers=centers
            , variance=variance
            , membership=membership)
    }

    # rationale for pairlist is that I think
    # it avoids the need to realloc space for
    # the whole vector with each new addition
    # since the new item is just cdr'd onto the
    # tail. should be faster.
    result = pairlist(list())
    step()

    return (step)
}



callbacks = list(

    .create=function(db, table, ...) {

        schema = "
            CREATE TABLE x(
                result       BLOB             -- mixture model results
                , data       HIDDEN TEXT      -- SQL query for input data
                , k          HIDDEN INTEGER   -- number of clusters
                , iter       HIDDEN INTEGER   -- current EM step
                , maxiter    HIDDEN INTEGER   -- max EM steps
                , tol        HIDDEN REAL      -- stopping tolerance
            );
        "
        env = new.env(parent=emptyenv())

        assign("schema", schema, envir=env)

        return (env)
    },

    .connect=function(db, table, ...) {
        .create(db, table, ...)
    },

    .bestindex=function(db, table, env, constraints, orderbys) {
        # look for equality constraints on
        #   - data
        #   - k
        #   - maxiter
        `%not in%` = function(x, table) {
            match(x, table, nomatch = 0L) == 0L
        }

        if (!length(constraints))
            return (NULL)

        ccols = sapply(constraints, "[[", 1L)
        if (length(constraints) && any(2:3 %not in% ccols)) {
            return (list(
                argv=rep(NA_integer_, length(constraints))
                , idxnum=0L
                , idxname=""
                , issorted=FALSE
                , cost=2147483647)
            )
        } else {
            argv = ccols
            argv[which(ccols %not in% c(2:3, 5:6))] = NA_integer_
            argv[!is.na(argv)] = order(argv[!is.na(argv)])
            return (list(
                argv=argv
                , idxnum=1L
                , idxname=""
                , issorted=FALSE
                , cost=25)
            )
        }
    },

    .filter=function(db, table, env, idxnum, idxname, ...) {
        if (idxnum == 0L) {

            assign("stepper", local({
                maxiter=-1
                iter=0
                delta=0
                eps=1
                function() {}
            }), envir=env)

        } else {

            nargs = ...length()

            dataq = ...elt(1L)

            assign(".data", dataq, envir=env)

            if (substr(dataq, 1L, 1L) == "(")
                data = db.eval(db, substr(dataq, 2L, nchar(dataq)-1L), df=TRUE)
            else
                data = db.eval(db, sprintf("SELECT * FROM %s", dataq), df=TRUE)

            k = as.integer(...elt(2L))

            maxiter = as.integer(ifelse(nargs >= 3, ...elt(3L), 1000))
            eps = as.numeric(ifelse(nargs >= 4, ...elt(4L), 1e-6))

            assign("stepper", gmm(data, k, eps, maxiter), envir=env)
        }
    },

    .rowid=function(db, table, env) {
        get("iter", envir=environment(get("stepper", envir=env)))
    },

    .eof=function(db, table, env) {
        stepper = environment(get("stepper", envir=env))
        A = get("delta", envir=stepper) < get("eps", envir=stepper)
        B = get("iter", envir=stepper) > get("maxiter", envir=stepper)
        if (A || B)
            return (TRUE)
        return (FALSE)
    },

    .next=function(db, table, env) {
        get("stepper", envir=env)()
    },

    .column=function(db, table, env, j) {
        stepper = environment(get("stepper", envir=env))
        switch(j,
            `1`=get("result", envir=stepper)[[get("iter", envir=stepper)]],
            `2`=get(".data", envir=env),
            `3`=get("k", envir=stepper),
            `4`=get("iter", envir=stepper),
            `5`=get("maxiter", envir=stepper),
            `6`=get("eps", envir=stepper)
        )
    }
)

library(db)
vtab = db.virtualtable("gmm", callbacks)

db = db.open()
vtab(db)

db.eval(db, 'create table iris(sl real, sw real, pl real, pw real, sp text)')
db.eval(db, 'insert into iris values(?,?,?,?,?)', iris)

db.eval(db, 'create virtual table gmm using gmm')
db.eval(db, "select * from gmm where data='iris' AND k=3 AND iter=14")
db.eval(db, "select * from gmm('iris', 3)")
