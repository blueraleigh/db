# tests for db.eval/db.exec
context("db.subst")

# test that output format is as expected
test1 = function() {
    db = db.open()
    on.exit(db.close(db))
    bar = function() return ("bar")
    c(
        # foo bar
        db.subst(db, "foo <%bar()%>")
        # foo # note a trailing space
        , db.subst(db, "foo <%bar%>")
        # foo function () return(\"bar\")
        , db.subst(db, "foo <%paste0(deparse(bar), collapse='')%>")
    )
}


test_that("subst", {
    expect_equal(test1(), c("foo bar", "foo ", "foo function () return(\"bar\")"))
})
