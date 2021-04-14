# tests for db.function
context("db.function")

test1 = function() {
    db = db.open()
    on.exit(db.close(db))

    db.eval(db, "CREATE TABLE foo(c1 INTEGER)")
    db.eval(db, "INSERT INTO foo VALUES(1),(2),(3)")
    db.function(db, "double", function(x) 2*x)

    sum(db.eval(db, "SELECT double(c1) FROM foo", df=TRUE)[,1])
}

test_that("function", {
    expect_equal(test1(), 12L)
})
