# tests for db.open
context("db.open")

test1 = function() {
    db = db.open()
    on.exit(db.close(db))
    is(db, "database")
}

test2 = function() {
    db = db.open()
    on.exit(db.close(db))
    db@registered()
}

test_that("open", {
    expect_true(test1())
    expect_false(test2())
})
