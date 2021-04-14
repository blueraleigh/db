# tests for db.eval/db.exec
context("db.eval")

# test that output format is as expected
test1 = function() {
    db = db.open()
    on.exit(db.close(db))
    db.exec(db, "
        CREATE TABLE foo(c1 INTEGER);
        INSERT INTO foo VALUES(1),(2),(3);
    ")
    db.eval(db, "SELECT * FROM foo LIMIT 1 OFFSET 1")
}

# test that output format is as expected
test2 = function() {
    db = db.open()
    on.exit(db.close(db))
    db.exec(db, "
        CREATE TABLE foo(c1 INTEGER);
        INSERT INTO foo VALUES(1),(2),(3);
    ")
    db.eval(db, "SELECT * FROM foo LIMIT 1 OFFSET 1", df=TRUE)
}


# test that transactions work as expected
test3 = function() {
    db = db.open()
    on.exit(db.close(db))
    db.exec(db, "
        BEGIN;
        CREATE TABLE foo(c1 INTEGER);
        INSERT INTO foo VALUES(1),(2),(3);
        SAVEPOINT a;
        INSERT INTO foo VALUES(4),(5),(6);
        ROLLBACK TO a;
        END;
    ")
    db.eval(db, "SELECT * FROM foo", df=TRUE)
}

# test that parameter binding works as expected
test4 = function() {
    db = db.open()
    on.exit(db.close(db))
    db.exec(db, "
        BEGIN;
        CREATE TABLE foo(c1 INTEGER);
        INSERT INTO foo VALUES(1),(2),(3);
        INSERT INTO foo VALUES(4),(5),(6);
        END;
    ")
    db.eval(db, "SELECT * FROM foo WHERE c1 > ?", 3, df=TRUE)
}

# test that parameter binding works as expected
test5 = function() {
    db = db.open()
    on.exit(db.close(db))
    db.exec(db, "
        BEGIN;
        CREATE TABLE foo(c1 INTEGER);
        INSERT INTO foo VALUES(1),(2),(3);
        INSERT INTO foo VALUES(4),(5),(6);
        END;
    ")
    db.eval(db, "INSERT INTO foo VALUES(?)", data.frame(c1=7:9))
    db.eval(db, "SELECT * FROM foo", df=TRUE)
}


test_that("eval", {
    expect_equal(test1(), matrix(list(2L), 1, 1, dimnames=list(NULL, "c1")))
    expect_equal(test2(), data.frame(c1=2L))
    expect_equal(test3(), data.frame(c1=1:3))
    expect_equal(test4(), data.frame(c1=4:6))
    expect_equal(test5(), data.frame(c1=1:9))
})
