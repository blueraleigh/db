#' Simple expression substitution within strings
#'
#' @param db The database connection.
#' @param txt The character string for substitution.
#' @param env The environment in which to perform the substitution.
#' @details The input \code{text} is searched for text enclosed
#' between two braces, which it treats as an expression and evaluates
#' in the calling frame, substituting any returned value in place of
#' the text between braces.
#' @return The input character string with the values returned from
#' any expressions between braces substituted in place.
#' @note Currently expressions do not nest. That is, the function will stop
#' at the first closing brace it finds and try to parse the extracted
#' expression. So "hello, {{greeting}}!" will attempt to parse `{greeting`,
#' which will return a parse error.
#' @examples
#' db = db.open()
#' greeting = "world!"
#' db.subst(db, "hello, {greeting}!") # => hello, world!!
#' db.subst(
#'   db
#'   , "hello, {greeting} what a grand {greeting}"
#' ) # => hello, world! what a grand world!
#' bar = function() return ("bar")
#' db.subst(db, "foo {bar()}") # => foo bar
#' db.subst(db, "foo {bar}") # => foo # note a trailing space
#' db.subst(db, "foo {paste0(deparse(bar), collapse='')}") # => foo function () return(\"bar\")
#' db.close(db)
#' @export
db.subst = function(db, text, env=parent.frame()) {
    stopifnot(is(db, "database"))
    stopifnot(is.character(text))
    .Call(db_subst, db@handle, enc2utf8(text), env)
}
