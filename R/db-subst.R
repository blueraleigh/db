#' Simple expression substitution within strings
#'
#' @param db The database connection.
#' @param txt The character string for substitution.
#' @param env The environment in which to perform the substitution.
#' @details The input \code{text} is searched for text enclosed
#' between <% and %>, which it treats as a sequence of expressions and evaluates
#' in the calling frame, substituting the value of the last evaluated
#' expression in place of the text between delimiters.
#' @return The input character string with the values returned from
#' any expressions between braces substituted in place.
#' @note Currently delimiters do not nest. That is, the function will stop
#' at the first closing delimiter sequence it finds and try to parse the extracted
#' expression. So "hello, <%<%greeting%>%>!" will attempt to parse `<%greeting`,
#' which will return a parse error.
#' @examples
#' db = db.open()
#' greeting = "world!"
#' db.subst(db, "hello, <% greeting %>!") # => hello, world!!
#' db.subst(
#'   db
#'   , "hello, <%greeting%> what a grand <%greeting%>"
#' ) # => hello, world! what a grand world!
#' bar = function() return ("bar")
#' db.subst(db, "foo <% bar() %>") # => foo bar
#' db.subst(db, "foo <% bar %>") # => foo # note a trailing space
#' db.subst(db, "foo <% paste0(deparse(bar), collapse='') %>") # => foo function () return(\"bar\")
#' db.close(db)
#' @export
db.subst = function(db, text, env=parent.frame()) {
    stopifnot(is(db, "database"))
    stopifnot(is.character(text))
    .Call(db_subst, db@handle, enc2utf8(text), env)
}
