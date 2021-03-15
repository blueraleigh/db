#' Retrieve the help handler for a database
#'
#' @param db An S4 object of class "database".
#' @description Databases that implement a help system are required to
#' export a custom handler for responding to HTTP requests. To determine
#' if a database implements a help system the database is checked for
#' the presence of a table named Rdb_help. This should have the following
#' schema
#' \preformatted{
#' CREATE TABLE dbpkg_doc(
#'     handler TEXT,
#'     pkg TEXT
#' )
#' }
#' where 'handler' is the name of an R function and 'pkg' is the name of
#' the R package that implements it. If such a table exists it should only
#' have a single row. The handler function is imported from the package
#' and assigned into the tools:::.httpd.handlers.env environment under the
#' name of the database file. For this reason the name of the database must
#' be under 64 characters long because R's built in HTTP server will only
#' utilized custom handlers that are under 64 characters. Handlers should
#' arrange to receive requests at URLs that look like /custom/<name>/.*,
#' where <name> is the name of the database file. Handling functions can
#' then retreive the database connection by passing <name> to
#' \code{\link{db.get}}.
#'
#' An example URL may look like:
#'
#' http:localhost:8080/custom/db.name/abc/def?a=1&b=2
#'
#' where
#' - db.name is the 'path name' and name of the database
#'   available as db.path_get(path, 'name')
#' - abc is the 'path head' available as db.path_get(path, 'head')
#' - def is the 'path tail' available as db.path_get(path, 'tail')
#' - a=1&b=2 is the query string, which R parses for us
#'
#' And a generic handler exported at package level would look like
#'
#' function(path, query, body, headers) {
#'    path_head = db.path_get(path, 'head')
#'    # dispatch to the handler for path_head
#'    # passing along the same arguments
#' }
#' where query, body, and headers are provided by R's http server.
#'
#' query will be either:
#'    - a NULL
#'    - a named character vector representing the query string. the names will
#'      be the query parameters and the items will be the values.
#' body will be either:
#'    - a NULL
#'    - a named character vector representing the query string (in the event
#'      that the body is a url-encoded form)
#'    - a RAWSXP with the body content, possibly with the attribute "content-type"
#' headers will be either:
#'    - a NULL
#'    - a RAWSXP with the header content
#'
#' All handlers should respond with the following:
#'
#' list(payload[, content-type[, headers[, status code]]])
#'
#' payload: can be a character vector of length one or a
#'          raw vector. if the character vector is named "file" then
#'          the content of a file of that name is the payload
#'
# content-type: must be a character vector of length one
#'              or NULL (if present, else default is "text/html")
#'
#' headers: must be a character vector - the elements will
#'          have CRLF appended and neither Content-type nor
#'          Content-length may be used
#'
#' status code: must be an integer if present (default is 200)
#' @export
db.httpd = function(path, query, body, headers) {
    name = db.urlpath(path, "name")
    # seems that findVarInFrame3, which Rhttpd.c:handler_for_path
    # uses to get the custom handler, drops attributes so that it
    # just returns db.httpd without the attributes added by db.open.
    # So here we get the handler again using get, which retains
    # the attributes we use for dispatching.
    handler = get(name, envir=tools:::.httpd.handlers.env)
    db = db.get(name)
    path_head = db.urlpath(path, "head")
    path_info = db.urlpath(path, "info")
    view = if (path_head == "" || path_head == "/") {
        #attr(db.httpd, "default-page", exact=TRUE)
        attr(handler, "default-page", exact=TRUE)
    } else {
        #attr(db.httpd, sprintf("%s-page", path_head), exact=TRUE)
        attr(handler, sprintf("%s-page", path_head), exact=TRUE)
    }

    if (is.null(view))
        return (db.error_page(path))

    response = view(db, path_info, query, body, headers)

    # process response to make sure correct
    return (response)
}

#' Open the help documentation for a database
#'
#' @param db An S4 object of class "database".
#' @seealso \code{\link{db.httpd}}
#' @export
db.help = function(db) {
    if (!exists(db@file, envir=tools:::.httpd.handlers.env)) {
        message("This database does not provide help pages")
        return (invisible())
    }
    port = tools::startDynamicHelp(NA)
    path = sprintf("http://localhost:%s/custom/%s/", port, db@file)
    browseURL(URLencode(path))
}


#' Retrieve elements of a URL path
#'
#' URL paths will all have the format /custom/name/head/tail?query
#' Because the query portion of the path is parsed by R we don't
#' need to retrieve it. Note that tail may be multipart. That is,
#' in the URL /custom/foo/abc/def/hij the tail consists of def/hij.
#'
#' @param path A URL path.
#' @param what The part of the path to retrieve ('name', 'head', 'tail', or 'info').
#' @return A character string.
#' @export
db.urlpath = local({
    urlpath_rmq = function(path) {
        stop = tail(gregexpr("?", path, fixed=TRUE)[[1]], 1) - 1L
        if (stop > 0)
            path = substr(path, 1, stop)
        return (path)
    }

    urlpath_normalize = function(path) {
        path = urlpath_rmq(path)
        path = paste0(paste0("/", path), "/")
        path = gsub("//", "/", path, fixed=TRUE)
        return (path)
    }

    urlpath_name = function(path) {
        # /custom/name/
        matches = gregexpr("/", path, fixed=TRUE)[[1]]
        if (length(matches) < 3)
            return ("")
        start = matches[2] + 1
        stop = matches[3] - 1
        return (substr(path, start, stop))
    }

    urlpath_head = function(path) {
        # /custom/name/head/
        matches = gregexpr("/", path, fixed=TRUE)[[1]]
        if (length(matches) < 4)
            return ("")
        start = matches[3] + 1
        stop = matches[4] - 1
        return (substr(path, start, stop))
    }

    urlpath_tail = function(path) {
        # /custom/name/head/tail/
        matches = gregexpr("/", path, fixed=TRUE)[[1]]
        if (length(matches) < 5)
            return ("")
        start = matches[4] + 1
        stop = matches[5] - 1
        return (substr(path, start, stop))
    }

    urlpath_info = function(path) {
        hd = urlpath_head(path)
        tl = urlpath_tail(path)
        if (tl != "")
            return (paste0(hd, "/", tl))
        return (hd)
    }

    function(path, what) {
        path = urlpath_normalize(path)
        switch(what,
            info=urlpath_info,
            head=urlpath_head,
            tail=urlpath_tail,
            name=urlpath_name)(path)
    }
})
