#' Respond to HTTP requests against a database
#'
#' Uses R's built in http server in src/modules/Rhttp.c
#'
#' @param path Request URL path.
#' @param reqquery Request query parameters.
#' @param reqbody Request body.
#' @param reqheaders Request headers.
#' @details
#' The URL \code{path} will always have a form that looks like
#' /custom/<name>/<head>/<tail>, where <name> is the name of the
#' database. Each of these parts can be extracted from the path
#' using the function \code{\link{db.urlpath}}. Note that <tail>
#' may consist of multiple parts. \code{db.httpd} will use the
#' path <head> to dispatch to a function of that name, which is
#' expected to generate the http response. This behavior follows closely
#' the Wapp framework (wpp.tcl.tk). The function will receive
#' the database connection, a named list of request data, and a response
#' object that it is expected to fill out using the \code{db.reply*}
#' family of functions.
#' @examples
#' \dontrun{
#'
#' index = function(db, req, resp) {
#'     db.reply(resp, db.subst(db,
#'     '
#'        <html>
#'        <body>
#'        <h1>Welcome!</h1>
#'        <ul>
#'        <li><a href="{req$PATH_ROOT}/page1">page 1</a></li>
#'        <li><a href="{req$PATH_ROOT}/page2">page 2</a></li>
#'        <ul>
#'        </body>
#'        </html>
#'    '))
#' }
#' page1 = function(db, req, resp) {
#'     db.reply(resp, db.subst(db, '
#'         <html>
#'         <body>
#'         <a href="{req$PATH_ROOT}">hello, world!</a>
#'         </body>
#'         </html>
#'     '))
#' }
#' page2 = function(db, req, resp) {
#'     db.reply(resp, db.subst(db, '
#'         <html>
#'         <body>
#'         <a href="{req$PATH_ROOT}">take me home, please.</a>
#'         </body>
#'         </html>
#'     '))
#' }
#'
#' db = db.open("db.sqlite",
#'     views=list(default=index, page1=page1, page2=page2))
#'
#' db.ui(db)
#'
#' db.close(db)
#' unlink("db.sqlite")
#'
#' }
#' @export
db.httpd = function(path, reqquery, reqbody, reqheaders) {
    # db.open will attach handler attributes to this function and
    # assign it under PATH_NAME in the tools:::.httpd.handlers.env
    # environment. it then gets evaluated in the tools namespace.

    # this bit modified from opencpu::rhttpd_handler
    METHOD = grep("Request-Method:"
        , strsplit(rawToChar(reqheaders), "\n")[[1]]
        , ignore.case=TRUE, value=TRUE);
    METHOD = sub("Request-Method: ?", "", METHOD, ignore.case=TRUE)
    if(!length(METHOD))
        METHOD = ifelse(is.null(reqbody), "GET", "POST")

    if (!METHOD %in% c("GET", "POST"))
        return (
            list("Requested method not supported", "text/plain", NULL, 405L))
    
    # e.g. /custom/specimen.db/specimen/images/1234
    PATH_ROOT = db.urlpath(path, "root") # => /custom/specimen.db
    PATH_NAME = db.urlpath(path, "name") # => specimen.db
    PATH_INFO = db.urlpath(path, "info") # => specimen/images/1234
    PATH_HEAD = db.urlpath(path, "head") # => specimen
    PATH_TAIL = db.urlpath(path, "tail") # => images/1234
    CTYPE = grep("Content-Type:"
        , strsplit(rawToChar(reqheaders), "\n")[[1]]
        , ignore.case=TRUE, value=TRUE)

    REQDATA = list(
        METHOD = METHOD,
        PATH_ROOT = PATH_ROOT,
        PATH_HEAD = PATH_HEAD,
        PATH_TAIL = PATH_TAIL,
        PATH_INFO = PATH_INFO,
        QUERY = reqquery,
        BODY = reqbody,
        CTYPE = CTYPE
    )
    # end ocpu modification

    db = db.get(PATH_NAME)

    # give each http request its own database connection?
    #.db = db.open(db@file, register=FALSE)
    #on.exit(db.close(.db))

    # these are attached by db.open
    handlers = attributes(sys.function())

    # the default method just returns REQDATA unmodified
    REQDATA = try(handlers[[".beforeDispatch"]](db, REQDATA), TRUE)

    if (inherits(REQDATA, "try-error"))
        return (list(REQDATA, "text/plain", NULL, 500L))

    view = if (REQDATA[["PATH_HEAD"]] == "") {
        handlers[["default"]]
    } else {
        handlers[[REQDATA[["PATH_HEAD"]]]]
    }

    if (is.null(view))
        return (list("Requested resource not found", "text/plain", NULL, 404L))

    db.response_start(RESPONSE)
    if (inherits(e <- try(view(db, REQDATA, RESPONSE), TRUE), "try-error")) {
        db.response_finish(RESPONSE)
        return (list(e, "text/plain", NULL, 500L))
    }

    # the default leaves response unmodified
    if (inherits(
            e <- try(handlers[[".beforeReply"]](db, REQDATA, RESPONSE), TRUE)
            , "try-error")
    ) {
        db.response_finish(RESPONSE)
        return (list(e, "text/plain", NULL, 500L))
    }

    response = db.response_finish(RESPONSE)

    return (response)
}

attr(db.httpd, ".beforeDispatch") = function(db, req) {
    return (req)
}

attr(db.httpd, ".beforeReply") = function(db, req, resp) {

}

delayedAssign("RESPONSE", db.response_init())

#' Open the help documentation for a database
#'
#' @aliases db.ui
#' @param db An S4 object of class "database".
#' @seealso \code{\link{db.httpd}}
#' @export
db.help = function(db) {
    if (!exists(db@name, envir=tools:::.httpd.handlers.env)) {
        message("This database does not provide help pages")
        return (invisible())
    }
    port = tools::startDynamicHelp(NA)
    path = sprintf("http://127.0.0.1:%s/custom/%s/", port, db@name)
    browseURL(URLencode(path))
}

#' @export
db.ui = db.help


#' Retrieve elements of a URL path
#'
#' URL paths will all have the format /custom/<name>/<head>/<tail>
#' Note that tail may be multipart. That is, in the URL
#' /custom/foo/abc/def/hij the <name> = foo, <head> = abc and
#' <tail> = def/hij.
#'
#' @param path A URL path.
#' @param what The part of the path to retrieve ('name', 'head', 'tail', or 'info').
#' @return A character string.
#' @export
db.urlpath = local({

    re = "(?:^|(?<=/))([a-zA-Z0-9 \\._%~-]+)(?=(?:[/?]|$))"

    urlpath_root = function(path) {
        matches = regmatches(path, gregexpr(re, path, perl=TRUE))[[1]]
        return (paste0("/", paste0(matches[1:2], collapse="/")))
    }

    urlpath_name = function(path) {
        # /custom/name/
        matches = regmatches(path, gregexpr(re, path, perl=TRUE))[[1]]
        return (matches[2])
    }

    urlpath_head = function(path) {
        # /custom/name/head/
        matches = regmatches(path, gregexpr(re, path, perl=TRUE))[[1]]
        if (length(matches) < 3)
            return ("")
        return (matches[3])
    }

    urlpath_tail = function(path) {
        # /custom/name/head/tail/tail/
        matches = regmatches(path, gregexpr(re, path, perl=TRUE))[[1]]
        return (paste0(matches[-(1:3)], collapse="/"))
    }

    urlpath_info = function(path) {
        matches = regmatches(path, gregexpr(re, path, perl=TRUE))[[1]]
        return (paste0(matches[-(1:2)], collapse="/"))
    }

    function(path, what) {
        switch(what,
            root=urlpath_root,
            info=urlpath_info,
            head=urlpath_head,
            tail=urlpath_tail,
            name=urlpath_name)(path)
    }
})


db.response_init = function() {
    resp = db.open()
    db.exec(resp, "
        BEGIN;
        CREATE TABLE headers(tag TEXT, value TEXT);
        CREATE TABLE content_type(ct TEXT);
        CREATE TABLE status_code(code INTEGER);
        CREATE TABLE reply(content BLOB);
        INSERT INTO reply VALUES ('');
        INSERT INTO content_type VALUES ('text/html');
        INSERT INTO status_code VALUES (200);
        COMMIT;
    ")
    reg.finalizer(
            resp@handle
            , function(handle) .Call(db_close, handle)
            , onexit=TRUE)
    return (resp)
}


db.response_start = function(resp) {
    db.exec(resp, "BEGIN")
}


db.response_finish = function(resp) {
    db.exec(resp, "COMMIT")
    reply = db.eval(resp, "SELECT content FROM reply")[[1]]
    headers = do.call(
        c
        , db.lapply(
            resp
            , "SELECT * FROM headers"
            , FUN=function(l) {
                paste(l$tag, l$value, sep=": ")
            }
        )
    )
    ct = db.eval(resp, "SELECT ct FROM content_type")[[1]]
    code = db.eval(resp, "SELECT code FROM status_code")[[1]]
    db.exec(resp, "
        BEGIN;
        UPDATE reply SET content = '';
        UPDATE content_type SET ct = 'text/html';
        UPDATE status_code SET code = 200;
        DELETE FROM headers;
        COMMIT;
    ")
    # we can pass NULL for ct and headers and Rhttpd.c:process_request_ will
    # know to use its default values but we have to pass an integer
    # status code so if it's not set we cannot return NULL
    if (!is.null(code))
        return (list(reply, ct, headers, code))
    else
        return (list(reply, ct, headers))
}

#' Add a header to the HTTP response
#' @export
db.reply_header = function(resp, tag, value) {
    db.eval(
        resp
        , "INSERT INTO headers VALUES(?,?)"
        , list(as.character(tag), as.character(value)))
}

#' Set the status code of the HTTP response
#' @export
db.reply_status = function(resp, code) {
    db.eval(
        resp
        , "UPDATE status_code SET code = ?"
        , as.integer(code))
}

#' Set the MIME type of the HTTP response
#' @export
db.reply_type = function(resp, type) {
    db.eval(
        resp
        , "UPDATE content_type SET ct = ?"
        , as.character(type))
}

#' Redirect to the given location
#' @export
db.reply_redirect = function(resp, location) {
    db.reply_status(resp, 307L)
    db.reply_header(resp, "Location", as.character(location))
}

#' Add text to the HTTP response
#'
#' @param resp The HTTP response being formed
#' @param reply Text to append to the HTTP response
#' @export
db.reply = function(resp, text) {
    db.eval(
        resp
        , "UPDATE reply SET content = content || ? || '\n'"
        , text)
}

#' Make the contents of a file the HTTP response
#'
#' @param resp The HTTP response being formed
#' @param filename Name of the file whose content forms the response
#' @export
db.reply_file = function(resp, filename) {
    db.eval(
        resp
        , 'UPDATE reply SET content = ?'
        , readBin(filename, "raw", file.size(filename)))
}


#' Return a raw vector as the HTTP response
#'
#' @param resp The HTTP response being formed
#' @param raw Binary data that forms the response
#' @export
db.reply_raw = function(resp, raw) {
    db.eval(
        resp
        , 'UPDATE reply SET content = ?'
        , raw)
}
