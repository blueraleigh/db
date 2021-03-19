# db

Package db provides an interface to the SQLite library and a subset of
its API. This package can be used to perform normal database
operations but also provides additional functionality for creating
virtual tables and SQL functions that invoke R callbacks. It also 
provides a mechanism for adding documentation to databases in the
form of dynamic web pages that can communicate with the database.

# Installation

Installation requires the sqlite3 library. Edit the src/Makevars file so that
PKG_CPPFLAGS and PKG_LIBS point to the location of the sqlite3 header files
and shared library on your system. After that the package can be installed
using `R CMD build` and `R CMD INSTALL`.
