"""Dewey is a catalog for the filesystem.

Dewey is designed so that you never actually index/unindex files yourself.
Instead, the catalog is constantly updating. Change the modtime of a file, and
Dewey [re]indexes it; remove a file and Dewey unindexes it.

    open -- call this on startup to open the database
    close -- call this on shutdown to close all db connections
    Catalog -- the index manager, stored in the ZODB
    Resource -- a single file or directory, stored in the catalog
    Collection -- a collection of filesystem resources; non-persistent

Not (currently) designed to track an entire filesystem, just a partial tree.

"""
import os
import stat
import threading
import warnings
from os.path import exists, isfile

import transaction
from ZEO.ClientStorage import ClientStorage
from ZODB.FileStorage import FileStorage
from ZODB import DB
from dewey.catalog import Catalog
from dewey.collection import Collection
from dewey import indices


__all__ = ['Catalog', 'Collection', 'close', 'indices', 'open']


db = None # to set, use open()
local = threading.local() # home for each thread's persistent connection
connections = [] # keep track of connections so we can close them all cleanly


globals_ = globals()
def open(dbconn, catalog_factory=None):
    """Open the database, and possibly create a new catalog object within in.

        dbconn -- connection string for ZODB; either file:// or zeo://
        catalog_factory -- a callable that returns a Catalog object

    dbconn is required; catalog_factory is only required to auto-create a new
    catalog in the db if not there.

    """
    global globals_


    # Deal with the db connection string.
    # ===================================

    if dbconn.count('://') != 1:
        raise ValueError("bad db connection string: '%s'" % dbconn)

    conntype, loc = dbconn.split('://')
    if conntype not in ('file', 'zeo'):
        raise ValueError("unsupported db connection type: '%s'" % conntype)
    elif conntype == 'file':
        #if not loc.startswith('/'):
        #    raise ValueError("file must be specified absolutely")
        if not exists(loc):
            warnings.warn("creating new database at %s" % loc)
        elif not isfile(loc):
            raise ValueError("%s does not point to a file" % loc)
        addr = loc
    else:
        assert conntype == 'zeo' # safety net
        if ':' in loc:                                      # AF_INET
            if not loc.count(':') == 1:
                raise ValueError("malformed AF_INET address: '%s'" % loc)
            (host, port) = loc.split(':') # no default port
            if port not in range(65536):
                raise ValueError("bad port number: '%s'" % port)
            port = int(port)
            addr = (host, port)
        else:                                               # AF_UNIX
            if not loc.startswith('/'):
                raise ValueError("bad AF_UNIX address: '%s'" % loc)
            addr = loc


    # Unlock, instantiate, and expose the database.
    # =============================================

    if conntype == 'file':
        if isfile(loc + '.lock'):
            raise EnvironmentError("database is locked by another process")

    Storage = (conntype == 'file') and FileStorage or ClientStorage
    db = globals_['db'] = DB(Storage(addr))


    # Populate the database.
    # ======================

    conn = db.open()
    try:
        dbroot = conn.root()
        if 'catalog' not in dbroot:
            if catalog_factory is None:
                raise LookupError( "catalog not in the db, "
                                 + "and catalog_factory not provided"
                                  )
            dbroot['catalog'] = catalog_factory()
            transaction.commit()

        assert isinstance(dbroot['catalog'], Catalog) # safety net
    finally:
        transaction.abort()
        conn.close()


def close():
#    for conn in connections:
#        conn.transaction_manager.abort()
#        conn.close()
    db.close()


def get_catalog():
    """Return the catalog object from the database.
    """
    if db is None:
        raise EnvironmentError( "db is uninitialized; "
                              + "call dewey.open() first"
                               )
    if getattr(local, 'conn', None) is None:
        local.conn = db.open() # connect to the db, once per thread
        #connections.append(conn) -- now just calling db.close() above
    return local.conn.root()['catalog']


def start_crawling():
    get_catalog().start_crawling()


def stop_crawling():
    get_catalog().stop_crawling()
