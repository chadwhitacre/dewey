import fnmatch
import os
import random
import stat
import sys
import time
import threading
import warnings
from os.path import exists, isdir, join

import persistent
import transaction
from BTrees.IIBTree import IITreeSet
from BTrees.IOBTree import IOBTree
from BTrees.OOBTree import OOBTree, OOTreeSet


MARKER = object()


# Let this be a lesson to you:
# ============================
#
# def path2int(path):
#     """Given a path, return a unique integer representation.
#
#     This is an optimization. The assumption is that the integer-variant BTrees
#     are enough faster to warrant using an integer to uniquely identify files
#     instead of a path string. Note that this 'rid' (resource ID) is used
#     throughout all sub-indices, not just Catalog.modtimes.
#
#     Ugh. Doesn't work. Int's aren't longs (go figure).
#
#     """
#     ints = [str(ord(c)).zfill(5) for c in path] # convert chars to digits
#     return int(''.join(ints)) # and then to an int/long


class Catalog(persistent.Persistent):
    """Represent a filesystem, indexed.

    I suppose we could optimize this by moving the indexing/unindexing work to a
    thread pool, with only crawling in the main thread. At that point we'd
    probably also want to break the crawl transaction into subtransactions.

    """

    root = None     # the path below which files should be indexed
    Resource = None # a class with which to represent filesystem resources
    indices = None  # a PersistentMapping of names to dewey...Index subclasses
    ridtimes = None # an OOBTree mapping paths to (rid, modtime) tuples
    resources = None # an IOBTree mapping rids to Resource instances
    rids = None     # an IITreeSet with all rids in it; used to merge all results
    _v_nextrid = None # used when generating new rids


    def __init__(self, root, Resource):
        """Takes a root path, and a Resource class.

        The root path may be specified absolutely or relatively. If it is
        specified relatively, we won't absolutize it, so the catalog should
        continue to work if relocated.

        """

        # Validate root.
        # ==============

        if not isinstance(root, basestring):
            raise TypeError("root is not a string: '%s'" % root)
        elif not isdir(root):
            raise ValueError("root doesn't point to a directory: '%s'" % root)
        self.root = root.rstrip(os.sep)


        # Validate Resource.
        # ==================

        if not issubclass(Resource, persistent.Persistent):
            raise TypeError("Resource doesn't subclass persistent.Persistent")
        self.Resource = Resource

        self.indices = persistent.mapping.PersistentMapping() # {name:Index}
        self.reset()


    def reset(self):
        """Empty the catalog.
        """
        for index in self.indices.values():
            index.reset()
        self.ridtimes = OOBTree() # {path:(rid,modtime)}
        self.resources = IOBTree() # {rid:Resource}
        self.rids = IITreeSet() # a list of all rids


    def start_crawling(self):
        """Start self.crawl() in its own thread.
        """
        self.STOP_CRAWLING = threading.Event()
        self.crawler = threading.Thread(target=self.crawl)
        self.crawler.setDaemon(True)
        self.crawler.start()


    def stop_crawling(self):
        """Signal to self.crawl() that it should quit.
        """
        self.STOP_CRAWLING.set()


    def crawl(self):
        """Constantly crawl the filesystem, indexing changes.
        """
        while not self.STOP_CRAWLING.isSet():
            try:
                self.crawl_once()
                transaction.commit()
            finally:
                transaction.abort()
            time.sleep(0.1)


    def crawl_once(self):
        """Add/update, then remove.
        """

        # Add/update new files.
        # =====================

        self._add_update(self.root)
        i = 0
        for dirpath, dirs, files in os.walk(self.root):
            for name in (dirs + files):
                path = join(dirpath, name)
                if self.ignore(path):
                    continue
                try:
                    self._add_update(path)
                except OSError, exc:
                    print >> sys.stderr, exc.args[0]
                if (i % 20) == 0:
                    print "committing transaction ..."
                    transaction.commit()
                i += 1


        # Remove non-existant files.
        # ==========================
        # On UNIX filesystems we could optimize this by tracking directory
        # modtimes: they change when files are deleted from the directory.
        # I'm not sure this is true on Windows though. Another possible
        # optimization would be to run this on a slightly more relaxed
        # schedule than add/update (maybe every few seconds instead of
        # constantly?)

        to_delete = OOTreeSet()
        for path in self.ridtimes:
            if not exists(path):
                print "unindexing " + path
                rid = self.ridtimes[path][0]
                for index in self.indices.values():
                    index.forget(rid)
                to_delete.insert(path)
                del self.resources[rid] # {rid:resource} (one:one)
                self.rids.remove(rid) # [rids] (seq)
        for path in to_delete:
            del self.ridtimes[path] # {path:(rid, modtime)}


    def ignore(self, path):
        """Given a path, return a boolean indicating whether to ignore it.

        You might be interested to override this in a subclass.

        """
        def match(pattern):
            return fnmatch.fnmatch(path, pattern)

        if match('*/.*'):
            return True
        elif match('*/_*'):
            return True
        else:
            return False


    def _add_update(self, path):
        """Add/update a single path.
        """

        if path not in self.ridtimes:
            print "indexing " + path
            rid = self._generate_rid()
            modtime = os.stat(path)[stat.ST_MTIME]
        else:
            rid, oldtime = self.ridtimes[path]
            modtime = os.stat(path)[stat.ST_MTIME]
            if modtime != oldtime:
                print "reindexing " + path
                pass
            else:
                return


        # Hint for using Resource
        # =======================
        # Use the property() built-in for live attributes. To keep
        # cached values, set them in __init__.

        resource_ = self.Resource(path)
        for name, index in self.indices.iteritems():
            value = getattr(resource_, name, MARKER)
            if value is MARKER:
                warnings.warn( "no corresponding attribute for "
                             + "index '%s'" % name
                              )
                continue
            index.learn(rid, value)
        self.ridtimes[path] = (rid, modtime)
        self.resources[rid] = resource_
        self.rids.insert(rid)


    def _generate_rid(self):
        """Generate an rid which is not yet taken.

        This tries to allocate sequential ids so they fall into the
        same BTree bucket, and randomizes if it stumbles upon a
        used one.

        """
        while 1:
            if self._v_nextrid is None:
                self._v_nextrid = random.randint(0, 2**31)
            rid = self._v_nextrid
            self._v_nextrid += 1
            if rid not in self.rids:
                return rid
            self._v_nextrid = None
