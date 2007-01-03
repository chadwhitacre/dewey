import os
import sys
from os.path import isdir

import persistent
from BTrees.IIBTree import IIBTree, IISet, IITreeSet
from BTrees.IIBTree import difference, intersection, multiunion
from BTrees.IOBTree import IOBTree
from BTrees.OIBTree import OIBTree
from BTrees.OOBTree import OOBTree, OOSet


class Index(persistent.Persistent):
    """Base class for all dewey indices.
    """

    __name__ = 'Index' # used in command-line interface

    def reset(self):
        """Forget everything; usually called from __init__.
        """

    def learn(self, rid, value):
        """Given an rid and a value, associate them.
        """
        raise NotImplementedError

    def forget(self, rid):
        """Given an rid, remove it from all indices.
        """
        raise NotImplementedError


class String(Index):
    """An index for short strings.

    Supported searches: is, startswith, endswith, contains, in. Can be
    case-insensitive.

    """

    __name__ = 'String'

    values = None   # IOBTree mapping rid to value (one:one)
    rids = None     # OOBTree mapping value to rids (one:many)
    beginnings = None # OOBTree mapping initial parts to rids (one:many)
    endings = None  # OOBTree mapping trailing parts to rids (one:many)
    middles = None  # OOBTree mapping internal parts to rids (one:many)
    case_sensitive = False

    sorted = None   # OOBTree per Collection's sorting contract


    def __init__(self, case_sensitive=False):
        if case_sensitive not in (False, True, 0, 1):
            raise TypeError( "case_sensitive isn't a boolean: "
                           + "'%s'" % case_sensitive
                            )
        self.case_sensitive = bool(case_sensitive)
        self.reset()


    # Index contract
    # ==============

    def reset(self):
        # Yes this nomenclature is different from, e.g., Enumeration. Sorry.
        self.rids = IOBTree()       # {rid:value}
        self.values = OOBTree()     # {value:rids}
        self.beginnings = OOBTree() # {beginnings:rids}
        self.endings = OOBTree()    # {ends:rids}
        self.middles = OOBTree()    # {middles:rids}

        self.sorted = self.values   # Collection sorting contract


    def learn(self, rid, value):

        # Validate
        # ========

        if not isinstance(value, basestring):
            raise TypeError("value is not a string: '%s'" % value)
        if not self.case_sensitive:
            value = value.lower()


        # Add to the (one:many) mapping, value to rids.
        # =============================================

        if value in self.values:
            self.values[value].insert(rid)
        else:
            self.values[value] = IITreeSet([rid])


        # Add to the (one:many) beginnings, middles, and endings indices.
        # ==================================================================
        # These are for the startswith, contains, and endswith searches, which
        # function basically like the is search, but we have to learn all
        # substrings of the string.

        substrings = OOSet()

        for i in range(len(value)):

            j = i + 1


            # beginnings/startswith
            # =====================

            part = value[:j]
            if part in self.beginnings:
                self.beginnings[part].insert(rid)
            else:
                self.beginnings[part] = IITreeSet([rid])


            # middles/contains
            # ================

            for k in range(len(value)-i):
                part = value[k:k+j]
                if part in self.middles:
                    self.middles[part].insert(rid)
                else:
                    self.middles[part] = IITreeSet([rid])
                substrings.insert(part)


            # endings/endswith
            # ================

            part = value[i:]
            if part in self.endings:
                self.endings[part].insert(rid)
            else:
                self.endings[part] = IITreeSet([rid])


        # Add to the (one:one) mapping of rid to value.
        # =============================================
        # This exists so we know where the rid is located when the time comes
        # to forget it.

        self.rids[rid] = substrings


    def forget(self, rid):

        # Remove from the (one:many) mappings of substrings to rid.
        # =========================================================
        # This bears optimizing.

        indices = (self.values, self.beginnings, self.middles, self.endings)
        for substring in self.rids[rid]: # an OOSet()
            for index in indices:
                index[substring].remove(rid)
                if len(self.substrings[substring]) == 0:
                    del self.substrings[substring]


        # Remove from the (one:one) mapping of rid to value.
        # ==================================================

        del self.rids[rid]


    # Searches
    # ========

    def is_(self, arg): # is
        return self._substring(self.values, arg)

    def startswith(self, arg):
        return self._substring(self.beginnings, arg)

    def contains(self, arg):
        return self._substring(self.middles, arg)

    def endswith(self, arg):
        return self._substring(self.endings, arg)

    def _substring(self, index, arg):
        if not self.case_sensitive:
            arg = arg.lower()
        if not isinstance(arg, basestring):
            raise TypeError("arg is not a string: '%s'" % arg)
        return index.get(arg, IISet())


    def in_(self, arg): # in
        """Given a sequence, return the union of rids for each.

        The argument a string of comma seperated tokens. It is split on the
        commas and the terms are whitespace-stripped to form a sequence of
        strings.

        """

        # Parse and validate.
        # ===================

        if not isinstance(arg, basestring):
            raise TypeError("arg is not a string: '%s'" % arg)
        elif not arg:
            raise ValueError("no arg given")
        elif ',' not in arg:
            raise ValueError("malformed arg [no comma]: '%s'" % arg)
        elif not self.case_sensitive:
            arg = arg.lower()


        # Build.
        # ======

        results= []
        for value in [v.strip() for v in arg.split(',')]:
            result = self.values.get(value, IISet())
            results.append((len(result), result))
        results.sort() # optimization; merge smallest to largest
        return multiunion([r[1] for r in results])


ENUMERATION_NO_DEFAULT = object()


class Enumeration(Index):

    __name__ = 'Enumeration'

    allowed = None  # list of allowed values
    default = None  # default value
    values = None   # IOBTree mapping rid to value (one:one)
    rids = None     # OIBTree mapping value to rids (one:many)
    case_sensitive = True

    sorted = None   # OOBTree per Collection's sorting contract


    def __init__(self, *allowed, **kw):
        default = kw.get('default', ENUMERATION_NO_DEFAULT)
        if default is not ENUMERATION_NO_DEFAULT:
            if default not in allowed:
                raise ValueError( "default '%s' not in " % default
                                + "values: %s" % values
                                 )

        self.allowed = allowed
        self.default = default

        self.reset()


    # Index contract
    # ==============

    def reset(self):
        self.values = IOBTree() # {rid:value}
        self.rids = OOBTree() # {value:rids}

        self.sorted = self.rids


    def learn(self, rid, value):

        # Validate
        # ========

        bad_value = ValueError("bad value: '%s'" % value)
        if value is None:
            if self.default is MARKER:
                raise bad_value
            value = self.default
        elif value not in self.allowed:
            raise bad_value


        # Add to the (one:many) mapping, value to rids.
        # =============================================

        if value in self.rids:
            self.rids[value].insert(rid)
        else:
            self.rids[value] = IITreeSet([rid])


        # Add to the (one:one) mapping of rid to value.
        # =============================================
        # This exists so we know which value to forget when the time comes.

        self.values[rid] = value


    def forget(self, rid, value):

        # Remove from the (one:many) mapping of value to rids.
        # ====================================================

        value = self.values[rid]
        self.rids[value].remove(rid)
        if len(self.rids[value]) == 0:
            del self.rids[value]


        # Remove from the (one:one) mapping of rid to value.
        # ==================================================

        del self.values[rid]


    # Searches
    # ========

    def is_(self, value): # is
        return self.rids.get(value, IISet())


    def in_(self, arg): # in
        """Given a sequence, return the union of rids for each.

        If the argument starts with a [ or (, it is evaled as a list or tuple.
        Otherwise, it is split on comma and stripped to form a sequence of
        strings.

        """

        # Parse and validate.
        # ===================

        if not isinstance(arg, basestring):
            raise TypeError("arg is not a string: '%s'" % arg)
        elif not arg:
            raise ValueError("no arg given")
        elif ',' not in arg:
            raise ValueError("malformed arg [no comma]: '%s'" % arg)

        if arg[0] in '[(':
            values = eval(arg)
            if not isinstance(values, (list, tuple)):
                raise TypeError("arg didn't define list or tuple")
        else:
            values = [v.strip() for v in arg.split(',')]


        # Build.
        # ======

        results= []
        for value in values:
            result = self.rids.get(value, IISet())
            results.append((len(result), result))
        results.sort() # optimization; merge smallest to largest
        return multiunion([r[1] for r in results])


class Path(String):

    root = None     # root as passed to Catalog()
    path2rid = None # OIBTree mapping path to rid (one:one)
    rid2path = None # IOBTree mapping rid to path (one:one)
    parts = None    # OOBTree mapping (level, part) to rids (one:many)
    levels = None   # IOBTree mapping level to a list of rids (one:many)
    case_sensitive = None

    sorted = None   # OOBTree for sorting; inherited from Path


    def __init__(self, root, case_sensitive=None):

        # Root
        # ====

        if not isinstance(root, basestring):
            raise TypeError("root is not a string: '%s'" % root)
        elif not isdir(root):
            raise ValueError("root doesn't point to a directory: '%s'" % root)
        self.root = root.rstrip(os.sep)


        # Case Sensitivity
        # ================

        if case_sensitive is None:
            if 'win' in sys.platform:
                case_sensitive = False
            else:
                case_sensitive = True
        if case_sensitive not in (False, True, 0, 1):
            raise TypeError( "case_sensitive isn't a boolean: "
                           + "'%s'" % case_sensitive
                            )
        self.case_sensitive = bool(case_sensitive)

        self.reset()


    # Index contract
    # ==============

    __name__ = 'Path' # used in command-line interface


    def reset(self):
        """Forget everything; usually called from __init__.
        """
        String.reset(self)

        self.path2rid = OIBTree()   # {path:rid}
        self.rid2path = IOBTree()   # {rid:path}
        self.parts = OOBTree()      # {(level,part):rids}
        self.rids = IOBTree()       # {rid:(level,part)s}
        self.levels = IOBTree()     # {level:rids}


    def learn(self, rid, value):
        """Given an rid and a value, associate them.
        """
        String.learn(self, rid, value)


        # Parse and validate.
        # ===================
        # Value is an absolute path, rooted in self.root.

        if not isinstance(value, basestring):
            raise TypeError("string expected")
        elif value and not value.startswith(os.sep):
            raise ValueError("path not specified absolutely: '%s'" % value)
        if self.case_sensitive:
            path = value
        else:
            path = value.lower()
        path = path.rstrip(os.sep) # safety net; should never need this
        parts = value.split(os.sep)
        #parts = value.split(os.sep)[1:]


        # Add to simple identity indices.
        # ===============================

        self.path2rid[path] = rid
        self.rid2path[rid] = path


        # Add to complex level/part indices.
        # ==================================

        for level in range(len(parts)):
            token_ = (level, parts[level])


            # Add to (one:many) mapping of (level,part) to [rids].
            # ====================================================

            if token_ not in self.parts:
                self.parts[token_] = IITreeSet([rid])
            else:
                self.parts[token_].insert(rid)


            # Add to the (one:many) mapping of rid to (level,part)s.
            # ======================================================
            # This exists so we know how to forget about this rid when the time
            # comes.

            if rid not in self.rids:
                self.rids[rid] = OOSet([token_])
            else:
                self.rids[rid].insert(token_)


        # Add to (one:many) mapping of levels to rids.
        # ============================================
        # This is used to implement level limits.

        if level not in self.levels:
            self.levels[level] = IITreeSet([rid])
        else:
            self.levels[level].insert(rid)


    def forget(self, rid):
        """Given an rid, remove it from all indices.
        """
        String.forget(self, rid)


        # Remove from the (one:many) mapping of (level, part) to rids.
        # ============================================================
        # We also track the level here and remove the rid from the (one:many)
        # mapping of levels to rids.

        level = -1
        for token_ in self.rids[rid]:
            if token_[0] > level:
                level = token_[0]
            self.parts[token_].remove(rid)
            if len(self.parts[token_]) == 0:
                del self.parts[token_]
        self.levels[level].remove(rid)
        if len(self.levels[level]) == 0:
            del self.levels[level]


        # Remove from the (one:many) mapping of rid to tokens.
        # ====================================================

        del self.rids[rid]


        # Remove from simple identity indices.
        # ====================================
        path = self.rid2path[rid]
        del self.path2rid[path]
        del self.rid2path[rid]


    # Searches
    # ========

    def above(self, arg):
        """Find all resources at or above path, within the limits given.

        Here we actually call below() on <path> and all of its ancestors,
        passing the limits straight through, with the exception that limits
        default to 0:1 rather than None:None. Use '0:' for the latter.

        """

        # Parse and validate.
        # ===================

        path, upper, lower = self._path_and_limits(arg)
        rid = self.path2rid.get(path, None)
        if rid is None:
            return


        # Build
        # =====

        tmpl = "%s "
        if (upper, lower) == (None, None):
            tmpl += '0:1' # default: breadcrumbs
        else:
            if upper is not None:
                tmpl += str(upper)
            tmpl += ":"
            if lower is not None:
                tmpl += str(lower)

        parts = path.split(os.sep)
        rids = []
        for level in range(len(parts)):
            ancestor = os.sep.join(parts[:level+1])
            ancestor = ancestor and ancestor or '/'
            rids.append(self.below(tmpl % ancestor))
        rids = multiunion(rids)


    def below(self, arg):
        """Find all resources at or below path, within the limits given.
        """

        # Parse and validate.
        # ===================

        path, upper, lower = self._path_and_limits(arg)
        rid = self.path2rid.get(path, None)
        if rid is None:
            return


        # Build
        # =====

        parts = path.split(os.sep)
        rids = None
        for level in range(len(parts)):
            rids = intersection(rids, self.parts[(level, parts[level])])
        if rids is None:
            return IISet() # short-cut


        # Limits
        # ======
        # Remove rids that are above any upper limit, and then only include rids
        # that are above any lower limit. Limits are relative to the level of
        # the requested path.

        if upper is not None:
            upper += level
            for i in range(level, upper):
                if i not in self.levels:
                    break
                rids = difference(rids, self.levels[i])
        if lower is not None:
            lower += level
            _rids = []
            for i in range(level, lower):
                if i not in self.levels:
                    break
                _rids.append(self.levels[i])
            rids = intersection(rids, multiunion(_rids))

        return rids


    def is_(self, arg):
        """Return the rid corresponding to a single path. Root is special-cased.
        """
        path, foo, bar = self._path_and_limits(arg)
        return self.path2rid.get(arg, None)


    # Parser
    # ======

    def _path_and_limits(self, arg):
        """Given an argument from a Collection constraint, return three params.

        Arg is of the form:

           /some/path 0:4

        The first token is the path, the second is a limits specification. The
        path must not contain a space (@@: really should support that). The
        limits spec is optional; if given, it must have a colon and at least one
        end specified. To the left of the colon is the upper bound; to the right
        is the lower bound. These bounds specify the tree levels that the path
        filter should apply to, but the specifics of how it applies depend on
        the searches above.

        (Yes this nomenclature is all wacky. The root is conceptually 'higher'
        for some reason, even though the root is 0 and a real tree's roots are
        lower than its branches. Go figure.)

        """

        path = ''
        upper = None
        lower = None

        parts = arg.split()
        nparts = len(parts)
        assert nparts in (1, 2), "either need path or path and limits"


        # Path
        # ====

        if nparts == 1:
            path = parts[0]
        elif nparts == 2:
            path = parts[0]


            # Limits
            # ======

            limits = parts[1]
            if not limits.count(':') == 1:
                raise ValueError("malformed limits (no colon): '%s'" % limits)
            upper, lower = limits.split(':')
            #if not (upper + lower):
            #    raise ValueError("no limits given: '%s'" % limits)

            if not upper:
                upper = None
            else:
                if not upper.isdigit():
                    raise ValueError("bad upper limit: '%s'" % upper)
                upper = int(upper)

            if not lower:
                lower = None
            else:
                if not lower.isdigit():
                    raise ValueError("bad lower limit: '%s'" % lower)
                lower = int(lower)

            if None not in (upper, lower):
                if upper > lower:
                    raise ValueError( "upper limit greater than lower: "
                                    + "%d > %d" % (upper, lower)
                                     )

        if path == os.sep:
            path = ''
        if not self.case_sensitive:
            path = path.lower()
        return path, upper, lower
