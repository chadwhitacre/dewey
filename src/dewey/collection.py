from BTrees.IIBTree import IISet, difference, intersection, multiunion


SORT_DIRECTIONS = dict()
SORT_DIRECTIONS['up'] = 'up'
SORT_DIRECTIONS['asc'] = 'up'
SORT_DIRECTIONS['ascending'] = 'up'
SORT_DIRECTIONS['down'] = 'down'
SORT_DIRECTIONS['desc'] = 'down'
SORT_DIRECTIONS['descending'] = 'down'


class Collection(object):
    """Represent a filtered collection of filesystem resources.

    Usage:

        >>> # set constraints
        >>> collection = Collection('isfile')
        >>> collection.NOT('path below /foo/bar')
        >>>
        >>> # then iterate over it
        >>> for resource in collection:
        ...   print resource.path


    Constraints are added with three methods:

        AND -- Exclude any resources that don't satisfy the constraint.
        NOT -- Exclude any resources that satisfy the constraint.
        OR -- Include any resources that satisfy the constraint.

        http://en.wikipedia.org/wiki/Disjunctive_normal_form

    AND and NOT bind tighter than OR. In other words, you get this:

        (foo AND bar) OR (baz NOT buz)

    Not this:

        foo AND (bar OR baz) NOT buz

    Constraints are of the form:

        <index> <search> <arg>

    <index> specifies an index previously added to the catalog. <search> and
    <arg> are optional. If given <search> must name a method of the relevant
    index; if not given, the index must be implement __call__. The meaning of
    <arg> is specific to each <search> callable.

    This object doesn't hit the database until you iterate over it.

    Optimization: fast sorts, with special indices or something?

    """

    data = None             # once loaded, an IOBTree: {rid:Resource}
    constraints = None      # a set of constraints to place on this collection
    limit = None            # the maximum number of objects to include
    sort = None   # the direction to sort (default: up/ascending)
    sort_key = None         # an attribute of Resource to sort on


    def __init__(self, constraint=None, sort=None, limit=None):
        """Construct the Collection object.
        """

        # Constraint
        # ==========
        # @@: Accept a multi-line string, as from a CLI session.

        if constraint is None:
            query = (None, None, None)
            call = (None, None)
        else:
            query = self.parse(constraint)
            call = self.validate(*query)
        self.constraints = [[(None, query, call)]]


        # Properties
        # ==========

        self.limit = limit
        self.sort = sort


    # limit
    # -----

    def __get_limit(self):
        return self.__limit

    def __set_limit(self, value):
        """Set with validation.
        """
        if value is None:
            self.__limit = None
            return # short-cut
        elif isinstance(value, basestring):
            if not value.isdigit():
                raise TypeError("int/digit expected")
            value = int(value)
        elif not isinstance(value, (int,long)):
            raise TypeError("int/digit expected")
        if value < 1:
            raise ValueError("limit less than 1: '%d'" % value)
        self.__limit = value

    def __del_limit(self):
        self.__limit = None

    limit = property(__get_limit, __set_limit, __del_limit)


    # sort
    # ----

    def __get_sort(self):
        return self.__sort

    def __set_sort(self, value):
        """Set with validation.
        """
        if value is None:
            self.__sort = None
            return # short-cut
        elif not isinstance(value, basestring):
            raise TypeError("string expected")
        import dewey # avoid circular import
        index = dewey.get_catalog().indices.get(value, None)
        if index is None:
            raise ValueError("no such index: '%s'" % value)
        sorted = getattr(index, 'sorted', None)
        if sorted is None:
            raise ValueError("index '%s' not sortable" % value)
        self.__sort = value

    def __del_sort(self):
        self.__sort = None

    sort = property(__get_sort, __set_sort, __del_sort)


    # Iterator protocol.
    # ===================
    # http://docs.python.org/lib/typeiter.html

    def __iter__(self):
        """Wrap self.data with limit awareness, yielding objects.
        """
        if self.data is None:
            self.refresh()

        import dewey # avoid circular import
        catalog = dewey.get_catalog()
        resources = catalog.resources # {rid:Resource}
        i = 0

        if self.sort is None:
            for rid in self.data:
                if i == self.limit:
                    break
                yield resources[rid]
                i += 1
        else:
            sorted = catalog.indices[self.sort].sorted
            for foo, rids in sorted.iteritems():
                for rid in rids:
                    if i == self.limit:
                        break
                    if rid in self.data:
                        yield resources[rid]
                        i += 1


    def __len__(self):
        """Wrap self.data with limit awareness.
        """
        if self.data is None:
            self.refresh()
        ndata = len(self.data)
        if self.limit and ndata > self.limit:
            return self.limit
        return ndata


    def refresh(self):
        """Load the data set from the database.

        self.constraints contains a list of lists. Within each sublist, the
        terms are either ANDed or NOTed together; the results are then ORed.

        The below could be optimized in a couple ways:

          - stop searching as soon as the result set proves empty
          - perform both levels of merge from smallest to largest

        See original ZCatalog code for implementation hints.

        """
        import dewey # avoid circular import
        all = dewey.get_catalog().rids

        if self.constraints is None:
            results = all
        else:
            results = []
            for grouping in self.constraints:
                for operation, query, (call, arg) in grouping:
                    if (operation is None) and (call is None):       # OR
                        result = all
                    elif (operation is None) and (call is not None): # OR ...
                        result = call(arg)
                    else:                                            # AND/NOT
                        assert None not in (operation, call) # safety net
                        result = operation(result, call(arg))
                if result is not None:
                    results.append(result)
            results = multiunion(results) # OR

            if results is None:
                results = IISet()

        self.data = results


    # Constraint API
    # ==============
    # AND and NOT bind tighter than OR; we handle this by adding AND and NOT
    # constraints to the last sublist, and starting a new sublist with OR.

    def AND(self, constraint):
        """Exclude any resources that don't satisfy the constraint.
        """
        query = self.parse(constraint)
        call = self.validate(*query)
        self.constraints[-1].append((intersection, query, call))

    def NOT(self, constraint):
        """Exclude any resources that satisfy the constraint.
        """
        query = self.parse(constraint)
        call = self.validate(*query)
        self.constraints[-1].append((difference, query, call))

    def OR(self, constraint=''):
        """Include any resources that satisfy the constraint.

        If the constraint is not given, the new grouping will initially include
        all resources. This is most useful for effectually starting off a new
        group with a NOT constraint.

        """
        if constraint == '':
            query = (None, None, None)
            call = (None, None)
        else:
            query = self.parse(constraint)
            call = self.validate(*query)
        self.constraints.append([(None, query, call)])


    def parse(self, constraint):
        """Given a constraint string, return (index, search, arg).
        """
        index = None # the index to search
        search = None # the type of search to perform
        arg = None # any query parameters for this search

        parts = constraint.split(None, 2)
        nparts = len(parts)
        if nparts not in (1,2,3):
            raise TypeError("bad constraint: '%s'" % constraint)
        elif nparts == 1:
            index = parts[0]
        elif nparts == 2:
            index, search = parts
        elif nparts == 3:
            index, search, arg = parts


        # Special Case
        # ============
        # I anticipate 'is' and 'in' to be common search types, but they are
        # reserved words in Python. I tried adding them to __dict__ in __init__,
        # but then the index can't be pickled ("can't pickle instancemethod
        # objects"). So this is my workaround. Let's keep an eye out for other
        # such cases, and/or another solution.

        if search == 'is':
            search = 'is_'
        elif search == 'in':
            search = 'in_'


        return index, search, arg


    def validate(self, index_name, search_name, arg):
        """Given an index, search, and arg, return search and arg.
        """
        import dewey # avoid circular import
        catalog = dewey.get_catalog()

        if index_name is None:
            return catalog.rids # everything
        if index_name not in catalog.indices:
            raise ValueError("unknown index: '%s'" % index_name)
        index = catalog.indices[index_name]

        call = getattr(index, search_name, None)
        if call is None:
            raise ValueError( "unknown search type '%s' " % search_name
                            + "for index '%s'" % index_name
                             )
        return call, arg
