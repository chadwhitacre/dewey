import code
import cmd
import os
import pprint
import sys
import traceback

try:
    import readline
except ImportError:
    pass

import dewey
import persistent
import transaction
from BTrees.IIBTree import difference, intersection


MATCH_ALL = [(None, (None, None, None), (None, None))]


class CLI(cmd.Cmd):

    catalog = None # a reference to the catalog in the ZODB
    collection = None # the current Collection
    ncollection = None # the number of elements in the current Collection
    nresources = None # the total number of resources in the catalog


    def __init__(self, *a, **kw):
        cmd.Cmd.__init__(self, *a, **kw)
        self.update()


    def update(self):
        self.catalog = dewey.get_catalog()
        self.nresources = len(self.catalog.rids)
        self.collection = None
        self.ncollection = None
        self.set_prompt()


        # Pre-compute Resource attribute listing.
        # =======================================

        fields = self.catalog.Resource.__dict__.keys()
        self.fields = [f for f in fields if not f.startswith('_')]
        self.fields.sort()
        fields = []
        for field in self.fields:
            if not field.startswith('_'):
                fields.append(' '+field)
        self.fields_listing = os.linesep.join(fields)


        # Pre-compute indices listing
        # ===========================

        indices = []
        longest = 0
        for name, index in self.catalog.indices.iteritems():
            len_name = len(name)
            longest = (len_name > longest) and len_name or longest
            indices.append((name, getattr(index, '__name__', repr(index))))
        indices.sort()
        listing = []
        for name, index_type in indices:
            listing.append(' %s  %s' % (name.ljust(longest), index_type))
        self.indices_listing = os.linesep.join(listing)
        self.indices = [i[0] for i in indices]


    def update_collection(self):
        self.collection.refresh()
        self.ncollection = len(self.collection)
        self.set_prompt()


    def set_prompt(self):
        right = str(self.nresources)
        if self.ncollection is None:
            left = ' ' * len(right)
            sep = ' '
            lbrace = '('
            rbrace = ')'
        else:
            if self.collection.limit is None:
                nleft = self.ncollection
                sep = '|'
            else:
                if self.ncollection > self.collection.limit:
                    nleft = self.collection.limit
                    sep = '\\'
                else:
                    nleft = self.ncollection
                    sep = '/'
            left = str(nleft).rjust(len(right))

            if self.collection.sort is None:
                lbrace = '('
                rbrace = ')'
            else:
                lbrace = '['
                rbrace = ']'

        self.prompt = 'dewey %s%s%s%s%s> ' % (lbrace, left, sep, right, rbrace)


    # Commands
    # ========

    def default(self, line):
        """Start a new collection."""
        if line == '':
            line = None
        try:
            self.collection = dewey.Collection(line)
        except (TypeError, ValueError), exc:
            print exc.args[0]
            return
        self.update_collection()


    def do_AND(self, line):
        """Add an AND constraint to the current collection."""
        self.do__constrain('AND ' + line)
    do_and = do_AND

    def do_NOT(self, line):
        """Add a NOT constraint to the current collection."""
        self.do__constrain('NOT ' + line)
    do_not = do_NOT

    def do_OR(self, line):
        """Add an OR constraint to the current collection."""
        self.do__constrain('OR ' + line)
    do_or = do_OR

    def do__constrain(self, line):
        """Manually add a constraint to the current collection."""

        # Parse and validate.
        # ===================

        parts = line.split(None, 1)
        if len(parts) == 1:
            if parts[0] != 'OR':
                print 'non-OR constraints require an explicit term'
                return
            parts.append('')
        elif len(parts) != 2:
            print "malformed constraint: '%s'" % line
            return

        operator_, constraint = parts
        if operator_ not in ('AND', 'NOT', 'OR'):
            print "unknown operator: '%s'" % operator_
            return


        # Add to collection.
        # ==================

        if self.collection is None:
            self.collection = dewey.Collection()
        if self.collection.constraints[-1] == MATCH_ALL:
            if parts == ['OR', '']: # redundant; skip
                self.update_collection()
                return
            elif operator_ == 'AND': # overly verbose; trim
                self.collection.constraints = self.collection.constraints[:-1]
                operator_ = 'OR'

        constrain = getattr(self.collection, operator_)
        try:
            constrain(constraint) # calls collection.parse/validate
        except (TypeError, ValueError), exc:
            print exc.args[0] # bad query
            return
        self.update_collection()


    def do_clear(self, line):
        """Clear out the collection (default) or the entire catalog."""
        if line == 'catalog':
            try:
                self.catalog.reset()
                transaction.commit()
            except:
                transaction.abort()
            self.update()
        else:
            self.collection = None
            self.ncollection = None
            self.set_prompt()


    def do_constraints(self, line):
        """Display the constraints on the current Collection."""

        # Short-cut
        # =========

        if self.collection is None:
            return


        # Long-cut
        # ========

        out = []
        w = out.append

        for i in range(len(self.collection.constraints)):
            grouping = self.collection.constraints[i]


            # First one
            # =========

            operation, constraint, call = grouping[0]
            if constraint == (None, None, None):
                line = 'OR'
            else:
                index, search, params = [str(t) for t in constraint]
                if search in ('in_', 'is_'):
                    search = search[:2]
                line = ' '.join([index, search, params]).strip()
                if i > 0:
                    line = 'OR ' + line
            w(line + os.linesep)


            # Subsequent
            # ==========

            for constraint in grouping[1:]:
                w(' ')

                operation, query, call = constraint
                if operation is intersection:
                    operation = 'AND'
                else:
                    assert operation is difference
                    operation = 'NOT'

                index, search, params = [str(t) for t in query]
                if search in ('in_', 'is_'):
                    search = search[:2]

                line = ' '.join([operation, index, search, params]).strip()

                w(line + os.linesep)


        self.stdout.write(''.join(out))


    def do_crawl(self, line):
        """Perform a catalog crawl."""
        try:
            self.catalog.crawl_once()
            transaction.commit()
        finally:
            transaction.abort()
        self.update()


    def do_fields(self, line):
        """Display the available report fields."""
        print self.fields_listing


    def do_indices(self, line):
        """Display the available indices."""
        print self.indices_listing


    def do_limit(self, arg):
        """Set the limit attribute of the current collection."""
        if self.collection is None:
            print "no collection to limit"
        elif not arg:
            print self.collection.limit
        else:
            try:
                self.collection.limit = arg
            except (TypeError, ValueError), exc:
                print exc.args[0]
            self.set_prompt()


    def do_ls(self, line):
        """Given a space-separated list of fields, print out a report."""

        if self.collection is None:
            return

        if line:
            fields = [n.strip() for n in line.split()]
        else:
            fields = self.fields

        colwidth = (78/len(fields)) - 1
        def trim(s):
            if len(s) > colwidth:
                s = s[:colwidth-1] + '~'
            return s.ljust(colwidth)

        # Headers
        # =======

        line = [' ']
        for field in fields:
            line.append(trim(field))
        print
        print ' '.join(line)
        print ' ', '=' * 77


        # Rows
        # ====

        for resource_ in self.collection:
            line = [' ']
            for field in fields:
                line.append(trim(str(getattr(resource_, field, '<n/a>'))))
            print ' '.join(line)
        print


    def do_sort(self, arg):
        """Set the sort attribute of the current collection."""
        if self.collection is None:
            print "no collection to sort"
        elif not arg:
            print self.collection.sort
        else:
            try:
                self.collection.sort = arg
            except (TypeError, ValueError), exc:
                print exc.args[0]
            self.set_prompt()


    def do_unlimit(self, ignored):
        """Unset the limit attribute on the current collection."""
        if self.collection is None:
            print "no collection to unlimit"
        else:
            del self.collection.limit
            self.set_prompt()


    def do_unsort(self, ignored):
        """Unset the sort attribute on the current collection."""
        if self.collection is None:
            print "no collection to unsort"
        else:
            del self.collection.sort
            self.set_prompt()


    # Completions
    # ===========
    # @@: We have completions for <index>; add completions for <search>, and
    # for Enumerations, <params>. Woo-hoo!

    def completenames(self, text, *foo):
        matches = cmd.Cmd.completenames(self, text, *foo)
        for name in self.catalog.indices:
            if (name not in matches) and name.startswith(text):
                matches.append(name)
        matches.sort()
        return matches


    def complete_collect(self, text, *foo):
        return [n for n in self.catalog.indices if n.startswith(text)]


    def complete_clear(self, text, *foo):
        return [s for s in ('collection', 'catalog') if s.startswith(text)]


    def complete_ls(self, text, line, begidx, endidx):
        matches = []
        for field in self.fields:
            if ' %s '%field in line:
                continue
            elif field.startswith(text):
                matches.append(field)
        return matches


    def complete_sort(self, text, line, begidx, endidx):
        matches = []
        for index in self.indices:
            if ' %s '%index in line:
                continue
            elif index.startswith(text):
                matches.append(index)
        return matches


    # Misc
    # ====

    def emptyline(self):
        pass

    def do_EOF(self, inStr=''):
        print >> self.stdout
        return self.do_exit()
    def do_exit(self, *foo):
        return True
    do_q = do_quit = do_exit

    def do__constraints(self, line):
        """Pretty-print the raw constraints structure."""
        if self.collection is None:
            return
        pprint.pprint(self.collection.constraints)


def main(catalog_factory=None, argv=None):
    """
    """

    if argv is None:
        argv = sys.argv


    # Parse the db connection string.
    # ===============================

    arg = argv[1:2]
    if not arg:
        print >> sys.stderr, "Please specify a db connection string"
        print >> sys.stderr, "e.g. file://~/foo.dat or zeo://localhost:9100"
        raise SystemExit()
    dbconn = arg[0]
    if dbconn in '.'+os.sep or not dbconn.startswith('zeo://'):
        dbconn = 'file://' + dbconn


    # Run a command or enter interactive mode.
    # ========================================

    dewey.open(dbconn, catalog_factory)
    cli = CLI()
    command = ' '.join(argv[2:])
    try:
        if command == 'crawl':
            dewey.get_catalog().crawl()
        else:
            try:
                cli.cmdloop()
            except KeyboardInterrupt:
                cli.onecmd('EOF')
    finally:
        dewey.close()
