try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


classifiers = [
    'Development Status :: 3 - Alpha'
  , 'Environment :: Console'
  , 'Intended Audience :: Developers'
  , 'License :: OSI Approved :: MIT License'
  , 'Natural Language :: English'
  , 'Operating System :: MacOS :: MacOS X'
  , 'Operating System :: Microsoft :: Windows'
  , 'Operating System :: POSIX'
  , 'Programming Language :: Python'
   ]

setup( name = 'dewey'
     , version = '0.3'
     , package_dir = {'':'src'}
     , packages = ['dewey']
     , scripts = ['bin/dewey']
     , description = 'Dewey is a fast index/search API for your filesystem.'
     , author = 'Chad Whitacre'
     , author_email = 'chad@zetaweb.com'
     , url = 'http://code.google.com/p/dewey/'
     , classifiers = classifiers
      )
