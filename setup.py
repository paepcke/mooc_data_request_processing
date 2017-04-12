
from setuptools import setup, find_packages
setup(
    name = "qualtrics_etl",
    version = "0.01",
    packages = find_packages(),

    # Dependencies on other packages:
    setup_requires   = ['nose>=1.1.2'],
    install_requires = ['mysql-connector==2.1.4',
                        #'MySQL-python>=1.2.5',
			           ],
    tests_require    = [],

    # Unit tests; they are initiated via 'python setup.py test'
    test_suite       = 'nose.collector', 

    package_data = {
        # If any package contains *.txt or *.rst files, include them:
     #   '': ['*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
     #   'hello': ['*.msg'],
    },

    # metadata for upload to PyPI
    author = "Andreas Paepcke",
    #author_email = "me@example.com",
    description = "Retrieve Qualtrics surveys from Qualtrics API.",
    license = "BSD",
    keywords = "openEdX, instruction",
    url = "https://github.com/paepcke/qualtrics_etl",   # project home page, if any
)
