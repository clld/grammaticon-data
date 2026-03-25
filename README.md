Grammaticon-data
================

Data repository for the Grammaticon.  The general structure of the project
mimics that of a [cldfbench](https://pypi.org/project/cldfbench/).

## Usage

Convert the excel sheets to csv:

    $ python3 grammaticon.py xlsx-to-csv

Recreate the data set:

    $ python3 grammaticon.py make-csvw

Check data well-formedness:

    $ csvwvalidate csvw/csvw-metadata.json
