Grammaticon-data
================

Data repository for the Grammaticon.  The general structure of the project
mimicks that of a [cldfbench](https://pypi.org/project/cldfbench/).

## Usage

Convert the excel sheets to csv:

    $ python3 raw/xlsx2csv.py

Recreate the data set:

    $ python3 grammaticon_makecsv.py

Check data well-formedness:

    $ csvwvalidate csvw/csvw-metadata.json
