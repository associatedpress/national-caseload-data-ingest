# National Caseload Data #

Ingest script to take the Department of Justice's
[National Caseload Data][ncd], which covers cases handled by U.S. Attorneys,
and load it into [Athena][] for querying.

[Athena]: https://aws.amazon.com/athena/
[ncd]: https://www.justice.gov/usao/resources/foia-library/national-caseload-data

## The files ##

The DOJ website provides data dumps of the entire database as of the end of
each month and as of the end of each fiscal year (ends Sept. 30). DOJ only
retains the three most recent monthly dumps, and there's usually a lag of a
month or so.

Each dump is cumulative, so the November 2017 dump includes all of the cases
from the October 2017 dump, plus whatever was added in November.

Each dump is split into a number of zip files (24 as of the December 2017
release). Each zip file has one of three structures:

*   **Normal tables:**

    Most of the NCD database tables are stored as fixed-width text files, with
    their schemas described in the zip file's `README.TXT`.

    For example, the `GS_COURT_HIST` table's contents are in a file called
    `gs_court_hist.txt` within one of the zip files, and that zip file's
    `README.TXT` will contain the schema for that table (and others contained
    in the zip).

    If a table is particularly large, its contents will be split into several
    text files--one for each [district][]--and distributed among several zip
    files.

    For example, the `GS_PARTICIPANT` table's contents are split into files
    such as `gs_participant_FLM.txt` for the Middle District of Florida and
    `gs_participant_CT` for the District of Connecticut, and these might live
    in separate zip files, each with the `GS_PARTICIPANT` schema in its
    `README.TXT`.

*   **Codebooks:**

    The last file in a given dump will contain codebooks that can be useful in
    interpreting the contents of the normal tables. These come in two forms,
    which this code describes as:

    *   **Lookup tables:**

        A lookup table consists of one fixed-width text file containing some
        metadata followed by a row of column headers and a separator row of
        hyphens (useful for determining column widths).

        These filenames start with `table_`; for example, the `GS_POSITION`
        table is in a file called `table_gs_position.txt`.

    *   **Global tables:**

        One file called `global_LIONS.txt` contains several distinct
        fixed-width tables all stacked on top of one another.

        This is as painful as it sounds.

*   **Documentation:**

    The second-to-last file in a given dump usually has no specific data in it;
    it just contains logs, statistics files and other semi-documentation.

[district]: https://en.wikipedia.org/wiki/United_States_federal_judicial_district

## Ingest architecture ##

There are two scripts in the root of this repo:

*   `import_zip.py`, which takes one already-downloaded NCD component zip file,
    converts it to [gzipped][athena-compression] [JSON][athena-json] for
    Athena, uploads it to S3 and creates the appropriate Athena tables.

*   `import_all.py` is an experimental script that takes a URL to a dump's
    landing page ([such as this one][dump_fy_2017]) and asynchronously
    processes the zip files listed there. (This is meant to make it easier to
    invoke automatically and to allow one file to be processed while another
    downloads. Still working out some of the kinks there, though.)

[athena-compression]: https://docs.aws.amazon.com/athena/latest/ug/compression-formats.html
[athena-json]: https://docs.aws.amazon.com/athena/latest/ug/json.html
[dump_fy_2017]: https://www.justice.gov/usao/resources/foia-library/national-caseload-data/2017
