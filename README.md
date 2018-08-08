mysql_dumper - an accelerated dumper / restore

This project was born out of the need for a high performance dump / restore utility for MySQL.
The mysqldump utility, included with the open-source mysql / mariadb, is written to
create 'insert' statements for each record, which is slow to write and slow to read. It does
have the -T option to produce tab-separated data, but it only works when run on the MySQL
server ( so won't work against RDS instances ), and produces 1 file per table, which MySQL
then has major issues loading.

Both the dumping and restoring modes are multi-processing jobs that fork and read / write
through pipes to do on-the-fly compression / decompression, minimising disk space requirements.
Note that named pipes are used for the restore process, so this will only work on *nix
operating systems.

Individual files are limited to a maximum of 1,000,000 records, as performance testing has shown
that load performance drops off significantly beyond this point. Any given table will only
ever have a single process either dumping or loading.

CSV encoding is done via Text::CSV. Note that you MUST have a recent version of this library
that supports the undef_str option. This is required for proper encoding of NULL values.
Additionally, you really should use the XS version of this library - Text::CSV_XS, or
your dumps will be a lot slower than they have to be.

