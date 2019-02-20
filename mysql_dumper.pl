#!/usr/bin/perl

use strict;
use warnings;

use v5.10;

use DBI;
use Getopt::Long;
use File::Basename;
use File::Find::Rule;
use File::Path qw | make_path rmtree |;
use POSIX;
use Text::CSV;
use JSON;
use IPC::Open3;
use IO::Select;
use Time::HiRes;

use constant MAX_ERRORS_PER_TABLE     => 5;

# @formatter:off

my ( $host
   , $port
   , $username
   , $password
   , $database
   , $action
   , $jobs
   , $directory
   , $sample
   , $file
   , $check_count
   , $fallback_tables_string
   , $tables_string
   , $page_size
   , $skip_create_db
   , $accel_keys
   , $post_schema_command
);

my ( $GLOBAL_TABLE , $ALL_FILES , $ALL_KEY_FILES , $LOGFILE ); # yeah, bite me

GetOptions(
    'host=s'              => \$host
  , 'port=i'              => \$port
  , 'username=s'          => \$username
  , 'password=s'          => \$password
  , 'database=s'          => \$database
  , 'action=s'            => \$action
  , 'jobs=i'              => \$jobs
  , 'directory=s'         => \$directory
  , 'sample=i'            => \$sample
  , 'file=s'              => \$file
  , 'check-count=i'       => \$check_count
  , 'fallback-tables=s'   => \$fallback_tables_string
  , 'tables-string=s'     => \$tables_string
  , 'page-size=s'         => \$page_size
  , 'accel-keys=i'        => \$accel_keys
  , 'skip-create-db'      => \$skip_create_db
  , 'post-schema-command' => \$post_schema_command
);

sub pretty_timestamp {

    # This function returns the current time as a human-readable timestamp
    #  ... without having to install further date/time manipulation libraries

    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime( time );

    #               print mask ... see sprintf
    return sprintf( "%04d%02d%02d_%02d%02d%02d", $year + 1900, $mon + 1, $mday, $hour, $min, $sec );

}

sub logline {

    my $msg = shift;

    my $line = pretty_timestamp() . " " . $msg;
    say $line;
    say $LOGFILE $line;

}

# Check args

if ( ! $directory ) {
    warn( "No --directory passed. Defaulting to /tmp" );
    $directory = "/tmp/";
}

open $LOGFILE, ">" . $directory . "/refresh.log"
    or die( "Failed to open log file!\n" . $! );

if ( ! $host ) {
    warn( "No --host passed. Defaulting to localhost" );
    $host = "localhost";
}

if ( ! $port ) {
    logline( "No --port passed. Defaulting to 3306" );
    $port = 3306;
}

if ( ! $username ) {
    logline( "You need to pass a --username" );
    die( "You need to pass a --username" );
}

if ( ! $database ) {
    logline( "You need to pass a --database" );
    die( "You need to pass a --database" );
}

if ( ! $action || ( $action ne "dump" && $action ne "restore" ) ) {
    logline( "You need to pass an --action ... and it needs to be either 'dump' or 'restore'" );
    die( "You need to pass an --action ... and it needs to be either 'dump' or 'restore'" );
} elsif ( $action eq 'restore' && ! $file ) {
    logline( "For '--action restore' mode, you need to pass a '--file' arg" );
    die( "For '--action restore' mode, you need to pass a '--file' arg" );
}

if ( ! $jobs ) {
    logline( "No --jobs passed. Defaulting to 4" );
    $jobs = 4;
}

if ( ! $page_size ) {
    logline( "No --page-size passed. Defaulting to 1000" );
    $page_size = 1000;
}

my $db_directory = $directory . "/" . $database . "/";

if ( $password ) {
    # If the password has been passed in on the command line, set the env variable
    $ENV{'MYSQL_PWD'} = $password;
} else {
    # Alternatively, assume it's already in the env variable, and copy it
    $password = $ENV{'MYSQL_PWD'};
}

my @fallback_tables = split( /,/ , $fallback_tables_string );

sub get_dbh {

    # my ( $package, $filename, $line, $subroutine, $hasargs,
    # #    5            6         7          8         9         10
    # $wantarray, $evaltext, $is_require, $hints, $bitmask, $hinthash )
    # = caller( 1 );
    #
    # logline( "get_dbh() called by $filename - line: $line , sub: $subroutine     ------------------- $GLOBAL_TABLE" );

    my $connection_string =
      "dbi:mysql:"
    . "database="  . $database
    . ";host="     . $host
    . ";port="     . $port
    . ";mysql_use_result=1"     # prevent $dbh->execute() from pulling all results into memory
    . ";mysql_compression=1";   # should give a good boost when dumping tables

    my $dbh;

    for my $try ( 1 .. 5 ) {

        logline( "Attempting to connect to MySQL ... try [$try]" );

        eval {
            $dbh = DBI->connect(
                $connection_string
              , $username
              , $password
              , {
                  RaiseError        => 0
                , AutoCommit        => 1
                , mysql_enable_utf8 => 1
                }
            ) || die( $DBI::errstr );
        };

        my $err = $@;

        if ( $err ) {
            logline( "Failed to connect to mysql!\n$err" );
            if ( $try > 1 ) {
                logline( "Sleeping for 60 seconds ..." );
                sleep( 60 );
            }
            $dbh = undef;
        } else {
            last;
        }

    }

    if ( $dbh ) {
        return $dbh;
    } else {
        logline( "Failed to connect to MySQL after 5 attempts. Giving up ..." );
        die( "Failed to connect to MySQL after 5 attempts. Giving up ..." );
    }
    
}

sub open_dump_file {

    my ( $table , $counter ) = @_;

    my $padded_counter = sprintf "%06d", $counter; # We want to make sure these are dumped / restored in order

    my $file_path = $db_directory . "/" . $table . "." . $padded_counter . ".csv.gz";

    my $csv_file;

    eval {
        open( $csv_file, "| /bin/gzip > $file_path" )
           || die( "Failed to open file for writing: [$file_path]\n" . $! );
    };

    my $err = $@;

    if ( $err ) {
        logline( $err );
        die( $err );
    }
    
    binmode( $csv_file, ":utf8" );

    push @{ $ALL_FILES->{ $table} }, $file_path;

    return $csv_file;

}

sub write_key_page_file {

    my ( $table , $counter , $key_page ) = @_;

    my $padded_counter = sprintf "%06d", $counter; # We want to make sure these are dumped / restored in order
    
    my $file_path = $db_directory . "/" . $table . "_keys." . $padded_counter . ".json";
    
    my $key_file;

    eval {
        open( $key_file, ">", $file_path )
           || die( "Failed to open file for writing: [$file_path]\n" . $! );
    };
    
    my $err = $@;
    
    if ( $err ) {
        logline( $err );
        die( $err );
    }
    
    binmode( $key_file, ":utf8" );
    
    push @{ $ALL_KEY_FILES->{ $table } }, $file_path;
    
    eval {
        
        print $key_file encode_json( $key_page )
            || die( "Failed to write to key file! [$file_path]\n" . $! );
        
        close $key_file
            || die( "Failed to close key file! [$file_path]\n" . $! );
        
    };
    
    $err = $@;
    
    if ( $err ) {
        logline( $err );
        die( $err );
    }
    
    return $file_path;
    
}

sub delete_all_files_for_table {

    my ( $table ) = @_;

    if ( exists $ALL_FILES->{ $table } && @{ $ALL_FILES->{ $table } } ) {
        foreach my $this_file ( @{ $ALL_FILES->{ $table } } ) {
            unlink $this_file;
        }
    }
}

sub dump_table_fallback {

    my ( $table ) = @_;

    # This sub is for cases where dump_table() fails - currently dumping BLOB columns, where we hit the max_allowed_packet.
    # We could break these tables up into smaller chunks, but it probably makes sense to have a fallback option anyway ...

    $0 = "mysqldumper - fallback [$table]";

    my $sql_dump_file = $db_directory . "/" . $table . ".sql.gz";
    
    my $success;
    
    for my $try ( 1 .. 20 ) {
        
        my @cmd = (
            "mysqldump"
          , "-h" , $host
          , "-P" , $port
          , "-u" , $username
          , "--no-create-info"
          , "--skip-triggers"
          , "--single-transaction=TRUE"
          , "--max_allowed_packet" , "2G"
          , $database
          , $table
        );

        my $pid;

        logline( "[$table] - attempt [$try] - in dump_table_fallback()" );

        eval {

            no warnings 'uninitialized';

            $pid = open3( *CMD_IN, *CMD_OUT, *CMD_ERR, @cmd )
                || die( "Failure in launching mysqldump!\n" . $! );

            my $exit_status = 0;

            # TODO Expand on signal handling a bit, these are just stubs.
            $SIG{CHLD} = sub {

                if ( waitpid( $pid, 0 ) > 0 ) {
                    $exit_status = $?;
                }

            };

            $SIG{TERM} = sub {
                die( "SIGTERM received ... exiting" );
            };

            # Allows the child to take input on STDIN.. Again, not required, just a stub
            print CMD_IN "Howdy...\n";
            close( CMD_IN );

            my $selector = IO::Select->new();
            $selector->add( *CMD_ERR, *CMD_OUT );

            open my $SQL_DUMP_PIPE , "| /bin/gzip > $sql_dump_file"
                || die( "Failed to open SQL dump file [$sql_dump_file]!\n" . $! );
            
            # DON'T do this - this double-encodes things:
            # binmode( $SQL_DUMP_PIPE, ":utf8" );
            
            my $errtxt = "";

            while ( my @ready = $selector->can_read() ) {

                foreach my $fh ( @ready ) {

                    my $t = "";

                    if ( fileno( $fh ) == fileno( CMD_ERR ) ) {

                        $t = scalar <CMD_ERR>;
                        $errtxt .= $t if $t;

                    } else {

                        $t = scalar <CMD_OUT>;
                        print $SQL_DUMP_PIPE $t if $t;

                    }

                    $selector->remove( $fh ) if eof( $fh );

                }

            }

            close $SQL_DUMP_PIPE
                || die( "Failed to close SQL dump file [$sql_dump_file]!\n" . $! );
            
            logline( "[$table] - mysqldump exited with exit status: [$exit_status]" );
            
            if ( $exit_status || $errtxt ) {
                die( "[$table] - mysqldump exited with exit status: [$exit_status]\nSTDERR: $errtxt" );
            }
            
        };

        my $eval_err = $@;

        if ( $eval_err ) {
            logline( "[$table] - Caught error in mysqldump: $eval_err\nThis is error number [$try]" );
        } else {
            $success = 1;
            last;
        }
        
    }
    
    if ( ! $success ) {
        logline( "[$table] - Too many errors for table. Giving up ..." );
        die( "[$table] - Too many errors for table. Giving up ..." );
    }
    
}

sub get_row_count {

    my ( $table ) = @_;

    my $dbh = get_dbh();

    my ( $count_sth , $row_count );

    logline( "Getting row count for table [$table]" );

    eval {
        $count_sth = $dbh->prepare( "select count(*) as row_count from $table" )
            || die( "Failed to prepare count statement!\n" . $dbh->errstr );
    };

    my $err = $@;

    if ( $err ) {
        logline( $err );
        die( $err );
    }

    eval {
        $count_sth->execute()
            || die( $count_sth->errstr );
    };

    $err = $@;

    if ( $err ) {
        logline( $err );
        die( $err );
    }

    if ( my $row = $count_sth->fetchrow_hashref ) {
        $row_count = $row->{row_count};
        logline( "Found [$row_count] records for table [$table]" );
    }

    return $row_count;

}

sub get_column_types_array {

    my ( $table ) = @_;

    # Fetch column types. We need some hacks to dump blobs.
    my $dbh = get_dbh();

    my $sth;

    $sth = $dbh->prepare( "select * from information_schema.COLUMNS where table_schema = ? and table_name = ? order by ordinal_position" )
        || die( $dbh->errstr );

    $sth->execute( $database , $table )
        || die( $sth->errstr );

    my $return;

    while ( my $col = $sth->fetchrow_hashref ) {
        push @{$return} , $col;
    }

    $sth->finish;

    $dbh->disconnect;

    return $return;

}

sub get_export_column_expressions {

    my ( $table ) = @_;

    my $column_types_array = get_column_types_array( $table );

    # according to https://jira.mariadb.org/browse/MDEV-11079 we need to wrap blob columns in the hex() function,
    #  and load them with unhex(), eg:
    #  LOAD DATA INFILE 'path.txt' INTO TABLE mytable (column1, column2, @hexColumn3) SET column3=UNHEX(@hexColumn3);
    my $export_column_expressions;

    my $do_paging = 0;
    
    foreach my $col ( @{$column_types_array} ) {
        if ( $col->{DATA_TYPE} =~ /blob/ ) {
            push @{$export_column_expressions}, 'hex( `' . $col->{COLUMN_NAME} . '` )';
            $do_paging = 1;
        } elsif ( $col->{DATA_TYPE} =~ /text/ ) {
            $do_paging = 1;
        } else {
            push @{$export_column_expressions}, '`' . $col->{COLUMN_NAME} . '`';
        }
    }

    return ( $export_column_expressions , $do_paging );

}

sub get_import_column_expressions {

    my ( $table ) = @_;

    my $column_types_array = get_column_types_array( $table );

    # according to https://jira.mariadb.org/browse/MDEV-11079 we need to wrap blob columns in the hex() function,
    #  and load them with unhex(), eg:
    #  LOAD DATA INFILE 'path.txt' INTO TABLE mytable (column1, column2, @hexColumn3) SET column3=UNHEX(@hexColumn3);

    my $import_column_expressions;
    my $set_expressions;

    foreach my $col ( @{$column_types_array} ) {
        if ( $col->{DATA_TYPE} =~ /blob/ ) {
            push @{$import_column_expressions}, '@' . $col->{COLUMN_NAME}; # TODO: how do we quote columns with spaces in them in this case ???
            push @{$set_expressions}, $col->{COLUMN_NAME} . '=unhex(@' . $col->{COLUMN_NAME} . ')';
        } else {
            push @{$import_column_expressions}, '`' . $col->{COLUMN_NAME} . '`';
        }
    }

    return ( $import_column_expressions , $set_expressions );

}

sub get_paged_keys {

    my ( $table ) = @_;

    my $dbh = get_dbh();

    my $sql = "select\n"
            . "    CONSTRAINT_NAME\n"
            . "  , TABLE_NAME\n"
            . "  , case when constraint_type = 'PRIMARY KEY' then 1 else 0 end as IS_PRIMARY\n"
            . "  , case when constraint_type = 'UNIQUE'      then 1 else 0 end as IS_UNIQUE\n"
            . "  , COLUMN_NAME\n"
            . "from\n"
            . "        information_schema.table_constraints t\n"
            . "join    information_schema.key_column_usage  k\n"
            . "                                                   using ( constraint_name , table_schema , table_name )\n"
            . "where\n"
            . "    t.constraint_type  = ?\n"
            . "and     t.table_name   = ?\n"
            . "and     t.table_schema = ?\n"
            . "order by\n"
            . "    ordinal_position";

    my @keys;

    eval {

        my $sth = $dbh->prepare( $sql )
            || die( $dbh->errstr );

        foreach my $constraint_type ( 'PRIMARY KEY' , 'UNIQUE' ) {

            logline( "[$table] - Attempting to locate primary key for table" );

            $sth->execute( $constraint_type , $table , $database )
                || die( $sth->errstr );

            while ( my $row = $sth->fetchrow_hashref() ) {
                push @keys, $row->{COLUMN_NAME};
            }

            if ( @keys ) {
                logline( "[$table] - Found $constraint_type: " . join( " , " , @keys ) );
                last;
            }

        }

    };

    my $err = $@;

    if ( $err ) {
        logline( "[$table] - Failed to fetch keys for table:\n$err" );
        die( "[$table] - Failed to fetch keys for table\n$err" );
    }

    if ( ! @keys ) {
        logline( "[$table] 0 No keys defined for table. Falling back to mysqldump for this table" );
        return {
            page_requests   => undef
          , fallback        => 1
        };
    }

    my $sth = $dbh->prepare( "select " . join( " , " , @keys ) . " from $table" )
        || die( $dbh->errstr );

    $sth->execute()
        || die( $sth->errstr );

    # We're trying to assemble a hash, consisting of the filter clause with placeholders embedded,
    # and an array of ID values to bind, eg:

    # $page_requests =
    #     {
    #         filter_clause    => "( key_1 , key_2 ) in "
    #       , key_count        => 2
    #       , pages            => [
    #                                [ 1 , 2 , 3 , 4 , 5 , 6 ]
    #                              , [ 7 , 8 , 9 , 10 ]
    #                             ]
    #     };

    # To save ( a bit ) on memory, we dynamically generate the list of placeholders immediately before executing each page.

    my $page_requests = {
        filter_clause => "where ( " . join( " , " , @keys ) . " ) in "
      , key_count     => scalar @keys
    };

    my $page_counter = 0;
    my $this_page_keys;
    
    while ( my @row = $sth->fetchrow_array ) {

        push ( @{$this_page_keys} , @row );
        $page_counter ++;
        
        if ( $page_counter % $page_size == 0 ) {
            #push @{$page_requests->{pages}} , $this_page_keys;
            push @{$page_requests->{pages}} , write_key_page_file( $table , $page_counter , $this_page_keys );
            $this_page_keys = [];
        }

    }

    if ( $this_page_keys ) {
        #push @{$page_requests->{pages}} , $this_page_keys;
        push @{$page_requests->{pages}} , write_key_page_file( $table , $page_counter , $this_page_keys );
    }
    
    return {
        page_requests   => $page_requests
      , fallback        => 0
    };
    
}

sub dump_table {
    
    my ( $table ) = @_;

    $GLOBAL_TABLE = $table;

    $0 = "mysqldumper - [$table]";

    logline( "[$table] - Dumping table ..." );

    my $error_count  = 0;
    my $done         = 0;
    my $counter      = 0;

    my ( $expected_rows , $err );

    if ( $check_count ) {
        $expected_rows = get_row_count( $table );
        if ( ! $expected_rows ) {
            $done = 1; # Need to set this, or we go into an infinite loop
        }
        eval {
            open( my $INFO_FILE , ">" , $db_directory . "/" . $table . ".info" )
                or die( "Failed to open info file:\n" . $! );
            my $info = {
                record_count    => $expected_rows
            };
            print $INFO_FILE encode_json( $info );
            close $INFO_FILE
                || die( "Failed to close info file:\n" . $! );
        };
        $err = $@;
        if ( $err ) {
            logline( "[$table] - $err" );
            die( "[$table] - $err" );
        }
    }

    my ( $export_column_expressions , $do_paging ) = get_export_column_expressions( $table );
    my $page_requests;
    
    if ( $do_paging ) {
        return dump_table_fallback( $table ); # TODO: remove when paging doesn't blow up mysql ...
        my $page_requests_response = get_paged_keys( $table );
        if ( $page_requests_response->{fallback} ) {
            return dump_table_fallback( $table );
        } else {
            $page_requests = $page_requests_response->{page_requests};
        }
    } else {
        $page_requests =
            {
                filter_clause   => "where 1 = "
              , key_count       => 1
              , pages           => [
                                      [ 1 ]
                ]
            };
    }
    
    while ( ( ! $done ) && $error_count < MAX_ERRORS_PER_TABLE ) {

        $err = undef;

        if ( $error_count ) {
            logline( "[$table] - Retrying table - attempt #" . $error_count );
        }

        my $dbh = get_dbh();

        my $page_no = 0;
        $counter = 0;
        
        PAGE_LOOP: foreach my $page_keys ( @{$page_requests->{pages}} ) {
            
            my $this_page = $page_keys;
            
            if ( $do_paging ) {
                
                # We have to slurp these in from a page keys file - the path of which should be in $page_keys
                local $/; # read while file at once
                my $page_key_file;
                eval {
                    open $page_key_file , $page_keys
                        || die( "Failed to open page key file: [$page_keys]\n" . $! );
                };
                
                $err = $@;
                
                if ( $err ) {
                    logline( "[$table] - $err" );
                    die( "[$table] - $err" );
                }
                
                my $page_keys_json = <$page_key_file>;
                $this_page = decode_json( $page_keys_json );
                
            }
            
            my $key_values_count = scalar @{$this_page};                                    # the number of id *values* we have in this page
            my $set_count = $key_values_count / $page_requests->{key_count};                # the number of sets of id values we have in this page
            my @placeholders_array = ( "?" ) x  $page_requests->{key_count};                # one for each key in a set
            my $placeholders = " ( " . join( " , " , @placeholders_array ) . " )";          # a string of placeholders for a key set
            my @page_of_placeholders = ( $placeholders ) x $set_count;                      # an array of all the placeholders in a page
            my $all_placeholders = " ( " . join( " , " , @page_of_placeholders ) . " ) ";   # final string of all placeholders

            my $sql = "select\n    " . join( "\n  , " , @{$export_column_expressions} ) . "\nfrom\n    $table\n"
                    . $page_requests->{filter_clause} . $all_placeholders;

            my $sth;

            eval {
                $sth = $dbh->prepare( $sql )
                    || die( $dbh->errstr );

                $sth->execute( @{$this_page} )
                    || die( $sth->errstr . "\n\n$sql" );
            };
            
            $err = $@;
            
            if ( $err ) {
                logline( "[$table] - $err" );
                die( "[$table] - $err" );
            }

            my $csv_writer = Text::CSV_XS->new(
                {
                    quote_char     => '"'
                  , binary         => 1
                  , eol            => "\n"
                  , sep_char       => ","
                  , escape_char    => "\\"
                  , quote_space    => 1
                  , always_quote   => 0
                  , undef_str      => "\\N"
                }
            );

            my $fields = $sth->{NAME};

            my $csv_file;
            my $file_open = 0;

            eval {
                while ( my $page = $sth->fetchall_arrayref( undef, 10000 ) ) { # We want to pull 10,000 records at a time
                    $page_no ++;
                    if ( $page_no == 1 || $page_no % 10 == 0 ) {
                        logline( "[$table] - Fetched page [" . comma_separated( $page_no ) . "]" );
                    }
                    if ( $counter && $file_open && $counter % 1000000 == 0 ) { # We want approx 1,000,000 records per file
                        close $csv_file
                            or die( "Failed to close file!\n" . $! );
                        $file_open = 0;
                    }
                    if ( ! $file_open ) {
                        $csv_file = open_dump_file( $table , $page_no );
                        # Write CSV header ( ie columns )
                        print $csv_file join( "," , @{$fields} ) . "\n";
                        $file_open = 1;
                    }
                    foreach my $record ( @{$page} ) {
                        $csv_writer->print( $csv_file, $record );
                        $counter ++;
                    }
                };
            };

            $err = $@;

            if ( $err ) {
                logline( "[$table] - WARNING: Encountered fatal error while dumping table:\n$err\nIncrementing error count ..." );
                $error_count ++;
                close $csv_file; # we don't really care if this works or not
                $file_open = 0;
                delete_all_files_for_table( $table );
                last PAGE_LOOP; # we break out of the page loop, and re-enter the while( error < MAX_ERROR ) loop
            } else {
                $done = 1;
            }

            if ( $file_open ) {
                eval {
                    close $csv_file
                        or die( $! );
                };
                $file_open = 0;
                $err = $@;
                if ( $err ) {
                    logline( "[$table] - WARNING: Encountered fatal error while closing file for table:\n$err\nIncrementing error count ..." );
                    $error_count ++;
                    delete_all_files_for_table( $table );
                    $done = 0;
                    last PAGE_LOOP; # we break out of the page loop, and re-enter the while( error < MAX_ERROR ) loop
                }
            }

        }

        if ( ! $err ) { # if there's an error, we've already dealt with it above. Just continue through the loop...
            if ( $expected_rows && $expected_rows != $counter ) {
                no warnings 'uninitialized';
                logline( "[$table] - We counted [$expected_rows] records for table, but then dumped [$counter] records!\nLast DBI error ( may be empty ):\n" . DBI::errstr );
                $done = 0;
                $error_count ++;
                delete_all_files_for_table( $table );
            } elsif ( ! $expected_rows ) {
                $done = 1;
            }
        }

    }
    
    if ( ! $done ) {
        logline( "[$table] - Gave up dumping table - too many errors ( " . MAX_ERRORS_PER_TABLE . " )" );
        die( "[$table] - Gave up dumping table - too many errors ( " . MAX_ERRORS_PER_TABLE . " )" );
    }
    
    if ( $do_paging ) {
        foreach my $page_file ( @{$ALL_KEY_FILES->{ $table }} ) {
            logline( "Removing key page file: [$page_file]" );
            unlink $page_file;
        }
    }
    
    logline( "[$table] - Wrote total of [" . comma_separated( $counter ) . "] records" );

}

sub restore_table {
    
    my ( $table , $source_dir , $table_file_list ) = @_;
    
    my $fifo_name = $table . ".fifo";
    
    my $table_info; # stores record counts, import column expressions, set expressions

    my $info_file_path = $source_dir . "/" . $table . ".info";
    
    if ( ! -e $info_file_path ) {

        logline( "[$table] - Info file [$info_file_path] doesn't exist! Skipping record count validation" );

    } else {

        eval {
            open INFO_FILE , $info_file_path
                || die( "Failed to open info file:\n" . $! );
            local $/; # read entire file at once
            my $table_info_string = <INFO_FILE>;
            close INFO_FILE
                || die( "Failed to close info file:\n" . $! );
            $table_info = decode_json( $table_info_string );
        };

        my $err = $@;
        if ( $err ) {
            logline( "[$table] - $err" );
            die( "[$table] - $err" );
        }

    }

    my $records;

    foreach my $file ( @{$table_file_list} ) {

        unlink( $fifo_name );

        if ( $file =~ /.*\.csv\.*/ ) {
            if ( ! $table_info->{import_column_expressions} ) {
                ( $table_info->{import_column_expressions} , $table_info->{set_expressions} ) = get_import_column_expressions( $table );
            }
        }

        my $result;

        eval {
            $result = POSIX::mkfifo( $fifo_name, 0600 )
                || die( "Failed to create FIFO!\n" . $! );
        };

        my $err = $@;

        if ( $err ) {
            logline( "[$table] - $err" );
            die( "[$table] - $err" );
        }

        my $pid = fork;

        if ( ! defined $pid ) {

            logline( "[$table] - fork() failed! Exiting ..." );
            die( "[$table] - fork() failed! Exiting ..." );

        } elsif ( $pid ) {

            # We're the master

            logline( "[$table] - Forked process with PID [$pid] for table" );

            if ( $file =~ /\.csv\.gz$/ ) {

                # We're loading an accelerated CSV dump ...

                sleep( 20 ); # give the forked process time to start up and begin writing to the pipe

                my $dbh = get_dbh();

                eval {

                    $dbh->do( "set foreign_key_checks=0" )
                        || die( $dbh->errstr );

                    $dbh->do( "set unique_checks=0" )
                        || die( $dbh->errstr );

                };

                $err = $@;

                if ( $err ) {
                    logline( "[$table] - $err" );
                    die( "[$table]- $err" );
                }

                my $load_command =   "load data local infile\n"
                                   . " '" . $table . ".fifo'\n"
                                   . " into table $table\n"
                                   . " character set utf8\n"
                                   . " columns\n"
                                   . "    terminated by ','\n"
                                   . "    optionally enclosed by '\"'\n"
                                   . " ignore 1 rows"
                                   . " ( " . join( " , " , @{$table_info->{import_column_expressions}} ) . " )\n";

                if ( $table_info->{set_expressions} ) {
                    $load_command .= " set " . join( " , " , @{$table_info->{set_expressions}} ) . "\n";
                }

                eval {

                    my $sth = $dbh->prepare( $load_command )
                        || die( $dbh->errstr );

                    $records += $sth->execute()
                        || die( $dbh->errstr );

                };

                $err = $@;

                if ( $err ) {
                    logline( "[$table] - $err" );
                    die( "[$table] - $err" );
                }

            } else { # end of CSV loading block ...

                # We're loading a fallback dump file ...

                my @cmd = (
                    "mysql"
                  , "-h" , $host
                  , "-P" , $port
                  , "-u" , $username
                  , $database
                  , " < " . $fifo_name
                );

                my $args_string = join( " ", @cmd );
                logline( $args_string );

                my $return = system( $args_string );

                if ( $return ) {
                    logline( "[$table] - mysql return code for file [$file]: [$return]" );
                    die( "[$table] - mysql return code for file [$file]: [$return]" );
                } else {
                    logline( "[$table] - mysql return code for file [$file]: [$return]" );
                }

            }

            eval {

                $pid = wait # it will be the same PID, and we don't use it anyway ( other than logline()ing it )
                    || die( "wait failed!\n" . $! );

                if ( $? ) {
                    die( "[$table] - gunzip with PID [$pid] failed!" );
                }

            };

            $err = $@;

            if ( $err ) {
                logline( $err );
                die( $err );
            }

            logline( "[$table] - Loaded [$records] records so far from file [$file]" );

        } else {

            # We're the child

            my $return = system( "gunzip -c $file > $fifo_name" );

            if ( $return ) {
                logline( "[$table] - gunzip returned exit code: [$return]" );
                die( "[$table] - gunzip returned exit code: [$return]" );
            }

            exit(0);

        }

    }

    if ( $table_info && $records && $table_info->{record_count} && $table_info->{record_count} != $records ) {
        logline( "[$table] - We expected [" . $table_info->{record_count} . "] ( from info file ). Fatal error!" );
        die( "[$table] - Loaded [$records]. We expected [" . $table_info->{record_count} . "] ( from info file ). Fatal error!" );
    }

}

sub comma_separated {
    
    my ( $number ) = @_;
    
    $number =~ s/(\d)(?=(\d{3})+(\D|$))/$1\,/g;
    
    return $number;
    
}

sub tokenise_ddl {
    
    my ( $schema_dir , $schema_path , $database ) = @_;
    
    my $tokenised_path = $schema_dir . "/" . "schema.ddl.tokenised";
    
    open( my $IN , $schema_path )
        || die( "Failed to open original ddls:\n" . $! );
    
    open( my $OUT , ">$tokenised_path" )
        || die( "Failed to open tokenised ddls:\n" . $! );
    
    while ( my $line = <$IN> ) {
        
        # The dumps have the 'DEFINER' embedded. Removing it is not easy. Later versions of mysqldump supposedly support a --skip-definer flag,
        # but my testing with various versions show that they don't. For now, we do our own stripping in perl. The original sed commands are above,
        # but they don't work because we have other BS thrown in for good measure, eg:
        #  /*!50003 CREATE*/ /*!50017 DEFINER=`dev`@`%`*/ /*!50003 TRIGGER destination_metadata_after_insert
        # All the quoting and comments and stuff make it hard ( for me ) to deal with in sed, and I'm very good at perl regex work, so that's how
        # we'll do it.
        
        # my $remove_definer_cmd = "sed '"
        #                        . "s/CREATE DEFINER.*PROCEDURE/CREATE PROCEDURE/g; "
        #                        . "s/CREATE DEFINER.*FUNCTION/CREATE FUNCTION/g; "
        #                        . "s/CREATE DEFINER.*TRIGGER/CREATE TRIGGER/g; "
        #                        . "s/DEFINER=[^]+@[^]+/DEFINER=CURRENT_USER/g; "
        #                        . "/ALTER DATABASE/d'";
        
        # https://regex101.com/ <==== test changes here please
        # This is basically stripping out all references to DEFINER directives, which prevent restoring into RDS
        $line =~ s/(\*\/)*\s*(\/\*![\d]*)*\s*DEFINER=`*[\w]*`*@`*[\w\.%]*`*(\*\/)*\s*(\/\*![\d]*)*\s*(SQL SECURITY DEFINER)*\s*(\*\/)*/ /g;
        
        # We also tokenise the database being dumped, so we can then detokenise during restores, effectively allowing
        # us to restore into a different-named database. There are various places in the 'mysqldump' output where this is required.
        # - create database $database
        # - alter $database set default characterset
        # - in some rare cases, I've seen other places ... I don't remember exactly where ...
        $line =~ s/\b$database\b/#DATABASE#/g;
        
        print $OUT $line;
        
    }
    
    close $IN
        || die( "[Whole schema] - Failed to close original ddls file:\n" . $! );

    close $OUT
        || die( "[Whole schema] - Failed to close tokenised ddls file:\n" . $! );
    
    return $tokenised_path;
    
}

sub split_schema_into_stages {
    
    my $original_ddl_path = shift;
    
    # This sub parses a MySQL schema dump into 3 stages.
    # The 1st stage has *just* the column definitions, views, functions and procedures.
    # The 2nd stage has keys ( including primary keys and auto_increment modifiers ).
    # The 3rd stage has foreign keys.
    # The idea is that we load the data between stage 1 and stage 2. Loading data with
    # all the keys already defined is SLOW.
    
    my $accel_schema_stage_1_path = $db_directory . "/accel_schema_stage_1.ddl";
    
    mkdir( $db_directory . "/stage_2" );
    mkdir( $db_directory . "/stage_3" );
    
    eval {
        
        open( my $IN , $original_ddl_path )
            || die( "Failed to open original ddls:\n" . $! );
        
        open( my $STAGE_1 , ">$accel_schema_stage_1_path" )
            || die( "Failed to open stage 1 ddl:\n" . $! );
        
        my $state = 'database';
        
        my $has_keys = 0;
        my $has_fkeys = 0;
        
        my $current_table;
        my @current_columns; # we need to strip commas off the end of column definition lines, as these are followed by keys ( sometimes )
        my @current_keys;    # same goes for keys
        my @current_fkeys;   # might as well do the same for everything
        my $is_auto_increment;
        my $has_auto_increment;
        
        while ( my $line = <$IN> ) {
            
            $is_auto_increment = 0;
            
            if ( $line =~ /--\sTable\sstructure\sfor\stable\s`(.*)`/ ) {                      # table definition preamble
                $state = 'table_preamble';
                $current_table = $1;
            } elsif ( $line =~ /^CREATE\sTABLE\s`.*`\s\(/ ) {                                 # start of table definition
                $state = 'table';
            } elsif ( $line =~ /\)\sENGINE=/ ) {                                              # end of table definition
                $state = 'table_end';
            } elsif ( $line =~ /^\s*(PRIMARY)*\s*(KEY)\s*(\(.*\))/ ) {                        # line of key definition
                $state = 'key';
                $has_keys = 1;
                my $is_primary = $1;
                if ( $is_primary && $has_auto_increment ) {
                    $state = 'skip_primary';
                }
            } elsif ( $line =~ /\s`(\w*)`\s(.*)\bauto_increment\b/i ) {
                # We have to strip out the auto_increment definition, and handle it differently ...
                # MySQL dumps the column definition WITH the auto_increment flag, but WITHOUT the PRIMARY KEY flag
                # But when we apply it later, we *must* do it in a single operation.
                my $column_name       = $1;
                my $column_definition = $2;
                $is_auto_increment    = 1;
                $has_auto_increment   = 1;
                push @current_keys , " modify $column_name $column_definition auto_increment primary key";
                $line =~ s/\bauto_increment\b//i;
            } elsif ( $line =~ /^\s*CONSTRAINT\s/ ) {                                         # line of constraint ( foreign key ) definition
                $state = 'foreign_key';
                $has_fkeys = 1;
            }
            
            if ( $state eq 'database' ) {
                print $STAGE_1 $line;
            } elsif ( $state eq 'table_preamble' ) {
                print $STAGE_1 $line;
            } elsif ( $state eq 'table' ) {
                print $STAGE_1 $line;
                $state = 'columns';
            } elsif ( $state eq 'columns' ) {
                $line =~ s/,*\n$//;                                                            # strip commas & new lines from end of column definitions
                push @current_columns , $line;
            } elsif ( $state eq 'table_end') {
                print $STAGE_1 join( ",\n" , @current_columns );
                print $STAGE_1 "\n$line";
                if ( $has_keys ) {
                    open( my $STAGE_2 , ">" . $db_directory . "/stage_2/" . $current_table . ".ddl" )
                        || die( "Failed to open stage 1 ddl:\n" . $! );
                    print $STAGE_2 "alter table `$current_table`\n";
                    print $STAGE_2 join( ",\n" , @current_keys );
                    print $STAGE_2 ";\n";
                    close $STAGE_2
                        || die( "[Whole schema] - Failed to close stage 2 file:\n" . $! );
                    $has_keys = 0;
                    $has_auto_increment = 0;
                }
                if ( $has_fkeys ) {
                    open( my $STAGE_3 , ">" . $db_directory . "/stage_3/" . $current_table . ".ddl" )
                        || die( "Failed to open stage 1 ddl:\n" . $! );
                    print $STAGE_3 "alter table `$current_table`\n";
                    print $STAGE_3 join( ",\n" , @current_fkeys );
                    print $STAGE_3 ";\n";
                    close $STAGE_3
                        || die( "[Whole schema] - Failed to close stage 3 file:\n" . $! );
                    $has_fkeys = 0;
                }
                @current_columns = ();
                @current_keys    = ();
                @current_fkeys   = ();
                $state = 'database';
            } elsif ( $state eq 'key' ) {
                $line =~ s/,*\n$//;                                                            # strip commas & new lines from end of column definitions
                push @current_keys , " add $line";
            } elsif ( $state eq 'foreign_key' ) {
                $line =~ s/,*\n$//;                                                            # strip commas & new lines from end of column definitions
                push @current_fkeys , " add $line";
            } elsif ( $state eq 'skip_primary' ) {
                # Do nothing.
            } else {
                warn "Unknown state: [$state]";
            }
            
        }
        
        close $IN
            || die( "[Whole schema] - Failed to close original ddls file:\n" . $! );
        
        close $STAGE_1
            || die( "[Whole schema] - Failed to close stage 1 file:\n" . $! );
        
    };

    my $err = $@;

    if ( $err ) {
        logline( $err );
        die( $err );
    }
    
}

if ( $action eq 'dump' ) {

    make_path( $db_directory );
    
    # First, we dump the schema ...

    my $original_ddl_path = $db_directory . "/schema.ddl.orig";
    
    my @cmd = (
        "mysqldump"
      , "--no-data"
      , "--routines"
      , "--single-transaction=TRUE"
      , "-B" # include the 'create database' command
      , "-h" , $host
      , "-P" , $port
      , "-u" , $username 
      , $database
      , ">" , $original_ddl_path
    );
    
    my $args_string = join( " ", @cmd );
    logline( $args_string );
    
    my $return = system( $args_string );
    
    if ( $return ) {
        logline( "[Whole schema] - mysqldump return code: [$return]" );
        die( "[Whole schema] mysqldump return code: [$return]" );
    } else {
        logline( "[Whole schema] - mysqldump return code: [$return]" );
    }
    
    my $tokenised_schema_path = tokenise_ddl( $db_directory , $original_ddl_path , $database );
    
    ###################################

    # Next, we dump the data ...
    my $dbh = get_dbh();

    my $sth;

    my $sql = "select TABLE_NAME from information_schema.TABLES\n"
            . " where TABLE_TYPE like 'BASE TABLE' and TABLE_SCHEMA = ?";

    if ( $tables_string ) {
        my @tables = split( /,/, $tables_string );
        foreach my $table ( @tables ) {
            $table = "'" . $table . "'";
        }
        $sql .= "\n and TABLE_NAME in ( " . join( ",", @tables ) . " )";
    }
    
    logline( $sql );
    
    eval {

        $sth = $dbh->prepare( $sql )
            || die( $dbh->errstr );

        $sth->execute( $database )
            || die( $sth->errstr );

    };

    my $err = $@;

    if ( $err ) {
        logline( $err );
        die( $err );
    }
    
    my $all_tables;

    while ( my $row = $sth->fetchrow_hashref ) {
        push @{$all_tables} , $row->{TABLE_NAME};
    }

    $sth->finish();

    $dbh->disconnect;

    my $active_processes = 0;
    my $pid_to_table_map;

    if ( $jobs > 1 ) {
        
        # In this mode, we fork worker jobs to do the dumping for us

        foreach my $table( @{$all_tables} ) {
            
            while ( $active_processes >= $jobs ) {

                my $pid;

                eval {

                    $pid = wait
                        || die( "wait failed!\n" . $! );

                    $active_processes --;

                    my $exiting_table = $pid_to_table_map->{ $pid };

                    if ( $? ) {
                        logline( "[$table] - Worker with PID [$pid] failed!" );
                        die( "[$table] - Worker with PID [$pid] failed!" );
                    } else {
                        logline( "[$table] - Worker with PID [$pid] completed successfully" );
                    }
                    
                };

                $err = $@;

                if ( $err ) {
                    logline( $err );
                    die( $err );
                }

            }
            
            my $pid = fork;

            if ( ! defined $pid ) {

                logline( "[$table] - Failed to fork()" );
                die( "[$table] - Failed to fork()" );

            } elsif ( $pid ) {
                
                # We're the master
                # Increment the number of active processes
                $active_processes ++;
                
                logline( "[$table] - Forked process with PID [$pid]" );
                
                $pid_to_table_map->{ $pid } = $table;
                
            } else {
                
                # We're the child
                if ( $table ~~ @fallback_tables ) {
                    dump_table_fallback( $table );
                } else {
                    dump_table( $table );
                }
                
                exit(0);
                
            }
            
        }
        
    } else {
        
        # In this mode, we do the dumping ourself, in a transaction ( for a consistent snapshot )
        
        foreach my $table( @{$all_tables} ) {
            if ( $table ~~ @fallback_tables ) {
                dump_table_fallback( $table );
            } else {
                dump_table( $table );
            }
        }
        
    }

    while ( $active_processes > 0 ) {

        eval {

            my $pid = wait
                || die( "wait failed!\n" . $! );

            $active_processes --;

            my $table = $pid_to_table_map->{ $pid };

            if ( $? ) {
                die( "[$table] - Worker with PID [$pid] failed!" );
            } else {
                logline( "[$table] - Worker with PID [$pid] completed successfully" );
            }

        };

        $err = $@;

        if ( $err ) {
            logline( $err );
            die( $err );
        }

    }

    chdir( $directory );

    system(
        "tar"
      , "-cvf"
      , $database . ".tar"
      , $database
    );
    
    # Rename it ... final name will be {database}.accel.dump
    # ... which is useful for my case to differentiate from other dumps
    rename $database . ".tar" , $database . ".accel.dump";

    rmtree( $database );
    
}

sub handle_exiting_process {

    my $active_processes = shift;
    my $pid_to_table_map = shift;

    logline( "In handle_exiting_process() with [$active_processes] active processes ..." );

    my $pid;

    eval {
        $pid = wait
            || die( "wait failed!\n" . $! );
    };

    my $err = $@;

    if ( $err ) {
        logline( "wait() for child process failed!\n$err" );
        die( "wait() for child process failed!\n$err" );
    }

    $active_processes --;

    my $exiting_table = $pid_to_table_map->{ $pid };

    if ( $? ) {
        logline( "Worker for table [$exiting_table] with PID [$pid] failed!" );
        die( "Worker for table [$exiting_table] with PID [$pid] failed!" );
    } else {
        logline( "Worker for table [$exiting_table] with PID [$pid] completed successfully" );
    }

    delete $pid_to_table_map->{ $pid };

    return $active_processes;

}

sub detokenise_ddl {
    
    my $original_ddl_path = shift;
    
    my $detokenised_dll_path = $original_ddl_path . ".detokenised";
    
    logline( "Detokenising to [$detokenised_dll_path]" );
    
    open( my $IN , $original_ddl_path )
        || die( "Failed to open original ddls:\n" . $! );

    open( my $OUT , ">$detokenised_dll_path" )
        || die( "Failed to open modded ddls:\n" . $! );
    
    while ( my $line = <$IN> ) {
        
        # Here we're replacing #DATABASE# with the actual target database ...
        $line =~ s/#DATABASE#/$database/g;
        
        print $OUT $line;
        
    }

    close $IN
        || die( "[Whole schema] - Failed to close original ddls file:\n" . $! );

    close $OUT
        || die( "[Whole schema] - Failed to close detokenised ddls file:\n" . $! );
    
    return $detokenised_dll_path;
    
}

sub execute_ddl {
    
    my $ddl_path = shift;
    
    logline( "Executing DDL: [$ddl_path]" );
    
    my @cmd = (
        "mysql"
      , "-h" , $host
      , "-P" , $port
      , "-u" , $username
      , $database
      , "<" , $ddl_path
    );
    
    my $args_string = join( " ", @cmd );
    logline( $args_string );
    
    my $return = system( $args_string );
    
    if ( $return ) {
        logline( "mysql return code: [$return]" );
        die( "mysql return code: [$return]" );
    } else {
        logline( "mysql return code: [$return]" );
    }
    
}

if ( $action eq 'restore' ) {
    
    chdir( $directory );
    
    logline( "Unpacking archive ..." );
    
    my @cmd = (
        "tar"
      , "-xvf"
      , $file
    );
    
    my $return = system( join( " ", @cmd ) );
    
    if ( $return ) {
        logline( "tar return code: [$return]" );
        die( "tar return code: [$return]" );
    } else {
        logline( "tar return code: [$return]" );
    }
    
    my $source_dir;
    
    if ( $file =~ /(.*)\.accel.dump/ ) {
        $source_dir = $1 . "/";
    } else {
        logline( "Failed to parse source archive: $file" );
        die( "Failed to parse source archive: $file" );
    }
    
    $db_directory = $source_dir;
    
    chdir( $source_dir );
    
    my $tokenised_ddl_path   = $db_directory . "/schema.ddl.tokenised";
    my $detokenised_ddl_path = detokenise_ddl( $tokenised_ddl_path );
    
    split_schema_into_stages( $detokenised_ddl_path );
    
    if ( ! $skip_create_db ) {
        
        my $schema_file;
        
        if ( $accel_keys ) {
            $schema_file = "accel_schema_stage_1.ddl";
        } else {
            $schema_file = "schema.ddl";
        }
        
        execute_ddl( $detokenised_ddl_path );
        
    }
    
    # Perform any defined post schema command. This can be used to, for example, mangle the schema before
    # inserting any data ( we've used this to convert to utf8mb4 )
    if ( $post_schema_command ) {
        logline( "Executing post schema command:\n$post_schema_command" );
        my $post_schema_command_output = `$post_schema_command`;
        logline( "Post schema command output:\n" . $post_schema_command_output );
    }
    
    # Next, we restore the data ...
    # Here, we get a list of tables. The files we have are in the form table.000001.csv.gz ... table.000002.csv.gz ... etc
    # We split each table into a manageable size. MySQL seems to choke loading very large files in 1 go.
    # Note that we DON'T want to have multiple concurrent inserts into the SAME table. So we aggregate our list of files
    # into unique tables, then call restore_table() on each of these.
    # restore_table() will then be responsible for loading ALL files for that table, sequentially.
    #
    my $all_files_array;
    my $all_tables;
    
    push @{$all_files_array}, sort( File::Find::Rule->file()
                                                    ->name( "*.csv.gz" )
                                                    ->in( $source_dir )
    );
    
    foreach my $file_name ( @{$all_files_array} ) {
        logline( "Found table file: [$file_name]" );
        my $table_name = $file_name;
        $table_name =~ s/\.(\d*)\.csv\.gz//;   # strip trailing ".00001.csv.gz"
        $table_name =~ s/.*\///;        # strip directory component ( leaving filename )
        push @{ $all_tables->{ $table_name } }, $file_name;
    }

    # Next, we get a list of fallback tables. These are dumped using mysqldump, and are in the form:
    #  table.sql.gz.
    my $all_fallback_tables_array;

    push @{$all_fallback_tables_array}, sort( File::Find::Rule->file()
                                                              ->name( "*.sql.gz" )
                                                              ->in( $source_dir )
    );
    
    # Get the list into the same structure as the above ...
    foreach my $file_name ( @{$all_fallback_tables_array} ) {
        my $table_name = $file_name;
        $table_name =~ s/\.sql\.gz//;   # strip trailing ".00001.csv.gz"
        $table_name =~ s/.*\///;        # strip directory component ( leaving filename )
        push @{ $all_tables->{ $table_name } }, $file_name;
    }
    
    my $stages;
    
    $stages->{all_tables} = $all_tables;
    
    my ( @stage_2 , @stage_3 );
    
    if ( $accel_keys ) {
        push @stage_2, sort( File::Find::Rule->file()
                                             ->name( "*.ddl" )
                                             ->in( $source_dir . "/stage_2" )
        );
        push @stage_3, sort( File::Find::Rule->file()
                                             ->name( "*.ddl" )
                                             ->in( $source_dir . "/stage_3" )
        );
        %{$stages->{stage_2}} = map { $_ => $_ } @stage_2;
        %{$stages->{stage_3}} = map { $_ => $_ } @stage_3;
    } else {
        $stages->{stage_2} = {};
        $stages->{stage_3} = {};
    }
    
    if ( $jobs > 1 ) {

        # In this mode, we fork worker jobs to do the restoring for us

        my $active_processes = 0;
        my $pid_to_table_map;
        
        foreach my $stage ( qw | all_tables stage_2 stage_3 | ) {
            
            foreach my $table( keys %{ $stages->{ $stage } } ) {
                
                while ( $active_processes >= $jobs ) {
                    $active_processes = handle_exiting_process( $active_processes , $pid_to_table_map );
                }
                
                my $pid = fork;
    
                if ( $pid ) {
                    
                    # We're the master
                    # Increment the number of active processes
                    $active_processes ++;
    
                    logline( "[$table] - Forked process with PID [$pid]" );
    
                    $pid_to_table_map->{ $pid } = $table;
    
                } else {
                    
                    if ( $stage eq 'all_tables' ) {
                        
                        restore_table( $table , $source_dir , $stages->{ $stage }->{ $table } );
                        
                    } else {
                        
                        my $start_ts = Time::HiRes::gettimeofday;
                        my $this_ddl =  $stages->{ $stage }->{ $table };
                        #my $detokenised_ddl_path = detokenise_ddl( $this_ddl );
                        #execute_ddl( $detokenised_ddl_path );
                        execute_ddl( $this_ddl );
                        my $end_ts = Time::HiRes::gettimeofday;
                        my $seconds = $end_ts - $start_ts;
                        logline( "[$table] - applied $stage DDL in [$seconds] seconds" );
                        
                    }
    
                    exit( 0 );
                    
                }
    
            }
            
            while ( $active_processes >= 1 ) {
                $active_processes = handle_exiting_process( $active_processes , $pid_to_table_map );
            }
            
            logline( "Completed stage: [$stage]" );
            
        }
        
        logline( "Active processes at 0. Exiting successfully" );

        close $LOGFILE
            or warn( "Failed to close log file!" . $! );

        exit( 0 );

    } else {
        
        foreach my $stage ( qw | all_tables stage_2 stage_3 | ) {
            foreach my $table( keys %{ $stages->{ $stage } } ) {
                if ( $stage eq 'all_tables' ) {
                    restore_table( $table , $source_dir , $stages->{ $stage }->{ $table } );
                } else {
                    #my $detokenised_ddl_path = detokenise_ddl( $stages->{ $stage }->{ $table } );
                    #execute_ddl( $detokenised_ddl_path );
                    execute_ddl( $detokenised_ddl_path );
                }
            }
        }
        
    }
    
}

if ( $action eq 'detokenise' ) {
    
    my $detokenised_schema_path = detokenise_ddl( $directory . "/schema.tokenised.ddl" );
    
}