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

use Data::Dumper;

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
);

GetOptions(
    'host=s'      => \$host
  , 'port=i'      => \$port
  , 'username=s'  => \$username
  , 'password=s'  => \$password
  , 'database=s'  => \$database
  , 'action=s'    => \$action
  , 'jobs=i'      => \$jobs
  , 'directory=s' => \$directory
  , 'sample=i'    => \$sample
  , 'file=s'      => \$file
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

    say pretty_timestamp() . " " . $msg;

}

# Check args
if ( ! $host ) {
    warn( "No --host passed. Defaulting to localhost" );
    $host = "localhost";
}

if ( ! $port ) {
    logline( "No --port passed. Defaulting to 3306" );
    $port = 3306;
}

if ( ! $username ) {
    die( "You need to pass a --username" );
}

if ( ! $database ) {
    die( "You need to pass a --database" );
}

if ( ! $action || ( $action ne "dump" && $action ne "restore" ) ) {
    die( "You need to pass an --action ... and it needs to be either 'dump' or 'restore'" );
} elsif ( $action eq 'restore' && ! $file ) {
    die( "For '--action restore' mode, you need to pass a '--file' arg" )
}

if ( ! $jobs ) {
    logline( "No --jobs passed. Defaulting to 4" );
    $jobs = 4;
}

if ( ! $directory ) {
    warn( "No --directory passed. Defaulting to /tmp" );
    $directory = "/tmp/";
}

my $db_directory = $directory . "/" . $database;

if ( $password ) {
    # If the password has been passed in on the command line, set the env variable
    $ENV{'MYSQL_PWD'} = $password;
} else {
    # Alternatively, assume it's already in the env variable, and copy it
    $password = $ENV{'MYSQL_PWD'};
}

sub get_dbh {
    
    my $connection_string =
      "dbi:mysql:"
    . "database="  . $database
    . ";host="     . $host
    . ";port="     . $port
    . ";mysql_use_result=1"     # prevent $dbh->execute() from pulling all results into memory
    . ";mysql_compression=1";   # should give a good boost when dumping tables
    
    my $dbh = DBI->connect(
        $connection_string
      , $username
      , $password
      , {
          RaiseError    => 0
        , AutoCommit    => 1
        }
    ) || die( $DBI::errstr );

    return $dbh;
        
}

sub open_dump_file {

    my ( $table , $counter ) = @_;

    my $padded_counter = sprintf "%06d", $counter; # We want to make sure these are dumped / restored in order

    my $file_path = $db_directory . "/" . $table . "." . $padded_counter . ".csv.gz";

    open( my $csv_file, "| /bin/gzip > $file_path" )
       || die( "Failed to open file for writing: [$file_path]\n" . $! );

    binmode( $csv_file, ":utf8" );

    return $csv_file;

}

sub dump_table {
    
    my ( $dbh , $table ) = @_;

    logline( "Dumping table: [$table]" );

    my $error_count  = 0;
    my $done         = 0;
    my $counter      = 0;

    while ( ( ! $done ) && $error_count < MAX_ERRORS_PER_TABLE ) {

        if ( $error_count ) {
            logline( "Retrying table [$table] - attempt #" . $error_count );
        }

        my $sql = "select * from $table";

        if ( $sample ) {
            $sql .= " limit $sample";
        }

        my $sth = $dbh->prepare( $sql )
            || die( $dbh->errstr );

        $sth->execute()
            || die( $sth->errstr );

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

        my $page_no = 0;
        my $csv_file;
        my $file_open = 0;

        eval {
            while ( my $page = $sth->fetchall_arrayref( undef, 10000 ) ) { # We want to pull 10,000 records at a time
                logline( "[$table] - Fetched page [" . comma_separated( $page_no ) . "]" );
                $page_no ++;
                if ( $counter && $counter % 1000000 == 0 ) { # We want approx 1,000,000 records per file
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

        my $err = $@;

        if ( $err ) {
            warn( "Encountered fatal error while dumping [$table]:\n$err" );
            $error_count ++;
        } else {
            $done = 1;
        }

        if ( $file_open ) {
            close $csv_file
                or die( "Failed to close file!\n" . $! );
        }

    }

    if ( ! $done ) {
        die( "Gave up dumping table [$table] - too many errors ( " . MAX_ERRORS_PER_TABLE . " )" );
    }

    logline( "========================================\n"
       . "[$table] - Wrote total of [" . comma_separated( $counter ) . "] records" );

}

sub restore_table {
    
    my ( $table , $table_file_list ) = @_;

    my $fifo_name = $table . ".csv";

    foreach my $file ( @{$table_file_list} ) {

        unlink( $fifo_name );

        my $result = POSIX::mkfifo( $fifo_name, 0600 )
            || die( "Failed to create FIFO!\n" . $! );

        my $pid = fork;

        if ( $pid ) {

            # We're the master

            logline( "Forked process with PID [$pid] for table [$table]" );

            my $dbh = get_dbh();

            $dbh->do( "set foreign_key_checks=0" )
                || die( $dbh->errstr );

            $dbh->do( "set unique_checks=0" )
                || die( $dbh->errstr );

            my $load_command = "load data local infile\n"
                             . " '" . $table . ".csv'\n"
                             . " into table $table\n"
                             . " columns\n"
                             . "    terminated by ','\n"
                             . "    optionally enclosed by '\"'\n"
                             . " ignore 1 rows";

            my $sth = $dbh->prepare( $load_command )
                || die( $dbh->errstr );

            my $records = $sth->execute()
                || die( $dbh->errstr );

            $pid = wait # it will be the same PID, and we don't use it anyway ( other than loglineging it )
                || die( "wait failed!\n" . $! );

            if ( $? ) {
                die( "gunzip for table [$table] with PID [$pid] failed!" );
            }

            logline( "Loaded [$records] records for table [$table] from file [$file]" );

        } else {

            # We're the child

            my $return = system( "gunzip -c $file > $fifo_name" );

            if ( $return ) {
                die( "gunzip returned exit code: [$return]" );
            }

            exit(0);

        }

    }

}

sub comma_separated {
    
    my ( $number ) = @_;
    
    $number =~ s/(\d)(?=(\d{3})+(\D|$))/$1\,/g;
    
    return $number;
    
}

if ( $action eq 'dump' ) {

    make_path( $db_directory );
    
    # First, we dump the schema ...
    my $remove_definer_cmd = "sed '"
                           . "s/CREATE DEFINER.*PROCEDURE/CREATE PROCEDURE/g; "
                           . "s/CREATE DEFINER.*FUNCTION/CREATE FUNCTION/g; "
                           . "s/CREATE DEFINER.*TRIGGER/CREATE TRIGGER/g; "
    #                       . "s/DEFINER=[^*]*\*/\*/g; "  # sed complains about this line
                           . "/ALTER DATABASE/d'";

    my @cmd = (
        "mysqldump"
      , "--no-data"
      , "--routines"
      , "-B" # include the 'create database' command
      , "-h" , $host
      , "-P" , $port
      , "-u" , $username 
      , $database
      , "|" , $remove_definer_cmd
      , ">" , $db_directory . "/schema.ddl"
    );

    my $args_string = join( " ", @cmd );
    logline( $args_string );

    my $return = system( $args_string );

    if ( $return ) {
        die( "mysqldump return code: [$return]" );
    } else {
        logline( "mysqldump return code: [$return]" );
    }
    
    # Next, we dump the data ...
    my $dbh = get_dbh();
    
    $dbh->do( "set session transaction isolation level repeatable read" )
        || die( "Can't set transaction isolation level to repeatable read!\n" . $dbh->errstr );
    
    my $sth = $dbh->prepare( "select TABLE_NAME from information_schema.TABLES\n"
                           . " where TABLE_TYPE like 'BASE TABLE' and TABLE_SCHEMA = ?" )
        || die( $dbh->errstr );
    
    $sth->execute( $database )
        || die( $sth->errstr );
    
    my $all_tables;

    while ( my $row = $sth->fetchrow_hashref ) {
        push @{$all_tables} , $row->{TABLE_NAME};
    }

    my $active_processes = 0;
    my $pid_to_table_map;

    if ( $jobs > 1 ) {
        
        # In this mode, we fork worker jobs to do the dumping for us

        $dbh->disconnect; # We don't want to keep this around if we're going to fork

        foreach my $table( @{$all_tables} ) {
            
            while ( $active_processes >= $jobs ) {
                
                my $pid = wait
                    || die( "wait failed!\n" . $! );
                
                $active_processes --;
                
                my $table = $pid_to_table_map->{ $pid };
                
                if ( $? ) {
                    die( "Worker for table [$table] with PID [$pid] failed!" );
                } else {
                    logline( "Worker for table [$table] with PID [$pid] completed successfully" );
                }
            }
            
            my $pid = fork;
            
            if ( $pid ) {
                
                # We're the master
                # Increment the number of active processes
                $active_processes ++;
                
                logline( "Forked process with PID [$pid] for table [$table]" );
                
                $pid_to_table_map->{ $pid } = $table;
                
            } else {
                
                # We're the child
                my $dbh = get_dbh();
                
                dump_table( $dbh , $table );
                
                exit(0);
                
            }
            
        }
        
    } else {
        
        # In this mode, we do the dumping ourself, in a transaction ( for a consistent snapshot )
        
        foreach my $table( @{$all_tables} ) {
            dump_table( $dbh , $table );
        }
        
    }

    while ( $active_processes > 0 ) {

        my $pid = wait
            || die( "wait failed!\n" . $! );

        $active_processes --;

        my $table = $pid_to_table_map->{ $pid };

        if ( $? ) {
            die( "Worker for table [$table] with PID [$pid] failed!" );
        } else {
            logline( "Worker for table [$table] with PID [$pid] completed successfully" );
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
        die( "tar return code: [$return]" );
    } else {
        logline( "tar return code: [$return]" );
    }

    $directory .= $database . "/";

    chdir( $directory );
    
    @cmd = (
        "mysql"
      , "-h" , $host
      , "-P" , $port
      , "-u" , $username
      , "<" , $directory . "schema.ddl"
    );
    
    my $args_string = join( " ", @cmd );
    logline( $args_string );
    
    $return = system( $args_string );
    
    if ( $return ) {
        die( "mysql return code: [$return]" );
    } else {
        logline( "mysql return code: [$return]" );
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
                                                    ->in( $directory )
    );
    
    foreach my $file_name ( @{$all_files_array} ) {
        my $table_name = $file_name;
        $table_name =~ s/\.(\d*)\.csv\.gz//;   # strip trailing ".00001.csv.gz"
        $table_name =~ s/.*\///;        # strip directory component ( leaving filename )
        push @{ $all_tables->{ $table_name } }, $file_name;
    }

    if ( $jobs > 1 ) {
        
        # In this mode, we fork worker jobs to do the restoring for us
        
        my $active_processes = 0;
        
        my $pid_to_table_map;
        
        foreach my $table( keys %{$all_tables} ) {
            
            while ( $active_processes >= $jobs ) {
                
                my $pid = wait
                    || die( "wait failed!\n" . $! );
                
                $active_processes --;
                
                my $table = $pid_to_table_map->{ $pid };
                
                if ( $? ) {
                    die( "Worker for table [$table] with PID [$pid] failed!" );
                } else {
                    logline( "Worker for table [$table] with PID [$pid] completed successfully" );
                }
            }
            
            my $pid = fork;
            
            if ( $pid ) {
                
                # We're the master
                # Increment the number of active processes
                $active_processes ++;
                
                logline( "Forked process with PID [$pid] for table [$table]" );
                
                $pid_to_table_map->{ $pid } = $table;
                
            } else {

                restore_table( $table , $all_tables->{ $table } );
                
                exit(0);
                
            }
            
        }
        
    } else {
        
        # In this mode, we do the dumping ourself, in a transaction ( for a consistent snapshot )

        foreach my $table( keys %{$all_tables} ) {
            restore_table( $table , $all_tables->{ $table } );
        }
        
    }

}
