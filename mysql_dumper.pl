#!/usr/bin/perl

use strict;
use warnings;

use v5.10;

use DBI;
use Getopt::Long;
use File::Basename;
use File::Find::Rule;
use File::Path qw | make_path rmtree |;
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

# Check args
if ( ! $host ) {
    warn( "No --host passed. Defaulting to localhost" );
    $host = "localhost";
}

if ( ! $port ) {
    say "No --port passed. Defaulting to 3306";
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
    say "No --jobs passed. Defaulting to 4";
    $jobs = 4;
}

if ( ! $directory ) {
    warn( "No --directory passed. Defaulting to /tmp" );
    $directory = "/tmp";
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

sub dump_table {
    
    my ( $dbh , $table ) = @_;
    
    say "Dumping table: [$table]";
    
    my $file_path = $db_directory . "/" . $table . ".csv.gz";

    my $error_count = 0;
    my $done        = 0;
    my $counter     = 0;

    while ( ( ! $done ) && $error_count < MAX_ERRORS_PER_TABLE ) {

        if ( $error_count ) {
            say "Retrying table [$table] - attempt #" . $error_count;
        }

        open( my $csv_file, "| /bin/gzip > $file_path" )
            || die( "Failed to open file for writing: [$file_path]\n" . $! );

        binmode( $csv_file, ":utf8" );

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

        # Write CSV header ( ie columns )
        my $fields = $sth->{NAME};
        print $csv_file join( "," , @{$fields} ) . "\n";

        my $page_no = 0;

        eval {
            while ( my $page = $sth->fetchall_arrayref( undef, 10000 ) ) {
                $page_no ++;
                say( "[$table] - Fetched page [" . comma_separated( $page_no ) . "]" );
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

        close $csv_file
            or die( "Failed to close file!\n" . $! );

    }

    if ( ! $done ) {
        die( "Gave up dumping table [$table] - too many errors ( " . MAX_ERRORS_PER_TABLE . " )" );
    }

    say( "========================================\n"
       . "[$table] - Wrote total of [" . comma_separated( $counter ) . "] records" );

}

sub restore_table {
    
    my ( $table ) = @_;

    my $fifo_name = $table . ".csv";

    my $result = POSIX::mkfifo( $fifo_name, 0600 )
        || die( "Failed to create FIFO!\n" . $! );

    my $pid = fork;

    if ( $pid ) {

        # We're the master

        say "Forked process with PID [$pid] for table [$table]";

        my $dbh = get_dbh();

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

        my $pid = wait
            || die( "wait failed!\n" . $! );

        if ( $? ) {
            die( "gunzip for table [$table] with PID [$pid] failed!" );
        }

        say "Loaded [$records] records for table [$table]";

    } else {

        # We're the child

        my $return = system( "gunzip -c " . $table . ".csv.gz > $fifo_name" );

        if ( $return ) {
            die( "gunzip returned exit code: [$return]" );
        }

        exit(0);

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
    say $args_string;

    my $return = system( $args_string );

    if ( $return ) {
        die( "mysqldump return code: [$return]" );
    } else {
        say "mysqldump return code: [$return]";
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
                    say "Worker for table [$table] with PID [$pid] completed successfully";
                }
            }
            
            my $pid = fork;
            
            if ( $pid ) {
                
                # We're the master
                # Increment the number of active processes
                $active_processes ++;
                
                say "Forked process with PID [$pid] for table [$table]";
                
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
            say "Worker for table [$table] with PID [$pid] completed successfully";
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
    
    say "Unpacking archive ...";
    
    my @cmd = (
        "tar"
      , "-xvf"
      , $file
    );
    
    my $return = system( join( " ", @cmd ) );

    if ( $return ) {
        die( "tar return code: [$return]" );
    } else {
        say "tar return code: [$return]";
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
    say $args_string;
    
    $return = system( $args_string );
    
    if ( $return ) {
        die( "mysql return code: [$return]" );
    } else {
        say "mysql return code: [$return]";
    }
    
    # Next, we restore the data ...
    my $all_tables;

    push @{$all_tables}, File::Find::Rule->file()
                                         ->name( "*.csv.gz" )
                                         ->in( $directory );
    
    foreach my $table_name ( @{$all_tables} ) {
        $table_name =~ s/\.csv\.gz//;   # strip trailing ".csv.gz"
        $table_name =~ s/.*\///;        # strip directory component ( leaving filename )
    }
     
    if ( $jobs > 1 ) {
        
        # In this mode, we fork worker jobs to do the restoring for us
        
        my $active_processes = 0;
        
        my $pid_to_table_map;
        
        foreach my $table( @{$all_tables} ) {
            
            while ( $active_processes >= $jobs ) {
                
                my $pid = wait
                    || die( "wait failed!\n" . $! );
                
                $active_processes --;
                
                my $table = $pid_to_table_map->{ $pid };
                
                if ( $? ) {
                    die( "Worker for table [$table] with PID [$pid] failed!" );
                } else {
                    say "Worker for table [$table] with PID [$pid] completed successfully";
                }
            }
            
            my $pid = fork;
            
            if ( $pid ) {
                
                # We're the master
                # Increment the number of active processes
                $active_processes ++;
                
                say "Forked process with PID [$pid] for table [$table]";
                
                $pid_to_table_map->{ $pid } = $table;
                
            } else {

                restore_table( $table );
                
                exit(0);
                
            }
            
        }
        
    } else {
        
        # In this mode, we do the dumping ourself, in a transaction ( for a consistent snapshot )

        foreach my $table( @{$all_tables} ) {
            restore_table( $table );
        }
        
    }

}
