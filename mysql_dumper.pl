use strict;
use warnings;

use v5.10;

use DBI;
use Getopt::Long;
use File::Basename;
use Text::CSV;

use Data::Dumper;

# @formatter:off

my ( $host
   , $port
   , $username
   , $password
   , $database
   , $action
   , $jobs
   , $directory
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
);

$directory .= "/" . $database;

# Set the mysql password so we don't expose it on the command line
$ENV{'MYSQL_PWD'} = $password;

sub get_dbh {
    
    my $connection_string =
      "dbi:mysql:"
    . "database="  . $database
    . ";host="     . $host
    . ";port="     . $port
    . ";mysql_use_result=1"; # prevent $dbh->execute() from pulling all results into memory
    
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
    
    my $file_path = $directory . "/" . $table . ".csv.bz2";
    
#    open my $csv_file, ">utf8", $file_path
#        || die( "Failed to open file for writing: [$file_path]\n" . $! );

    open( my $csv_file, "| /bin/gzip > $file_path" )
        || die( "Failed to open file for writing: [$file_path]\n" . $! );

    binmode( $csv_file, ":utf8" );

    my $sth = $dbh->prepare( "select * from $table" )
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
    my $counter = 0;
    
    while ( my $page = $sth->fetchall_arrayref( undef, 10000 ) ) {

        $page_no ++;
        say( "[$table] - Fetched page [" . comma_separated( $page_no ) . "]" );
        
        foreach my $record ( @{$page} ) {
            $csv_writer->print( $csv_file, $record );
            $counter ++;
        }
        
    };
    
    say( "========================================\n"
       . "[$table] - Wrote total of [" . comma_separated( $counter ) . "] records" );
    
    close $csv_file
        or die( "Failed to close file!\n" . $! );
    
}

sub restore_table {
    
    my ( $dbh , $table ) = @_;
    
    my $load_command = "load data local infile "
                     . "'" . $table . ".csv'\n"
                     . "into table $table\n"
                     . "columns\n"
                     . "    terminated by ','\n"
                     . "    optionally enclosed by '\"'\n"
                     . "ignore 1 rows";
    
    my $records = $dbh->do( $load_command )
        || die( $dbh->errstr );
    
    say( "[$table] = Loaded total of [" . comma_separated( $records ) . "] records" );
    
}

sub comma_separated {
    
    my ( $number ) = @_;
    
    $number =~ s/(\d)(?=(\d{3})+(\D|$))/$1\,/g;
    
    return $number;
    
}

if ( $action eq 'dump' ) {
    
    mkdir( $directory );
    
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
      , ">" , $directory . "/schema.ddl"
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
    
    if ( $jobs > 1 ) {
        
        # In this mode, we fork worker jobs to do the dumping for us

        $dbh->disconnect; # We don't want to keep this around if we're going to fork
                
        my $active_processes;
        
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

    system(
        "tar"
      , "-c"
      , "-v"
      , $directory . ".tar"
      , $directory
    );

}

if ( $action eq 'restore' ) {

    my @cmd = (
        "mysql"
      , "-h" , $host
      , "-P" , $port
      , "-u" , $username
      , "<" , $directory . "/schema.ddl"
    );
    
    my $args_string = join( " ", @cmd );
    say $args_string;
    
    my $return = system( $args_string );
    
    if ( $return ) {
        die( "mysql return code: [$return]" );
    } else {
        say "mysql return code: [$return]";
    }
    
    # Next, we restore the data ...
    my $all_tables;

    push @{$all_tables}, File::Find::Rule->file()
                                         ->name( "*.csv" )
                                         ->in( $directory );
    
    foreach my $table ( @{$all_tables} ) {
        $table =~ s/\.csv//;
    }
     
    if ( $jobs > 1 ) {
        
        # In this mode, we fork worker jobs to do the restoring for us
        
        my $active_processes;
        
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
                
                # We're the child
                my $dbh = get_dbh();
                
                restore_table( $dbh , $table );
                
                exit(0);
                
            }
            
        }
        
    } else {
        
        # In this mode, we do the dumping ourself, in a transaction ( for a consistent snapshot )

        my $dbh = get_dbh();

        foreach my $table( @{$all_tables} ) {
            restore_table( $dbh , $table );
        }
        
    }

}
