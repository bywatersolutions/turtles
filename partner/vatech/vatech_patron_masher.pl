#!/usr/bin/perl
#---------------------------------
# Copyright 2018 ByWater Solutions
#
#---------------------------------
#
# -D Ruth Bavousett
#  edited:
#     8.23.2012 Joy Nelson -added Notes for patron attribute format in runtime options
#     04.10.2018 Joy Nelson - used for vatech banner import
#     This script will take an incoming pipe delimieted| file and rename the columns.  
#     It will also do the following:
#       check for the existence of a currently existing patron and kick them out if they are in Koha with a different cardnumber
#       if cardnumber and userid match from file to Koha it will pull the patrons: dateenrolled, branchcode and categorycode (if LIBSTAFF)
#           and put that in the 'mashed borrower' file.
#       it will calculate a new expiration date for all patrons as well Based on their categorycode configuration in Koha.
#
#    This is how the current script should be run - sending any output to a log file
#     perl path/to/file/vatech_patron_masher.pl 
#        --in=/whereever/the/incomingfile/islocated/ORIG_FILE.csv 
#        --out=/wherever/youput/thefiletobeimported/MASHED_FILENAME.csv 
#        --static=sort2:CONFIDENTIAL 
#        --col="STUDENT ID:cardnumber" 
#        --col="STUDENT ID:userid" 
#        --col="LAST NAME:surname" 
#        --col="FIRST NAME:firstname" 
#        --col="MIDDLE NAME:firstname+" 
#        --col="SUFFIX NAME:surname+" 
#        --col=ENROLLED:sort1 
#        --col=CONTACT_ADDRESS_LINE1:address 
#        --col=CONTACT_ADDRESS_LINE2:address2 
#        --col=CONTACT_CITY:city 
#        --col=CONTACT_STATE:state 
#        --col=CONTACT_ZIPCODE:zipcode 
#        --col=CONTACT_FOREIGN_STATE:country 
#        --col=CONTACT_PHONE:phone 
#        --col=PERMANENT_ADDRESS_LINE1:B_address 
#        --col=PERMANENT_ADDRESS_LINE2:B_address2 
#        --col=PERMANENT_CITY:B_city 
#        --col=PERMANENT_STATE:B_state 
#        --col=PERMANENT_ZIPCODE:B_zipcode 
#        --col=PERMANENT_FOREIGN_STATE:B_country 
#        --col=PERMANENT_PHONE:B_phone 
#        --col=STUDENT_TYPE:categorycode 
#        --col="EMAIL ADDRESS:email" 
#        --col=MAJOR:EXT:STUDENTDEP 
#        --col="WHOLE RECORD CONFIDENTIAL IND:EXT:CONF" > /path/to/file/Patron_mashing.log
#
#        the log should be mailed to someone at Virginia Tech for review same as the import scripts.
#---------------------------------
#
# EXPECTS:
#   -file of pipe-delimited patron records
#
# DOES:
#   -nothing
#
# CREATES:
#   -Koha patron CSV file
#
# REPORTS:
#   -count of records manipulated
#
# Notes:
#   This script uses command-line directives to do stuff to the MARC records.  Possible directives:
#
#   --in=<filename>               Incoming csv file
#
#   --out=<filename>              Resulting csv file
#
#   --col=<colhead>:<column><~tool>
#                    Inserts data from the named column into the patron field listed; i.e. BARCODE:cardnumber.
#                     Repeatable.  Suffixable by a tool for data cleanup:
#           date     Tidies up dates, renders in ISO form
#
#   --static=<column>:<data>       Inserts static data into the named field.  Repeatable.
#
#   To specify the patron_attributes use this format:
#            --col=<colhead>:EXT:<attribute_name>
#

use Modern::Perl;

use autodie;

use Carp;
use Data::Dumper;
use DateTime;
use English qw( -no_match_vars );
use Getopt::Long;
use Readonly;
use Text::CSV_XS;
use Koha::Patrons;

local $OUTPUT_AUTOFLUSH = 1;
Readonly my $NULL_STRING => q{};

my $debug   = 0;
my $doo_eet = 0;
my $i       = 0;
my $j       = 0;
my $k       = 0;
my $written = 0;
my $problem = 0;

my $input_filename  = $NULL_STRING;
my $output_filename = $NULL_STRING;
my $csv_delim       = 'pipe';
my @col;
my @static;

GetOptions(
    'in=s'        => \$input_filename,
    'out=s'       => \$output_filename,
    'col=s'       => \@col,
    'delimiter=s' => \$csv_delim,
    'static=s'    => \@static,
    'debug'       => \$debug,
);

my %delimiter = (
    'comma' => ',',
    'tab'   => "\t",
    'pipe'  => '|',
);

for my $var ( $input_filename, $output_filename ) {
    croak("You're missing something") if $var eq $NULL_STRING;
}

my @field_mapping;
foreach my $map (@col) {
    my ( $col, $field ) = $map =~ /^(.*?):(.*)$/;
    if ( !$col || !$field ) {
        croak("--col=$map is ill-formed!\n");
    }
    push @field_mapping,
      {
        'column' => $col,
        'field'  => $field,
      };
}

#$debug and print Dumper(@field_mapping);

my @field_static;
foreach my $map (@static) {
    my ( $field, $data ) = $map =~ /^(.*?):(.*)$/;
    if ( !$field || !$data ) {
        croak("--static=$map is ill-formed!\n");
    }
    push @field_static,
      {
        'field' => $field,
        'data'  => $data,
      };
}

my @borrower_fields = qw /
  cardnumber          surname
  firstname           branchcode
  categorycode        dateenrolled
  dateexpiry          sort2
  sort1
  address             address2
  city                state
  zipcode
  email               phone
  B_address           B_address2
  B_city              B_state
  B_zipcode           userid
  privacy_guarantor_checkouts
  /;

my $csv = Text::CSV_XS->new( { binary => 1, sep_char => $delimiter{$csv_delim} } );
open my $input_file, '<', $input_filename;
$csv->column_names( $csv->getline($input_file) );

#$debug and print Dumper( $csv->column_names() );
open my $output_file, '>:utf8', $output_filename;
for my $k ( 0 .. scalar(@borrower_fields) - 1 ) {
    print {$output_file} $borrower_fields[$k] . ',';
}
print {$output_file} "patron_attributes\n";

RECORD:
while ( my $patronline = $csv->getline_hr($input_file) ) {
    last RECORD if ( $debug && $i > 10 );
    $i++;

    #    print '.'    unless ( $i % 10 );
    #    print "\r$i" unless ( $i % 100 );
    my %record;
    my $addedcode = $NULL_STRING;

    foreach my $map (@field_static) {
        $record{ $map->{'field'} } = $map->{'data'};
    }

    foreach my $map (@field_mapping) {

        #        $debug and print Dumper($map);
        if (   ( defined $patronline->{ $map->{'column'} } )
            && ( $patronline->{ $map->{'column'} } ne $NULL_STRING ) )
        {
            my $sub = $map->{'field'};

            #$debug and warn $sub;
            my $tool;
            my $appendflag;
            ( $sub, $tool ) = split( /~/, $sub, 2 );
            if ( $sub =~ /\+$/ ) {
                $sub =~ s/\+//g;
                $appendflag = 1;
            }

            my $data = $patronline->{ $map->{'column'} };
            $data =~ s/^\s+//g;
            $data =~ s/\s+$//g;

            #            $debug and print "$map->{'column'}: $data\n";

            if ($tool) {
                if ( $tool eq 'phone' ) {
                    $data =~ s/ //g;
                    $data = substr( $data, -4 );
                }

                #date is mm/dd/yy
                if ( $tool eq 'date' ) {
                    $data =~ s/ //g;
                    my ( $month, $day, $year ) = $data =~ /(\d+).(\d+).(\d+)/;
                    if ( $month && $day && $year ) {
                        my @time     = localtime();
                        my $thisyear = $time[5] + 1900;
                        $thisyear = substr( $thisyear, 2, 2 );
                        if ( $year < $thisyear ) {
                            $year += 2000;
                        }
                        elsif ( $year < 100 ) {
                            $year += 1900;
                        }
                        $data = sprintf "%4d-%02d-%02d", $year, $month, $day;
                        if ( $data eq "0000-00-00" ) {
                            $data = $NULL_STRING;
                        }
                    }
                    else {
                        $data = $NULL_STRING;
                    }
                }
            }
            if ( $data ne $NULL_STRING ) {
                if ( $sub =~ /EXT/ ) {
                    $sub =~ s/EXT://g;
                    $addedcode .= ',' . $sub . ':' . $data;
                }
                elsif ($appendflag) {
                    $record{$sub} .= ' ' . $data;
                }
                else {
                    $record{$sub} = $data;
                }
            }
        }
    }

    if ( !defined $record{cardnumber} ) {
        $record{cardnumber} = sprintf "TEMP%06d", $i;
    }

    $record{cardnumber} =~ s/ //g;

# added this exp date for Virginia Tech - will update exp to 1 year from today (updates and new users)
#per jessica on 5/2/2018 they want the exp date to update based on patron categories settings.  ?can i do that?
#    my $dt = DateTime->today();
#    $dt = $dt->add( years => 1 );
#    $dt =~ s/T00:00:00//;
#    $record{dateexpiry} = $dt;

    #assign userid from email if email is vt.edu
    if ( ( $record{email} ) && ( $record{email} =~ m/vt\.edu/ ) ) {
        my ( $user_id, $ignoreme ) = split( /@/, $record{email}, 2 );
        $record{userid} = $user_id;
    }
    if (!$record{userid}) {
        print "Skipping this record ($record{cardnumber}) as the email is not vt.edu address\n";
        $problem++;
        next RECORD;
    }
          
    my $patron = Koha::Patrons->find( { userid => $record{userid} } );
    if ($patron) {
        #check to see if the cardnumber matches existing Koha cardnumber for this user. if not skip this user and report it.
        if ($patron->cardnumber ne $record{cardnumber} ) {
         my $existingcard = $patron->cardnumber;
         print "skipping this patron with userid: $record{userid} cardnumber:$record{cardnumber} - this userid exists in Koha but with different cardnumber $existingcard\n";
         $problem++;
         next RECORD;
        }

        #alwasy preserve branchcode found in Koha for patron
        $record{branchcode} = $patron->branchcode;

        #if patron is libstaff keep categorycode if not use what's in file
        if ( $patron->categorycode eq 'LIBSTAFF' ) {
            $record{categorycode} = $patron->categorycode;
        }

        # Always retain existing dateenrolled
        $record{dateenrolled} = $patron->dateenrolled;
        
        #adding in check for privacy_guarantor_checkouts to avoid 'warn' during import
        $record{privacy_guarantor_checkouts} = $patron->privacy_guarantor_checkouts;
    }

    # New patron defaults
    $record{branchcode}   ||= 'newman';
    $record{dateenrolled} ||= DateTime->now->ymd;
    $record{privacy_guarantor_checkouts} ||= "0";
    next RECORD if ( !exists $record{categorycode} );

    # Needs to be updated for *all* patrons, both existing and new
    $record{dateexpiry} =
      Koha::Patron::Categories->find( $record{categorycode} )->get_expiry_date();
      #removing the hour from timestamp returned by get_expiry_date
      $record{dateexpiry} =~ s/T\d\d:\d\d:\d\d//;

  for $k ( 0 .. scalar(@borrower_fields) - 1 ) {
        if ( defined $record{ $borrower_fields[$k] } ) {
            $record{ $borrower_fields[$k] } =~ s/\"/'/g;
            if ( $record{ $borrower_fields[$k] } =~ /,/ ) {
                print {$output_file} '"'
                  . $record{ $borrower_fields[$k] } . '"';
            }
            else {
                print {$output_file} $record{ $borrower_fields[$k] };
            }
        }
        print {$output_file} ",";
    }
    if ($addedcode) {
        $addedcode =~ s/^,//;
        print {$output_file} qq{"$addedcode"};
    }
    
    print {$output_file} "\n";
    $written++;
}

close $input_file;
close $output_file;

print << "END_REPORT";

$i patron records read.
$problem records skipped.
$written records mashed.
END_REPORT

exit;
