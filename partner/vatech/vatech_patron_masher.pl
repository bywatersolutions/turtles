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
#
#---------------------------------
#
# EXPECTS:
#   -file of CSV-delimited patron records
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

my @borrower_fields = qw /cardnumber          surname
  firstname           sort2
  sort1
  address             address2
  city                state
  zipcode
  email               phone
  B_address           B_address2
  B_city              B_state
  B_zipcode
  categorycode        userid
  /;

my $csv =  Text::CSV_XS->new( { binary => 1, sep_char => $delimiter{$csv_delim} } );
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

    my $patron = Koha::Patrons->find( { cardnumber => $record{cardnumber} } );
    if ($patron) {

        #alwasy preserve branchcode found in Koha for patron
        $record{branchcode} = $patron->branchcode;

        #if patron is libstaff keep categorycode if not use what's in file
        if ( $patron->categorycode eq 'LIBSTAFF' ) {
            $borrower{categorycode} = $Apatron->categorycode;
        }

        # Always retain existing dateenrolled
        $borrower{dateenrolled} = $patron->dateenrolled;
    }

    # New patron defaults
    $borrower{branchcode} ||= 'newman';
    $borrower{dateenrolled} ||= DateTime->now->ymd;

    next RECORD if ( !exists $record{categorycode} );

    # Needs to be updated for *all* patrons, both existing and new
    $record{dateexpiry} = Koha::Patron::Categories->find($record{categorycode})->get_expiry_date()

    #assign userid from email if email is vt.edu
    if ( ($record{email}) && ($record{email} =~ m/vt\.edu/ ) ) {
       my ($user_id, $ignoreme) = split(/@/,$record{email},2);
       $record{userid} = $user_id;
    }

    for $k ( 0 .. scalar(@borrower_fields) - 1 ) {
        if ( $record{ $borrower_fields[$k] } ) {
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
        print {$output_file} '"' . "$addedcode" . '"';
    }
    print {$output_file} "\n";
    $written++;
}

close $input_file;
close $output_file;

exit;
