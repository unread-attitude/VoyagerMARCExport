#!/m1/shared/bin/perl

######################################################################
#
#  Perl script: vmarc-export.pl
#
#  2010 Michael Doran
#  University of Texas at Arlington Libraries
#
#  2018 Maintained by decimal.uprisen.onto@gmail.com
#    with permission (implicit in this case) of Michael Doran
#
#  This is a Voyager-ILS-specific script for exporting
#    -- bibliographic, holdings, and authority MARC records
#    -- item record data
#
#  Script uses Voyager's "Oracle Stored Functions"
#  (Voyager 6.5 Technical User's Guide, section 31)
#  See IMPORTANT Oracle credentials note in config section.
#
######################################################################
#
#  Changes:
#
#  1.1  Applied RTRIM function to getBib/MFHD/AuthBlob functions
#
######################################################################
#
#  Usage:  vmarc-export.pl [options] 
#
#    Default is a full export of MARC records with no FTP transfer
#
#  Options:
#
#      --incr=[lastfull|lastincr|YYYY-MM-DD]
#
#           Incremental export of new/changed records since date
#
#           Examples:
#
#      		--incr=lastfull		from last full extraction 
#
#      		--incr=lastincr		from last incremental extraction 
#
#      		--incr=YYYY-MM-DD	from date provided
#
#           Each time the script is run the run date is output to a file.
#           Do not delete that file, since it is used to determine the
#           last full/incremental extraction date.
#
#      --library=N
#
#           Export records associated with a Voyager LIBRARY.LIBRARY_ID. 
#           Most Voyager databases will only have one "library", this
#           option is for the exceptions.
#
#           Example:
#
#      		--library=1		
#
#      --noauth
#
#           Do not export authority records.           
#
#      --noitem
#
#           Do not export item level data.           
#
#      --ftp
#
#           FTP the files of records after they are generated (per the values 
#           provided in the configuration section)
#
#      --man 	
#
#      --usage	
#
#      --help	
#
######################################################################
#
#  Terminology:
#
#    bib = MARC bibliographic [record(s)]
#   MFHD = MARC holdings [record(s)] ("MARC 21 Format for Holdings Data")
#   auth = MARC authority [record(s)]
#   item = Voyager item [record(s)|data]
#
######################################################################

# Perl modules used (plus DBD::Oracle)

use strict;
use Getopt::Long;
use Pod::Usage;
use File::Basename;
use DBI;
use Net::FTP;

######################################################################
#                                                                    #
#    >   >   >   >   >   CONFIGURATION START   <   <   <   <   <     #
#                                                                    #
######################################################################

# The $org_id value is an arbitrary code used to help identify the
# output files.  You can choose anything as the value.

my $org_id     = "TEST";

# A list of email recipients for the log file. Use commas to separate
# email addresses.

my $log_recips = 'somebody@some.site';

############################################################
#
# Oracle parameters 

# Voyager/Oracle database name

my $db_name    = "xxxdb";

# Oracle environment variables
# The appropriate values can usually be determined by running the 
# 'env' command while logged in as the voyager user.

$ENV{ORACLE_SID} = "VGER";
#$ENV{TWO_TASK} = $ENV{ORACLE_SID}; # UnComment for remote database installs
$ENV{ORACLE_HOME} = "/oracle/app/oracle/product/12.1.0.2/db_1";

# Oracle database username/password

# IMPORTANT -- IMPORTANT -- IMPORTANT -- IMPORTANT -- IMPORTANT 
# NOTE: Your read-only Oracle username/password may not have 
#       access to the Oracle Stored Functions.  If that is the
#       case, then you must either 1) use the production Oracle
#       username/password, or 2) contact Ex Libris support
#       to add the necessary permissions to the read-only user.

my $username   = "ro_xxxdb";
my $password   = "ro_xxxdb";

############################################################
#
# Directory
#
# Directory for output files, log file, and run dates file.
# Must be an existing directory; script will not create directory.

my $out_dir      = "/m1/voyager/xxxdb/rpt";

############################################################
#
# FTP parameters
#
# Only needed if you intend to automate an FTP transfer

my $ftp_hostname = '';
my $ftp_username = '';
my $ftp_password = '';
my $ftp_dir_auth = '';
my $ftp_dir_bibs = '';
my $ftp_dir_mfhd = '';
my $ftp_dir_item = '';

######################################################################
#                                                                    #
#    >   >   >   >   >    CONFIGURATION END    <   <   <   <   <     #
#                                                                    #
######################################################################

my @command_options = @ARGV;

if (! ($out_dir =~ /\/$/)) { 
    $out_dir .= "/"
}

if (! -w $out_dir) {
    print "ABORT: $out_dir is not a writable directory" . "\n"; 
    exit(1);
}

my ($dbh,$sth);

my $script_name = "$0";
my $script_vers = "1.1";

# Default of full extract 
my $export_type = 'full';

my $option_incr   = '';
my $option_lib    = '';
my $option_noauth = '';
my $option_noitem = '';
my $option_ftp    = '';
my $option_help   = '';
my $option_man    = '';

GetOptions (
    'incr=s'         => \$option_incr,
    'library=i'      => \$option_lib,
    'noauth'         => \$option_noauth,
    'noitem|noitems' => \$option_noitem,
    'ftp'            => \$option_ftp,
    'usage|help|?'   => \$option_help,
    'man'            => \$option_man
    );

pod2usage(1) if $option_help;
pod2usage(-exitstatus => 0, -verbose => 2) if $option_man;

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);

my $today_date = sprintf("%04d-%02d-%02d", $year+1900, $mon+1, $mday);

my $log_file   = "${out_dir}${org_id}-vygr-export.log";
my $date_file  = "${out_dir}${org_id}-run-dates.txt";

open (my $LOGFILE, ">$log_file")
        || die "Cannot open $log_file: $!";

open (my $DATEFILE, "+>>$date_file")
        || die "Cannot open $date_file: $!";

print $LOGFILE "Script:  $script_name" . "\n";
print $LOGFILE "Version: $script_vers" . "\n";
print $LOGFILE `date` . "\n";
print $LOGFILE "\t" . "Command options: @command_options" . "\n\n";

if ($option_incr) {
    $export_type = "incr";
}
my $incr_date = GetDate($option_incr); 

my $auth_file  = "${out_dir}${org_id}-${export_type}-auth.mrc";
my $bib_file   = "${out_dir}${org_id}-${export_type}-bibs.mrc";
my $mfhd_file  = "${out_dir}${org_id}-${export_type}-mfhd.mrc";
my $bad_file   = "${out_dir}${org_id}-${export_type}-errs.mrc";
my $item_file  = "${out_dir}${org_id}-${export_type}-item.txt";

my $auth_file_base = basename($auth_file);
my $bib_file_base  = basename($bib_file);
my $mfhd_file_base = basename($mfhd_file);
my $bad_file_base  = basename($bad_file);
my $item_file_base = basename($item_file);

open (my $BIBFILE, ">$bib_file")
        || die "Cannot open $bib_file: $!";

open (my $MFHDFILE, ">$mfhd_file")
        || die "Cannot open $mfhd_file: $!";

open (my $AUTHFILE, ">$auth_file")
        || die "Cannot open $auth_file: $!";

open (my $BADFILE, ">$bad_file")
        || die "Cannot open $bad_file: $!";

open (my $ITEMFILE, ">$item_file")
        || die "Cannot open $item_file: $!";

my $blurb_files =  qq( 
	Output directory is : $out_dir);

unless($option_noauth) {
    $blurb_files .=  qq( 
	Auth file: ${auth_file_base});
};

$blurb_files .=  qq( 
	 Bib file: ${bib_file_base}
	MFHD file: ${mfhd_file_base});

unless($option_noitem) {
    $blurb_files .=  qq( 
	Item file: ${item_file_base});
}


CheckLibraryID($option_lib);

GetRecords($incr_date,$option_lib);

ExitScript();


############################################################
#                                                          #
#  Nothing below here except us subroutines... ;-)         #
#                                                          #
############################################################


############################################################
#  GetDate
############################################################

sub GetDate {
    my ($incr_value) = @_;
    my $date = '';
    if ($incr_value =~ /lastfull/i || $incr_value =~ /lastincr/i) {

        my @run_dates = <$DATEFILE>;

        my @reversed_run_dates = reverse @run_dates;

        my $last_date = '';
      
        my $option_type = '';
        if ($incr_value =~ /full/i) {
            $option_type = "full";
        } elsif ($incr_value =~ /incr/i) {
            $option_type = "incr";
        } 

        foreach my $line (@reversed_run_dates) {
            chomp($line);
            my ($file_date, $type) = split(/\t/, $line);
            if ($incr_value =~ /full/i && $type =~ /full/i) {
                $last_date = $file_date;
                last;
            } elsif ($incr_value =~ /incr/i && $type =~ /incr/i) {
                $last_date = $file_date;
                last;
            }
        }
        if ($last_date =~ /^(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$/) {
            print $LOGFILE "\t" . "Previous $option_type extract run on $last_date " . "\n";
            $date = $last_date;
        } else {
            ExitScript('1',"ABORT: Can't determine last $option_type extract date.");
        }
    } elsif ($incr_value =~ /^(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$/) {
        if ($incr_value gt $today_date) {
            ExitScript('1',"ABORT: --incr date: $incr_value later than today's date: $today_date");
        }
        $date = $incr_value;
        print $LOGFILE "\t" . "Doing an incremental record extract using this date: $date" . "\n";
    } elsif ($incr_value) {
        ExitScript('1',"ABORT: Bad --incr option value: $incr_value");
    }
    return($date);
}


############################################################
#  CheckLibraryID
############################################################

sub CheckLibraryID {
    my ($library_integer) = @_;

    if (! $library_integer) {
         return(0);
    }

    ConnectVygrDB();
 
    my $sth = $dbh->prepare("select library_id, library_name from $db_name.library")
        || die $dbh->errstr;

    $sth->execute
        || die $dbh->errstr;

    my (%library_list);
    while( my ($library_id, $library_name) = $sth->fetchrow_array() ) {
        %library_list  = (%library_list, ($library_id => $library_name));
    }

    DisconnectVygrDB();

    if ($library_list{$library_integer}) {
        print $LOGFILE "\n" . "	Exporting records for: $library_integer - $library_list{$library_integer}" . "\n"; 
    } else {
        print "These are valid library IDs: " . "\n"; 
        foreach my $key (sort keys (%library_list)) {
            print "\t" . "$key	$library_list{$key}" . "\n";
        }
        ExitScript('1',"ABORT: '$library_integer' is an invalid library_id");
    } 
}


############################################################
#  GetRecords
############################################################

sub GetRecords {
    my ($incr_date,$library_id) = @_;

    ConnectVygrDB();

    # Setting LongReadLen is necessary when retrieving blob data.
    # MARC 21 records are limited to 99999 bytes (per Gary Strawn)
    $dbh->{LongReadLen} = 99999;

    # Remove dashes from incr date
    $incr_date =~ s/-//g;

    my $count_blurb = '';
    my $bib_count   = 0;
    my $mfhd_count  = 0;
    my $auth_count  = 0;
    my $item_count  = 0;
    my $bad_count   = 0;
    my $prev_bib_id = '';
    my ($array_ref);
    
    #
    #  Bibs and MFHDs
    #
    
    my $sth = $dbh->prepare(ConstructMarcSQL($incr_date,$library_id))
        || die $dbh->errstr;

    $sth->execute
        || die $dbh->errstr;

    while ( $array_ref = $sth->fetchrow_arrayref ) {

        my ($bib_id,
            $bib_create,
            $bib_update,
            $mfhd_id,
            $mfhd_create,
            $mfhd_update,
            $location_id,
            $bib_blob,
            $mfhd_blob
            );

	$bib_id             = $array_ref->[0];
	$bib_create         = $array_ref->[1];
	$bib_update         = $array_ref->[2];
    	$mfhd_id            = $array_ref->[3];
    	$mfhd_create        = $array_ref->[4];
    	$mfhd_update        = $array_ref->[5];
    	$location_id        = $array_ref->[6];
    	$bib_blob           = $array_ref->[7];
    	$mfhd_blob          = $array_ref->[8];

        if ($bib_id != $prev_bib_id) {
            #print $BIBFILE $bib_blob;
            if ($bib_blob =~ /\n/) {
                print $BADFILE $bib_blob;
                $bad_count++;
            } elsif ($incr_date) {
                if ($incr_date < $bib_create || $incr_date < $bib_update) {
                    print $BIBFILE $bib_blob;
                    $bib_count++;
                }
            } else {
                print $BIBFILE $bib_blob;
                $bib_count++;
            }
        }

        $prev_bib_id = $bib_id;

        if ($mfhd_blob =~ /\n/) {
            print $BADFILE $mfhd_blob;
            $bad_count++;
        } elsif ($incr_date) {
            if ($incr_date < $mfhd_create || $incr_date < $mfhd_update) {
                print $MFHDFILE $mfhd_blob;
                $mfhd_count++;
            }
        } else {
            print $MFHDFILE $mfhd_blob;
            $mfhd_count++;
        }

        # debug
        #if ($bib_count >= 100) {
        #    last;
        #}

    }

    if ($today_date && $export_type eq "full") {
        print $DATEFILE $today_date . "\t" . "full" . "\n";
    } elsif ($today_date && $export_type eq "incr") {
        print $DATEFILE $today_date . "\t" . "incr" . "\n";
    } else {
        LogError("No entry added to $date_file");
    }

    if ($option_ftp) {
        # args are:
        #  - file
        #  - type of transfer [binary|ascii]
        #  - remote directory
        if ( -s $bib_file) {
            SendFile($bib_file,'binary',$ftp_dir_bibs);
        }
        if ( -s $mfhd_file) {
            SendFile($mfhd_file,'binary',$ftp_dir_mfhd);
        }
    }
 
    #
    #  Item level data 
    #
    
    unless ($option_noitem) {

        $sth = $dbh->prepare("select location_id, location_code from $db_name.location")
            || die $dbh->errstr;

        $sth->execute
            || die $dbh->errstr;

        my %location_hash;
        while ( $array_ref = $sth->fetchrow_arrayref ) {
            $location_hash{$array_ref->[0]} = $array_ref->[1];
        }

        $sth = $dbh->prepare("select item_status_type, 
                              item_status_desc from $db_name.item_status_type")
            || die $dbh->errstr;

        $sth->execute
            || die $dbh->errstr;

        my %status_hash;
        while ( $array_ref = $sth->fetchrow_arrayref ) {
            $status_hash{$array_ref->[0]} = $array_ref->[1];
        }

        $sth = $dbh->prepare(ConstructItemSQL($incr_date))
            || die $dbh->errstr;

        $sth->execute
            || die $dbh->errstr;

        while ( $array_ref = $sth->fetchrow_arrayref ) {

            my ($bib_id,
                $mfhd_id,
                $item_id,
                $location_mfhd,
                $location_perm,
                $location_temp,
                $status,
                $enum,
                $chron,
                $year,
                $copy_number,
                $item_type_disp,
                $item_barcode,
                $item_barcode_status,
                );

            $bib_id             = $array_ref->[0];
            $mfhd_id            = $array_ref->[1];
            $item_id            = $array_ref->[2];
            $location_mfhd      = $location_hash{$array_ref->[3]};
            $location_perm      = $location_hash{$array_ref->[4]};
            $location_temp      = $location_hash{$array_ref->[5]};
            $status             = $status_hash{$array_ref->[6]};
            $enum               = $array_ref->[7];
            $chron              = $array_ref->[8];
            $year               = $array_ref->[9];
            $copy_number        = $array_ref->[10];
            $item_type_disp     = $array_ref->[11];
            $item_barcode       = $array_ref->[12];
            $item_barcode_status = $array_ref->[13];

            if ($item_id) {
                print $ITEMFILE
                      $bib_id         . "|"
                    . $mfhd_id        . "|"
                    . $item_id        . "|"
                    . $status         . "|"
                    . $item_type_disp . "|"
                    . $enum           . "|"
                    . $chron          . "|"
                    . $location_perm  . "|"
                    . $location_temp  . "|"
                    . $item_barcode   . "|"
                    . $item_barcode_status 
                    . "\n";
                $item_count++;
            }
        }
        if ($option_ftp) {
            # args are:
            #  - file
            #  - type of transfer [binary|ascii]
            #  - remote directory
            SendFile($item_file,'ascii',$ftp_dir_item);
        }
    } 

    #
    #  Authorities
    #
    
    unless ($option_noauth) {

        $sth = $dbh->prepare(ConstructAuthSQL($incr_date))
            || die $dbh->errstr;

        $sth->execute
            || die $dbh->errstr;

        while ( $array_ref = $sth->fetchrow_arrayref ) {

            my ($auth_id,
                $auth_create,
                $auth_update,
                $auth_blob
                );

	    $auth_id             = $array_ref->[0];
	    $auth_create         = $array_ref->[1];
	    $auth_update         = $array_ref->[2];
	    $auth_blob           = $array_ref->[3];

            if ($auth_blob =~ /\n/) {
                print $BADFILE $auth_blob;
                $bad_count++;
            } elsif ($incr_date) {
                if ($incr_date < $auth_create || $incr_date < $auth_update) {
                    print $AUTHFILE $auth_blob;
                    $auth_count++;
                }
            } else {
                print $AUTHFILE $auth_blob;
                $auth_count++;
            }
        }

        if ($option_ftp) {
            # args are:
            #  - file
            #  - type of transfer [binary|ascii]
            #  - remote directory
            SendFile($auth_file,'binary',$ftp_dir_auth);
        }
 
    }

    DisconnectVygrDB();

    if ($option_noauth) {
        $count_blurb = "\n";
    } else {
        $count_blurb = "	AUTHs: $auth_count" . "\n";
    }
    $count_blurb .= "	 BIBs: $bib_count" . "\n";
    $count_blurb .= "	MFHDs: $mfhd_count" . "\n";
    unless ($option_noitem) {
        $count_blurb .= "	Items: $item_count" . "\n";
    }

    print $LOGFILE $blurb_files . "\n";

    print $LOGFILE "\n" . $count_blurb;

}


############################################################
#  ConstructMarcSQL
############################################################

sub ConstructMarcSQL {
    my ($incr_date,$library_id) = @_;

    my $incr_conditional   = '';

    my $lib_id_conditional = '';

    # Never hurts to check one last time... this time sans the dashes
    if ($incr_date =~ /^(19|20)\d\d(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])$/) {
        $incr_conditional = "
        and ((to_char(bib_master.create_date, 'YYYYMMDD') > '$incr_date') or
             (to_char(bib_master.update_date, 'YYYYMMDD') > '$incr_date') or 
             (to_char(mfhd_master.create_date,'YYYYMMDD') > '$incr_date') or 
             (to_char(mfhd_master.update_date,'YYYYMMDD') > '$incr_date')) 
        "; 
    }

    if ($library_id) {
        $lib_id_conditional = "
        and bib_master.library_id = '$library_id'
        ";
    }

    return (" 
    select
	bib_master.bib_id,
	to_char(bib_master.create_date,'YYYYMMDD'),
	to_char(bib_master.update_date,'YYYYMMDD'),
	mfhd_master.mfhd_id,
	to_char(mfhd_master.create_date,'YYYYMMDD'),
	to_char(mfhd_master.update_date,'YYYYMMDD'),
	location.location_id,
	RTRIM($db_name.getBibBlob(bib_master.bib_id)),
	RTRIM($db_name.getMFHDBlob(mfhd_master.mfhd_id))
    from
        $db_name.bib_master,
        $db_name.bib_mfhd,
        $db_name.mfhd_master,
	$db_name.location
    where
        bib_master.bib_id=bib_mfhd.bib_id and
        mfhd_master.mfhd_id=bib_mfhd.mfhd_id and
        bib_master.suppress_in_opac not in 'Y' and
        mfhd_master.suppress_in_opac not in 'Y' and
        mfhd_master.location_id=location.location_id and 
        location.suppress_in_opac not in 'Y' 
        $incr_conditional
        $lib_id_conditional
    ");
}


############################################################
#  ConstructAuthSQL
############################################################

sub ConstructAuthSQL {
    my ($incr_date) = @_;

    my $incr_conditional   = '';

    # Never hurts to check one last time... this time sans the dashes
    if ($incr_date =~ /^(19|20)\d\d(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])$/) {
        $incr_conditional = "
    where
        (to_char(auth_master.create_date, 'YYYYMMDD') > '$incr_date') or
        (to_char(auth_master.update_date, 'YYYYMMDD') > '$incr_date') 
        "; 
    }

    return (" 
    select
	auth_master.auth_id,
	to_char(auth_master.create_date,'YYYYMMDD'),
	to_char(auth_master.update_date,'YYYYMMDD'),
	RTRIM($db_name.getAuthBlob(auth_master.auth_id))
    from
        $db_name.auth_master
    $incr_conditional
    ");

}


############################################################
#  ConstructItemSQL
############################################################

sub ConstructItemSQL {
    my ($incr_date) = @_;

    my $incr_conditional   = '';

    # Never hurts to check one last time... this time sans the dashes
    if ($incr_date =~ /^(19|20)\d\d(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])$/) {
        $incr_conditional = "
        and ((to_char(item.create_date, 'YYYYMMDD') > '$incr_date') or
             (to_char(item.modify_date, 'YYYYMMDD') > '$incr_date') or
             (to_char(item_status.item_status_date, 'YYYYMMDD') > '$incr_date')) 
        "; 
    }

    #    $incr_conditional = "
    #    and ((to_char(item.create_date, 'YYYYMMDD') > '$incr_date') or
    #         (to_char(item.modify_date, 'YYYYMMDD') > '$incr_date')) 
    #    "; 

    return ("
    select distinct
        bib_master.bib_id,
        mfhd_master.mfhd_id,
        item.item_id,
        mfhd_master.location_id,
        item.perm_location,
        item.temp_location,
        $db_name.getLatestItemStatus(item.item_id),
        mfhd_item.item_enum,
        mfhd_item.chron,
        mfhd_item.year,
        item.copy_number,
        item_type.item_type_display,
        item_barcode.item_barcode,
        item_barcode.barcode_status
    from
        $db_name.bib_master,
        $db_name.bib_mfhd,
        $db_name.mfhd_master,
        $db_name.mfhd_item,
        $db_name.item,
        $db_name.item_status,
        $db_name.item_type,
        $db_name.item_barcode,
        $db_name.location permanent,
        $db_name.location temporary,
        $db_name.location holdings
    where
        bib_master.bib_id=bib_mfhd.bib_id and
        mfhd_master.mfhd_id=bib_mfhd.mfhd_id and
        mfhd_master.mfhd_id=mfhd_item.mfhd_id(+) and
        mfhd_item.item_id=item.item_id(+) and
        mfhd_item.item_id=item_status.item_id(+) and
        item.item_type_id=item_type.item_type_id(+) and
        mfhd_item.item_id=item_barcode.item_id(+) and
        mfhd_master.suppress_in_opac not in 'Y' and
        bib_master.suppress_in_opac not in 'Y' and
        item.perm_location=permanent.location_id(+) and
        item.temp_location=temporary.location_id(+) and
        mfhd_master.location_id=holdings.location_id(+) and
        (permanent.suppress_in_opac not in 'Y' or
         temporary.suppress_in_opac not in 'Y' or
          holdings.suppress_in_opac not in 'Y')
        $incr_conditional
    order by
        bib_master.bib_id,
        mfhd_master.mfhd_id,
        item.item_id
    ");
}


############################################################
#  SendFile
############################################################
#
#  FTPs extracted files

sub SendFile {
    my ($file,$type,$remote_dir) = @_;

    unless($file && $type) {
        return(1);
    }

    #my $ftp = Net::FTP->new("foobar", Debug => 0)
    my $ftp = Net::FTP->new("$ftp_hostname", Debug => 0)
	or (LogError("Cannot connect to $ftp_hostname: $@")
	 && return(1));

    $ftp->login("$ftp_username","$ftp_password")
	or (LogError("Cannot login : $ftp->message")
	 && return(1));

    if ($remote_dir) {
        $ftp->cwd("$remote_dir")
	    or (LogError("Cannot change to remote directory $remote_dir : $ftp->message")
	     && return(1));
    }

    if ($type =~ /binary/i) {
        $ftp->binary()
	    or (LogError("Cannot specify $type : $ftp->message")
	     && return(1));
    } elsif ($type =~ /ascii/i) {
        $ftp->ascii()
	    or (LogError("Cannot specify $type : $ftp->message")
	     && return(1));
    } else {
	    LogError("No transfer \$type specified in SendFile");
    }

    $ftp->put("$file")
	or (LogError("Cannot put $file : $ftp->message")
	 && return(1));

    $ftp->quit;

    print $LOGFILE "\t" . "FTP'd $file to $ftp_hostname:$remote_dir" . "\n";

}


############################################################
#  ConnectVygrDB
############################################################
#
#  Connects to the Voyager database
#  (in read-only mode, natch!)

sub ConnectVygrDB {
    $dbh = DBI->connect('dbi:Oracle:', $username, $password)
        || die "Could not connect: $DBI::errstr";
}

############################################################
#  DisconnectVygrDB
############################################################
#
#  Exits gracefully from the Voyager database

sub DisconnectVygrDB {
    if ($sth) {
        $sth->finish;
    }
    $dbh->disconnect;
}


############################################################
#  LogError
############################################################
#
#  Puts error message into log file.
#
#  Mainly for errors that you do not want to abort script. 

sub LogError {
    my ($message) = @_;
    print $LOGFILE "\n";
    print $LOGFILE "BAD ERROR" . "\n";
    print $LOGFILE "BAD ERROR: $message" . "\n";
    print $LOGFILE "BAD ERROR" . "\n";
    print $LOGFILE "\n";
}



############################################################
#  ExitScript
############################################################
#
#  Exits script

sub ExitScript {
    my ($exit_status,$message) = @_;

    if ($message) {
        print $LOGFILE "$message" . "\n";
        if ($exit_status > 0) {
            print "$message" . "\n";
        }
    }

    print $LOGFILE "\n" . `date` . "\n";

    # Close any open files
    if ($BIBFILE) { 
        close ($BIBFILE);
    }
    if ($MFHDFILE) { 
        close ($MFHDFILE);
    }
    if ($BADFILE) { 
        close ($BADFILE);
    }
    if ($LOGFILE) { 
        close ($LOGFILE);
    }
    if ($DATEFILE) { 
        close ($DATEFILE);
    }

    if ($option_ftp) {
        # args are:
        #  - file
        #  - type of transfer [binary|ascii]
        #  - remote directory
        SendFile($log_file,'ascii');
    }

    if ( -s $log_file) {
        system qq(cat $log_file | /usr/bin/mailx -s "Log: $script_name" $log_recips); 
    }

    if ($exit_status =~ /\d{1,2}/) {
        exit($exit_status);
    } else {
        exit(0);
    }
}
 
# This exit command shouldn't be used, so will give an exit status of 1
exit(1);

__END__

=head1 NAME

vmarc-export.pl

=head1 SYNOPSIS

vmarc-export.pl [options]

    Default is a full export of MARC records with no FTP

=head1 OPTIONS

  --incr=[lastfull|lastincr|YYYY-MM-DD]

     Incremental export of new/changed records since date

     Examples:

        --incr=lastfull         from last full extraction

        --incr=lastincr         from last incremental extraction

        --incr=YYYY-MM-DD       from date provided


  --library=N

    Export records associated with a Voyager library_id 

    Example:

      	--library=1		

  --noauth

     Do not export authority records.

  --noitem

     Do not export item level data.

  --ftp

     FTP the files of records after they are generated

  --man

  --usage

  --help

=head1 DESCRIPTION

  This is a Voyager ILS specific script for exporting
  bibliographic, authority, and holdings records.

  Script uses Voyager's "Oracle Stored Functions"
  (Voyager 6.5 Technical User's Guide, section 31)

=cut
