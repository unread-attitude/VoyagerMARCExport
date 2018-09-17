If you want to use this script here are some simple instructions. 

Place the script in: /m1/incoming 

Make sure it is executable.

You will need to change the following parameters:


# The $org_id value is an arbitrary code used to help identify the
# output files.  You can choose anything as the value.

my $org_id     = "TEST";


# A list of email recipients for the log file. Use commas to separate
# email addresses.

my $log_recips = 'myemail@institution.edu';


# Voyager/Oracle database name

my $db_name    = "xxxdb";



# IMPORTANT -- IMPORTANT -- IMPORTANT -- IMPORTANT -- IMPORTANT 
# NOTE: Your read-only Oracle username/password may not have 
#       access to the Oracle Stored Functions.  If that is the
#       case, then you must either 1) use the production Oracle
#       username/password, or 2) contact Ex Libris support
#       to add the necessary permissions to the read-only user.

my $username   = "ro_xxxdb";
my $password   = "ro_xxxdb";



# Directory for output files, log file, and run dates file.
# Must be an existing directory; script will not create directory.

my $out_dir      = "/m1/voyager/xxxdb/rpt";


# Oracle environment variables
# The appropriate values can usually be determined by running the 
# 'env' command while logged in as the voyager user.

$ENV{ORACLE_SID} = "VGER";
#$ENV{TWO_TASK} = $ENV{ORACLE_SID};
$ENV{ORACLE_HOME} = "/oracle/app/oracle/product/12.1.0.2/db_1";



See other usage options here:  https://rocky.uta.edu/doran/voyager/export/

