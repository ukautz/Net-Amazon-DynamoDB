#!/usr/bin/perl


use strict;
use warnings;
use FindBin qw/ $Bin /;
use Data::Dumper;
use lib "$Bin/../lib";
use Net::Amazon::DynamoDB;
use Cache::File;

$| = 1;

my $table_prefix = $ENV{ AWS_TEST_TABLE_PREFIX } || 'test_';
my $table = $table_prefix. 'sometable';

# create ddb
my $ddb = Net::Amazon::DynamoDB->new(
    access_key => $ENV{ AWS_ACCESS_KEY_ID },
    secret_key => $ENV{ AWS_SECRET_ACCESS_KEY },
    namespace  => 'dings_',
    cache      => Cache::File->new( cache_root => '/tmp/testcache', default_expires => '60 sec' ),
    tables     => {
        $table => {
            hash_key  => 'hid',
            attributes  => {
                hid       => 'S',
                str_array => 'SS',
                num_array => 'NS',
            }
        }
    },
    raise_error => 1
);

print "Create table $table?\n";
$ddb->create_table( $table, 10, 5 )
    unless $ddb->exists_table( $table );
print "Waiting for $table ";
while(1) {
    my $descr_ref = $ddb->describe_table( $table );
    last if $descr_ref && $descr_ref->{ status } eq 'ACTIVE';
    print ".";
    sleep 1;
}
print " OK\n";

print "Put items ";
$ddb->put_item( $table => {
    hid       => 'something'. $_,
    str_array => [ "bla $_" ],
    num_array => [ $_ ** 2 ]
} ) for 1..10;
print " OK\n";

print "Get items 1 ";
my $start = time();
$ddb->get_item( $table => {
    hid       => 'something'. $_,
    str_array => [ "bla $_" ],
    num_array => [ $_ ** 2 ]
} ) for 1..10;
my $end = time();
print " OK (". ( $end - $start ). " secs)\n";

print "Get items 2 ";
$start = time();
$ddb->get_item( $table => {
    hid       => 'something'. $_,
    str_array => [ "bla $_" ],
    num_array => [ $_ ** 2 ]
} ) for 1..10;
$end = time();
print " OK (". ( $end - $start ). " secs)\n";

print "Put items 2";
$ddb->put_item( $table => {
    hid       => 'something'. $_,
    str_array => [ "bla $_" ],
    num_array => [ $_ ** 2 ]
} ) for 1..10;
print " OK\n";

print "Get items 3 ";
$start = time();
$ddb->get_item( $table => {
    hid       => 'something'. $_,
    str_array => [ "bla $_" ],
    num_array => [ $_ ** 2 ]
} ) for 1..10;
$end = time();
print " OK (". ( $end - $start ). " secs)\n";

print "Get items 4 ";
$start = time();
$ddb->get_item( $table => {
    hid       => 'something'. $_,
    str_array => [ "bla $_" ],
    num_array => [ $_ ** 2 ]
} ) for 1..10;
$end = time();
print " OK (". ( $end - $start ). " secs)\n";

