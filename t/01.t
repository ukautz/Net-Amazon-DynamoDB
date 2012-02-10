#!/usr/bin/perl


use strict;
use warnings;
use Test::More;
use FindBin qw/ $Bin /;
use lib "$Bin/../lib";

use_ok( "Net::Amazon::DynamoDB" );

SKIP: {
    
    my $ddb = Net::Amazon::DynamoDB->new(
        access_key => $ENV{ AWS_ACCESS_KEY_ID },
        secret_key => $ENV{ AWS_SECRET_ACCESS_KEY },
        tables     => {
            'sometable' => {
                hash_key => 'id',
                attributes  => {
                    id => 'N',
                    name => 'S'
                }
            },
            'othertable' => {
                hash_key => 'id',
                range_key => 'range_id',
                attributes  => {
                    id => 'N',
                    range_id => 'N',
                    attrib1 => 'S',
                    attrib2 => 'S'
                }
            }
        }
    );
    
    # $ddb->create_table( 'sometable' => 10, 5 )
    #     unless $ddb->exists_table( 'sometable' );
    # $ddb->create_table( 'othertable' => 12, 6 )
    #     unless $ddb->exists_table( 'othertable' );
    # exit;
    
    # $ddb->put_item( 'sometable' => {
    #     id   => 3,
    #     name => 'test was anderes '. localtime()
    # } );
    #$ddb->exists_table( 'sometable' );
    
    # $ddb->create_table( 'sometable' => 10, 5 )
    #     unless $ddb->exists_table( 'sometable' );
    # $ddb->describe_table( 'sometable' );
    
    # $ddb->delete_table( 'sometable' );
    #$ddb->create_table( 'sometable' => 10, 5 );
    #$ddb->put_item( 'othertable' => {
    #     id       => $_,
    #     range_id => $_ * 2,
    #     attrib1  => 'test was anderes '. localtime(),
    #     attrib2  => 'Irgend was anderes '. time()
    # }, undef, 1 ) for 1..5;
    #sleep 5;
    #$ddb->describe_table( 'othertable' );
    $ddb->scan_items( 'sometable', { id => 1 }, { limit => 10 } );
    #$ddb->query_items( 'othertable', { id => 1, range_id => 2 }, { limit => 10 } );
    # my $start = time();
    # foreach my $i( 1..100 ) {
    #     $ddb->get_item( 'othertable', { id => 1, range_id => $i * 2 } );
    # }
    # my $end = time();
    # print "TOOK ". ( $end - $start ). "sec -> ". sprintf( '%0.2f', 100 / ( ( $end - $start ) || 1 ) ). "r/sec\n";
    print "ERR ". $ddb->error(). "\n";
}
