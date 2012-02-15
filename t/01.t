#!/usr/bin/perl


use strict;
use warnings;
use Test::More tests => 2;
use FindBin qw/ $Bin /;
use Data::Dumper;
use lib "$Bin/../lib";

use_ok( "Net::Amazon::DynamoDB" );

SKIP: {
    
    skip 'No AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY set in ENV. Not running any tests.', 1
        unless defined $ENV{ AWS_ACCESS_KEY_ID } && defined $ENV{ AWS_SECRET_ACCESS_KEY };
    
    subtest( 'Online Tests' => sub {
        
        # create ddb
        my $ddb = eval { Net::Amazon::DynamoDB->new(
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
        ) };
        if ( $@ ) {
            # <<< HERE
        }
        
        # $ddb->exists_table( $_ ) || $ddb->create_table( $_, 10, 5 )
        #     for qw/ sometable othertable /;
        #print Dumper( { TABLES => $ddb->list_tables() } );
        
        #$ddb->update_table( sometable => 10, 6 );
        $ddb->describe_table( 'sometable' );
        
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
        # print Dumper( { ITEM => $ddb->put_item( 'sometable' => {
        #     id       => $_,
        #     name     => "Item $_"
        #     # range_id => $_ * 2,
        #     # attrib1  => 'test was anderes '. localtime(),
        #     # attrib2  => 'Irgend was anderes '. time()
        # }, { id => 1 }, 1 ) } ) for 1..1;
        #sleep 5;
        #$ddb->describe_table( 'othertable' );
        #print Dumper( { r => [ $ddb->scan_items( 'sometable', { id => 1 }, { limit => 10 } ) ] } );
        #print Dumper( { r => [ $ddb->update_item( 'sometable', { name => 'Bla Bla Blub' }, { id => 1, name => 'asdasd' }, 1 ) ] } );
        #$ddb->scan_items( 'othertable', { id => 1, range_id => 2 }, { limit => 10 } );
        #$ddb->scan_items( 'othertable', undef, { count => 1, limit => 2 } );
        # print Dumper( { R => [ $ddb->batch_get_item( {
        #     sometable => [
        #         { id => 1 },
        #         { id => 2 },
        #         { id => 3 },
        #     ],
        #     othertable => [
        #         { id => 1, range_id => 2 },
        #         { id => 2, range_id => 4 },
        #         { id => 3, range_id => 6 },
        #     ]
        # } ) ] } );
        #$ddb->delete_item( 'sometable', { id => 1 } );
        #$ddb->query_items( 'othertable', { id => 1, range_id => 2 }, { limit => 10 } );
        # my $start = time();
        # foreach my $i( 1..100 ) {
        #     $ddb->get_item( 'othertable', { id => 1, range_id => $i * 2 } );
        # }
        # my $end = time();
        # print "TOOK ". ( $end - $start ). "sec -> ". sprintf( '%0.2f', 100 / ( ( $end - $start ) || 1 ) ). "r/sec\n";
        
        #print Dumper( { r => [ $ddb->delete_table( 'sometable' ) ] } );
        #print Dumper( { r => [ $ddb->describe_table( 'othertable' ) ] } );
        print "ERR ". $ddb->error(). "\n";
    } );
}
