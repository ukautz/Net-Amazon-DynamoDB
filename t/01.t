#!/usr/bin/perl


use strict;
use warnings;
use Test::More tests => 4;
use FindBin qw/ $Bin /;
use Data::Dumper;
use lib "$Bin/../lib";

use_ok( "Net::Amazon::DynamoDB" );
use_ok( 'Cache::Memory' );

SKIP: {
    
    skip "No AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY set in ENV. Not running any tests.\n"
        ."CAUTION: Tests require to create new tables which will cost you money!!", 2
        unless defined $ENV{ AWS_ACCESS_KEY_ID } && defined $ENV{ AWS_SECRET_ACCESS_KEY };
    my $table_prefix = $ENV{ AWS_TEST_TABLE_PREFIX } || 'test_';
    
    my @tests = (
        {
            namespace => '',
        },
        {
            namespace => 'cached_',
            cache     => Cache::Memory->new(),
        }
    );
    
    foreach my $test_ref( @tests ) {
        
        subtest( 'Online Tests ['. join( ', ', map {
            sprintf( '%s: %s', $_, defined $test_ref->{ $_ } && $test_ref->{ $_ } ? 'yes' : 'no' );
        } qw/ namespace cache / ). ']' => sub {
            
            my $table1 = $table_prefix. 'table1';
            my $table2 = $table_prefix. 'table2';
            my $table3 = $table_prefix. 'table3';
            
            # create ddb
            my $ddb = eval { Net::Amazon::DynamoDB->new(
                %$test_ref,
                access_key  => $ENV{ AWS_ACCESS_KEY_ID },
                secret_key  => $ENV{ AWS_SECRET_ACCESS_KEY },
                raise_error => 1,
                tables      => {
                    $table1 => {
                        hash_key => 'id',
                        attributes  => {
                            id => 'N',
                            name => 'S'
                        }
                    },
                    $table2 => {
                        hash_key => 'id',
                        range_key => 'range_id',
                        attributes  => {
                            id => 'N',
                            range_id => 'N',
                            attrib1 => 'S',
                            attrib2 => 'S'
                        }
                    },
                    $table3 => {
                        hash_key => 'id',
                        attributes  => {
                            id   => 'N',
                            data => 'B'
                        }
                    },
                }
            ) };
            BAIL_OUT( "Failed to instantiate Net::Amazon::DynamoDB: $@" ) if $@;
            
            # create tables
            foreach my $table( $table1, $table2, $table3 ) {
                if ( $ddb->exists_table( $table ) ) {
                    pass( "Table $table already exists" );
                    next;
                }
                my $create_ref = $ddb->create_table( $table, 10, 5 );
                ok( $create_ref && ( $create_ref->{ status } eq 'ACTIVE' || $create_ref->{ status } eq 'CREATING' ), "Create response for $table" );
                
                subtest( "Waiting for $table being created", sub {
                    if ( $create_ref->{ status } eq 'ACTIVE' ) {
                        plan skip_all => "Table $table already created";
                        return;
                    }
                    plan tests => 1;
                    foreach my $num( 1..60 ) {
                        my $desc_ref = $ddb->describe_table( $table );
                        if ( $desc_ref && $desc_ref->{ status } eq 'ACTIVE' ) {
                            pass( "Table $table has been created" );
                            last;
                        }
                        sleep 1;
                    }
                } );
            }
            
            # put test
            ok( $ddb->put_item( $table1 => { id => 1, name => "First entry" } ), "First entry in $table1 created" );
            
            # put large binary
            my $data = pack("C*",map { $_ % 256 } 0..65526);
            ok( $ddb->put_item( $table3 => { id => 1, data => $data } ), "Large binary entry in in $table3 created" );

            # Get binary back
            my $bin_read_ref = $ddb->get_item( $table3 => { id => 1 } );
            ok( $bin_read_ref && $bin_read_ref->{ data } eq $data, 'Returned binary data matches' );

            my $data2 = pack("C*",map { $_ % 256 } 0..500);

            my $update_ref = $ddb->update_item( $table3 => { data => $data2 }, { id => 1 }, {
                return_mode => 'ALL_NEW'
            } );
            ok( $update_ref && $update_ref->{ data } eq $data2, "Binary update in $table3 ok" );

            # read test
            my $read_ref = $ddb->get_item( $table1 => { id => 1 } );
            ok( $read_ref && $read_ref->{ id } == 1 && $read_ref->{ name } eq 'First entry', "First entry from $table1 read" );
            
            # update test
            $update_ref = $ddb->update_item( $table1 => { name => "Updated first entry" }, { id => 1 }, {
                return_mode => 'ALL_NEW'
            } );
            ok( $update_ref && $update_ref->{ name } eq 'Updated first entry', "Update in $table1 ok" );
            
            # create multiple in table1
            foreach my $num( 2..10 ) {
                $ddb->put_item( $table1 => { id => $num, name => "${num}. entry" } )
            }
            
            # scan search in table1
            my $search_ref = $ddb->scan_items( $table1 );
            ok( $search_ref && scalar( @$search_ref ) == 10, "Scanned for 10 items in $table1" );
            #print Dumper( $search_ref );
            
            # create multiple in table2 in range table and search there
            foreach my $num( 1..10 ) {
                $ddb->put_item( $table2 => {
                    id       => ( $num % 2 )+ 1,
                    range_id => $num,
                    attrib1  => "The time string ". localtime(),
                    attrib2  => "The time unix ". time()
                } );
            }
            my $query_ref = $ddb->query_items( $table2 => { id => 1, range_id => { GT => 5 } } );
            ok( $query_ref && scalar( @$query_ref ) == 3, "Query for 3 items in $table2" );
            
            # batch_write test
            $ddb->batch_write_item({
                $table1 => {
                  put => [ { id => 11, name => "11entry" } ],
                },
                $table3 => {
                  put => [ { id => 2, data => $data } ],
                },
            });

            # Get string back
            my $batched_str_ref = $ddb->get_item( $table1 => { id => 11 } );
            ok( $batched_str_ref && $batched_str_ref->{ name } eq '11entry',
                'Returned string data from batch write matches'
            );


            # Get binary back
            my $batched_bin_ref = $ddb->get_item( $table3 => { id => 2 } );
            ok( $batched_bin_ref && $batched_bin_ref->{ data } eq $data,
                'Returned binary data from batch write matches'
            );

            # Test batch get with derive_table on and off
            for my $derive_table ( 0, 1 ) {
                $ddb->derive_table($derive_table);
                # batch get multiple
                my $batch_ref = $ddb->batch_get_item( {
                    $table1 => [
                        { id => 1 },
                        { id => 10 }
                    ],
                    $table2 => [
                        { id => 2, range_id => 1 },
                        { id => 1, range_id => 2 },
                    ],
                    $table3 => [
                        { id => 2 },
                    ],
                } );

                #print Dumper( $batch_ref );
                ok(
                    defined $batch_ref->{ $table1 } && scalar( @{ $batch_ref->{ $table1 } } ) == 2
                    && defined $batch_ref->{ $table2 } && scalar( @{ $batch_ref->{ $table2 } } ) == 2
                    && defined $batch_ref->{ $table3 } && scalar( @{ $batch_ref->{ $table3 } } ) == 1,
                    "Found 5 entries from $table1, $table2 and $table3 with batch get (derive_table = $derive_table)"
                );

                ok( $batch_ref->{ $table3 }->[0]->{data} eq $data, "Binary data returned from batch_get matches (derive_table = $derive_table)");
            }

            # clean up
            foreach my $table( $table1, $table2, $table3 ) {
                ok( $ddb->delete_table( $table ), "Table $table delete initialized" );
                
                foreach my $num( 1..60 ) {
                    unless( $ddb->exists_table( $table ) ) {
                        pass( "Table $table is deleted" );
                        last;
                    }
                }
            }
        } );
    }
}
