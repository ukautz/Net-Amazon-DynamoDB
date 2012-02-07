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
        secret_key => $ENV{ AWS_SECRET_ACCESS_KEY }
    );
    # $ddb->create_table( 'some-table',
    #     read_amount      => 10,
    #     write_amount     => 5,
    #     primary_key      => 'id',
    #     primary_key_type => 'N'
    # );
    # use Data::Dumper;
    # use JSON;
    # my $str = '{"TableDescription":{"CreationDateTime":1.328630494765E9,"KeySchema":{"HashKeyElement":{"AttributeName":"id","AttributeType":"N"}},"ProvisionedThroughput":{"ReadCapacityUnits":10,"WriteCapacityUnits":5},"TableName":"some-table","TableStatus":"CREATING"}}';
    # print Dumper( JSON::decode_json( $str ) );
    $ddb->put_item( 'some-table' => {
        item => {
            id => { N => 1 },
            name => { S => 'test' }
        }
    } );
    print "ERR ". $ddb->error(). "\n";
}