package Net::Amazon::DynamoDB;

=head1 NAME

Net::Amazon::DynamoDB - Simple interface for Amazon DynamoDB

=head1 DESCRIPTION

Simple to use interface for Amazon DynamoDB

If you want an ORM-like interface with real objects to work with, this is implementation is not for you. If you just want to access DynamoDB in a simple/quick manner - you are welcome.

See L<https://github.com/ukautz/Net-Amazon-DynamoDB> for latest release.

=head1 SYNOPSIS

    my $ddb = Net::Amazon::DynamoDB->new(
        access_key => $my_access_key,
        secret_key => $my_secret_key,
        tables     => {
            
            # table with only hash key
            sometable => {
                hash_key   => 'id',
                attributes => {
                    id   => 'N',
                    name => 'S'
                }
            },
            
            # table with hash and reange key key
            othertable => {
                hash_key   => 'id',
                range_key  => 'range_id',
                attributes => {
                    id       => 'N',
                    range_id => 'N',
                    attrib1  => 'S',
                    attrib2  => 'S'
                }
            }
        }
    );
    
    # create both tables with 10 read and 5 write unites
    $ddb->exists_table( $_ ) || $ddb->create_table( $_, 10, 5 )
        for qw/ sometable othertable /;
    
    # insert something into tables
    $ddb->put_item( sometable => {
        id   => 5,
        name => 'bla'
    } ) or die $ddb->error;
    $ddb->put_item( sometable => {
        id        => 5,
        range_key => 7,
        attrib1   => 'It is now '. localtime(),
        attrib1   => 'Or in unix timstamp '. time(),
    } ) or die $ddb->error;

=cut

use Moose;

use v5.10;
use version 0.74; our $VERSION = qv( "v0.1.5" );

use DateTime::Format::HTTP;
use DateTime;
use Digest::SHA qw/ sha256 hmac_sha256_base64 /;
use HTTP::Request;
use JSON;
use LWP::UserAgent;
use Net::Amazon::AWSSign;
use XML::Simple qw/ XMLin /;
use Data::Dumper;
use Carp qw/ croak /;
use Time::HiRes qw/ usleep /;

=head1 CLASS ATTRIBUTES

=head2 tables

The table definitions

=cut

has tables => ( isa => 'HashRef[HashRef]', is => 'rw', required => 1, trigger => sub {
    my ( $self ) = @_;
    return unless $self->namespace;
    my %new_table = ();
    my $updated = 0;
    foreach my $table( keys %{ $self->tables } ) {
        my $table_updated = index( $table, $self->namespace ) == 0 ? $table : $self->_table_name( $table );
        $new_table{ $table_updated } = $self->tables->{ $table };
        $updated ++ unless $table_updated eq $table;
    }
    if ( $updated ) {
        $self->{ tables } = \%new_table;
    }
} );

=head2 lwp

Contains C<LWP::UserAgent> instance.

=cut

has lwp => ( isa => 'LWP::UserAgent', is => 'rw', default => sub { LWP::UserAgent->new( timeout => 5 ) } );

=head2 json

Contains C<JSON> instance for decoding/encoding json.

=cut

has json => ( isa => 'JSON', is => 'rw', default => sub { JSON->new() } );

=head2 host

DynamoDB API Hostname

Default: dynamodb.us-east-1.amazonaws.com

=cut

has host => ( isa => 'Str', is => 'rw', default => 'dynamodb.us-east-1.amazonaws.com' );

=head2 access_key

AWS API access key

=cut

has access_key => ( isa => 'Str', is => 'rw', required => 1 );

=head2 secret_key

AWS API secret key

=cut

has secret_key => ( isa => 'Str', is => 'rw', required => 1 );

=head2 read_consistent

Whether reads (get_item) consistent per default or not. This does not affcect batch_get_item or scan_items or query_items, which are always eventually consistent.

Default: 0 (eventually consistent)

=cut

has read_consistent => ( isa => 'Bool', is => 'rw', default => 0 );

=head2 namespace

Table prefix, prepended before table name on usage

Default: ''

=cut

has namespace => ( isa => 'Str', is => 'ro', default => '' );

=head2 raise_error

Whether database errors (eg 4xx Response from DynamoDB) raise errors or not.

Default: 0

=cut

has raise_error => ( isa => 'Bool', is => 'ro', default => 0 );

=head2 max_retries

Amount of retries a query will be tries if ProvisionedThroughputExceededException is raised until final error.

Default: 0 (do only once, no retries)

=cut

has max_retries => ( isa => 'Int', is => 'ro', default => 1 );

=head2 retry_timeout

Wait period in seconds between tries. Float allowed.

Default: 0.1 (100ms)

=cut

has retry_timeout => ( isa => 'Num', is => 'ro', default => 0.1 );

#
# _aws_signer
#   Contains C<Net::Amazon::AWSSign> instance.
#

has _aws_signer => ( isa => 'Net::Amazon::AWSSign', is => 'rw', predicate => '_has_aws_signer' );

#
# _security_token_url
#   URL for receiving security token
#

has _security_token_url => ( isa => 'Str', is => 'rw', default => 'https://sts.amazonaws.com/?Action=GetSessionToken&Version=2011-06-15' );

#
# _credentials
#   Contains credentials received by GetSession
#

has _credentials => ( isa => 'HashRef[Str]', is => 'rw', predicate => '_has_credentials' );

#
# _error
#   Contains credentials received by GetSession
#

has _error => ( isa => 'Str', is => 'rw', predicate => '_has_error' );

=head1 METHODS


=head2 create_table $table_name, $read_amount, $write_amount

Create a new Table. Returns description of the table

    my $desc_ref = $ddb->create_table( 'table_name', 10, 5 )
    $desc_ref = {
        count           => 123,         # amount of "rows"
        status          => 'CREATING',  # or 'ACTIVE' or 'UPDATING' or some error state?
        created         => 1328893776,  # timestamp
        read_amount     => 10,          # amount of read units
        write_amount    => 5,           # amount of write units
        hash_key        => 'id',        # name of the hash key attribute
        hash_key_type   => 'S',         # or 'N',
        #range_key      => 'id',        # name of the hash key attribute (optional)
        #range_key_type => 'S',         # or 'N' (optional)
    }

=cut

sub create_table {
    my ( $self, $table, $read_amount, $write_amount ) = @_;
    $table = $self->_table_name( $table );
    $read_amount ||= 10;
    $write_amount ||= 5;
    
    # check & get table definition
    my $table_ref = $self->_check_table( "create_table", $table );
    
    # init create definition
    my %create = (
        TableName => $table,
        ProvisionedThroughput => {
            ReadCapacityUnits  => $read_amount + 0,
            WriteCapacityUnits => $write_amount + 0,
        }
    );
    
    # build keys
    $create{ KeySchema } = {
        HashKeyElement => {
            AttributeName => $table_ref->{ hash_key },
            AttributeType => $table_ref->{ attributes }->{ $table_ref->{ hash_key } }
        }
    };
    if ( defined $table_ref->{ range_key } ) {
        $create{ KeySchema }->{ RangeKeyElement } = {
            AttributeName => $table_ref->{ range_key },
            AttributeType => $table_ref->{ attributes }->{ $table_ref->{ range_key } }
        };
    }
    
    # perform create
    my ( $res, $res_ok, $json_ref ) = $self->request( CreateTable => \%create );
    
    # got res
    if ( $res_ok && defined $json_ref->{ TableDescription } ) {
        return {
            status        => $json_ref->{ TableDescription }->{ TableStatus },
            created       => int( $json_ref->{ TableDescription }->{ CreationDateTime } ),
            read_amount   => $json_ref->{ TableDescription }->{ ProvisionedThroughput }->{ ReadCapacityUnits },
            write_amount  => $json_ref->{ TableDescription }->{ ProvisionedThroughput }->{ WriteCapacityUnits },
            hash_key      => $json_ref->{ Table }->{ KeySchema }->{ HashKeyElement }->{ AttributeName },
            hash_key_type => $json_ref->{ Table }->{ KeySchema }->{ HashKeyElement }->{ AttributeType },
            ( defined $json_ref->{ Table }->{ KeySchema }->{ RangeKeyElement }
                ? (
                    range_key      => $json_ref->{ Table }->{ KeySchema }->{ RangeKeyElement }->{ AttributeName },
                    range_key_type => $json_ref->{ Table }->{ KeySchema }->{ RangeKeyElement }->{ AttributeType },
                )
                : ()
            ),
        }
    }
    
    # set error
    $self->error( 'create_table failed: '. $self->_extract_error_message( $res ) );
    return ;
}



=head2 delete_table $table

Delete an existing (and defined) table.

Returns bool whether table is now in deleting state (succesfully performed)

=cut

sub delete_table {
    my ( $self, $table ) = @_;
    $table = $self->_table_name( $table );
    
    # check & get table definition
    my $table_ref = $self->_check_table( delete_table => $table );
    
    # perform create
    my ( $res, $res_ok, $json_ref ) = $self->request( DeleteTable => { TableName => $table } );
    
    # got result
    if ( $res_ok && defined $json_ref->{ TableDescription } ) {
        return $json_ref->{ TableDescription }->{ TableStatus } eq 'DELETING';
    }
    
    # set error
    $self->error( 'delete_table failed: '. $self->_extract_error_message( $res ) );
    return ;
}



=head2 describe_table $table

Returns table information

    my $desc_ref = $ddb->describe_table( 'my_table' );
    $desc_ref = {
        existing        => 1,
        size            => 123213,      # data size in bytes
        count           => 123,         # amount of "rows"
        status          => 'ACTIVE',    # or 'DELETING' or 'CREATING' or 'UPDATING' or some error state
        created         => 1328893776,  # timestamp
        read_amount     => 10,          # amount of read units
        write_amount    => 5,           # amount of write units
        hash_key        => 'id',        # name of the hash key attribute
        hash_key_type   => 'S',         # or 'N',
        #range_key      => 'id',        # name of the hash key attribute (optional)
        #range_key_type => 'S',         # or 'N' (optional)
    }

If no such table exists, return is

    {
        existing => 0
    }

=cut

sub describe_table {
    my ( $self, $table ) = @_;
    $table = $self->_table_name( $table );
    
    # check table definition
    $self->_check_table( "describe_table", $table );
    
    my ( $res, $res_ok, $json_ref ) = $self->request( DescribeTable => { TableName => $table } );
    # got result
    if ( $res_ok ) {
        if ( defined $json_ref->{ Table } ) {
            return {
                existing      => 1,
                size          => $json_ref->{ Table }->{ TableSizeBytes },
                count         => $json_ref->{ Table }->{ ItemCount },
                status        => $json_ref->{ Table }->{ TableStatus },
                created       => int( $json_ref->{ Table }->{ CreationDateTime } ),
                read_amount   => $json_ref->{ Table }->{ ProvisionedThroughput }->{ ReadCapacityUnits },
                write_amount  => $json_ref->{ Table }->{ ProvisionedThroughput }->{ WriteCapacityUnits },
                hash_key      => $json_ref->{ Table }->{ KeySchema }->{ HashKeyElement }->{ AttributeName },
                hash_key_type => $json_ref->{ Table }->{ KeySchema }->{ HashKeyElement }->{ AttributeType },
                ( defined $json_ref->{ Table }->{ KeySchema }->{ RangeKeyElement }
                    ? (
                        range_key      => $json_ref->{ Table }->{ KeySchema }->{ RangeKeyElement }->{ AttributeName },
                        range_key_type => $json_ref->{ Table }->{ KeySchema }->{ RangeKeyElement }->{ AttributeType },
                    )
                    : ()
                ),
            };
        }
        else {
            return {
                existing => 0
            }
        }
    }
    
    # set error
    $self->error( 'describe_table failed: '. $self->_extract_error_message( $res ) );
    return ;
}


=head2 update_table $table, $read_amount, $write_amount

Update read and write amount for a table

=cut

sub update_table {
    my ( $self, $table, $read_amount, $write_amount ) = @_;
    $table = $self->_table_name( $table );
    
    my ( $res, $res_ok, $json_ref ) = $self->request( UpdateTable => {
        TableName             => $table,
        ProvisionedThroughput => {
            ReadCapacityUnits  => $read_amount + 0,
            WriteCapacityUnits => $write_amount + 0,
        }
    } );
    
    if ( $res_ok ) {
        return 1;
    }
    
    # set error
    $self->error( 'update_table failed: '. $self->_extract_error_message( $res ) );
    return ;
}



=head2 exists_table $table

Returns bool whether table exists or not

=cut

sub exists_table {
    my ( $self, $table ) = @_;
    $table = $self->_table_name( $table );
    
    # check table definition
    $self->_check_table( "exists_table", $table );
    
    my ( $res, $res_ok, $json_ref );
    eval {
        ( $res, $res_ok, $json_ref ) = $self->request( DescribeTable => { TableName => $table } );
    };
    
    return defined $json_ref->{ Table } && defined $json_ref->{ Table }->{ ItemCount } ? 1 : 0
        if $res_ok;
    
    # set error
    return 0;
}



=head2 list_tables

Returns tables names as arrayref (or array in array context)

=cut

sub list_tables {
    my ( $self ) = @_;
    
    my ( $res, $res_ok, $json_ref ) = $self->request( ListTables => {} );
    if ( $res_ok ) {
        my $ns_length = length( $self->namespace );
        my @table_names = map {
            substr( $_, $ns_length );
        } grep {
            ! $self->namespace || index( $_, $self->namespace ) == 0
        } @{ $json_ref->{ TableNames } };
        return wantarray ? @table_names : \@table_names;
    }
    
    # set error
    $self->error( 'list_tables failed: '. $self->_extract_error_message( $res ) );
    return ;
}



=head2 put_item $table, $item_ref, [$where_ref], [$return_old]

Write a single item to table. All primary keys are required in new item.

    # just write
    $ddb->put_item( my_table => {
        id => 123,
        some_attrib => 'bla',
        other_attrib => 'dunno'
    } );
    
    # write conditionally
    $ddb->put_item( my_table => {
        id => 123,
        some_attrib => 'bla',
        other_attrib => 'dunno'
    }, {
        some_attrib => { # only update, if some_attrib has the value 'blub'
            value => 'blub'
        },
        other_attrib => { # only update, if a value for other_attrib exists
            exists => 1
        }
    } );

=over

=item * $table

Name of the table

=item * $item_ref

Hashref containing the values to be inserted

=item * $where_ref [optional]

Filter containing expected values of the (existing) item to be updated

{}

=cut

sub put_item {
    my ( $self, $table, $item_ref, $where_ref, $return_old ) = @_;
    $table = $self->_table_name( $table );
    
    # check definition
    my $table_ref = $self->_check_table( "put_item", $table );
    
    # check primary keys
    croak "put_item: Missing value for hash key '$table_ref->{ hash_key }'"
        unless defined $item_ref->{ $table_ref->{ hash_key } }
        && length( $item_ref->{ $table_ref->{ hash_key } } );
    
    # check other attributes
    $self->_check_keys( "put_item: item values", $table, $item_ref );
    
    # having where -> check now
    $self->_check_keys( "put_item: where clause", $table, $where_ref ) if $where_ref;
    
    # build put
    my %put = (
        TableName => $table,
        Item      => {}
    );
    
    # build the item
    foreach my $key( keys %$item_ref ){
        my $type = $self->_attrib_type( $table, $key );
        my $value = $item_ref->{ $key } .'';
        $put{ Item }->{ $key } = { $type => $value };
    }
    
    # build possible where clause
    if ( $where_ref ) {
        $self->_build_attrib_filter( $table, $where_ref, $put{ Expected } = {} );
    }
    
    # add return value, if set
    $put{ ReturnValues } = 'ALL_OLD' if $return_old;
    
    # perform create
    my ( $res, $res_ok, $json_ref ) = $self->request( PutItem => \%put );
    
    # get result
    if ( $res_ok ) {
        if ( $return_old ) {
            return defined $json_ref->{ Attributes }
                ? $self->_format_item( $table, $json_ref->{ Attributes } )
                : undef;
        }
        else {
            return $json_ref->{ ConsumedCapacityUnits } > 0;
        }
    }
    
    # set error
    $self->error( 'put_item failed: '. $self->_extract_error_message( $res ) );
    return ;
}



=head2 update_item $table, $update_ref, $where_ref, [$return_old]

Update existing item in database. All primary keys are required in where clause

    # update existing
    $ddb->update_item( my_table => {
        id => 123,
        some_attrib => 'bla',
        other_attrib => 'dunno'
    } );
    
    # write conditionally
    $ddb->update_item( my_table => {
        id => 123,
        some_attrib => 'bla',
        other_attrib => 'dunno'
    }, {
        some_attrib => { # only update, if some_attrib has the value 'blub'
            value => 'blub'
        },
        other_attrib => { # only update, if a value for other_attrib exists
            exists => 1
        }
    } );

=over

=item * $table

Name of the table

=item * $update_ref

Hashref containing the updates.

=over

=item * delete a single values

    { attribname => undef }

=item * replace a values

    { 
        attribname1 => 'somevalue',
        attribname2 => [ 1, 2, 3 ]
    }

=item * add values (arrays only)

    { attribname => \[ 4, 5, 6 ] }

=back

=item * $where_ref [optional]

Filter 

=cut

sub update_item {
    my ( $self, $table, $update_ref, $where_ref, $return_mode ) = @_;
    $table = $self->_table_name( $table );
    
    # check definition
    my $table_ref = $self->_check_table( "put_item", $table );
    
    croak "update_item: Cannot update hash key value, do not set it in update-clause"
        if defined $update_ref->{ $table_ref->{ hash_key } };
    
    croak "update_item: Cannot update range key value, do not set it in update-clause"
        if defined $table_ref->{ range_key }
        && defined $update_ref->{ $table_ref->{ range_key } };
    
    # check primary keys
    croak "update_item: Missing value for hash key '$table_ref->{ hash_key }' in where-clause"
        unless defined $where_ref->{ $table_ref->{ hash_key } }
        && length( $where_ref->{ $table_ref->{ hash_key } } );
    croak "update_item: Missing value for range key '$table_ref->{ hash_key }' in where-clause"
        if defined $table_ref->{ range_key } && !(
            defined $where_ref->{ $table_ref->{ range_key } }
            && length( $where_ref->{ $table_ref->{ range_key } } )
        );
    
    # check other attributes
    $self->_check_keys( "put_item: item values", $table, $update_ref );
    croak "update_item: Cannot update hash key '$table_ref->{ hash_key }'. You have to delete and put the item!"
        if defined $update_ref->{ $table_ref->{ hash_key } };
    croak "update_item: Cannot update range key '$table_ref->{ hash_key }'. You have to delete and put the item!"
        if defined $table_ref->{ range_key } && defined $update_ref->{ $table_ref->{ range_key } };
    
    # having where -> check now
    $self->_check_keys( "put_item: where clause", $table, $where_ref );
    
    # build put
    my %update = (
        TableName        => $table,
        AttributeUpdates => {},
        Key              => {}
    );
    
    # build the item
    foreach my $key( keys %$update_ref ) {
        my $type = $self->_attrib_type( $table, $key );
        my $value = $update_ref->{ $key };
        
        # delete
        if ( ! defined $value ) {
            $update{ AttributeUpdates }->{ $key } = {
                Action => 'DELETE'
            };
        }
        
        # replace for scalar
        elsif ( $type eq 'N' || $type eq 'S' ) {
            $update{ AttributeUpdates }->{ $key } = {
                Value  => { $type => $value. '' },
                Action => 'PUT'
            };
        }
        
        # replace or add for array types
        elsif ( $type =~ /^[NS]S$/ ) {
            
            # add
            if ( ref( $value ) eq 'REF' ) {
                $update{ AttributeUpdates }->{ $key } = {
                    Value  => { $type => [ map { "$_" } @$$value ] },
                    Action => 'ADD'
                };
            }
            
            # replace
            else {
                $update{ AttributeUpdates }->{ $key } = {
                    Value  => { $type => [ map { "$_" } @$value ] },
                    Action => 'PUT'
                };
            }
        }
    }
    
    # build possible where clause
    my %where = %$where_ref;
    
    # primary key
    $self->_build_pk_filter( $table, \%where, $update{ Key } );
    
    # additional filters
    if ( keys %where ) {
        $self->_build_attrib_filter( $table, \%where, $update{ Expected } = {} );
    }
    
    # add return value, if set
    if ( $return_mode ) {
        $update{ ReturnValues } = "$return_mode" =~ /^(?:ALL_OLD|UPDATED_OLD|ALL_NEW|UPDATED_NEW)$/i
            ? uc( $return_mode )
            : "ALL_OLD";
    }
    
    # perform create
    my ( $res, $res_ok, $json_ref ) = $self->request( UpdateItem => \%update );
    
    # get result
    if ( $res_ok ) {
        if ( $return_mode ) {
            return defined $json_ref->{ Attributes }
                ? $self->_format_item( $table, $json_ref->{ Attributes } )
                : undef;
        }
        else {
            return $json_ref->{ ConsumedCapacityUnits } > 0;
        }
    }
    
    # set error
    $self->error( 'put_item failed: '. $self->_extract_error_message( $res ) );
    return ;
}



=head2 get_item $table, $pk_ref, [$args_ref]

Read a single item by hash (and range) key.

    # only with hash key
    my $item1 = $ddb->get_item( my_table => { id => 123 } );
    print "Got $item1->{ some_key }\n";
    
    # with hash and range key, also consistent read and only certain attributes in return
    my $item2 = $ddb->get_item( my_other_table =>, {
        id    => $hash_value, # the hash value
        title => $range_value # the range value
    }, {
        consistent => 1,
        attributes => [ qw/ attrib1 attrib2 ]
    } );
    print "Got $item2->{ attrib1 }\n";

=cut

sub get_item {
    my ( $self, $table, $pk_ref, $args_ref ) = @_;
    $table = $self->_table_name( $table );
    $args_ref ||= {
        consistent => undef,
        attributes => undef
    };
    $args_ref->{ consistent } //= $self->read_consistent;
    
    # check definition
    my $table_ref = $self->_check_table( "get_item", $table );
    
    # check primary keys
    croak "get_item: Missing value for hash key '$table_ref->{ hash_key }'"
        unless defined $pk_ref->{ $table_ref->{ hash_key } }
        && length( $pk_ref->{ $table_ref->{ hash_key } } );
    croak "get_item: Missing value for Range Key '$table_ref->{ range_key }'"
        if defined $table_ref->{ range_key } && !(
            defined $pk_ref->{ $table_ref->{ range_key } }
            && length( $pk_ref->{ $table_ref->{ hash_key } } )
        );
    
    # build get
    my %get = (
        TableName => $table,
        ( defined $args_ref->{ attributes } ? ( AttributesToGet => $args_ref->{ attributes } ) : () ),
        ConsistentRead => $args_ref->{ consistent } ? \1 : \0,
        Key => {
            HashKeyElement => {
                $self->_attrib_type( $table, $table_ref->{ hash_key } ) =>
                    $pk_ref->{ $table_ref->{ hash_key } }
            }
        }
    );
    
    # add range key ?
    if ( defined $table_ref->{ range_key } ) {
        $get{ Key }->{ RangeKeyElement } = {
            $self->_attrib_type( $table, $table_ref->{ range_key } ) =>
                    $pk_ref->{ $table_ref->{ range_key } }
        };
    }
    
    # perform create
    my ( $res, $res_ok, $json_ref ) = $self->request( GetItem => \%get );
    
    # return on success
    return $self->_format_item( $table, $json_ref->{ Item } ) if $res_ok && defined $json_ref->{ Item };
    
    # return on success, but nothing received
    return undef if $res_ok;
    
    # set error
    $self->error( 'get_item failed: '. $self->_extract_error_message( $res ) );
    return ;
}



=head2 batch_get_item

Read multiple items (possible accross multiple tables) identified by their hash and range key (if required).

    my $res = $ddb->batch_get_item( {
        table_name => [
            { $hash_key => $value1 },
            { $hash_key => $value2 },
            { $hash_key => $value3 },
        ],
        other_table_name => {
            keys => [
                { $hash_key => $value1, $range_key => $rvalue1 },
                { $hash_key => $value2, $range_key => $rvalue2 },
                { $hash_key => $value3, $range_key => $rvalue3 },
            ],
            attributes => [ qw/ attrib1 attrib2 / ]
        ]
    } );
    
    foreach my $table( keys %$res ) {
        foreach my $item( @{ $res->{ $table } } ) {
            print "$item->{ some_attrib }\n";
        }
    }

=cut

sub batch_get_item {
    my ( $self, $tables_ref ) = @_;
    
    # check definition
    my %table_map;
    foreach my $table( keys %$tables_ref ) {
        $table = $self->_table_name( $table );
        my $table_ref = $self->_check_table( "batch_get_item", $table );
        $table_map{ $table } = $table_ref;
    }
    
    my %get = ( RequestItems => {} );
    foreach my $table( keys %table_map ) {
        my $table_out = $self->_table_name( $table, 1 );
        my $t_ref = $tables_ref->{ $table_out };
        
        # init items for table
        $get{ RequestItems }->{ $table } = {};
        
        # init / get keys
        my $k_ref = $get{ RequestItems }->{ $table }->{ Keys } = [];
        my @keys = ref( $t_ref ) eq 'ARRAY'
            ? @$t_ref
            : @{ $t_ref->{ keys } };
        
        # get mapping for table
        my $m_ref = $table_map{ $table };
        
        # get hash key
        my $hash_key = $m_ref->{ hash_key };
        my $hash_key_type = $self->_attrib_type( $table, $hash_key );
        
        # get range key?
        my ( $range_key, $range_key_type );
        if ( defined $m_ref->{ range_key } ) {
            $range_key = $m_ref->{ range_key };
            $range_key_type = $self->_attrib_type( $table, $range_key );
        }
        
        # build request items
        foreach my $key_ref( @keys ) {
            push @$k_ref, {
                HashKeyElement => { $hash_key_type => $key_ref->{ $hash_key }. '' },
                ( defined $range_key ? ( RangeKeyElement => { $range_key_type => $key_ref->{ $range_key }. '' } ) : () )
            };
        }
        
        # having attributes limitation?
        if ( ref( $t_ref ) eq 'HASH' && defined $t_ref->{ attributes } ) {
            $get{ RequestItems }->{ $table }->{ AttributesToGet } = $t_ref->{ attributes };
        }
    }
    
    # perform create
    my ( $res, $res_ok, $json_ref ) = $self->request( BatchGetItem => \%get );
    
    # return on success
    if ( $res_ok && defined $json_ref->{ Responses } ) {
        my %res;
        foreach my $table_out( keys %$tables_ref ) {
            my $table = $self->_table_name( $table_out );
            next unless defined $json_ref->{ Responses }->{ $table } && defined $json_ref->{ Responses }->{ $table }->{ Items };
            my $items_ref = $json_ref->{ Responses }->{ $table };
            $res{ $table_out } = [];
            foreach my $item_ref( @{ $items_ref->{ Items } } ) {
                my %res_item;
                foreach my $attrib( keys %$item_ref ) {
                    my $type = $self->_attrib_type( $table, $attrib );
                    $res_item{ $attrib } = $item_ref->{ $attrib }->{ $type };
                }
                push @{ $res{ $table_out } }, \%res_item;
            }
        }
        return \%res;
    }
    
    # set error
    $self->error( 'batch_get_item failed: '. $self->_extract_error_message( $res ) );
    return ;
}



=head2 delete_item

Deletes a single item by primary key (hash or hash+range key). 

    # only with hash key
    

=cut

sub delete_item {
    my ( $self, $table, $where_ref ) = @_;
    $table = $self->_table_name( $table );
    
    # check definition
    my $table_ref = $self->_check_table( "delete_item", $table );
    
    # check primary keys
    croak "delete_item: Missing value for hash key '$table_ref->{ hash_key }'"
        unless defined $where_ref->{ $table_ref->{ hash_key } }
        && length( $where_ref->{ $table_ref->{ hash_key } } );
    croak "delete_item: Missing value for Range Key '$table_ref->{ range_key }'"
        if defined $table_ref->{ range_key } && ! (
            defined $where_ref->{ $table_ref->{ range_key } }
            && length( $where_ref->{ $table_ref->{ range_key } } )
        );
    
    # check other attributes
    $self->_check_keys( "delete_item: where-clause", $table, $where_ref );
    
    # build delete
    my %delete = (
        TableName    => $table,
        Key          => {},
        ReturnValues => 'ALL_OLD'
    );
    
    # setup pk
    my %where = %$where_ref;
    
    # for hash key
    my $hash_value = delete $where{ $table_ref->{ hash_key } };
    $delete{ Key }->{ HashKeyElement } = {
        $self->_attrib_type( $table, $table_ref->{ hash_key } ) => $hash_value
    };
    
    # for range key
    if ( defined $table_ref->{ range_key } ) {
        my $range_value = delete $where{ $table_ref->{ range_key } };
        $delete{ Key }->{ RangeKeyElement } = {
            $self->_attrib_type( $table, $table_ref->{ range_key } ) => $range_value
        };
    }
    
    # build filter for other attribs
    if ( keys %where ) {
        $self->_build_attrib_filter( $table, \%where, $delete{ Expected } = {} );
    }
    
    # perform create
    my ( $res, $res_ok, $json_ref ) = $self->request( DeleteItem => \%delete );
    
    if ( $res_ok ) {
        if ( defined $json_ref->{ Attributes } ) {
            my %res;
            foreach my $attrib( $self->_attribs( $table ) ) {
                next unless defined $json_ref->{ Attributes }->{ $attrib };
                $res{ $attrib } = $json_ref->{ Attributes }->{ $attrib }->{ $self->_attrib_type( $table, $attrib ) };
            }
            return \%res;
        }
        return {};
    }
    
    $self->error( 'delete_item failed: '. $self->_extract_error_message( $res ) );
    return;
}



=head2 query_items $table, $where, $args

Search in a table with hash AND range key.

    my ( $count, $items_ref, $next_start_keys_ref )
        = $ddb->qyery_items( some_table => { id => 123, my_range_id => { GT => 5 } } );
    print "Found $count items, where last id is ". $items_ref->[-1]->{ id }. "\n";
    
    # iterate through al all "pages"
    my $next_start_keys_ref;
    do {
        ( my $count, my $items_ref, $next_start_keys_ref )
            = $ddb->qyery_items( some_table => { id => 123, my_range_id => { GT => 5 } }, {
                start_key => $next_start_keys_ref
            } );
    } while( $next_start_keys_ref );

=over

=item * $table

Name of the table

=item * $where

Search condition. Has to contain a value of the primary key and a search-value for the range key.

Search-value for range key can be formated in two ways

=over

=item * Scalar

Eg

    { $range_key_name => 123 }

Performs and EQ (equal) search

=item * HASHREF

Eg

    { $range_key_name => { GT => 1 } }
    { $range_key_name => { CONTAINS => "Bla" } }
    { $range_key_name => { IN => [ 1, 2, 5, 7 ] } }

See L<http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Query.html>

=back

=item * $args

    {
        limit => 5,
        consistent => 0,
        backward => 0,
        #start_key =>  { .. }
        attributes => [ qw/ attrib1 attrib2 / ],
        #count => 1
    }

HASHREF containing:

=over

=item * limit

Amount of items to return

Default: unlimited

=item * consistent

If set to 1, consistent read is performed

Default: 0

=item * backward

Whether traverse index backward or forward.

Default: 0 (=forward)

=item * start_key

Contains start key, as return in C<LastEvaluatedKey> from previous query. Allows to iterate above a table in pages.

    { $hash_key => 5, $range_key => "something" }

=item * attributes

Return only those attributes

    [ qw/ attrib attrib2 / ]

=item * count

Instead of returning the actual result, return the count.

Default: 0 (=return result)

=item * all

Iterate through all pages (see link to API above) and return them all.

Can take some time. Also: max_retries might be needed to set, as a scan/query create lot's of read-units, and an immediate reading of the next "pages" lead to an Exception due to too many reads.

Default: 0 (=first "page" of items)

=back

=back


=cut

sub query_items {
    my ( $self, $table, $filter_ref, $args_ref ) = @_;
    my $table_orig = $table;
    $table = $self->_table_name( $table );
    $args_ref ||= {
        limit       => undef,   # amount of items
        consistent  => 0,       # default: eventually, not hard, conistent
        backward    => 0,       # default: forward
        start_key   => undef,   # eg { pk_name => 123, pk_other => 234 }
        attributes  => undef,   # eq [ qw/ attrib1 attrib2 / ]
        count       => 0,       # returns amount instead of the actual result
        all         => 0,       # read all entries (runs possibly multiple queries)
    };
    
    # check definition
    croak "query_items: Table '$table' does not exist in table definition"
        unless defined $self->tables->{ $table };
    my $table_ref = $self->tables->{ $table };
    
    # die "query_items: Can run query_items only on tables with range key! '$table' does not have a range key.."
    #     unless defined $table_ref->{ range_key };

    # build put
    my %query = (
        TableName        => $table,
        ConsistentRead   => $args_ref->{ consistent } ? \1 : \0,
        ScanIndexForward => $args_ref->{ backward } ? \0 : \1,
        ( defined $args_ref->{ limit } ? ( Limit => $args_ref->{ limit } ) : () ),
    );
    
    # using filter
    my %filter = %$filter_ref;
    
    if ( defined $filter{ $table_ref->{ hash_key } } ) {
        croak "query_items: Missing hash key value in filter-clause"
            unless defined $filter{ $table_ref->{ hash_key } };
        $query{ HashKeyValue } = {
            $self->_attrib_type( $table, $table_ref->{ hash_key } ) =>
                ( delete $filter{ $table_ref->{ hash_key } } ) . ''
        };
    }
    
    # adding range to filter
    if ( defined $table_ref->{ range_key }) {
        croak "query_items: Missing range key value in filter-clause"
            unless defined $filter{ $table_ref->{ range_key } };
        # r_ref = { GT => 1 } OR { BETWEEN => [ 1, 5 ] } OR { EQ => [ 1 ] } OR 5 FOR { EQ => 5 }
        my $r_ref = delete $filter{ $table_ref->{ range_key } };
        $r_ref = { EQ => $r_ref } unless ref( $r_ref );
        my ( $op, $vals_ref ) = %$r_ref;
        $vals_ref = [ $vals_ref ] unless ref( $vals_ref );
        my $type = $self->_attrib_type( $table, $table_ref->{ range_key } );
        $query{ RangeKeyCondition } = {
            AttributeValueList => [ map {
                { $type => $_. '' }
            } @$vals_ref ],
            ComparisonOperator => uc( $op )
        };
    }
    
    # too much keys
    croak "query_items: Cannot use keys ". join( ', ', sort keys %filter ). " in in filter - only hash and range key allowed."
        if keys %filter;
    
    
    # with start key?
    if( defined( my $start_key_ref = $args_ref->{ start_key } ) ) {
        $self->_check_keys( "query_items: start_key", $table, $start_key_ref );
        my $e_ref = $query{ ExclusiveStartKey } = {};
        
        # add hash key
        if ( defined $start_key_ref->{ $table_ref->{ hash_key } } ) {
            my $type = $self->_attrib_type( $table, $table_ref->{ hash_key } );
            $e_ref->{ HashKeyElement } = { $type => $start_key_ref->{ $table_ref->{ hash_key } } };
        }
        
        # add range key?
        if ( defined $table_ref->{ range_key } && defined $start_key_ref->{ $table_ref->{ range_key } } ) {
            my $type = $self->_attrib_type( $table, $table_ref->{ range_key } );
            $e_ref->{ RangeKeyElement } = { $type => $start_key_ref->{ $table_ref->{ range_key } } };
        }
    }
    
    # only certain attributes
    if ( defined( my $attribs_ref = $args_ref->{ attributes } ) ) {
        my @keys = $self->_check_keys( "query_items: attributes", $table, $attribs_ref );
        $query{ AttributesToGet } = \@keys;
    }
    
    # or count?
    elsif ( $args_ref->{ count } ) {
        $query{ Count } = \1;
    }
    
    # perform query
    #print Dumper( { QUERY => \%query } );
    my ( $res, $res_ok, $json_ref ) = $self->request( Query => \%query );
    
    # format & return result
    if ( $res_ok && defined $json_ref->{ Items } ) {
        my @res;
        foreach my $from_ref( @{ $json_ref->{ Items } } ) {
            push @res, $self->_format_item( $table, $from_ref );
        }
        my $count = $json_ref->{ Count };
        
        # build start key for return or use
        my $next_start_key_ref;
        if ( defined $json_ref->{ LastEvaluatedKey } ) {
            $next_start_key_ref = {};
            
            # add hash key to start key
            my $hash_type = $self->_attrib_type( $table, $table_ref->{ hash_key } );
            $next_start_key_ref->{ $table_ref->{ hash_key } } = $json_ref->{ LastEvaluatedKey }->{ HashKeyElement }->{ $hash_type };
            
            # add range key to start key
            if ( defined $table_ref->{ range_key } && defined $json_ref->{ LastEvaluatedKey }->{ RangeKeyElement } ) {
                my $range_type = $self->_attrib_type( $table, $table_ref->{ range_key } );
                $next_start_key_ref->{ $table_ref->{ range_key } } = $json_ref->{ LastEvaluatedKey }->{ RangeKeyElement }->{ $range_type };
            }
        }
        
        # cycle through all?
        if ( $args_ref->{ all } && $next_start_key_ref ) {
            
            # make sure we do not run into a loop by comparing last and current start key
            my $new_start_key = join( ';', map { sprintf( '%s=%s', $_, $next_start_key_ref->{ $_ } ) } sort keys %$next_start_key_ref );
            my %key_cache     = defined $args_ref->{ _start_key_cache } ? %{ $args_ref->{ _start_key_cache } } : ();
            #print Dumper( { STARTKEY => $next_start_key_ref, LASTEVAL => $json_ref->{ LastEvaluatedKey }, KEYS => [ \%key_cache, $new_start_key ] } );
            
            if ( ! defined $key_cache{ $new_start_key } ) {
                $key_cache{ $new_start_key } = 1;
                
                # perform sub-query
                my ( $sub_count, $sub_res_ref ) = $self->query_items( $table_orig, $filter_ref, {
                    %$args_ref,
                    _start_key_cache => \%key_cache,
                    start_key        => $next_start_key_ref
                } );
                #print Dumper( { SUB_COUNT => $sub_count } );
                
                # add result
                if ( $sub_count ) {
                    $count += $sub_count;
                    push @res, @$sub_res_ref;
                }
            }
        }
        
        return wantarray ? ( $count, \@res, $next_start_key_ref ) : \@res;
    }
    
    # error
    $self->error( 'query_items failed: '. $self->_extract_error_message( $res ) );
    return;
}



=head2 scan_items $table, $filter, $args

Performs scan on table. The result is B<eventually consistent>. Non hash or range keys are allowed in the filter.

See query_items for argument description.

Main difference to query_items: A whole table scan is performed, which is much slower. Also the amount of data scanned is limited in size; see L<http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Scan.html>

=cut

sub scan_items {
    my ( $self, $table, $filter_ref, $args_ref ) = @_;
    my $table_orig = $table;
    $table = $self->_table_name( $table );
    $args_ref ||= {
        limit       => undef,   # amount of items
        start_key   => undef,   # eg { hash_key => 1, range_key => "bla" }
        attributes  => undef,   # eq [ qw/ attrib1 attrib2 / ]
        count       => 0,       # returns amount instead of the actual result
        all         => 0,       # read all entries (runs possibly multiple queries)
    };
    
    # check definition
    croak "scan_items: Table '$table' does not exist in table definition"
        unless defined $self->tables->{ $table };
    my $table_ref = $self->tables->{ $table };
    
    # build put
    my %query = (
        TableName => $table,
        ScanFilter => {},
        ( defined $args_ref->{ limit } ? ( Limit => $args_ref->{ limit } ) : () ),
    );
    
    # using filter
    if ( $filter_ref && keys %$filter_ref ) {
        my @filter_keys = $self->_check_keys( "scan_items: filter keys", $table, $filter_ref );
        my $s_ref = $query{ ScanFilter };
        foreach my $key( @filter_keys ) {
            my $type = $self->_attrib_type( $table, $key );
            my $val_ref = $filter_ref->{ $key };
            my $rvalue = ref( $val_ref ) || '';
            if ( $rvalue eq 'HASH' ) {
                my ( $op, $value ) = %$val_ref;
                $s_ref->{ $key } = {
                    AttributeValueList => [ { $type => $value. '' } ],
                    ComparisonOperator => uc( $op )
                };
            }
            elsif( $rvalue eq 'ARRAY' ) {
                $s_ref->{ $key } = {
                    AttributeValueList => [ { $type => $val_ref } ],
                    ComparisonOperator => 'IN'
                };
            }
            else {
                $s_ref->{ $key } = {
                    AttributeValueList => [ { $type => $val_ref. '' } ],
                    ComparisonOperator => 'EQ'
                };
            }
        }
    }
    
    # with start key?
    if( defined( my $start_key_ref = $args_ref->{ start_key } ) ) {
        $self->_check_keys( "scan_items: start_key", $table, $start_key_ref );
        my $e_ref = $query{ ExclusiveStartKey } = {};
        
        # add hash key
        if ( defined $start_key_ref->{ $table_ref->{ hash_key } } ) {
            my $type = $self->_attrib_type( $table, $table_ref->{ hash_key } );
            $e_ref->{ HashKeyElement } = { $type => $start_key_ref->{ $table_ref->{ hash_key } } };
        }
        
        # add range key?
        if ( defined $table_ref->{ range_key } && defined $start_key_ref->{ $table_ref->{ range_key } } ) {
            my $type = $self->_attrib_type( $table, $table_ref->{ range_key } );
            $e_ref->{ RangeKeyElement } = { $type => $start_key_ref->{ $table_ref->{ range_key } } };
        }
    }
    
    # only certain attributes
    if ( defined( my $attribs_ref = $args_ref->{ attributes } ) ) {
        my @keys = $self->_check_keys( "scan_items: attributes", $table, $attribs_ref );
        $query{ AttributesToGet } = \@keys;
    }
    
    # or count?
    elsif ( $args_ref->{ count } ) {
        $query{ Count } = \1;
    }
    
    # perform query
    my ( $res, $res_ok, $json_ref ) = $self->request( Scan => \%query );
    
    # format & return result
    if ( $res_ok && defined $json_ref->{ Items } ) {
        my @res;
        foreach my $from_ref( @{ $json_ref->{ Items } } ) {
            push @res, $self->_format_item( $table, $from_ref );
        }
        
        my $count = $json_ref->{ Count };
        
        # build start key for return or use
        my $next_start_key_ref;
        if ( defined $json_ref->{ LastEvaluatedKey } ) {
            $next_start_key_ref = {};
            
            # add hash key to start key
            my $hash_type = $self->_attrib_type( $table, $table_ref->{ hash_key } );
            $next_start_key_ref->{ $table_ref->{ hash_key } } = $json_ref->{ LastEvaluatedKey }->{ HashKeyElement }->{ $hash_type };
            
            # add range key to start key
            if ( defined $table_ref->{ range_key } && defined $json_ref->{ LastEvaluatedKey }->{ RangeKeyElement } ) {
                my $range_type = $self->_attrib_type( $table, $table_ref->{ range_key } );
                $next_start_key_ref->{ $table_ref->{ range_key } } = $json_ref->{ LastEvaluatedKey }->{ RangeKeyElement }->{ $range_type };
            }
        }
        
        # cycle through all?
        if ( $args_ref->{ all } && $next_start_key_ref ) {
            
            # make sure we do not run into a loop by comparing last and current start key
            my $new_start_key = join( ';', map { sprintf( '%s=%s', $_, $next_start_key_ref->{ $_ } ) } sort keys %$next_start_key_ref );
            my %key_cache     = defined $args_ref->{ _start_key_cache } ? %{ $args_ref->{ _start_key_cache } } : ();
            #print Dumper( { STARTKEY => $next_start_key_ref, LASTEVAL => $json_ref->{ LastEvaluatedKey }, KEYS => [ \%key_cache, $new_start_key ] } );
            
            if ( ! defined $key_cache{ $new_start_key } ) {
                $key_cache{ $new_start_key } = 1;
                
                # perform sub-query
                my ( $sub_count, $sub_res_ref ) = $self->scan_items( $table_orig, $filter_ref, {
                    %$args_ref,
                    _start_key_cache => \%key_cache,
                    start_key        => $next_start_key_ref
                } );
                #print Dumper( { SUB_COUNT => $sub_count } );
                
                # add result
                if ( $sub_count ) {
                    $count += $sub_count;
                    push @res, @$sub_res_ref;
                }
            }
        }
        
        return wantarray ? ( $count, \@res, $next_start_key_ref ) : \@res;
    }
    
    # error
    $self->error( 'scan_items failed: '. $self->_extract_error_message( $res ) );
    return;
}



=head2 request

Arbitrary request to DynamoDB API

=cut

sub request {
    my ( $self, $target, $json ) = @_;
    
    # assure security token existing
    $self->_init_security_token();
    
    # convert to string, if required
    $json = $self->json->encode( $json ) if ref $json;
    
    # get date
    my $http_date = DateTime::Format::HTTP->format_datetime( DateTime->now );
    
    # build signable content
    my $sign_content = join( "\n",
        'POST', '/', '',
        'host:'. $self->host,
        'x-amz-date:'. $http_date,
        'x-amz-security-token:'. $self->_credentials->{ SessionToken },
        'x-amz-target:DynamoDB_20111205.'. $target,
        '',
        $json
    );
    my $signature = hmac_sha256_base64( sha256( $sign_content ), $self->_credentials->{ SecretAccessKey } );
    $signature .= '=' while( length( $signature ) % 4 != 0 );
    
    # build request
    my $request = HTTP::Request->new( POST => 'http://'. $self->host. '/' );
    
    # .. setup headers
    $request->header( host => $self->host );
    $request->header( 'x-amz-date' => $http_date );
    $request->header( 'x-amz-target', 'DynamoDB_20111205.'. $target );
    $request->header( 'x-amzn-authorization' => join( ',',
        'AWS3 AWSAccessKeyId='. $self->_credentials->{ AccessKeyId },
        'Algorithm=HmacSHA256',
        'SignedHeaders=host;x-amz-date;x-amz-security-token;x-amz-target',
        'Signature='. $signature
    ) );
    $request->header( 'x-amz-security-token' => $self->_credentials->{ SessionToken } );
    $request->header( 'content-type' => 'application/x-amz-json-1.0' );
    
    # .. add content
    $request->content( $json );
    
    my ( $json_ref, $response );
    my $tries = $self->max_retries + 1;
    while( 1 ) {
        
        # run request
        $response = $self->lwp->request( $request );
        $ENV{ DYNAMO_DB_DEBUG } && warn Dumper( $response );
        
        # get json
        $json_ref = $response
            ? eval { $self->json->decode( $response->decoded_content ) } || { error => "Failed to parse JSON result" }
            : { error => "Failed to get result" };
        if ( defined $json_ref->{ __type } && $json_ref->{ __type } =~ /ProvisionedThroughputExceededException/ && $tries-- > 0 ) {
            usleep( $self->retry_timeout * 1_000_000 );
            next;
        }
        last;
    }
    
    
    # handle error
    if ( defined $json_ref->{ error } && $json_ref->{ error } ) {
        $self->error( $json_ref->{ error } );
    }
    
    # handle exception
    elsif ( defined $json_ref->{ __type } && $json_ref->{ __type } =~ /Exception/ && $json_ref->{ Message } ) {
        $self->error( $json_ref->{ Message } );
    }
    
    return wantarray ? ( $response, $response ? $response->is_success : 0, $json_ref ) : $json_ref;
}



=head2 error [$str]

Get/set last error

=cut

sub error {
    my ( $self, $str ) = @_;
    croak $str if $self->raise_error();
    if ( $str ) {
        $self->_error( $str );
    }
    return $self->_error if $self->_has_error;
    return ;
}



#
# _init_security_token
#   Creates new temporary security token (, access and secret key), if not exist
#

sub _init_security_token {
    my ( $self ) = @_;
    
    return if $self->_has_credentials();
    
    # build aws signed request
    $self->_aws_signer( Net::Amazon::AWSSign->new(
        $self->access_key, $self->secret_key ) )
        unless $self->_has_aws_signer;
    my $url = $self->_aws_signer->addRESTSecret( $self->_security_token_url );
    
    # get token
    my $res = $self->lwp->get( $url );
    
    # got response
    if ( $res->is_success) {
        my $result_ref = XMLin( $res->decoded_content );
        
        # got valid result
        if( ref $result_ref && defined $result_ref->{ GetSessionTokenResult }
            && defined $result_ref->{ GetSessionTokenResult }
            && defined $result_ref->{ GetSessionTokenResult }->{ Credentials }
        ) {
            # SessionToken, AccessKeyId, Expiration, SecretAccessKey
            $self->_credentials( $result_ref->{ GetSessionTokenResult }->{ Credentials } )
        }
    }
}


#
# _check_table $table
#   Check whether table exists and returns definition
#

sub _check_table {
    my ( $self, $meth, $table ) = @_;
    unless( $table ) {
        $table = $meth;
        $meth = "check_table";
    }
    croak "$meth: Table '$table' not defined"
        unless defined $self->tables->{ $table };
    
    return $self->tables->{ $table };
}


#
# _check_keys $meth, $table, $key_ref
#   Check attributes. Dies on invalid (not registererd) attributes.
#

sub _check_keys {
    my ( $self, $meth, $table, $key_ref ) = @_;
    my $table_ref = $self->_check_table( $meth, $table );
    
    my @keys = ref( $key_ref )
        ? ( ref( $key_ref ) eq 'ARRAY'
            ? @$key_ref
            : keys %$key_ref
        )
        : ( $key_ref )
    ;
    
    my @invalid_keys = grep { ! defined $table_ref->{ attributes }->{ $_ } } @keys;
    croak "$meth: Invalid keys: ". join( ', ', @invalid_keys )
        if @invalid_keys;
    
    return wantarray ? @keys : \@keys;
}


#
# _build_pk_filter $table, $where_ref, $node_ref
#   Build attribute filter "HashKeyElement" and "RangeKeyElement".
#   Hash key and range key will be deleted from where clause
#

sub _build_pk_filter {
    my ( $self, $table, $where_ref, $node_ref ) = @_;
    # primary key
    my $table_ref = $self->_check_table( $table );
    my $hash_value = delete $where_ref->{ $table_ref->{ hash_key } };
    my $hash_type  = $self->_attrib_type( $table, $table_ref->{ hash_key } );
    $node_ref->{ HashKeyElement } = { $hash_type => $hash_value . '' };
    if ( defined $table_ref->{ range_key } ) {
        my $range_value = delete $where_ref->{ $table_ref->{ range_key } };
        my $range_type  = $self->_attrib_type( $table, $table_ref->{ range_key } );
        $node_ref->{ RangeKeyElement } = { $range_type => $range_value . '' };
    }
}


#
# _build_attrib_filter $table, $where_ref, $node_ref
#   Build attribute filter "Expected" from given where-clause-ref
# {
#     attrib1 => 'somevalue', # -> { attrib1 => { Value => { S => 'somevalue' } } }
#     attrib2 => \1,          # -> { attrib2 => { Exists => true } }
#     attrib3 => {            # -> { attrib3 => { Value => { S => 'bla' } } }
#         value => 'bla'
#     }
# }
#

sub _build_attrib_filter {
    my ( $self, $table, $where_ref, $node_ref ) = @_;
    my $table_ref = $self->_check_table( $table );
    foreach my $key( keys %$where_ref ){
        my $type = $table_ref->{ attributes }->{ $key };
        my %cur;
        unless( ref( $where_ref->{ $key } ) ) {
            $where_ref->{ $key } = { value => $where_ref->{ $key } };
        }
        if ( ref( $where_ref->{ $key } ) eq 'SCALAR' ) {
            $cur{ Exists } = $where_ref->{ $key };
        }
        else {
            if ( defined( my $value = $where_ref->{ $key }->{ value } ) ) {
                $cur{ Value } = { $type => $value. '' };
            }
            if ( defined $where_ref->{ $key }->{ exists } ) {
                $cur{ Exists } = $where_ref->{ $key }->{ exists } ? \1 : \0;
            }
        }
        $node_ref->{ $key } = \%cur if keys %cur;
    }
}


#
# _attrib_type $table, $key
#   Returns type ("S", "N", "NS", "SS") of existing attribute in table
#

sub _attrib_type {
    my ( $self, $table, $key ) = @_;
    my $table_ref = $self->_check_table( $table );
    return defined $table_ref->{ attributes }->{ $key } ? $table_ref->{ attributes }->{ $key } : "S";
}


#
# _attribs $table
#   Returns list of attributes in table
#

sub _attribs {
    my ( $self, $table ) = @_;
    my $table_ref = $self->_check_table( $table );
    return sort keys %{ $table_ref->{ attributes } };
}


#
# _format_item $table, $from_ref
#
#   Formats result item into simpler format
# {
#     attrib => { S => "bla" }
# }
#
#   to
# {
#     attrib => 'bla'
# }
#

sub _format_item {
    my ( $self, $table, $from_ref ) = @_;
    my $table_ref = $self->_check_table( format_item => $table );
    my %formatted;
    while( my( $attrib, $type ) = each %{ $table_ref->{ attributes } } ) {
        next unless defined $from_ref->{ $attrib };
        $formatted{ $attrib } = $from_ref->{ $attrib }->{ $type };
    }
    return \%formatted;
}


#
# _table_name
#   Returns prefixed table name
#

sub _table_name {
    my ( $self, $table, $remove ) = @_;
    return $remove ? substr( $table, length( $self->namespace ) ) : $self->namespace. $table;
}


#
# _extract_error_message
#

sub _extract_error_message {
    my ( $self, $response ) = @_;
    my $msg = '';
    if ( $response ) {
        my $json = eval { $self->json->decode( $response->decoded_content ) } || { error => "Failed to parse JSON result" };
        if ( defined $json->{ __type } ) {
            $msg = join( ' ** ',
                "ErrorType: $json->{ __type }",
                "ErrorMessage: $json->{ message }",
            );
        }
        else {
            $msg = $json->{ error };
        }
    }
    else {
        $msg = 'No response received. DynamoDB down?'
    }
}

__PACKAGE__->meta->make_immutable;


=head1 AUTHOR

Ulrich Kautz <uk@fortrabbit.de>

=head1 COPYRIGHT

Copyright (c) 2011 the L</AUTHOR> as listed above

=head1 LICENCSE

Same license as Perl itself.

=cut

1;