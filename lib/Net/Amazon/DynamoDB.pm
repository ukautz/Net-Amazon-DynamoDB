package Net::Amazon::DynamoDB;

=head1 NAME

Net::Amazon::DynamoDB - Description

=head1 DESCRIPTION

Long description

=head1 SYNOPSIS

    Usage Example

=cut

use Moose;

use version 0.74; our $VERSION = qv( "v0.1.0" );

use LWP::UserAgent;
use HTTP::Request;
use JSON;
use Net::Amazon::AWSSign;
use DateTime;
use DateTime::Format::HTTP;
use XML::Simple qw/ XMLin /;
use Digest::SHA qw/ sha256 hmac_sha256_base64 /;
use Params::Validate qw/ validate SCALAR ARRAYREF HASHREF /;

=head1 CLASS ATTRIBUTES

=head2 tables

The table definitions

=cut

has tables => ( isa => 'HashRef[HashRef]', is => 'rw', required => 1 );

=head2 lwp

Contains C<LWP::UserAgent> instance.

=cut

has lwp => ( isa => 'LWP::UserAgent', is => 'rw', default => sub { LWP::UserAgent->new() } );

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

=for _aws_signer

Contains C<Net::Amazon::AWSSign> instance.

=cut

has _aws_signer => ( isa => 'Net::Amazon::AWSSign', is => 'rw', predicate => '_has_aws_signer' );

=for _security_token_url

URL for receiving security token

=cut

has _security_token_url => ( isa => 'Str', is => 'rw', default => 'https://sts.amazonaws.com/?Action=GetSessionToken&Version=2011-06-15' );


=for _credentials

Contains credentials received by GetSession

=cut

has _credentials => ( isa => 'HashRef[Str]', is => 'rw', predicate => '_has_credentials' );


=for _error

Contains credentials received by GetSession

=cut

has _error => ( isa => 'Str', is => 'rw', predicate => '_has_error' );

=head1 METHODS

=head2 create_table

Create a new Table

=cut

sub create_table {
    my ( $self, $table, $read_amount, $write_amount ) = @_;
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
    
    if ( $res_ok && defined $json_ref->{ TableDescription } && defined $json_ref->{ TableDescription }->{ TableName } ) {
        return wantarray ? ( 1, $json_ref->{ TableDescription } ) : 1;
    }
    $self->error( "Failed to create table '$table' (status: '". $res->status_line. "', response: '" . $res->decoded_content. "')" )
        && return wantarray ? ( 0, $json_ref ) : 0;
}

=head2 delete_table

Delete an existing (and defined) table

=cut

sub delete_table {
    my ( $self, $table ) = @_;
    
    # check & get table definition
    my $table_ref = $self->_check_table( "create_table", $table );
    
    # perform create
    my ( $res, $res_ok, $json_ref ) = $self->request( DeleteTable => { TableName => $table } );
    
    use Data::Dumper; print Dumper( [ $json_ref ] );
}

=head2 describe_table

Returns bool whether table exists or not

=cut

sub describe_table {
    my ( $self, $table ) = @_;
    
    # check table definition
    $self->_check_table( "exists_table", $table );
    
    my ( $res, $res_ok, $json_ref ) = $self->request( DescribeTable => { TableName => $table } );
    use Data::Dumper; print Dumper( [ $json_ref ] );
}

=head2 exists_table

Returns bool whether table exists or not

=cut

sub exists_table {
    my ( $self, $table ) = @_;
    
    # check table definition
    $self->_check_table( "exists_table", $table );
    
    my ( $res, $res_ok, $json_ref ) = $self->request( DescribeTable => { TableName => $table } );
    return defined $json_ref->{ Table } && defined $json_ref->{ Table }->{ ItemCount } ? 1 : 0;
}

=head2 put_item

Write item to table

=cut

sub put_item {
    my ( $self, $table, $item_ref, $where_ref, $return_old ) = @_;
    
    # check definition
    my $table_ref = $self->_check_table( "put_item", $table );
    
    # check primary keys
    die "put_item: Missing value for Hash Key '$table_ref->{ hash_key }'"
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
        my $type = $table_ref->{ attributes }->{ $key };
        #my $value = $type eq 'N' ? $item_ref->{ $key } + 0 : $item_ref->{ $key } .'';
        my $value = $item_ref->{ $key } .'';
        $put{ Item }->{ $key } = {
            $type => $value
        };
    }
    
    # build possible where clause
    if ( $where_ref ) {
        $put{ Expected } = {};
        
        foreach my $key( keys %$where_ref ){
            my $type = $table_ref->{ attributes }->{ $key };
            my %cur;
            if ( defined( my $value = $where_ref->{ $key }->{ value } ) ) {
                $value = $type eq 'N' ? $value + 0 : $value .'';
                $cur{ $type } = $value;
            }
            if ( defined $where_ref->{ $key }->{ exists } ) {
                $cur{ Exists } = $where_ref->{ $key }->{ exists } ? \1 : \0;
            }
            $put{ Expected }->{ $key } = \%cur if keys %cur;
        }
    }
    
    # add return value, if set
    $put{ ReturnValues } = 'ALL_OLD' if $return_old;
    
    
    # perform create
    my ( $res, $res_ok, $json_ref ) = $self->request( PutItem => \%put );
    
    use Data::Dumper; print Dumper( { PUTRES => $json_ref } );
    
    return $json_ref;
}

=head2 get_item

Read a single item by hash (and range) key.

=cut

sub get_item {
    my ( $self, $table, $pk_ref, $args_ref ) = @_;
    $args_ref ||= {
        consistent => 0,
        attributes => undef
    };
    
    # check definition
    my $table_ref = $self->_check_table( "get_item", $table );
    
    # check primary keys
    die "get_item: Missing value for Hash Key '$table_ref->{ hash_key }'"
        unless defined $pk_ref->{ $table_ref->{ hash_key } }
        && length( $pk_ref->{ $table_ref->{ hash_key } } );
    die "get_item: Missing value for Range Key '$table_ref->{ range_key }'"
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
    return $json_ref->{ Item } if $res_ok && defined $json_ref->{ Item };
    
    # set error
    $self->error( 'get_item failed: '. $res->decoded_content );
    return ;
}

=head2 query_items $table, $where, $args

Search in a table with hash AND range key.

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

=back

=back


=cut

sub query_items {
    my ( $self, $table, $filter_ref, $args_ref ) = @_;
    $args_ref ||= {
        limit       => undef,   # amount of items
        consistent  => 0,       # default: eventually, not hard, conistent
        backward    => 0,       # default: forward
        start_key   => undef,   # eg { pk_name => 123, pk_other => 234 }
        attributes  => undef,   # eq [ qw/ attrib1 attrib2 / ]
        count       => 0,       # returns amount instead of the actual result
    };
    
    # check definition
    die "query_items: Table '$table' does not exist in table definition"
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
    die "query_items: Missing hash key value in filter-clause"
        unless defined $filter{ $table_ref->{ hash_key } };
    $query{ HashKeyValue } = {
        $self->_attrib_type( $table, $table_ref->{ hash_key } ) =>
            ( delete $filter{ $table_ref->{ hash_key } } ) . ''
    };
    
    # adding range to filter
    if ( defined $table_ref->{ range_key }) {
        die "query_items: Missing range key value in filter-clause"
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
    die "query_items: Cannot use keys ". join( ', ', sort keys %filter ). " in in filter - only hash and range key allowed."
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
    my ( $res, $res_ok, $json_ref ) = $self->request( Query => \%query );
    
    use Data::Dumper; die Dumper( [ \%query, $json_ref ] );
}




=head2 scan_items $table, $filter, $args

Performs scan on table. The result is B<eventually consistent>. Non hash or range keys are allowed in the filter.

=cut

sub scan_items {
    my ( $self, $table, $filter_ref, $args_ref ) = @_;
    $args_ref ||= {
        limit       => undef,   # amount of items
        start_key   => undef,   # eg { hash_key => 1, range_key => "bla" }
        attributes  => undef,   # eq [ qw/ attrib1 attrib2 / ]
        count       => 0,       # returns amount instead of the actual result
    };
    
    # check definition
    die "scan_items: Table '$table' does not exist in table definition"
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
    my ( $res, $res_ok, $json_ref ) = $self->request( Query => \%query );
    
    use Data::Dumper; die Dumper( \%query, $json_ref );
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
    
    # run request
    my $response = $self->lwp->request( $request );
    use Data::Dumper; print Dumper( $response );
    
    # get json
    my $json_ref = $response
        ? eval { $self->json->decode( $response->decoded_content ) } || { error => "Failed to parse JSON result" }
        : { error => "Failed to get result" };
    
    return wantarray ? ( $response, $response ? $response->is_success : 0, $json_ref ) : $json_ref;
}


sub error {
    my ( $self, $str ) = @_;
    if ( $str ) {
        $self->_error( $str );
    }
    return $self->_error if $self->_has_error;
    return ;
}

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



sub _check_table {
    my ( $self, $meth, $table ) = @_;
    unless( $table ) {
        $table = $meth;
        $meth = "check_table";
    }
    die "$meth: Table '$table' not defined"
        unless defined $self->tables->{ $table };
    
    return $self->tables->{ $table };
}

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
    die "$meth: Invalid keys: ". join( ', ', @invalid_keys )
        if @invalid_keys;
    
    return wantarray ? @keys : \@keys;
}

sub _attrib_type {
    my ( $self, $table, $key ) = @_;
    my $table_ref = $self->_check_table( $table );
    return defined $table_ref->{ attributes }->{ $key } ? $table_ref->{ attributes }->{ $key } : "S";
}

=head1 AUTHOR

Ulrich Kautz <uk@fortrabbit.de>

=head1 COPYRIGHT

Copyright (c) 2011 the L</AUTHOR> as listed above

=head1 LICENCSE

Same license as Perl itself.

=cut

1;