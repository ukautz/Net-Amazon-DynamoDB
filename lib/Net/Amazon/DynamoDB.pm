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
    my ( $self, $table, @args ) = @_;
    
    # validate args
    my %args_ok = validate( @args, {
        primary_key => {
            type => SCALAR,
        },
        primary_key_type => {
            type     => SCALAR,
            default  => 'N',
            regex    => qr/^(?:S|N)$/,
            optional => 1
        },
        ranged_key => {
            type     => SCALAR,
            optional => 1,
            depends  => [ qw/ primary_key / ]
        },
        ranged_key_type => {
            type     => SCALAR,
            default  => 'N',
            regex    => qr/^(?:S|N)$/,
            optional => 1,
            depends  => [ qw/ ranged_key / ]
        },
        read_amount => {
            type  => SCALAR,
            regex => qr/^[0-9]+$/,
        },
        write_amount => {
            type  => SCALAR,
            regex => qr/^[0-9]+$/,
        }
    } );
    
    # build PK
    my %pk;
    $pk{ HashKeyElement } = {
        AttributeName => $args_ok{ primary_key },
        AttributeType => $args_ok{ primary_key_type },
    };
    if ( $args_ok{ ranged_key } ) {
        $pk{ RangedKeyElement } = {
            AttributeName => $args_ok{ primary_key },
            AttributeType => $args_ok{ primary_key_type },
        };
    }
    
    # perform create
    my ( $res ) = $self->request( CreateTable => {
        TableName => $table,
        KeySchema => \%pk,
        ProvisionedThroughput => {
            ReadCapacityUnits  => $args_ok{ read_amount } + 0,
            WriteCapacityUnits => $args_ok{ write_amount } + 0,
        }
    } );
    
    if ( $res && $res->is_success ) {
        my $json = eval { $self->json->decode( $res->decoded_content ) };
        $self->error( "Failed to decoded response for 'create_table'" ) && return if $@;;
        if ( defined $json->{ TableDescription } && defined $json->{ TableDescription }->{ TableName } ) {
            return wantarray ? ( 1, $json->{ TableDescription } ) : 1;
        }
        $self->error( 'Error creating table. Response: '. $res->decoded_content ) && return
    }
    $self->error( "Did not receive result in 'create_table' (status: ". $res->status_line. ")" ) && return;
}

=head2 put_item

Write item to table

=cut

sub put_item {
    my ( $self, $table, @args ) = @_;
    
    # validate args
    my %args_ok = validate( @args, {
        item => {
            type => HASHREF,
        },
        where => { # { attr1 => { type => value }, attr2 => { type => value }, ... }
            type     => HASHREF,
            optional => 1
        },
        check_exists => { # [ qw/ attr1 attr2 / ]
            type     => ARRAYREF,
            optional => 1,
        },
        return_old => {
            type     => SCALAR,
            optional => 1,
            regex    => qr/^(0|1)$/,
            default  => 0
        },
    } );
    $self->_check_item_structure( "put_item: item" => $args_ok{ item } );
    
    my %expected;
    if ( defined $args_ok{ where } ) {
        $self->_check_where_structure( "put_item: where" => $args_ok{ where } );
        foreach my $key( keys %{ $args_ok{ where } } ){
            my ( $type, $value ) = %{ $args_ok{ where }->{ $key } };
            $expected{ $key }->{ $type } = $value;
        }
    }
    if ( defined $args_ok{ check_exists } ) {
        foreach my $key( @{ $args_ok{ check_exists } } ){
            $expected{ $key }->{ Exists } = \1;
        }
    }
    
    
    # perform create
    my ( $res ) = $self->request( PutItem => {
        TableName => $table,
        Item      => $args_ok{ item },
        ( keys %expected ? ( Expected => \%expected ) : () ),
        ( $args_ok{ return_old } ? ( ReturnValues => 'ALL_OLD' ) : () )
    } );
    
    if ( $res && $res->is_success ) {
        my $json = eval { $self->json->decode( $res->decoded_content ) };
        $self->error( "Failed to decoded response for 'put_item'" ) && return if $@;;
        return $json;
    }
    $self->error( "Did not receive result in 'put_item' (status: ". $res->status_line. ")" ) && return;
}

=head2 request

Arbirtrary request to DynamoDB API

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
    
    use Data::Dumper; print Dumper( $request );
    
    # run request
    my $response = $self->lwp->request( $request );
    print Dumper( $response );
    return wantarray ? ( $response ) : $response->is_success ? $self->json->decode( $response->decoded_content ) : undef;
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



sub _check_item_structure {
    my ( $self, $method_name, $item_ref ) = @_;
    
    my @error;
    foreach my $key( sort keys %$item_ref ) {
        my $value_ref = $item_ref->{ $key };
        unless( ( ref( $value_ref ) || '' ) eq 'HASH' ) {
            push @error, "Item '$key' of wrong type (HASH expected, got '". ( ref( $value_ref ) || '' ). "')";
            next;
        }
        if ( scalar( keys %$value_ref ) != 1 ) {
            push @error, "Item '$key' not in correct format. Expect {'<S|N>'=>'<value>'}, got ". scalar( keys %$value_ref ). " keys";
            next;
        }
        
        my ( $type, $value ) = %$value_ref;
        unless( $type =~ /^(?:S|N)$/ ) {
            push @error, "Item '$key' type not allowed. Expected 'S' or 'N', got '$type'";
            next;
        }
    }
    
    die "$method_name: ". join( ' / ', @error ) if @error;
    return;
}


=head1 AUTHOR

Ulrich Kautz <uk@fortrabbit.de>

=head1 COPYRIGHT

Copyright (c) 2011 the L</AUTHOR> as listed above

=head1 LICENCSE

Same license as Perl itself.

=cut

1;