#!/usr/bin/perl

use strict;
use warnings;
use Test::More tests => 9;

use Net::Amazon::DynamoDB;

use Test::MockObject;
use Test::LWP::UserAgent;
use HTTP::Response;

my $signer = Test::MockObject->new();
$signer->set_isa('Net::Amazon::AWSSign');
$signer->mock('addRESTSecret',sub{
  return 'http://example.com';
});

# A DDB that doesn't yet have a security token.
{
  my $ua = get_mock_ua();

  my $ddb = Net::Amazon::DynamoDB->new(
    access_key  => 'abcdefghijklmnopqrstuvwxyz',
    secret_key  => 'zyxwvutsrqponmlkjihgfedcba',
    tables      => {},
    _aws_signer => $signer,
    lwp         => $ua,
  );

  $ddb->_init_security_token;
  my $expiration = $ddb->_credentials_expire;

  isa_ok($expiration,'DateTime');
  cmp_ok($expiration,'>',DateTime->now,'Token is not expired yet');

  my $last_request = $ua->last_http_request_sent;

  ok($last_request,'A token request was made');
}

# Now do a test with expired credentials.
{
  my $ua = get_mock_ua();

  my $ddb = Net::Amazon::DynamoDB->new(
    access_key  => 'abcdefghijklmnopqrstuvwxyz',
    secret_key  => 'zyxwvutsrqponmlkjihgfedcba',
    tables      => {},
    _aws_signer => $signer,
    lwp         => $ua,
  );

  my $one_minute_ago = DateTime->now->subtract( minutes => 1 );
  $ddb->_credentials_expire( $one_minute_ago );

  $ddb->_init_security_token;

  my $expiration = $ddb->_credentials_expire;

  isa_ok($expiration,'DateTime');
  cmp_ok($expiration,'>',DateTime->now,'Token is not expired yet');

  my $last_request = $ua->last_http_request_sent;

  ok($last_request,'A token request was made');
}

# Now do a test with still valid credentials.
{
  my $ua = get_mock_ua();

  my $ddb = Net::Amazon::DynamoDB->new(
    access_key  => 'abcdefghijklmnopqrstuvwxyz',
    secret_key  => 'zyxwvutsrqponmlkjihgfedcba',
    tables      => {},
    _aws_signer => $signer,
    lwp         => $ua,
  );

  my $plus_one_hour  = DateTime->now->add( hours => 1 );
  $ddb->_credentials_expire( $plus_one_hour );
  $ddb->_credentials({ these => "aren't", real => "credentials" });

  $ddb->_init_security_token;

  my $expiration = $ddb->_credentials_expire;

  isa_ok($expiration,'DateTime');
  cmp_ok($expiration,'>',DateTime->now,'Token is not expired yet');

  my $last_request = $ua->last_http_request_sent;

  ok(! defined($last_request),'A token request was NOT made');
}

sub get_mock_ua {
  my $token_expiration = shift || DateTime->now->add( hours => 1 );
  my $ua = Test::LWP::UserAgent->new;

  my $exp_iso8601 = $token_expiration->iso8601;
  my $token_response = <<EOF;
  <GetSessionTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <GetSessionTokenResult>
      <Credentials>
        <SessionToken>AQoDY8d5ECYa0AH820eMIj5H8bfCf5zRG4VF4heIxuHeuzBWcHftN9Cqf+6tcAK8vGLB76ja0Wq9iM9GIKE7Y9f8anynRG4KlsjpOzbL5UNIj6fgHsdbJFBmyu9eby4lSTLWOstgcTQZt3gwYa7nI7lU7JyoQx+3J7rlQJKyZMs7zSQ4vxe7eXYJO7tC2WbY70guBzTU57pzNP2T7QpZ9S3h75rGzW6E7rJxeIaIuAu7hTfWoyTlyA2pkD007dAHN2ntDjU7HQNudzTUZCotRsh45vcqf0E+JOahIKrf8osF</SessionToken>
        <SecretAccessKey>b7yo7Mv7WIVJ7l1ftp7NLH7F0ga7GHmnKeaL30WZ</SecretAccessKey>
        <Expiration>$exp_iso8601</Expiration>
        <AccessKeyId>XXXXXXXXXXXXXXXXXXXX</AccessKeyId>
      </Credentials>
    </GetSessionTokenResult>
    <ResponseMetadata>
      <RequestId>2e08a198-afc2-dead-beef-978228beea7d</RequestId>
    </ResponseMetadata>
  </GetSessionTokenResponse>
EOF

  $ua->map_response(
    qr{example.com},
    HTTP::Response->new(
      200, 'OK', [ 'Content-Type' => 'application/xml' ],
      $token_response
    ));

  return $ua;
}
