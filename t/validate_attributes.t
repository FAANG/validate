#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw/$Bin/;
use lib "$Bin/../lib";
use Data::Dumper;
use Test::More;

use Bio::Metadata::Attribute;
use Bio::Metadata::Rules::Rule;
use Bio::Metadata::Validate::TextAttributeValidator;
use Bio::Metadata::Validate::NumberAttributeValidator;
use Bio::Metadata::Validate::EnumAttributeValidator;

my $text_attr = Bio::Metadata::Attribute->new( value => 'text', );
my $num_attr  = Bio::Metadata::Attribute->new( value => 10, );

my %base_outcome_h = (
    rule_group_name => undef,
    entity_id       => undef,
    entity_type     => undef,
    rule            => undef,
    attributes      => [],
);

text_rules();
num_rules();
enum_rules();

sub text_rules {
    my $text_rule = Bio::Metadata::Rules::Rule->new( type => 'text', );
    my $text_attr_validator =
      Bio::Metadata::Validate::TextAttributeValidator->new();

    my %expected_t_outcome = (
        %base_outcome_h,
        message    => undef,
        outcome    => 'pass',
    );

    my $t_outcome =
      $text_attr_validator->validate_attribute( $text_rule, $text_attr );

    is_deeply( $t_outcome->to_hash, \%expected_t_outcome,
        "text rule passes any content" );

}

sub num_rules {
    my $num_rule = Bio::Metadata::Rules::Rule->new( type => 'number' );
    my $num_attr_validator =
      Bio::Metadata::Validate::NumberAttributeValidator->new();

    my %expected_n_outcome = (
        %base_outcome_h,
        message => undef,
        outcome => 'pass',
    );

    my $n_outcome =
      $num_attr_validator->validate_attribute( $num_rule, $num_attr );

    is_deeply( $n_outcome->to_hash, \%expected_n_outcome,
        "number rule passes integer" );

    my %expected_t_outcome = (
        %base_outcome_h,
        message    => 'value is not a number',
        outcome    => 'error',
    );

    my $t_outcome =
      $num_attr_validator->validate_attribute( $num_rule, $text_attr );
    is_deeply( $t_outcome->to_hash, \%expected_t_outcome,
        "number rule rejects text" );
}

sub enum_rules {
    my $enum_rule = Bio::Metadata::Rules::Rule->new( type => 'enum', valid_values => ['text','horse'] );
    my $enum_attr_validator =
      Bio::Metadata::Validate::EnumAttributeValidator->new();

    my %expected_t_outcome = (
        %base_outcome_h,
        message => undef,
        outcome => 'pass',
    );

    my $t_outcome =
      $enum_attr_validator->validate_attribute( $enum_rule, $text_attr );

    is_deeply( $t_outcome->to_hash, \%expected_t_outcome,
        "enum rule passes expected value" );

    my %expected_n_outcome = (
        %base_outcome_h,
        message    => 'value is not in list of valid values:text,horse',
        outcome    => 'error',
    );

    my $n_outcome =
      $enum_attr_validator->validate_attribute( $enum_rule, $num_attr );
    is_deeply( $n_outcome->to_hash, \%expected_n_outcome,
        "enum rule rejects unexpected value" );
}

done_testing();