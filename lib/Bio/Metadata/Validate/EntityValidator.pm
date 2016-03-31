
=head1 LICENSE
   Copyright 2015 EMBL - European Bioinformatics Institute
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
     http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
=cut

package Bio::Metadata::Validate::EntityValidator;

use strict;
use warnings;

use Carp;
use Moose;
use namespace::autoclean;

use Data::Dumper;

use Bio::Metadata::Types;
use Bio::Metadata::Validate::ValidationOutcome;
use Bio::Metadata::Entity;

use MooseX::Params::Validate;
use Bio::Metadata::Validate::RequirementValidator;
use Bio::Metadata::Validate::UnitAttributeValidator;
use Bio::Metadata::Validate::TextAttributeValidator;
use Bio::Metadata::Validate::NumberAttributeValidator;
use Bio::Metadata::Validate::EnumAttributeValidator;
use Bio::Metadata::Validate::OntologyUriAttributeValidator;
use Bio::Metadata::Validate::OntologyTextAttributeValidator;
use Bio::Metadata::Validate::OntologyIdAttributeValidator;
use Bio::Metadata::Validate::UriValueAttributeValidator;
use Bio::Metadata::Validate::DateAttributeValidator;
use Bio::Metadata::Validate::RelationshipValidator;
use Bio::Metadata::Validate::NcbiTaxonomyValidator;
use Bio::Metadata::Validate::OntologyAttrNameValidator;
use Bio::Metadata::Validate::FaangBreedValidator;

has 'rule_set' =>
  ( is => 'rw', isa => 'Bio::Metadata::Rules::RuleSet', required => 1 );
has 'requirement_validator' => (
  is       => 'rw',
  isa      => 'Bio::Metadata::Validate::RequirementValidator',
  required => 1,
  default  => sub { Bio::Metadata::Validate::RequirementValidator->new() }
);
has 'unit_validator' => (
  is       => 'rw',
  isa      => 'Bio::Metadata::Validate::UnitAttributeValidator',
  required => 1,
  default  => sub { Bio::Metadata::Validate::UnitAttributeValidator->new() }
);
has 'type_validator' => (
  traits   => ['Hash'],
  is       => 'rw',
  isa      => 'HashRef[Bio::Metadata::Validate::AttributeValidatorRole]',
  required => 1,
  handles  => { get_type_validator => 'get', },
  default  => sub {
    {
      text   => Bio::Metadata::Validate::TextAttributeValidator->new(),
      number => Bio::Metadata::Validate::NumberAttributeValidator->new(),
      enum   => Bio::Metadata::Validate::EnumAttributeValidator->new(),
      ontology_uri =>
        Bio::Metadata::Validate::OntologyUriAttributeValidator->new(),
      ontology_text =>
        Bio::Metadata::Validate::OntologyTextAttributeValidator->new(),
      ontology_id =>
        Bio::Metadata::Validate::OntologyIdAttributeValidator->new(),
      ontology_attr_name =>
        Bio::Metadata::Validate::OntologyAttrNameValidator->new(),
      uri_value => Bio::Metadata::Validate::UriValueAttributeValidator->new(),
      date      => Bio::Metadata::Validate::DateAttributeValidator->new(),
      relationship => Bio::Metadata::Validate::RelationshipValidator->new(),
      ncbi_taxon   => Bio::Metadata::Validate::NcbiTaxonomyValidator->new(),
      faang_breed  => Bio::Metadata::Validate::FaangBreedValidator->new(),
    };
  },
);
has 'outcome_for_unexpected_attributes' => (
  is       => 'rw',
  isa      => 'Bio::Metadata::Validate::OutcomeEnum',
  required => 1,
  default  => sub { 'warning' },
);
has 'message_for_unexpected_attributes' => (
  is       => 'rw',
  isa      => 'Str',
  required => 1,
  default  => sub { 'attribute not in rule set' },
);

sub check_all {
  my ( $self, $entities ) = @_;

  my %entity_status;
  my %entity_outcomes;
  my %attribute_status;
  my %attribute_outcomes;
  my %entity_rule_groups;

  my %dupe_check;
  my %entities_by_id;
  for my $e (@$entities) {
    if ( $dupe_check{ $e->id } ) {
      $dupe_check{ $e->id } = [];
    }
    push @{ $dupe_check{ $e->id } }, $e;
    $entities_by_id{ $e->id } = $e;
  }

  $self->get_type_validator('relationship')->entities_by_id( \%entities_by_id );

  for my $e (@$entities) {
    my ( $status, $outcomes, $rule_groups ) = $self->check($e);

    if ( scalar @{ $dupe_check{ $e->id } } > 1 ) {
      push @$outcomes,
        Bio::Metadata::Validate::ValidationOutcome->new(
        outcome => 'error',
        message => 'multiple entities with this id found'
        );
      $status = 'error';
    }

    $entity_status{$e}      = $status;
    $entity_outcomes{$e}    = $outcomes;
    $entity_rule_groups{$e} = $rule_groups;

    for my $o (@$outcomes) {
      for my $a ( $o->all_attributes ) {
        if ( !exists $attribute_outcomes{$a} ) {
          $attribute_status{$a}   = $o->outcome;
          $attribute_outcomes{$a} = [];
        }
        push @{ $attribute_outcomes{$a} }, $o;

        if ( $o->outcome eq 'error'
          && $attribute_status{$a} ne 'error' )
        {
          $attribute_status{$a} = 'error';
        }
        if ( $o->outcome eq 'warning'
          && $attribute_status{$a} eq 'pass' )
        {
          $attribute_status{$a} = 'warning';
        }
      }
    }

  }

  return (
    \%entity_status,      \%entity_outcomes, \%attribute_status,
    \%attribute_outcomes, \%entity_rule_groups
  );
}

sub check {
  my ( $self, $entity ) = @_;

  my @all_outcomes;
  my @rule_groups_used;
  my $organised_attributes  = $entity->organised_attr;
  my $requirement_validator = $self->requirement_validator;
  my $unit_validator        = $self->unit_validator;

RULE_GROUP: for my $rule_group ( $self->rule_set->all_rule_groups ) {

    if ( $rule_group->condition
      && !$rule_group->condition->entity_passes($entity) )
    {
      next RULE_GROUP;
    }
    push @rule_groups_used, $rule_group;

    for my $rule ( $rule_group->all_rules ) {
      my @r_outcomes;

      my $type_validator = $self->get_type_validator( $rule->type );
      croak( "No type validator for " . $rule->type )
        if ( !$type_validator );

      my $normalised_rule_name =
        $entity->normalise_attribute_name( $rule->name );

      my $attrs = $organised_attributes->{$normalised_rule_name} // [];
      delete $organised_attributes->{$normalised_rule_name};

      push @r_outcomes,
        $requirement_validator->validate_requirements( $rule, $attrs );

      for my $a (@$attrs) {
        if ( $a->allow_further_validation ) {
          if ( $rule->count_valid_units ) {
            push @r_outcomes, $unit_validator->validate_attribute( $rule, $a );
          }
          push @r_outcomes, $type_validator->validate_attribute( $rule, $a );
        }
      }

      for my $o (@r_outcomes) {
        $o->rule_group($rule_group);
        $o->entity($entity);
      }
      push @all_outcomes, grep { $_->outcome ne 'pass' } @r_outcomes;
    }
  }

  for my $unexpected_attributes ( values %$organised_attributes ) {
    push @all_outcomes,
      Bio::Metadata::Validate::ValidationOutcome->new(
      attributes => $unexpected_attributes,
      entity     => $entity,
      outcome    => $self->outcome_for_unexpected_attributes,
      message    => $self->message_for_unexpected_attributes,
      );
  }

  my $outcome_overall = 'pass';
  for my $o (@all_outcomes) {
    if ( $o->outcome eq 'error' ) {
      $outcome_overall = 'error';
      last;
    }
    if ( $o->outcome eq 'warning' ) {
      $outcome_overall = 'warning';
    }
  }

  return pos_validated_list(
    [ $outcome_overall, \@all_outcomes, \@rule_groups_used ],
    { isa => 'Bio::Metadata::Validate::OutcomeEnum' },
    { isa => 'Bio::Metadata::Validate::ValidationOutcomeArrayRef' },
    { isa => 'Bio::Metadata::Rules::RuleGroupArrayRef' }
  );
}

__PACKAGE__->meta->make_immutable;

1;
