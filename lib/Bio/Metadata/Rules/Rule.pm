# Copyright 2015 European Molecular Biology Laboratory - European Bioinformatics Institute
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
package Bio::Metadata::Rules::Rule;

use strict;
use warnings;

use Moose;
use namespace::autoclean;
use Bio::Metadata::Types;

has 'name' => ( is => 'rw', isa => 'Str' );
has 'type' => ( is => 'rw', isa => 'Bio::Metadata::Rules::Rule::TypeEnum' );
has 'mandatory' =>
  ( is => 'rw', isa => 'Bio::Metadata::Rules::Rule::MandatoryEnum' );
has 'allow_multiple' => ( is => 'rw', isa => 'Bool', default => sub { '' } );
has 'valid_values' => (
    traits  => ['Array'],
    is      => 'rw',
    isa     => 'ArrayRef[Str]',
    handles => {
        all_valid_values   => 'elements',
        add_valid_values   => 'push',
        count_valid_values => 'count',
        get_valid_values   => 'get',
        find_valid_value   => 'first',
        join_valid_values  => 'join',
    },
    default => sub { [] },
);

sub to_hash {
    my ($self) = @_;

    return {
        name           => $self->name,
        type           => $self->type,
        mandatory      => $self->mandatory,
        allow_multiple => $self->allow_multiple,
    };
}
1;
