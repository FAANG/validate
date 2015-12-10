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

package Bio::Metadata::ValidateSchema::ValidationOutcome;

use strict;
use warnings;

use Moose;
use MooseX::Params::Validate;
use namespace::autoclean;

use Bio::Metadata::Entity;
use Bio::Metadata::ValidateSchema::Warning;

has 'outcome' => ( is => 'rw', isa => 'Bio::Metadata::ValidateSchema::OutcomeEnum' );
has 'message' => ( is => 'rw', isa => 'Str' );

has 'entity' => ( is => 'rw', isa => 'Bio::Metadata::Entity' );

has 'warnings' => (
		   traits  => ['Array'],
		   is => 'rw',
		   isa => 'Bio::Metadata::ValidateSchema::WarningArrayRef',
		    handles => {
				all_warnings   => 'elements',
				add_warning    => 'push',
				count_warnings => 'count',
				get_warning    => 'get',
			       },
		   default => sub { [] },
		   coerce  => 1,
		  );

__PACKAGE__->meta->make_immutable;
1;
