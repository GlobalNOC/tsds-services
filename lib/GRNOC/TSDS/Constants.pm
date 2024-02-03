package GRNOC::TSDS::Constants;

use strict;
use warnings;

use lib '/opt/grnoc/venv/grnoc-tsds-services/lib/perl5';

use base 'Exporter';

our @EXPORT = qw( HIGH_RESOLUTION_DOCUMENT_SIZE AGGREGATE_DOCUMENT_SIZE IGNORE_DATABASES );

use constant AGGREGATE_DOCUMENT_SIZE => 1000;
use constant HIGH_RESOLUTION_DOCUMENT_SIZE => 1000;

use constant IGNORE_DATABASES => {'admin' => 1,
                                  'config' => 1,
                                  'test' => 1};

1;
