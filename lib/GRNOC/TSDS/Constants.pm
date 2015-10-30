package GRNOC::TSDS::Constants;

use strict;
use warnings;

use base 'Exporter';

our @EXPORT = qw( HIGH_RESOLUTION_DOCUMENT_SIZE AGGREGATE_DOCUMENT_SIZE EVENT_DOCUMENT_DURATION IGNORE_DATABASES );


use constant EVENT_DOCUMENT_DURATION => 60 * 60 * 24;
use constant AGGREGATE_DOCUMENT_SIZE => 1000;
use constant HIGH_RESOLUTION_DOCUMENT_SIZE => 1000;

use constant IGNORE_DATABASES => {'admin' => 1,
                                  'config' => 1,
                                  'test' => 1};

1;
