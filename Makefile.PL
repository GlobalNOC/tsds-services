use strict;
use warnings;

use ExtUtils::MakeMaker;

sub MY::postamble { <<'END'; }

rpm:	dist
	rpmbuild -ta grnoc-tsds-services-$(VERSION).tar.gz
END

sub MY::test
	{
    q(
TEST_VERBOSE=1

test :	pure_all
	$(FULLPERL) t/TEST $(TEST_VERBOSE)

test_jenkins : pure_all
	$(FULLPERL) t/TEST $(TEST_VERBOSE) --formatter=TAP::Formatter::Console

par :
	pp -M utf8 -M XML::LibXML::SAX -I lib/ -c -x bin/tsds_writer -o tsds_writer

    );
}

WriteMakefile(
              NAME                => 'grnoc-tsds-services',
              AUTHOR              => 'GRNOC Software Engineering <globalnoc@iu.edu>',
              VERSION_FROM        => 'lib/GRNOC/TSDS.pm',
              PL_FILES            => {},
              PREREQ_PM => {
                           },
              dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
              clean               => { FILES => 'grnoc-tsds-services-*' },
             );
