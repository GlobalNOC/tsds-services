Summary: GRNOC TSDS Services
Name: grnoc-tsds-services
Version: 1.6.2
Release: 4%{?dist}
License: GRNOC
Group: Measurement
URL: http://globalnoc.iu.edu
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildArch: noarch
BuildRequires: httpd-devel
BuildRequires: mod_perl-devel
BuildRequires: perl-Test-Simple
BuildRequires: perl-Net-RabbitMQ-Management-API
Requires: perl >= 5.8.8
Requires: gcc
Requires: mod_perl
Requires: perl-rrdtool
Requires: perl-boolean
Requires: perl-GRNOC-Config >= 1.0.7
Requires: perl-GRNOC-WebService >= 1.2.8
Requires: perl-GRNOC-WebService-Client >= 1.2.2
Requires: perl-GRNOC-CLI >= 1.0.2-2
Requires: perl-GRNOC-Log
Requires: perl-GRNOC-LockFile >= 1.0.1
Requires: ImageMagick-perl
Requires: perl-JSON
Requires: perl-JSON-XS
Requires: perl-MongoDB >= 1.0
Requires: perl-Data-Compare
Requires: perl-Time-HiRes
Requires: perl-DateTime
Requires: perl-DateTime-Format-Strptime
Requires: perl-Parallel-ForkManager
Requires: perl-List-MoreUtils
Requires: perl-Template-Toolkit
Requires: perl-HTML-Parser
Requires: perl-XML-Writer
Requires: perl-WWW-Mechanize-PhantomJS >= 0.11-2
Requires: perl-Math-Round
Requires: perl-Test-Deep
Requires: perl-Tie-IxHash
Requires: perl-Clone
Requires: perl-Statistics-LineFit
Requires: perl-Marpa-R2
Requires: perl-GDGraph
Requires: perl-Number-Format
Requires: perl-File-Slurp
Requires: perl-Class-Accessor
Requires: perl-Env-C
Requires: perl-Moo
Requires: perl-Net-AMQP-RabbitMQ
Requires: perl-LockFile-Simple
Requires: perl-Type-Tiny
Requires: perl-Types-XSD-Lite
Requires: perl-Redis
Requires: perl-Redis-DistLock
Requires: perl-Cache-Memcached-Fast
Requires: perl-Hash-Merge
Requires: perl-Proc-Daemon
Requires: perl-Sort-Versions
Requires: perl-List-Flatten-Recursive
Requires: perl-GRNOC-TSDS-Aggregate-Histogram
Requires: perl-DBI
Requires: perl-DBD-MySQL
Requires: perl-GRNOC-Counter
Requires: perl-Try-Tiny
Requires: perl(List::Util)
Requires: phantomjs


%description
GRNOC TSDS Services

%prep
%setup -q -n grnoc-tsds-services-%{version}

%build
%{__perl} Makefile.PL PREFIX="%{buildroot}%{_prefix}" INSTALLDIRS="vendor"
make

%install
rm -rf $RPM_BUILD_ROOT
make pure_install

%{__install} -d -p %{buildroot}/etc/grnoc/tsds/services/
%{__install} -d -p %{buildroot}/etc/grnoc/tsds/services/report_templates
%{__install} -d -p %{buildroot}/etc/grnoc/tsds/services/sphinx_templates
%{__install} -d -p %{buildroot}/etc/httpd/conf.d/grnoc/
%{__install} -d -p %{buildroot}/etc/cron.d/
%{__install} -d -p %{buildroot}/etc/sphinx/
%{__install} -d -p %{buildroot}/usr/lib/systemd/system/
%{__install} -d -p %{buildroot}/etc/init.d/
%{__install} -d -p %{buildroot}/usr/bin/
%{__install} -d -p %{buildroot}/usr/lib/grnoc/tsds/services/cgi-bin/
%{__install} -d -p %{buildroot}/usr/share/doc/grnoc/tsds/
%{__install} -d -p %{buildroot}/usr/share/doc/grnoc/tsds/install/
%{__install} -d -p %{buildroot}/var/lib/grnoc/tsds/
%{__install} -d -p %{buildroot}/var/run/grnoc/tsds/services/
%{__install} -d -p %{buildroot}/usr/share/grnoc/tsds-services/
%{__install} -d -p %{buildroot}/usr/share/grnoc/tsds-services/temp/
%{__install} -d -p %{buildroot}/var/lib/mongo/config1
%{__install} -d -p %{buildroot}/var/lib/mongo/config2
%{__install} -d -p %{buildroot}/var/lib/mongo/config3
%{__install} -d -p %{buildroot}/var/lib/mongo/shard1
%{__install} -d -p %{buildroot}/var/lib/mongo/shard2
%{__install} -d -p %{buildroot}/var/lib/mongo/shard3
%{__install} -d -p %{buildroot}/usr/share/grnoc/tsds-services/temp


%{__install} CHANGES.md %{buildroot}/usr/share/doc/grnoc/tsds/CHANGES.md
%{__install} INSTALL.md %{buildroot}/usr/share/doc/grnoc/tsds/INSTALL.md

%{__install} conf/config.xml.example %{buildroot}/etc/grnoc/tsds/services/config.xml
%{__install} conf/meta_config.xml %{buildroot}/etc/grnoc/tsds/services/meta_config.xml
%{__install} conf/mappings.xml.example %{buildroot}/etc/grnoc/tsds/services/mappings.xml
%{__install} conf/constraints.xml.example %{buildroot}/etc/grnoc/tsds/services/constraints.xml
%{__install} conf/logging.conf %{buildroot}/etc/grnoc/tsds/services/logging.conf
%{__install} conf/meta_logging.conf %{buildroot}/etc/grnoc/tsds/services/meta_logging.conf
%{__install} conf/receiver_logging.conf %{buildroot}/etc/grnoc/tsds/services/receiver_logging.conf
%{__install} conf/report_templates/basic.tt %{buildroot}/etc/grnoc/tsds/services/report_templates/basic.tt
%{__install} conf/apache-tsds-services.conf.example %{buildroot}/etc/httpd/conf.d/grnoc/tsds-services.conf
%{__install} conf/apache-tsds-services-temp.conf.example %{buildroot}/etc/httpd/conf.d/grnoc/tsds-services-temp.conf
%{__install} conf/tsds-services.cron %{buildroot}/etc/cron.d/tsds-services.cron
%{__install} conf/query_language.bnf %{buildroot}/usr/share/doc/grnoc/tsds/query_language.bnf
%{__install} conf/mongod-config1.conf %{buildroot}/etc/mongod-config1.conf
%{__install} conf/mongod-config2.conf %{buildroot}/etc/mongod-config2.conf
%{__install} conf/mongod-config3.conf %{buildroot}/etc/mongod-config3.conf
%{__install} conf/mongod-shard1.conf %{buildroot}/etc/mongod-shard1.conf
%{__install} conf/mongod-shard2.conf %{buildroot}/etc/mongod-shard2.conf
%{__install} conf/mongod-shard3.conf %{buildroot}/etc/mongod-shard3.conf
%{__install} conf/mongos.conf %{buildroot}/etc/mongos.conf
%{__install} conf/sphinx_templates/xmlpipe2_schema.xml %{buildroot}/etc/grnoc/tsds/services/sphinx_templates
%{__install} conf/sphinx_templates/xmlpipe2_document.xml %{buildroot}/etc/grnoc/tsds/services/sphinx_templates
%{__install} conf/sphinx.conf.example %{buildroot}/etc/sphinx/sphinx.conf.tsds

%{__install} conf/install/interface.json %{buildroot}/usr/share/doc/grnoc/tsds/install/interface.json
%{__install} conf/install/power.json %{buildroot}/usr/share/doc/grnoc/tsds/install/power.json
%{__install} conf/install/cpu.json %{buildroot}/usr/share/doc/grnoc/tsds/install/cpu.json
%{__install} conf/install/meta_tsds_db.json %{buildroot}/usr/share/doc/grnoc/tsds/install/meta_tsds_db.json
%{__install} conf/install/meta_tsds_shard.json %{buildroot}/usr/share/doc/grnoc/tsds/install/meta_tsds_shard.json
%{__install} conf/install/meta_tsds_rabbit.json %{buildroot}/usr/share/doc/grnoc/tsds/install/meta_tsds_rabbit.json
%{__install} conf/install/temperature.json %{buildroot}/usr/share/doc/grnoc/tsds/install/temperature.json

%{__install} bin/tsds_search_indexer.pl %{buildroot}/usr/bin/tsds_search_indexer.pl
%{__install} bin/tsds_expire.pl %{buildroot}/usr/bin/tsds_expire.pl
%{__install} bin/tsds_firehose.pl %{buildroot}/usr/bin/tsds_firehose.pl
%{__install} bin/tsds_install.pl %{buildroot}/usr/bin/tsds_install.pl
%{__install} bin/tsds_upgrade.pl %{buildroot}/usr/bin/tsds_upgrade.pl
%{__install} bin/tsds_writer %{buildroot}/usr/bin/tsds_writer
%{__install} bin/tsds_meta.pl %{buildroot}/usr/bin/tsds_meta.pl
%{__install} bin/tsds_fix_measurements.pl %{buildroot}/usr/bin/tsds_fix_measurements.pl
%{__install} bin/tsds-decom.pl %{buildroot}/usr/bin/tsds-decom.pl


%{__install} systemd/mongod-config1.service %{buildroot}/usr/lib/systemd/system/mongod-config1.service
%{__install} systemd/mongod-config2.service %{buildroot}/usr/lib/systemd/system/mongod-config2.service
%{__install} systemd/mongod-config3.service %{buildroot}/usr/lib/systemd/system/mongod-config3.service
%{__install} systemd/mongod-shard1.service %{buildroot}/usr/lib/systemd/system/mongod-shard1.service
%{__install} systemd/mongod-shard2.service %{buildroot}/usr/lib/systemd/system/mongod-shard2.service
%{__install} systemd/mongod-shard3.service %{buildroot}/usr/lib/systemd/system/mongod-shard3.service
%{__install} systemd/mongos.service %{buildroot}/usr/lib/systemd/system/mongos.service
%{__install} init.d/tsds_writer %{buildroot}/etc/init.d/tsds_writer
%{__install} init.d/tsds_meta %{buildroot}/etc/init.d/tsds_meta

%{__install} www/atlas.cgi %{buildroot}/usr/lib/grnoc/tsds/services/cgi-bin/atlas.cgi
%{__install} www/forge.cgi %{buildroot}/usr/lib/grnoc/tsds/services/cgi-bin/forge.cgi
%{__install} www/image.cgi %{buildroot}/usr/lib/grnoc/tsds/services/cgi-bin/image.cgi
%{__install} www/metadata.cgi %{buildroot}/usr/lib/grnoc/tsds/services/cgi-bin/metadata.cgi
%{__install} www/push.cgi %{buildroot}/usr/lib/grnoc/tsds/services/cgi-bin/push.cgi
%{__install} www/query.cgi %{buildroot}/usr/lib/grnoc/tsds/services/cgi-bin/query.cgi
%{__install} www/report.cgi %{buildroot}/usr/lib/grnoc/tsds/services/cgi-bin/report.cgi
%{__install} www/aggregation.cgi %{buildroot}/usr/lib/grnoc/tsds/services/cgi-bin/aggregation.cgi
%{__install} www/admin.cgi %{buildroot}/usr/lib/grnoc/tsds/services/cgi-bin/admin.cgi
%{__install} www/search.cgi %{buildroot}/usr/lib/grnoc/tsds/services/cgi-bin/search.cgi

# clean up buildroot
find %{buildroot} -name .packlist -exec %{__rm} {} \;

%{_fixperms} $RPM_BUILD_ROOT/*

%clean
rm -rf $RPM_BUILD_ROOT

%files

%defattr(644, root, root, 755)

%config(noreplace) /etc/grnoc/tsds/services/config.xml
%config(noreplace) /etc/grnoc/tsds/services/meta_config.xml
%config(noreplace) /etc/grnoc/tsds/services/mappings.xml
%config(noreplace) /etc/grnoc/tsds/services/constraints.xml
%config(noreplace) /etc/grnoc/tsds/services/logging.conf
%config(noreplace) /etc/grnoc/tsds/services/meta_logging.conf
%config(noreplace) /etc/grnoc/tsds/services/receiver_logging.conf
%config(noreplace) /etc/grnoc/tsds/services/report_templates/basic.tt
%config(noreplace) /etc/httpd/conf.d/grnoc/tsds-services.conf
%config(noreplace) /etc/httpd/conf.d/grnoc/tsds-services-temp.conf
%config(noreplace) /etc/cron.d/tsds-services.cron
%config(noreplace) /etc/mongod-config1.conf
%config(noreplace) /etc/mongod-config2.conf
%config(noreplace) /etc/mongod-config3.conf
%config(noreplace) /etc/mongod-shard1.conf
%config(noreplace) /etc/mongod-shard2.conf
%config(noreplace) /etc/mongod-shard3.conf
%config(noreplace) /etc/mongos.conf

/etc/sphinx/sphinx.conf.tsds

/etc/grnoc/tsds/services/sphinx_templates/xmlpipe2_document.xml
/etc/grnoc/tsds/services/sphinx_templates/xmlpipe2_schema.xml

/usr/share/doc/grnoc/tsds/CHANGES.md
/usr/share/doc/grnoc/tsds/INSTALL.md
/usr/share/doc/grnoc/tsds/query_language.bnf

/usr/share/doc/grnoc/tsds/install/interface.json
/usr/share/doc/grnoc/tsds/install/power.json
/usr/share/doc/grnoc/tsds/install/cpu.json
/usr/share/doc/grnoc/tsds/install/meta_tsds_db.json
/usr/share/doc/grnoc/tsds/install/meta_tsds_shard.json
/usr/share/doc/grnoc/tsds/install/meta_tsds_rabbit.json
/usr/share/doc/grnoc/tsds/install/temperature.json

%{perl_vendorlib}/GRNOC/TSDS.pm
%{perl_vendorlib}/GRNOC/TSDS/AggregateDocument.pm
%{perl_vendorlib}/GRNOC/TSDS/AggregatePoint.pm
%{perl_vendorlib}/GRNOC/TSDS/SearchIndexer.pm
%{perl_vendorlib}/GRNOC/TSDS/Constants.pm
%{perl_vendorlib}/GRNOC/TSDS/Constraints.pm
%{perl_vendorlib}/GRNOC/TSDS/DataService.pm
%{perl_vendorlib}/GRNOC/TSDS/DataService/Aggregation.pm
%{perl_vendorlib}/GRNOC/TSDS/DataService/Atlas.pm
%{perl_vendorlib}/GRNOC/TSDS/DataService/Image.pm
%{perl_vendorlib}/GRNOC/TSDS/DataService/MetaData.pm
%{perl_vendorlib}/GRNOC/TSDS/DataService/Push.pm
%{perl_vendorlib}/GRNOC/TSDS/DataService/Query.pm
%{perl_vendorlib}/GRNOC/TSDS/DataService/Report.pm
%{perl_vendorlib}/GRNOC/TSDS/DataService/Search.pm
%{perl_vendorlib}/GRNOC/TSDS/DataType.pm
%{perl_vendorlib}/GRNOC/TSDS/DataPoint.pm
%{perl_vendorlib}/GRNOC/TSDS/DataDocument.pm
%{perl_vendorlib}/GRNOC/TSDS/EventDocument.pm
%{perl_vendorlib}/GRNOC/TSDS/Event.pm
%{perl_vendorlib}/GRNOC/TSDS/Expire.pm
%{perl_vendorlib}/GRNOC/TSDS/GWS.pm
%{perl_vendorlib}/GRNOC/TSDS/GWS/Admin.pm
%{perl_vendorlib}/GRNOC/TSDS/GWS/Aggregation.pm
%{perl_vendorlib}/GRNOC/TSDS/GWS/Atlas.pm
%{perl_vendorlib}/GRNOC/TSDS/GWS/Forge.pm
%{perl_vendorlib}/GRNOC/TSDS/GWS/Image.pm
%{perl_vendorlib}/GRNOC/TSDS/GWS/MetaData.pm
%{perl_vendorlib}/GRNOC/TSDS/GWS/Push.pm
%{perl_vendorlib}/GRNOC/TSDS/GWS/Query.pm
%{perl_vendorlib}/GRNOC/TSDS/GWS/Report.pm
%{perl_vendorlib}/GRNOC/TSDS/GWS/Search.pm
%{perl_vendorlib}/GRNOC/TSDS/Install.pm
%{perl_vendorlib}/GRNOC/TSDS/MeasurementDecommer.pm
%{perl_vendorlib}/GRNOC/TSDS/MongoDB.pm
%{perl_vendorlib}/GRNOC/TSDS/Parser.pm
%{perl_vendorlib}/GRNOC/TSDS/Parser/Actions.pm
%{perl_vendorlib}/GRNOC/TSDS/RedisLock.pm
%{perl_vendorlib}/GRNOC/TSDS/Upgrade.pm
%{perl_vendorlib}/GRNOC/TSDS/Upgrade/*.pm
%{perl_vendorlib}/GRNOC/TSDS/Util/ConfigChooser.pm
%{perl_vendorlib}/GRNOC/TSDS/Writer.pm
%{perl_vendorlib}/GRNOC/TSDS/Writer/AggregateMessage.pm
%{perl_vendorlib}/GRNOC/TSDS/Writer/DataMessage.pm
%{perl_vendorlib}/GRNOC/TSDS/Writer/EventMessage.pm
%{perl_vendorlib}/GRNOC/TSDS/Writer/Worker.pm
%{perl_vendorlib}/GRNOC/TSDS/MetaStats.pm

%defattr(754, apache, apache, -)

/usr/lib/grnoc/tsds/services/cgi-bin/atlas.cgi
/usr/lib/grnoc/tsds/services/cgi-bin/forge.cgi
/usr/lib/grnoc/tsds/services/cgi-bin/image.cgi
/usr/lib/grnoc/tsds/services/cgi-bin/metadata.cgi
/usr/lib/grnoc/tsds/services/cgi-bin/push.cgi
/usr/lib/grnoc/tsds/services/cgi-bin/query.cgi
/usr/lib/grnoc/tsds/services/cgi-bin/report.cgi
/usr/lib/grnoc/tsds/services/cgi-bin/aggregation.cgi
/usr/lib/grnoc/tsds/services/cgi-bin/admin.cgi
/usr/lib/grnoc/tsds/services/cgi-bin/search.cgi

%defattr(754, root, root, -)

/usr/bin/tsds_search_indexer.pl
/usr/bin/tsds_expire.pl
/usr/bin/tsds_firehose.pl
/usr/bin/tsds_install.pl
/usr/bin/tsds_meta.pl
/usr/bin/tsds_fix_measurements.pl
/usr/bin/tsds-decom.pl
/usr/bin/tsds_upgrade.pl
/usr/bin/tsds_writer

%config(noreplace) /etc/init.d/tsds_writer
%config(noreplace) /etc/init.d/tsds_meta

%defattr(444, root, root, -)

/usr/lib/systemd/system/mongod-config1.service
/usr/lib/systemd/system/mongod-config2.service
/usr/lib/systemd/system/mongod-config3.service
/usr/lib/systemd/system/mongod-shard1.service
/usr/lib/systemd/system/mongod-shard2.service
/usr/lib/systemd/system/mongod-shard3.service
/usr/lib/systemd/system/mongos.service

%defattr(-, root, root, 755)

%dir /var/lib/grnoc/tsds/
%dir /var/run/grnoc/tsds/services/

%defattr(-, mongod, mongod, 755)

%dir /var/lib/mongo/config1
%dir /var/lib/mongo/config2
%dir /var/lib/mongo/config3
%dir /var/lib/mongo/shard1
%dir /var/lib/mongo/shard2
%dir /var/lib/mongo/shard3

%defattr(-, apache, apache, 755)

%dir /usr/share/grnoc/tsds-services/temp
