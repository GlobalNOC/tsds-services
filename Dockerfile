FROM oraclelinux:8 AS rpmbuild

# set working directory
WORKDIR /app

# add mongodb repo
COPY conf/mongodb-org-3.6.repo /etc/yum.repos.d/mongodb-org-3.6.repo

# add globalnoc and epel repos
RUN dnf install -y \
    https://build.grnoc.iu.edu/repo/rhel/8/x86_64/globalnoc-release-8-1.el8.noarch.rpm \
    oracle-epel-release-el8

# enable additional ol8 repos
RUN yum-config-manager --enable \
    ol8_appstream ol8_baseos_latest ol8_codeready_builder \
    ol8_developer_EPEL  ol8_developer_EPEL_modular

# run makecache
RUN dnf makecache

# install external app dependencies
RUN dnf install -y \
    sphinx rpm-build httpd mod_perl mod_perl-devel gcc wget mongodb-org-shell

# install globalnoc app dependencies
RUN dnf install -y \
    perl-GRNOC-CLI perl-GRNOC-Config perl-GRNOC-Counter \
    perl-GRNOC-LockFile perl-GRNOC-Log perl-GRNOC-WebService \
    perl-GRNOC-WebService-Client perl-GRNOC-TSDS-Aggregate-Histogram

# install venv dependencies 
RUN dnf install -y \
    openssl-devel perl-App-cpanminus expat-devel
RUN cpanm Carton

# copy everything in
COPY . /app

# build & install rpm
RUN make rpm


FROM oraclelinux:8

COPY --from=rpmbuild /root/rpmbuild/RPMS/x86_64/grnoc-tsds-services*x86_64.rpm /root/

RUN dnf install -y \
    https://build.grnoc.iu.edu/repo/rhel/8/x86_64/globalnoc-release-8-1.el8.noarch.rpm \
    oracle-epel-release-el8

# enable additional ol8 repos
RUN yum-config-manager --enable \
    ol8_appstream ol8_baseos_latest ol8_codeready_builder \
    ol8_developer_EPEL  ol8_developer_EPEL_modular

# run makecache
RUN dnf makecache

RUN dnf install httpd mod_perl mod_perl-devel
RUN dnf install -y /root/grnoc-tsds-services*x86_64.rpm

# setup apache config
RUN rm /etc/httpd/conf.d/welcome.conf
RUN rm /etc/grnoc/tsds/services/config.xml

RUN echo 'IncludeOptional "/etc/httpd/conf.d/grnoc/*.conf"' >> /etc/httpd/conf.d/grnoc_include.conf
RUN sed -i 's/ErrorLog "logs\/error_log"/ErrorLog \/dev\/stderr/g' /etc/httpd/conf/httpd.conf
RUN sed -i 's/CustomLog "logs\/access_log"/CustomLog \/dev\/stdout/g' /etc/httpd/conf/httpd.conf

# set entrypoint
ENTRYPOINT ["/bin/echo", "'Welcome to TSDS!'"]
