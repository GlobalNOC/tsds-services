#--------------------------------------------------------------------
#----- GRNOC TSDS Image DataService Library
#-----
#----- Copyright(C) 2015 The Trustees of Indiana University
#--------------------------------------------------------------------
#----- $HeadURL: svn+ssh://svn.grnoc.iu.edu/grnoc/tsds/services/trunk/lib/GRNOC/TSDS/DataService/Image.pm $
#----- $Id: Image.pm 39799 2015-10-20 15:33:37Z mrmccrac $
#-----
#----- This module inherits the base GRNOC::TSDS::DataService class
#----- and provides all of the methods to interact with image
#--------------------------------------------------------------------

package GRNOC::TSDS::DataService::Image;

use strict;
use warnings;

use base 'GRNOC::TSDS::DataService';

use GRNOC::TSDS::DataService::Query;
use GRNOC::TSDS::DataService::MetaData;

use GRNOC::Log;

use Env::C;
use DateTime;
use Data::Dumper;
use File::Temp;
use MIME::Base64;
use WWW::Mechanize::PhantomJS;
use Template;
use JSON;
use Sys::Hostname;
use Time::HiRes qw (usleep);

# this will hold the only actual reference to this object
my $singleton;

sub new {

    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    # if we've created this object (singleton) before, just return it
    return $singleton if ( defined( $singleton ) );

    my $self = $class->SUPER::new( @_ );

    bless( $self, $class );

    # store our newly created object as the singleton
    $singleton = $self;

    $self->query_ds( GRNOC::TSDS::DataService::Query->new( @_ ) );
    $self->metadata_ds( GRNOC::TSDS::DataService::MetaData->new( @_ ) );

    return $self;
}

sub get_image {

    my ( $self, %args ) = @_;

    my $content = $args{'content'};

    my $mech = WWW::Mechanize::PhantomJS->new();

    $mech->get(0);	
    $mech->update_html("<html><body style='background-color: white;'>$content</body></html>");

    my $png = $mech->content_as_png();
	
    return encode_base64($png);
}

sub get_chart {

    my ( $self, %args ) = @_;

    my $query = $args{'query'};
    my $output_format = $args{'output_format'};
    my $type = _format_chart_type($args{'type'});
    my $query_info = _process_query($query);

    my $remote_user = $args{'remote_user'};

    if ( !defined($query_info) ) {
        $self->error( 'Error processing query.' );
        return;
    }

    # verify that the GRNOC glue is available
    if ( ( ! -e '/usr/share/grnoc/glue/GRNOC/util/Bootstrap/1/Bootstrap.html' ) ||
	 ( ! -e '/usr/share/grnoc/glue/GRNOC/widget/Chart/1/Group/Group.html' ) ) {

	$self->error( "glue installation not found." );
	return;
    }

    if (defined($query_info->{'tz'})) {
        Env::C::setenv('TZ', $query_info->{'tz'});
    }

    my $hostname = hostname();
    my $temp_path = '/usr/share/grnoc/tsds-services/temp/';

    my $output;

    my $vars = {
        'title' => 'Chart',
        'favicon' => '#',
        'application_id' => 'server_side_chart',
        'server_history' => 0,
        'debug' => 0
    };

    my $tt = Template->new(INCLUDE_PATH => '/usr/share/grnoc/glue/GRNOC/util/Bootstrap/1/', ABSOLUTE => 1);

    $tt->process("Bootstrap.html", $vars, \$output);

    my $html = "<html><head><base href=\"https://$hostname\"/>";

    $html .= "$output</head><body>";
    
    $html .= "<div id='mychart' style='height:20px;width:1000px'></div>";

    # get chart data
    my $json_result;
    my $result = $self->query_ds()->run_query(query => $query);

    if (defined($result)) {
        $json_result = encode_json($result);
    }
    else {
        $self->error( 'Error processing query.' );
        return;
    }

    # get metadata type values
    my $mtv;
    $result = $self->metadata_ds()->get_measurement_type_values(measurement_type => $query_info->{'measurement_type'});

    if (defined($result)) {
        $mtv = encode_json($result);
    }
    else {
        $self->error( 'Error processing query.' );
        return;
    }

    # Chart widget
    my $chart_widget_output;
    my $chart_widget = '[% INCLUDE "/usr/share/grnoc/glue/GRNOC/widget/Chart/1/Group/Group.html" %]';

    $tt->process(\$chart_widget, undef, \$chart_widget_output);

    # phantomjs 1.X is missing bind
    my $polyfill = "<script>if (!Function.prototype.bind) {
  Function.prototype.bind = function(oThis) {
    if (typeof this !== 'function') {
      // closest thing possible to the ECMAScript 5
      // internal IsCallable function
      throw new TypeError('Function.prototype.bind - what is trying to be bound is not callable');
    }

    var aArgs   = Array.prototype.slice.call(arguments, 1),
        fToBind = this,
        fNOP    = function() {},
        fBound  = function() {
          return fToBind.apply(this instanceof fNOP
                 ? this
                 : oThis,
                 aArgs.concat(Array.prototype.slice.call(arguments)));
        };

    if (this.prototype) {
      // Function.prototype doesn't have a prototype property
      fNOP.prototype = this.prototype; 
    }
    fBound.prototype = new fNOP();

    return fBound;
  };
}</script>";

    my $local_data = "<script>var chart_data = $json_result;
                              var query = '$query';
                              var mtv = $mtv;
                              var type = '$type';
                              var between = [new Date('$query_info->{'start'}'), new Date('$query_info->{'end'}')];
                      </script>";


    my $app = "<script>

                var flag = 0;

                GRNOC.loaded.subscribe(function(){

                      var chart_div = Y.one('#mychart');
                      var chart_group = new GRNOC.widget.Chart.Group('mychartgroup',
                                                                     {
                                                                        dynamic_markup: { parent: chart_div },
                                                                        between: between,
                                                                        type: type,
                                                                        chart_data: {data:chart_data, query:query, measurement_type_values:mtv}
                                                                     });

                      chart_group.renderEvent.subscribe(function(){                         
                          flag = 1;
                      },null);

                });
               </script>";

    $html .= "$chart_widget_output $polyfill $local_data $app</body></html>";

    my $filename = rand(time());
    $filename = 'chart_'.$filename.'.html';
    my $filepath = $temp_path.$filename;
    open(my $fh, '>', $filepath) or die "Could not open file '$filepath' $!";
    print $fh $html;
    close $fh;

    my $chart_url = "https://$hostname/tsds-services/temp/".$filename;

    my $mech = WWW::Mechanize::PhantomJS->new(phantomjs_arg => ['--ssl-protocol=any']);

    my $r_obj = $mech->get($chart_url);

    my ($value) = $mech->eval('flag');
    my $max = 500;
    while ($value != 1) {
       ($value) = $mech->eval('flag');
       $max--;
       if ($max == 0) {
           last;
       }
       usleep(10 * 1000);
    }

    my $png = $mech->content_as_png();

    unlink $filepath;

    # return PNG binary if specified, return base64 by default
    if (defined($output_format) and (lc($output_format) eq 'binary')) { 
        $self->{'output_formatter'} =  sub { shift };
        return $png;
    }
    else {
        return encode_base64($png);
    }

}

sub _process_query {
    my $query = shift;

    my $query_info = {};
    my $tz;

    $query =~ /between\s*\(\s*"\s*(\S+)\s*"\s*,\s*"\s*(\S+)\s*"\s*\)/;

    if ($1 and $2) {
        $query_info->{'start'} = $1;
        $query_info->{'end'} = $2;
        $tz = undef;
    }
    else {
        $query =~ /between\s*\(\s*"\s*(\S+)\s+(\S+)\s*"\s*,\s*"\s*(\S+)\s+(\S+)\s*"\s*\)/;

        if ($1 and $2 and $3 and $4) {
            $query_info->{'start'} = "$1 $2";
            $query_info->{'end'} = "$3 $4";

            my $t1 = $2;
            my $t2 = $4;

            if (_validate_time($t1) and _validate_time($t2)) {
                $tz = undef;
            }
            else {
                $tz = _get_unix_timezone($t1, $t2);
            }
        }
        else {
           $query =~ /between\s*\(\s*"\s*(\S+)\s+(\S+)\s+(\S+)\s*"\s*,\s*"\s*(\S+)\s+(\S+)\s+(\S+)\s*"\s*\)/;

           if ($1 and $2 and $3 and $4 and $5 and $6) {
               $query_info->{'start'} = "$1 $2 $3";
               $query_info->{'end'} = "$4 $5 $6";
           }
           else {
               return;
           }

           $tz = _get_unix_timezone($3, $6);
        }
    }

    $query_info->{'tz'} = $tz;

    my $measurement_type;
    if ($query =~ /\s+from\s+(\S+)/) {
        $query_info->{'measurement_type'} = $1;
    }

    return $query_info;
}

sub _format_chart_type {
    my $type = shift;

    if (!defined($type)) {
        return 'Default';
    }

    $type = lc($type);

    if ($type eq 'aggregate') {
        return 'Aggregate';
    }
    else {
        return 'Default';
    }
    # more types in the future
}

sub _get_unix_timezone {
    my $tz1 = uc($_[0]);
    my $tz2 = uc($_[1]);

    if ($tz1 ne $tz2) {
    }

    my $tz = $tz1;

    if ($tz eq 'EST' or $tz eq 'EDT') {
        $tz = 'US/Eastern';
    }
    elsif ($tz eq 'CST' or $tz eq 'CDT') {
        $tz = 'US/Central';
    }
    elsif ($tz eq 'MST' or $tz eq 'MDT') {
        $tz = 'US/Mountain';
    }
    elsif ($tz eq 'PST' or $tz eq 'PDT') {
        $tz = 'US/Pacific';
    }
    elsif ($tz eq 'UTC') {
        $tz = 'UTC';
    }
    else {
        $tz = undef;
    }
}

sub _validate_time {
    my $t = shift;

    if ($t =~ /^(?:(?:([01]?\d|2[0-3]):)?([0-5]?\d):)?([0-5]?\d)$/) {
        return 1;
    }
    else {
        return 0;
    }
}

1;

