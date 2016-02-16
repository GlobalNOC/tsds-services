#!/usr/bin/perl

use strict;
use warnings;

package GRNOC::TSDS::Parser::Actions;

use DateTime;
use Data::Dumper;

sub collapse {
    my $node = shift;
    return join("", @_);
}

sub spaces {
    my $node = shift;
    return join(" ", @_);
}

sub flatten {
    my $node = shift;

    my @datum = @_;

    my @arr;

    foreach my $data (@datum){
	next if (defined $data && $data eq ',');
	if (ref $data eq "ARRAY"){
	    push(@arr, @{flatten(undef, @$data)}); 
	}
	else {
	    push(@arr, $data);
	}
    }

    return \@arr;
}

sub remove_symbols {
    my $node = shift;

    my @words = @_;

    my @good;
    foreach my $word (@words){
	next if ($word eq ')' || 
		 $word eq '(' ||
		 $word eq ',');
	push(@good, $word);
    }
    return \@good;
}

sub parse_quotes {
    my $node = shift;

    my @words = @_;
    
    my @good;
    foreach my $word (@words){
	next if ($word eq '"' || 
		 $word eq "'");
	push(@good, $word);
    }
    return \@good;
}

sub parse_phrase {
    my $node = shift;
    my @words = @_;

    # if it's an empty string handle that specially
    if (@words == 3 && ! defined $words[1]){
	return [""];
    }

    my @good;
    # skip the end quotes
    for (my $i = 1; $i < @words - 1; $i++){
	push(@good, $words[$i]);
    }
    return \@good;
}

sub parse_null {
    my $node  = shift;
    my @words = @_;

    my @good;
    foreach my $word (@words){
	$word = undef if $word eq 'null';
	push(@good, $word);
    }
    return \@good;
}


sub make_date {
    my $node = shift;
    
    my @dates = @_;

    # Run through and remove all the various symbols that we don't care about
    my @values;
    foreach my $value (@dates){
	next if ($value eq '/' || 
		 $value eq '"' ||
		 $value eq ':');
	push(@values, $value);
    }

    my $dt;


    # If we're doing relative time, we can do that
    if ($values[0] =~ /^now$/i){
        $dt = DateTime->now();

        # Are we doing something like now - 5m?
        if (@values > 1){
            my $op   = $values[1];
            my $num  = $values[2];
            my $type = $values[3];

            # Cast the incoming to whatever DateTime::Duration 
            # is expecting
            my %lookup = (
                's'  => 'seconds',                
                'm'  => 'minutes',
                'h'  => 'hours',
                'd'  => 'days',
                'w'  => 'weeks',
                'mo' => 'months',
                'y'  => 'years'
            );

            my $name = $lookup{$type};

            if ($op eq '+'){
                $dt->add($name => $num);
            }
            else {
                $dt->subtract($name => $num);
            }
        }
    }
    # If they just specified an epoch timestamp, this is easy
    # just use that.
    elsif (@values == 1){
        $dt = DateTime->from_epoch(epoch => $values[0]);
    }        
    # Otherwise we have a long human readable date
    # passed in
    else {

        # These are always required. 
        my $month = shift @values;
        my $day   = shift @values;
        my $year  = shift @values;

        $dt = new DateTime(year  => $year,
                           month => $month,			  

                           day   => $day
            );
        
        
        my $hour   = 0; 
        my $minute = 0;
        my $second = 0;
        my $timezone = "local";
        
        # There are 4 possibilities now: 
        #   1.) We have full specification with 4 items left in the array
        #   2.) We have just the time specification
        #   3.) We have just the timezone specification
        #   4.) We have nothing else
        
        if (@values >= 3){
            $hour   = shift @values; 
            $minute = shift @values;
            $second = shift @values;
        }
        
        if (@values == 1){
            $timezone = shift @values;
        }
        
        $dt->set_hour($hour);
        $dt->set_minute($minute);
        $dt->set_second($second);    
        $dt->set_time_zone($timezone);    

    }

    # should this be isodate or epoch?
    #return '"' . $dt->epoch() . '"';
    return 'Date(' . $dt->epoch() . ')';
}

1;

