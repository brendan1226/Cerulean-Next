#!/usr/bin/env perl
#
# Reference Cerulean subprocess plugin — reads a UTF-8 value from the
# input file, writes uppercase + "!" to the output file. The exit code
# is what Cerulean inspects; non-zero falls back to the input unchanged.
#
# IMPORTANT: `chmod +x shout.pl` before zipping, or the plugin will
# fail to load with "not executable".

use strict;
use warnings;
use Getopt::Long;

my ($in, $out, $config);
GetOptions(
    "in=s"     => \$in,
    "out=s"    => \$out,
    "config=s" => \$config,
) or die "invalid args\n";

open my $in_fh, "<:encoding(UTF-8)", $in or die "open $in: $!";
my $value = do { local $/; <$in_fh> };
close $in_fh;

$value //= "";
my $transformed = uc($value) . "!";

open my $out_fh, ">:encoding(UTF-8)", $out or die "open $out: $!";
print $out_fh $transformed;
close $out_fh;

exit 0;
