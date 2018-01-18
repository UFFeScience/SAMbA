#!/usr/bin/perl -w
#execute: perl numberFasta.pl i.fasta

# Name:                 numberFasta.pl
# Description file:     To number fasta before an alignment
# Designed for:         Kary Ocana
# Lab:                  COPPE
# Date:                 10 / 06 / 2011

use strict;

my $seqnum = 0;
my @seq;
my @seqheader;

readFasta($ARGV[0]);

# Add your code here

sub readFasta
{
    my $line;
    my $first;
    my $temp;
    open(FILE, "<$_[0]") || die "Cannot open FASTA file. Stop.\n";
    $seqnum = 0;
    $first = 0;
    while (defined($line = <FILE>))
    {
        chomp($line);
        $temp = substr($line, length($line) - 1, 1);
        if ($temp eq "\r" || $temp eq "\n")
        {
            chop($line);
        }
        if ($line =~ /^>/)
        {
           $seqnum = $seqnum + 1;
            $line =~ s/>.*/>$seqnum/; #enumera
            print $line;
            print "\n";
            if ($first == 0)
            {
                $first = 1;
            }
            next;
        }
        if ($first == 0)
        {
            die "Not a standard FASTA file. Stop.\n";
        }
#print               $seq[$seqnum - 1] = $seq[$seqnum - 1].$line;

        print $line."\n";
    }
    close(FILE);
}
