#!/usr/bin/perl -w
# kw10@sanger.ac.uk

# Use this perl wrapper to parse parameters from a config file
# and format and submit jobs to lsf. The callers supported are:
# BreakDancerMax, Pindel, SECluster, cnD, and RDXplorer. New callers
# can easily be incorporated in this script and in RunSVcallers.pm

use strict;
use Getopt::Std;
use File::Basename;
use lib dirname(__FILE__).'/../lib/';
use ParseConfig;

use vars "%opts";
getopts('c:r:j:v', \%opts);


if (!%opts || !$opts{c} || !$opts{r}) {
	die<<END;

This is a wrapper to format and submit jobs to LSF for 
various SV callers indicated in your configfile.

Usage: $0 -c configFile -r runNumber -j jobDispatcher [-v]

where configFile is your SVMerge config file, runNumber 
is a number appended to your JobID, jobDispatcher is the 
grid software you are using to run your jobs (eg. LSF, SGE)
and -v is used to print submitted commands to STDERR.

END
}
my $configfile = $opts{c};
my $vers = $opts{r};
my $verbose = 1 if $opts{v};
my $jobdispatcher = $opts{j} || 'LSF';

eval("require RunSVcallers::$jobdispatcher");
my $svrun = "RunSVcallers::$jobdispatcher"->new();
my $jobindex = $svrun->jobindex;

#-- Parse SV caller output --

my %params = ParseConfig::getParams($configfile);
ParseConfig::checkParams('parse',\%params);

my $maindir="$params{projdir}/$params{version}";

print STDERR "Submitting jobs for SV callers: $params{callerlist}\n";

# Breakdancer:

if ($params{breakdancer} ) {
	# Go to BD directory
	my $workdir = &checkDir('breakdancer');
	chdir($workdir) || die "Can't chdir to $workdir";

	my @commands = ();
	my %bd=();

	# Parameters for BDMax config file generation
	if ($params{BDconf}) {
		# If chromosomes are in separate bams
		if ($params{chrRange} && $params{bamdir}) {
			%bd = $svrun->setParams(\%params,'range','bdconf',$vers);
                        my $command = $svrun->makeCommand("$params{bam2conf} $params{BDconfParams} $params{bamdir}/$jobindex.bam > $params{name}.$jobindex.config", %bd);
                        $svrun->submit($command);
		}
		if ($params{chrOther} && $params{bamdir}) {
			my @list = split /\s+/, $params{chrOther};
			foreach my $other (@list) {
				%bd=$svrun->setParams(\%params,$other,'bdconf',$vers);
				$bd{other}=$other;
				#my $lsfcommand = $svrun->makeCommand(%bd);
				#push @commands, qq($lsfcommand "$params{bam2conf} $params{BDconfParams} $params{bamdir}/$jobindex.bam > $params{name}.$other.config");
                                my $command = $svrun->makeCommand("$params{bam2conf} $params{BDconfParams} $params{bamdir}/$jobindex.bam > $params{name}.$other.config", %bd);
                                push @commands, $command;
				$bd{other}='';
			}
		}
		# If all data is in a single bam
		if (($params{chrRange} || $params{chrOther}) && $params{bam}) {
			%bd=$svrun->setParams(\%params,'all','bdconf',$vers);
			# Command for config file creation
			#my $lsfcommand = $svrun->makeCommand(%bd);
			#push @commands, qq($lsfcommand "$params{bam2conf} $params{BDconfParams} $params{bam} > $params{name}.config");
                        my $command = $svrun->makeCommand("$params{bam2conf} $params{BDconfParams} $params{bam} > $params{name}.config", %bd);
                        push @commands, $command;
		}
	}
	# Parameters for running BDMax

	if ($params{chrRange}) {
		%bd=$svrun->setParams(\%params,'range','bd',$vers);
		$bd{prevjob}="$params{name}.$vers.bdconf" if $params{BDconf}; # set job dependency
		# Command for running BDMax

		#my $lsfcommand = $svrun->makeCommand(%bd);
		my $config = $params{bam} ? "$params{name}.config" : "$params{name}.$jobindex.config";
		#push @commands, qq($lsfcommand "$params{bdexe}  -o $jobindex $params{BDparams}  $config > $params{name}.$jobindex.max");
                my $command = $svrun->makeCommand("$params{bdexe}  -o $jobindex $params{BDparams}  $config > $params{name}.$jobindex.max", %bd);
                push @commands, $command;
	}
	if ($params{chrOther}) {
		my @list = split /\s+/, $params{chrOther};
		foreach my $other (@list) {
			%bd=$svrun->setParams(\%params,$other,'bd',$vers);
			$bd{other}=$other;
			$bd{prevjob}="$params{name}.$vers.bdconf" if $params{BDconf}; # set job dependency
			$bd{prevjob}.=".$other" if $params{BDconf} && $params{bamdir};
			#my $lsfcommand = $svrun->makeCommand(%bd);
			my $config = $params{bam} ? "$params{name}.config" : "$params{name}.$other.config";
			#push @commands, qq($lsfcommand "$params{bdexe}  -o $other $params{BDparams} $config > $params{name}.$other.max");
                        my $command = $svrun->makeCommand("$params{bdexe}  -o $other $params{BDparams} $config > $params{name}.$other.max",%bd);
			$bd{other}='';
		}
	}
	print STDERR "Submitting BreakDancerMax jobs\n";
	&printout(@commands) if $verbose;
	my $res = $svrun->submit(@commands);
	chdir($maindir);
	die if !$res;
}

# Pindel

if ($params{pindel}) {
	my $workdir = &checkDir('pindel');
	chdir($workdir) || die "Can't chdir to $workdir";

	my @commands = ();
	my %pd=();

	# Set parameters for Pindel jobs
	if ($params{chrRange}) {
		%pd=$svrun->setParams(\%params,'range','pd',$vers);
		$pd{prevjob}="$params{name}.$vers.pdFilter" if $params{PDgetReads};
		my $outfiles = 'out';
		my $reference = $params{chrrefdir} ? "$params{chrrefdir}/$jobindex.fa" : $params{reffile};
                my $cmd = $params{PDoptParams} ?
                    $svrun->makeCommand("$params{pinexe} -f $reference -c $jobindex -o $params{name}.$jobindex  -i $params{PDconf} $params{PDoptParams}", %pd) :
                    $svrun->makeCommand("$params{pinexe} -f $reference -c $jobindex -o $params{name}.$jobindex  -i $params{PDconf}" , %pd);
                push @commands, $cmd;
	}
	if ($params{chrOther}) {
		my @list = split /\s+/, $params{chrOther};
		foreach my $other (@list) {
			%pd=$svrun->setParams(\%params,$other,'pd',$vers);
			$pd{other}=$other;
			$pd{prevjob}= "$params{name}.$vers.pdFilter.$other" if $params{PDgetReads}; # set job dependency
			my $outfiles = "out";
			my $reference = $params{chrrefdir} ? "$params{chrrefdir}/$other.fa" : $params{reffile};
                        my $cmd = $params{PDoptParams} ? 
                            $svrun->makeCommand("$params{pinexe} -f $reference -c $other -o $params{name} -i $params{PDconf}.$other $params{PDoptParams}", %pd):
                            $svrun->makeCommand("$params{pinexe} -f $reference -c $other -o $params{name} -i $params{PDconf}.$other", %pd);
                        push @commands, $cmd;
			$pd{other}='';
		}
	}
	# Submit
	print STDERR "Submitting Pindel jobs\n";
	&printout(@commands) if $verbose;
	my $res = $svrun->submit(@commands);
	chdir($maindir);
	die if !$res;
}

# SECluster

if ($params{sec}) {
	my $workdir = &checkDir('sec');
	chdir($workdir) || die "Can't chdir to $workdir";
	my @commands = ();
	my %sec=();
	# Get one-end mapped reads
	if ($params{SECfilter}) {
		if ($params{chrRange}) {
			%sec=$svrun->setParams(\%params,'range','SECfilter',$vers);
			#my $lsfcommand = $svrun->makeCommand(%sec);
			my $refbam = $params{bam} ? $params{bam} : "$params{bamdir}/$jobindex.bam";
			#push @commands, qq($lsfcommand "$params{exedir}/samfilter.sh $refbam $jobindex 1");
                        my $cmd = $svrun->makeCommand("$params{exedir}/samfilter.sh $refbam $jobindex 1", %sec);
                        push @commands, $cmd;
		}
		if ($params{chrOther}) {
			my @list = split /\s+/, $params{chrOther};
			foreach my $other (@list) {
				%sec=$svrun->setParams(\%params,$other,'SECfilter',$vers);
				$sec{other}=$other;
				#my $lsfcommand = $svrun->makeCommand(%sec);
				my $refbam = $params{bam} ? $params{bam} : "$params{bamdir}/$other.bam";
				#push @commands, qq($lsfcommand "$params{exedir}/samfilter.sh $refbam $other 1");
                                my $command = $svrun->makeCommand("$params{exedir}/samfilter.sh $refbam $other 1", %sec);
                                push @commands, $command; 
				$sec{other}='';
			}
		}
	}
	# Set parameters for SECluster jobs
	if ($params{chrRange}) {
		%sec=$svrun->setParams(\%params,'range','sec',$vers);
		$sec{prevjob}="$params{name}.$vers.SECfilter" if $params{SECfilter};
		#my $lsfcommand = $svrun->makeCommand(%sec);
		#push @commands, qq($lsfcommand "$params{secexe} -f $jobindex.se.sam -q $params{SECqual} -m $params{SECmin} -c $params{SECmin} -r $jobindex -x $params{SECmax}  > $params{name}.$jobindex.clusters");
                my $command = $svrun->makeCommand("$params{secexe} -f $jobindex.se.sam -q $params{SECqual} -m $params{SECmin} -c $params{SECmin} -r $jobindex -x $params{SECmax}  > $params{name}.$jobindex.clusters", %sec);
                push @commands, $command;

	}
	if ($params{chrOther}) {
		my @list = split /\s+/, $params{chrOther};
		foreach my $other (@list) {
			%sec=$svrun->setParams(\%params,$other,'sec',$vers);
			$sec{other}=$other;
			$sec{prevjob}="$params{name}.$vers.SECfilter.$other" if $params{SECfilter};
			#my $lsfcommand = $svrun->makeCommand(%sec);
			#push @commands, qq($lsfcommand "$params{secexe} -f $other.se.sam -q $params{SECqual} -m $params{SECmin} -c $params{SECmin} -r $other -x $params{SECmax}  > $params{name}.$other.clusters");
                        my $command = $svrun->makeCommand("$params{secexe} -f $other.se.sam -q $params{SECqual} -m $params{SECmin} -c $params{SECmin} -r $other -x $params{SECmax}  > $params{name}.$other.clusters", %sec);
                        push @commands, $command;
			$sec{other}='';
		}
	}
  
	# Submit
	print STDERR "Submitting SECluster jobs\n";
	&printout(@commands) if $verbose;
	my $res = $svrun->submit(@commands);
	chdir($maindir);
	die if !$res;
}

# cnD 
if ($params{cnd}) {
	my $workdir = &checkDir('cnd');
	chdir($workdir) || die "Can't chdir to $workdir";
	my @commands = ();
	my %cnd=();
	# pileup command to generate .win files
	if ($params{CNDpileup}) {
		if ($params{chrRange}) {
			%cnd=$svrun->setParams(\%params,'range','cndpile',$vers);
			my $pileup = "$params{cnddir}/pileup2win.pl > $jobindex.win";
			# A single bam only
			if ($params{bam}) {
                                my $command = $svrun->makeCommand("$params{samtools} view -u $params{bam} $jobindex | $params{samtools} pileup -c -r $params{CNDsnprate} -f $params{reffile} - | cut -f 1-8 | $pileup", %cnd);
                                push @commands, $command;
			}
			# Directory of chromosome bams
			elsif ($params{bamdir}) {
                                my $command = $svrun->makeCommand("$params{samtools} pileup -c -r $params{CNDsnprate} -f $params{reffile} $params{bamdir}/$jobindex.bam | cut -f 1-8 | $pileup", %cnd);
                                push @commands, $command;
			}
		}
		if ($params{chrOther}) {
			my @list = split /\s+/, $params{chrOther};
			foreach my $other (@list) {
				%cnd=$svrun->setParams(\%params,$other,'cndpile',$vers);
				$cnd{other}=$other;
				my $pileup = "$params{cnddir}/pileup2win.pl > $other.win";
				if ($params{bam}) {
                                        my $command = $svrun->makeCommand("$params{samtools} view -u $params{bam} $other | $params{samtools} pileup -c -r $params{CNDsnprate} -f $params{reffile} - | cut -f 1-8 | $pileup", %cnd);
                                        push @commands, $command;
				}
				elsif ($params{bamdir}) {
                                        my $command = $svrun->makeCommand("$params{samtools} pileup -c -r $params{CNDsnprate} -f $params{reffile} $params{bamdir}/$other.bam | cut -f 1-8 | $pileup", %cnd);
                                        push @commands, $command;
				}
				$cnd{other}='';
			}
		}
	}
	
	# GC correction
	if ($params{CNDgccorrect}) {
		if ($params{chrRange}){
			%cnd=$svrun->setParams(\%params,'range','gccorrect',$vers);
			$cnd{prevjob}="$params{name}.$vers.cndpile" if $params{CNDpileup};
                        my $command = $svrun->makeCommand("$params{exedir}/gcmedian.pl $jobindex.win > $jobindex.gchash", %cnd);
                        push @commands, $command;
			%cnd=$svrun->setParams(\%params,'range','gccorrect',$vers);
			$cnd{prevjob}="$params{name}.$vers.gccorrect";
                        my $command2 = $svrun->makeCommand("$params{exedir}/gcCorrect.pl --gchash $jobindex.gchash --infile $jobindex.win > $jobindex.corr.win", %cnd);
                        push @commands, $command2;
		}
		if ($params{chrOther}) {
			my @list = split /\s+/, $params{chrOther};
			foreach my $other (@list) {
				%cnd=$svrun->setParams(\%params,$other,'gccorrect',$vers);
				$cnd{other}=$other;
				$cnd{prevjob}="$params{name}.$vers.cndpile" if $params{CNDpileup};
                                my $command = $svrun->makeCommand("$params{exedir}/gcmedian.pl $other.win > $other.gchash", %cnd);
                                push @commands, $command;
				%cnd=$svrun->setParams(\%params,$other,'gccorrect',$vers);
				$cnd{other}=$other;
				$cnd{prevjob}="$params{name}.$vers.gccorrect.$other";
                                my $command2 = $svrun->makeCommand("$params{exedir}/gcCorrect.pl --gchash $other.gchash --infile $other.win > $other.corr.win", %cnd);
                                push @commands, $command2;
				$cnd{other}='';
			}
		}
	}
	# Set parameters for cnD
	if ($params{chrRange}){
		%cnd=$svrun->setParams(\%params,'range','cnd',$vers);
		if ($params{CNDgccorrect}) {
			$cnd{prevjob}="$params{name}.$vers.gccorrect";
		}
		elsif ($params{CNDpileup}) {
			$cnd{prevjob}="$params{name}.$vers.cndpile";
		}
		my $winfile = $params{CNDgccorrect} ? 'corr.win' : 'win';
                my $command = $svrun->makeCommand("$params{cnddir}/cnD $params{CNDparams} --prefix=$jobindex $jobindex.$winfile", %cnd);
                push @commands, $command;

	}
	if ($params{chrOther}) {
		my @list = split /\s+/, $params{chrOther};
		foreach my $other (@list) {
			%cnd=$svrun->setParams(\%params,$other,'cnd',$vers);
			$cnd{other}=$other;
			if ($params{CNDgccorrect}) {
				$cnd{prevjob}="$params{name}.$vers.gccorrect.$other";
			}
			elsif ($params{CNDpileup}) {
				$cnd{prevjob}="$params{name}.$vers.cndpile.$other";
			}
			my $winfile = $params{CNDgccorrect} ? "$other.corr.win" : "$other.win";
                        my $command = $svrun->makeCommand("$params{cnddir}/cnD $params{CNDparams} --prefix=$other $winfile", %cnd);
                        push @commands, $command;
			$cnd{other}='';
		}
	}
	# Set up parameters for final steps
	if ($params{chrRange}){
		%cnd=$svrun->setParams(\%params,'range','cndext',$vers);
		$cnd{prevjob}="$params{name}.$vers.cnd";
                my $command = $params{CNDnohet} ? 
                              $svrun->makeCommand("$params{cnddir}/extractCNFromVit.pl --no-losses $jobindex\\_posterior.txt > $jobindex.calls", %cnd):
                              $svrun->makeCommand("$params{cnddir}/extractCNFromVit.pl $jobindex\\_posterior.txt > $jobindex.calls", %cnd);
                push @commands, $command;
	}
	if ($params{chrOther}) {
		my @list = split /\s+/, $params{chrOther};
		foreach my $other (@list) {
			%cnd=$svrun->setParams(\%params,$other,'cndext',$vers);
			$cnd{other}=$other;
			$cnd{prevjob}="$params{name}.$vers.cnd.$other";
                        my $command = $params{CNDnohet} ?
                                       $svrun->makeCommand("$params{cnddir}/extractCNFromVit.pl --no-losses $other\_posterior.txt > $other.calls", %cnd):
                                       $svrun->makeCommand("$params{cnddir}/extractCNFromVit.pl $other\_posterior.txt > $other.calls", %cnd);
                        push @commands, $command;
			$cnd{other}='';
		}
	}
	# Submit
	print STDERR "Submitting cnD jobs\n";
	&printout(@commands) if $verbose;
	my $res = $svrun->submit(@commands);
	chdir($maindir);
	die if !$res;
}

if ($params{rdx}) {
	my $workdir = &checkDir('rdx');
	print `pwd`;
	chdir($workdir) || die "Can't chdir to $workdir";
	my @commands = ();
	my %rdx=();
	`mkdir bams`; 
    chdir('bams') || die "Can't chdir to bams";
	# Split bams by chr first
	if ($params{bam} && $params{RDXsplitBam}) {
		if ($params{chrRange}){
 			%rdx=$svrun->setParams(\%params,'range','rdxsplit',$vers);
                        my $command = $svrun->makeCommand("$params{samtools} view -b $params{bam} $jobindex > $params{name}.chrom$jobindex.bam", %rdx);
                        push @commands, $command;
		}
		if ($params{chrOther}) {
			my @list = split /\s+/, $params{chrOther};
			foreach my $other (@list) {
				%rdx=$svrun->setParams(\%params,$other,'rdxsplit',$vers);
				$rdx{other}=$other;
                                my $command = $svrun->makeCommand("$params{samtools} view -b $params{bam} $other > $params{name}.chrom$other.bam", %rdx);
                                push @commands, $command;
				$rdx{other}='';
			}	
		}
	}
	# Create sym links to original bam chromosoms files if bamdir given
	if ($params{bamdir}) {
		`bash -c 'for f in \`dir -1 $params{bamdir}/*.bam | cut -f 1 -d "."\`; do g=\`basename \$f\`; ln -sv \$f.bam $params{name}.chrom\$g.bam; done'`;
	}
	# Get parameters for rdx
	%rdx=$svrun->setParams(\%params,$params{name},'rdx',$vers);
	$rdx{prevjob}="$params{name}.$vers.rdxsplit" if $params{RDXsplitBam};
        my $command = $svrun->makeCommand("$params{rdxdir}/run.sh", %rdx);
        push @commands, $command;
	
	# Submit
	print STDERR "Submitting RDXplorer jobs\n";
	&printout(@commands) if $verbose;
	my $res = $svrun->submit(@commands);
	chdir($maindir);
	die if !$res;

	
}

sub checkDir {
	my $newdir = shift @_;
	my $workdir = "$params{projdir}/$params{version}/$newdir/";
	if (! -d $workdir) {
		print STDERR "Making directories $workdir/logs\n";
		`mkdir -p $workdir/logs`;
	}
	return $workdir;
}

sub printout {
	my @print = @_;
	print STDERR join "\n", @print, "\n";	
}
