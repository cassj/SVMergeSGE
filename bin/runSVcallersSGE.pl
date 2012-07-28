#!/usr/bin/env perl
# cassjohnston@gmail.com

# This is a modifed version of the RunSVCallers.pl script that 
# generates SGE rather than LSF jobs.

use strict;
use Getopt::Std;
use File::Basename;
use lib dirname(__FILE__).'/../lib/';
use ParseConfig;
use RunSVcallers::SGE;
use Data::Dumper;

use vars "%opts";
getopts('c:r:v', \%opts);


if (!%opts || !$opts{c} || !$opts{r}) {
	die<<END;

This is a wrapper to format and submit jobs to SGE for 
various SV callers indicated in your configfile.

Usage: $0 -c configfile -r runNumber [-v]

where configfile is your SVMerge config file, runNumber 
is a number appended to your JobID, and -v is used to 
print submitted commands to STDERR.

END
}
my $configfile = $opts{c};
my $vers = $opts{r};
my $verbose = 1 if $opts{v};

my $svrun = new RunSVcallers::SGE;

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
			%bd=$svrun->setParams(\%params,'range','bdconf',$vers);
			my $lsfcommand = $svrun->makeCommand(%bd);
			push @commands, qq($lsfcommand "$params{bam2conf} $params{BDconfParams} $params{bamdir}/\\\$SGE_TASK_ID.bam > $params{name}.\\\$SGE_TASK_ID.config");
		}
		if ($params{chrOther} && $params{bamdir}) {
			my @list = split /\s+/, $params{chrOther};
			foreach my $other (@list) {
				%bd=$svrun->setParams(\%params,$other,'bdconf',$vers);
				$bd{other}=$other;
				my $lsfcommand = $svrun->makeCommand(%bd);
				push @commands, qq($lsfcommand "$params{bam2conf} $params{BDconfParams} $params{bamdir}/\\\$SGE_TASK_ID.bam > $params{name}.$other.config");
				$bd{other}='';
			}
		}
		# If all data is in a single bam
		if (($params{chrRange} || $params{chrOther}) && $params{bam}) {
			%bd=$svrun->setParams(\%params,'all','bdconf',$vers);
			# Command for config file creation
			my $lsfcommand = $svrun->makeCommand(%bd);
			push @commands, qq($lsfcommand "$params{bam2conf} $params{BDconfParams} $params{bam} > $params{name}.config");
		}
	}
	# Parameters for running BDMax

	if ($params{chrRange}) {
		%bd=$svrun->setParams(\%params,'range','bd',$vers);
		$bd{prevjob}="$params{name}.$vers.bdconf" if $params{BDconf}; # set job dependency
		# Command for running BDMax

		my $lsfcommand = $svrun->makeCommand(%bd);
		my $config = $params{bam} ? "$params{name}.config" : "$params{name}.\\\$SGE_TASK_ID.config";
		push @commands, qq($lsfcommand "$params{bdexe}  -o \\\$SGE_TASK_ID $params{BDparams}  $config > $params{name}.\\\$SGE_TASK_ID.max");
	}
	if ($params{chrOther}) {
		my @list = split /\s+/, $params{chrOther};
		foreach my $other (@list) {
			%bd=$svrun->setParams(\%params,$other,'bd',$vers);
			$bd{other}=$other;
			$bd{prevjob}="$params{name}.$vers.bdconf" if $params{BDconf}; # set job dependency
			$bd{prevjob}.=".$other" if $params{BDconf} && $params{bamdir};
			my $lsfcommand = $svrun->makeCommand(%bd);
			my $config = $params{bam} ? "$params{name}.config" : "$params{name}.$other.config";
			push @commands, qq($lsfcommand "$params{bdexe}  -o $other $params{BDparams} $config > $params{name}.$other.max");
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
		my $lsfcommand = $svrun->makeCommand(%pd);
		my $outfiles = 'out';
		my $reference = $params{chrrefdir} ? "$params{chrrefdir}/\\\$SGE_TASK_ID.fa" : $params{reffile};
		push @commands, qq($lsfcommand "$params{pinexe} -f $reference -c \\\$SGE_TASK_ID -o $params{name}.\\\$SGE_TASK_ID  -i $params{PDconf}") if !$params{PDoptParams};
		push @commands, qq($lsfcommand "$params{pinexe} -f $reference -c \\\$SGE_TASK_ID -o $params{name}.\\\$SGE_TASK_ID  -i $params{PDconf} $params{PDoptParams}") if $params{PDoptParams};
	}
	if ($params{chrOther}) {
		my @list = split /\s+/, $params{chrOther};
		foreach my $other (@list) {
			%pd=$svrun->setParams(\%params,$other,'pd',$vers);
			$pd{other}=$other;
			$pd{prevjob}= "$params{name}.$vers.pdFilter.$other" if $params{PDgetReads}; # set job dependency
			my $lsfcommand = $svrun->makeCommand(%pd);
			my $outfiles = "out";
			my $reference = $params{chrrefdir} ? "$params{chrrefdir}/$other.fa" : $params{reffile};
			push @commands, qq($lsfcommand "$params{pinexe} -f $reference -c $other -o $params{name} -i $params{PDconf}.$other") if !$params{PDoptParams};;
			push @commands, qq($lsfcommand "$params{pinexe} -f $reference -c $other -o $params{name} -i $params{PDconf}.$other $params{PDoptParams}") if $params{PDoptParams};
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
			my $lsfcommand = $svrun->makeCommand(%sec);
			my $refbam = $params{bam} ? $params{bam} : "$params{bamdir}/\\\$SGE_TASK_ID.bam";
			push @commands, qq($lsfcommand "$params{exedir}/samfilter.sh $refbam \\\$SGE_TASK_ID 1");
		}
		if ($params{chrOther}) {
			my @list = split /\s+/, $params{chrOther};
			foreach my $other (@list) {
				%sec=$svrun->setParams(\%params,$other,'SECfilter',$vers);
				$sec{other}=$other;
				my $lsfcommand = $svrun->makeCommand(%sec);
				my $refbam = $params{bam} ? $params{bam} : "$params{bamdir}/$other.bam";
				push @commands, qq($lsfcommand "$params{exedir}/samfilter.sh $refbam $other 1");
				$sec{other}='';
			}
		}
	}
	# Set parameters for SECluster jobs
	if ($params{chrRange}) {
		%sec=$svrun->setParams(\%params,'range','sec',$vers);
		$sec{prevjob}="$params{name}.$vers.SECfilter" if $params{SECfilter};
		my $lsfcommand = $svrun->makeCommand(%sec);
		push @commands, qq($lsfcommand "$params{secexe} -f \\\$SGE_TASK_ID.se.sam -q $params{SECqual} -m $params{SECmin} -c $params{SECmin} -r \\\$SGE_TASK_ID -x $params{SECmax}  > $params{name}.\\\$SGE_TASK_ID.clusters");

	}
	if ($params{chrOther}) {
		my @list = split /\s+/, $params{chrOther};
		foreach my $other (@list) {
			%sec=$svrun->setParams(\%params,$other,'sec',$vers);
			$sec{other}=$other;
			$sec{prevjob}="$params{name}.$vers.SECfilter.$other" if $params{SECfilter};
			my $lsfcommand = $svrun->makeCommand(%sec);
			push @commands, qq($lsfcommand "$params{secexe} -f $other.se.sam -q $params{SECqual} -m $params{SECmin} -c $params{SECmin} -r $other -x $params{SECmax}  > $params{name}.$other.clusters");
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
			my $lsfcommand = $svrun->makeCommand(%cnd);
			my $pileup = "$params{cnddir}/pileup2win.pl > \\\$SGE_TASK_ID.win";
			# A single bam only
			if ($params{bam}) {
				push @commands, qq($lsfcommand "$params{samtools} view -u $params{bam} \\\$SGE_TASK_ID | $params{samtools} pileup -c -r $params{CNDsnprate} -f $params{reffile} - | cut -f 1-8 | $pileup");
			}
			# Directory of chromosome bams
			elsif ($params{bamdir}) {
				push @commands, qq($lsfcommand "$params{samtools} pileup -c -r $params{CNDsnprate} -f $params{reffile} $params{bamdir}/\\\$SGE_TASK_ID.bam | cut -f 1-8 | $pileup");
			}
		}
		if ($params{chrOther}) {
			my @list = split /\s+/, $params{chrOther};
			foreach my $other (@list) {
				%cnd=$svrun->setParams(\%params,$other,'cndpile',$vers);
				$cnd{other}=$other;
				my $lsfcommand = $svrun->makeCommand(%cnd);
				my $pileup = "$params{cnddir}/pileup2win.pl > $other.win";
				if ($params{bam}) {
					push @commands, qq($lsfcommand "$params{samtools} view -u $params{bam} $other | $params{samtools} pileup -c -r $params{CNDsnprate} -f $params{reffile} - | cut -f 1-8 | $pileup");
				}
				elsif ($params{bamdir}) {
					push @commands, qq($lsfcommand "$params{samtools} pileup -c -r $params{CNDsnprate} -f $params{reffile} $params{bamdir}/$other.bam | cut -f 1-8 | $pileup");
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
			my $lsfcommand = $svrun->makeCommand(%cnd);
			push @commands, qq($lsfcommand "$params{exedir}/gcmedian.pl \\\$SGE_TASK_ID.win > \\\$SGE_TASK_ID.gchash");
			%cnd=$svrun->setParams(\%params,'range','gccorrect',$vers);
			$cnd{prevjob}="$params{name}.$vers.gccorrect";
			my $lsfcommand2 = $svrun->makeCommand(%cnd);
			push @commands, qq($lsfcommand2 "$params{exedir}/gcCorrect.pl --gchash \\\$SGE_TASK_ID.gchash --infile \\\$SGE_TASK_ID.win > \\\$SGE_TASK_ID.corr.win");
		}
		if ($params{chrOther}) {
			my @list = split /\s+/, $params{chrOther};
			foreach my $other (@list) {
				%cnd=$svrun->setParams(\%params,$other,'gccorrect',$vers);
				$cnd{other}=$other;
				$cnd{prevjob}="$params{name}.$vers.cndpile" if $params{CNDpileup};
				my $lsfcommand = $svrun->makeCommand(%cnd);
				push @commands, qq($lsfcommand "$params{exedir}/gcmedian.pl $other.win > $other.gchash");
				%cnd=$svrun->setParams(\%params,$other,'gccorrect',$vers);
				$cnd{other}=$other;
				$cnd{prevjob}="$params{name}.$vers.gccorrect.$other";
				my $lsfcommand2 = $svrun->makeCommand(%cnd);
				push @commands, qq($lsfcommand2 "$params{exedir}/gcCorrect.pl --gchash $other.gchash --infile $other.win > $other.corr.win");
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
		my $lsfcommand = $svrun->makeCommand(%cnd);
		my $winfile = $params{CNDgccorrect} ? 'corr.win' : 'win';
		push @commands, qq($lsfcommand "$params{cnddir}/cnD $params{CNDparams} --prefix=\\\$SGE_TASK_ID \\\$SGE_TASK_ID.$winfile");

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
			my $lsfcommand = $svrun->makeCommand(%cnd);
			my $winfile = $params{CNDgccorrect} ? "$other.corr.win" : "$other.win";
			push @commands, qq($lsfcommand "$params{cnddir}/cnD $params{CNDparams} --prefix=$other $winfile");
			$cnd{other}='';
		}
	}
	# Set up parameters for final steps
	if ($params{chrRange}){
		%cnd=$svrun->setParams(\%params,'range','cndext',$vers);
		$cnd{prevjob}="$params{name}.$vers.cnd";
		my $lsfcommand = $svrun->makeCommand(%cnd);
		push @commands, qq($lsfcommand "$params{cnddir}/extractCNFromVit.pl --no-losses \\\$SGE_TASK_ID\\_posterior.txt > \\\$SGE_TASK_ID.calls") if $params{CNDnohet};
		push @commands, qq($lsfcommand "$params{cnddir}/extractCNFromVit.pl \\\$SGE_TASK_INDEX\\_posterior.txt > \\\$SGE_TASK_ID.calls") if !$params{CNDnohet};
	}
	if ($params{chrOther}) {
		my @list = split /\s+/, $params{chrOther};
		foreach my $other (@list) {
			%cnd=$svrun->setParams(\%params,$other,'cndext',$vers);
			$cnd{other}=$other;
			$cnd{prevjob}="$params{name}.$vers.cnd.$other";
			my $lsfcommand = $svrun->makeCommand(%cnd);
			push @commands, qq($lsfcommand "$params{cnddir}/extractCNFromVit.pl --no-losses $other\_posterior.txt > $other.calls") if $params{CNDnohet};
			push @commands, qq($lsfcommand "$params{cnddir}/extractCNFromVit.pl $other\_posterior.txt > $other.calls") if !$params{CNDnohet};
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
			my $lsfcommand = $svrun->makeCommand(%rdx);
			push @commands, qq($lsfcommand "$params{samtools} view -b $params{bam} \\\$SGE_TASk_ID > $params{name}.chrom\\\$SGE_TASK_ID.bam");
		}
		if ($params{chrOther}) {
			my @list = split /\s+/, $params{chrOther};
			foreach my $other (@list) {
				%rdx=$svrun->setParams(\%params,$other,'rdxsplit',$vers);
				$rdx{other}=$other;
				my $lsfcommand = $svrun->makeCommand(%rdx);
				push @commands, qq($lsfcommand "$params{samtools} view -b $params{bam} $other > $params{name}.chrom$other.bam");
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
	my $lsfcommand = $svrun->makeCommand(%rdx);
	push @commands, qq($lsfcommand "$params{rdxdir}/run.sh");
	
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
