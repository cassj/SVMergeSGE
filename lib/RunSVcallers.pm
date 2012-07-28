package RunSVcallers;

use warnings;
use strict;
use Carp;

=head1 NAME

RunSVcallers

=head1 AUTHOR

kw10@sanger.ac.uk, cassjohnston@gmail.com

=head1 DESCRIPTION

Base class for modules that generate commands to submit farm jobs
for the SVMerge local assembly step. 

=head1 METHODS

=head2 new

	Arguments   :  none
	Description :  Creates a new RunSVcallers object

=cut

sub new {
	my ($class,%arg) = @_;
	my $self={};
	bless($self,$class || ref($class));
	return $self;
}

=head2 makeCommand 

        To be implemented by subclasses for specific job dispatchers.

	Arg[1]      :  hash containing parameters for the job dispatcher
	Example     :  makeCommand(%Params)
	Description :  Creates a job submission command using %Params
	Returns     :  Runnable command
        
=cut 

sub makeCommand {
    die "makeCommand not implemented in base class";
}

=head2 format_out

      Arg[1]        : job array or single job ['range' for job array]
      Arg[2]        : job type [bd,pd,pdFilter,SECfilter,sec,rdx,cnd,cndpile,assembly,parse]]
      Example       : $p{out} = $self->format_out($which,$caller)
      Description   : generates an output file name

=cut
sub format_out{
  die "format_out not implemented in base class";
}

=head2 format_err

      Arg[1]        : job array or single job ['range' for job array]
      Arg[2]        : job type [bd,pd,pdFilter,SECfilter,sec,rdx,cnd,cndpile,assembly,parse]]
      Example       : $p{out} = $self->format_out($which,$caller)
      Description   : generates an error file name

=cut
sub format_err{
  die "format_err not implemented in base class";
}



=head2 setParams

	Arg[1]      :  reference to a hash with parameters from the SVMerge config file
	Arg[2]      :  job array or single job ['range' for job array]
	Arg[3]      :  job type [bd,pd,pdFilter,SECfilter,sec,rdx,cnd,cndpile,assembly,parse]
	Arg[4]      :  a number appended to jobIDs to create unique IDs
	Example     :  setParams(\$SVMergeParams,'range','bd','2')
	Description :  sets parameters for running SVMerge jobs
	Returns     :  a hash with jobs parameters

=cut
sub setParams {
	my ($self,$param,$which,$caller,$vers) = @_;
	my %param=%$param;
	$param{queue} = $param{defaultQueue} if !$param{queue};
	my %p=();
	$p{queue} = $param{queue};
	$vers='0' if !$vers;
	$p{jobID} = "$param{name}.$vers.$caller";

        $p{err} = $self->format_err($which, $caller);
        $p{out} = $self->format_out($which, $caller);

        if ($which eq 'range'){
               if ($param{chrRange} =~ /^(\d+)-(\d+)/) {
                       ($p{min},$p{max}) = ($1,$2) if $param{chrRange} =~ /^(\d+)-(\d+)/;
               }
               else {
                       $p{min} = $1 if $param{chrRange} =~ /^(\d+)/;
               }
        }

	if ($caller eq 'bd') {
		$p{mem} = $param{BDmem} || '';
		$p{queue} = $param{BDqueue} || $param{queue};
	}
	elsif ($caller eq 'pd') {
		$p{mem} = $param{PDmem} || '';
		$p{queue} = $param{PDqueue} || $param{queue} ;
	}
	elsif ($caller eq 'pdFilter') {
		$p{mem} = $param{PDfilterMem} || '';
		$p{queue} = $param{PDfilterQueue} || $param{queue} ;
	}
	elsif ($caller eq 'SECfilter') {
		$p{mem} = $param{SECfilterMem} || '';
		$p{queue} = $param{SECfilterQueue} || $param{queue} ;
	}
	elsif ($caller eq 'sec') {
		$p{mem} = $param{SECmem} || '';
		$p{queue} = $param{SECqueue} || $param{queue} ;
	}
	elsif ($caller eq 'rdx') {
		$p{mem} = $param{RDXmem} || '';
		$p{queue} = $param{RDXqueue} || $param{queue} ;
	}
	elsif ($caller eq 'cndpile') {
		$p{mem} = $param{CNDpileupMem} ||'';
		$p{queue} = $param{CNDpileupQueue} || $param{queue};
	}
	elsif ($caller eq 'cnd') {
		$p{mem} = $param{CNDmem} || '';
		$p{queue} = $param{CNDqueue} || $param{queue};
	}
	elsif ($caller eq 'assembly') {
		$p{mem} = $param{assemMem} || '';
		$p{queue} = $param{assemQueue} || $param{queue};
	}
	elsif ($caller eq 'parse') {
		$p{mem} = $param{parseMem} || '';
		$p{queue} = $param{parseQueue} || $param{queue};
	}
	return %p;
}

=head2 jobindex

=cut

sub jobindex{
  die "jobindex not implemented in base class";
}

=head2 submit

	Arg[1]      :  array of LSF commands
	Example     :  submit(@commands)
	Description :  submits each job and checks for successful submission
	Returns     :  True if successful

=cut

sub submit {
	my ($self,@command) = @_;
        my $success_string = $self->success_string;
	for (@command)  {
		my @status = `$_`;
		my ($count,$not) = ();
		for (@status) {
			if (/$success_string/) {
				$count++;
			} 
			else {
				$not++;
				print @status;
				return '';
			}
		}
		print @status;
	}
	return 1;
}



1;
