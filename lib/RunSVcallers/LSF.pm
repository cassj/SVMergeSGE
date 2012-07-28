package RunSVcallers::LSF;

use warnings;
use strict;

use base 'RunSVcallers';
use Carp;

=head1 NAME

RunSVcallers::LSF

=head1 AUTHOR

kw10@sanger.ac.uk, cassjohnston@gmail.com

=head1 DESCRIPTION

A module to generate LSF commands to submit farm jobs for the SVMerge local assembly step.

=head1 METHODS

=head2 makeCommand 

        Arg[1]      :  string - the command to be run
	Arg[2]      :  hash containing LSF parameters, eg: queue, memory 
	               requirements, job dependency, job array range, jobID,
	               and .e and .o file name
	Example     :  makeCommand(%lsfParams)
	Description :  Creates a 'bsub' command with LSF options in %lsfParams
	Returns     :  LSF command

=cut 

sub makeCommand {
	my ($self,$cmd, %param) = @_;
	$param{queue} = $param{defaultQueue} if !$param{queue};
	my $command = 'bsub ';
	# job dependancy
	$command .= qq(-w "done("$param{prevjob}")" ) if $param{prevjob};
	# job name
	if ($param{jobID} && $param{other}) {
		$command .= qq(-J"$param{jobID}.$param{other}");
	}
	elsif ($param{jobID} && !$param{min}) {
		$command .= qq(-J"$param{jobID}");
	}
	elsif ($param{jobID} && $param{min} && !$param{max}) {
	# or one job from an array
		$command .= qq(-J"$param{jobID}\[$param{min}\]");
	}
	elsif ($param{jobID} && $param{min} && $param{max}) {
	# or job array
		$command .= qq(-J"$param{jobID}\[$param{min}-$param{max}\]);
		$command .= "\%$param{joblimit}" if $param{joblimit};
		$command .= "$param{parseJoblimit}%" if $param{parseJoblimit};
		$command .= '"';
	}
	# log files and queue
	$command .= qq( -e $param{err} -o $param{out} -q $param{queue}) if !$param{outdir};
	$command .= qq( -e $param{outdir}/$param{err} -o $param{outdir}/$param{out} -q $param{queue}) if $param{outdir};
	# memory requirements
	$command .= qq( -R"select[mem>$param{mem}] rusage[mem=$param{mem}]" -M $param{mem}000) if $param{mem};
	return qq($command "$cmd");

}

=head2 format_out

      Arg[1]        : job array or single job ['range' for job array]
      Arg[2]        : job type [bd,pd,pdFilter,SECfilter,sec,rdx,cnd,cndpile,assembly,parse]]
      Example       : $p{out} = $self->format_out($which,$caller)
      Description   : generates an output file name

=cut
sub format_out{
  my ($self, $which, $caller) = @_;
  return "logs/$caller.\%I.o" if ($which eq 'range');
  return "logs/$caller.$which.o" if ($caller ne 'rdx');
  return "$caller.$which.o";
}

=head2 format_err

      Arg[1]        : job array or single job ['range' for job array]
      Arg[2]        : job type [bd,pd,pdFilter,SECfilter,sec,rdx,cnd,cndpile,assembly,parse]]
      Example       : $p{out} = $self->format_out($which,$caller)
      Description   : generates an error file name

=cut
sub format_err{
  my ($self, $which, $caller) = @_;
  return "logs/$caller.\%I.e" if ($which eq 'range');
  return "logs/$caller.$which.e" if ($caller ne 'rdx');
  return "$caller.$which.e";
}

=head2 jobindex

  Example      : $runner->jobindex
  Description  : returns the placeholder string for the job index (ie. $SGE_TASK_ID)

=cut

sub jobindex{
  return '\\\$LSB_JOBINDEX';
}

=head2 success_string

  Example     : $runner->success_string
  Description : A string that will match in the output of a successful submission

=cut
sub success_string{
  return 'submitted to';
}

1;
