package RunSVcallers::SGE;

use base RunSVcallers;

use warnings;
use strict;
use Carp;
use Cwd;
use File::Temp;

=head1 NAME

RunSVcallers::SGE

=head1 AUTHOR

cassjohnston@gmail.com

=head1 DESCRIPTION

A module to generate SGE commands to submit farm jobs for the SVMerge local assembly step.
Inherits from the original RunSVcallers class and overrides the makeCommand method.

=head1 METHODS

=head2 makeCommand 

        Arg[1]      :  string - the command to be run
	Arg[2]      :  hash containing SGE parameters, eg: queue, memory 
	               requirements, job dependency, job array range, jobID,
	               and .e and .o file name
	Example     :  makeCommand(%sgeParams)
	Description :  Creates a 'qsub' command with SGE options in %sgeParams
	Returns     :  SGE command

=cut 

sub makeCommand {
	my ($self, $cmd,  %param) = @_;
        $param{queue} = $param{defaultQueue} if !$param{queue};

        # write command out to a temporary shell script, otherwise you can't use $SGE_TASK_ID in array jobs.
        my $cwd = Cwd::cwd;
        my $script_dir =  "$cwd/sge_scripts";
        mkdir $script_dir unless -d $script_dir;
        my $tmp = File::Temp->new( UNLINK => 0,
                                   SUFFIX => '.sh',
                                   DIR => $script_dir );
        print $tmp qq(#!/bin/sh\n);
        print $tmp qq(#\$ -S /bin/sh\n);

        # working dir
        print $tmp qq(#\$ -cwd\n);

	# job dependancy
        print $tmp  qq(#\$ -hold_jid $param{prevjob}\n) if $param{prevjob};

	# job name
	if ($param{jobID} && $param{other}) {
	  print $tmp qq(#\$ -N "$param{jobID}.$param{other}"\n);
	}
	elsif ($param{jobID} ) {
	  print $tmp qq(#\$  -N "$param{jobID}"\n);
	}
        # array job?
        if ($param{min} ) {
          $param{max} = $param{min} unless $param{max};
          print $tmp qq(#\$ -t '$param{min}-$param{max}'\n);
        }

	# log files and queue
	print $tmp qq(#\$ -e $param{err}\n#\$ -o $param{out}\n#\$ -q $param{queue}\n) if !$param{outdir};
	print $tmp qq(#\$ -e $param{outdir}/$param{err}\n#\$ -o $param{outdir}/$param{out}\n#\$ -q $param{queue}\n) if $param{outdir};

        # memory requirements
        print $tmp qq(#\$ -l "h_vmem=$param{mem}M"\n) if $param{mem};

        # command
        print $tmp qq($cmd\n);

	return "qsub $tmp";
      

}

=head2 format_out

      Arg[1]        : job array or single job ['range' for job array]
      Arg[2]        : job type [bd,pd,pdFilter,SECfilter,sec,rdx,cnd,cndpile,assembly,parse]]
      Example       : $p{out} = $self->format_out($which,$caller)
      Description   : generates an output file name

=cut
sub format_out{
  my ($self, $which, $caller) = @_;
  return ($caller eq 'rdx') ? "$caller.o" : "logs/$caller.o";
}

=head2 format_err

      Arg[1]        : job array or single job ['range' for job array]
      Arg[2]        : job type [bd,pd,pdFilter,SECfilter,sec,rdx,cnd,cndpile,assembly,parse]]
      Example       : $p{out} = $runner->format_out($which,$caller)
      Description   : generates an error file name

=cut
sub format_err{
  my ($self, $which, $caller) = @_;
  return ($caller eq 'rdx') ? "$caller.e" : "logs/$caller.e";
}

=head2 jobindex

  Example      : $runner->jobindex
  Description  : returns the placeholder string for the job index (ie. $SGE_TASK_ID)

=cut

sub jobindex{
  return '$SGE_TASK_ID';
}

=head2 success_string

  Example     : $runner->success_string
  Description : A string that will match in the output of a successful submission

=cut
sub success_string{
  return 'has been submitted';
}


1;
