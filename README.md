<!-- vim: set textwidth=80 colorcolumn=81 : -->

## Slurm metascheduler

This program works similarly to
[GNU parallel](https://www.gnu.org/software/parallel/), but schedules jobs on a
Slurm cluster. You will need to install the
[Python json5 module](https://pypi.org/project/json5/) and provide a config file
in [JSON5 format](https://json5.org/) that specifies the parameters of your
Slurm cluster. Example config files for University of Utah servers are provided.

The config file defines an array of Slurm partitions. Each partition is
automatically scored by the number of jobs it has completed within the past 24
hours. New jobs will be submitted to whichever parition is likely to complete
the job the fastest given its current load and previous performance. If two
partitions are estimated to complete the job in the same amount of time, the job
will be submitted to the partition that appears first in the list. Jobs that
have not started yet will likewise be reassinged to faster partitions as the
partitions' performance scores change over time.

As a rule of thumb, set the maximum jobs per partition to the lesser of 1.5
times the number of nodes in the partition and 0.5 times the QoS-allowed number
of queued jobs per user in that partition.

The number of commands per job should generally be equal to the number of
logical CPUs on whichever node of the partition has the greatest number of them.
However, if jobs on the partition are preemptable and the CPUs have
hyperthreading, it's better to set the number of commands per job to the number
of physical CPUs (i.e. the number of logical CPUs divided by 2), which will
decrease overall throughput a little, but minimize the amount of work lost when
a job is preeempted.
