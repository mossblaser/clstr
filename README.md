`clstr`: A quick'n'dirty cluster manager for quick'n'dirty clusters
===================================================================

`clstr` is a simple program which spawns jobs in parallel on remote machines
using SSH. Key features include:

* Can spawn jobs on hundreds of remote machines at once.
* Usable in shell-one-liners to full-blown shell scripts.
* Quickly reject dead or broken machines.
* Automatically respawn failed jobs.
* Provide status information during execution.

`clstr` was inspired by [GNU Parallel's](https://www.gnu.org/software/parallel/)
`ssh-hosts` feature which (in theory...) allows running jobs on remote machines
but in practice quickly becomes very unstable. Because of its much narrower
scope, `clstr` manages to be both simpler, more reliable and somewhat faster
than GNU parallel when running jobs remotely.

Quick Example
-------------

If the documentation below is tl;dr:

    $ pip install clstr
    $ clstr --node-file nodes.txt --job-file jobs.txt

Where `nodes.txt` is a newline-separated list of machine hostnames and
`jobs.txt` is a newline separated list of commands to execute. The jobs will be
executed in parallel on the set of nodes, at most one per node at a time. If a
node fails, the job will be rescheduled on another node.

Installation
------------

You can install `clstr` from [PyPI](https://pypi.python.org/pypi/clstr/) using:

    $ pip install clstr

Or you can install it from a download of this repository using:

    $ python setup.py install

Usage examples/tutorial
-----------------------

The following documentation gives examples of `clstr`'s usage for various
common tasks while demonstrating its key features. Terser and more complete
documentation can be found by running `clstr --help`.

### Running jobs on nodes

To use `clstr` you need two things: a set of jobs to run and a set of nodes
(machines you can SSH into) to run them on. Note that you should have
public-key based authentication set up for all remote hosts. SSH should also be
configured to automatically use the correct username, for example by using a
line such as this in your `~/.ssh/config` file:

    Host e-c07ki*
        User mbax9jh2

In the most basic case you can enumerate both jobs and nodes on the command
line using the `--node` and `--job` options:

    $ clstr --node e-c07kilf901 --node e-c07kilf902 \
            --job 'echo "Hello, world."'  --job 'echo "Hello again."' \
            --job 'echo "What do you want?"'
    Hello, world.
    What do you want?
    Hello again.

Here we ran three jobs on two nodes and the stdout from each job was merged
into the stdout of `clstr`. Each node is allowed to run one job at a time and
nodes are used in a random order. If you want to run more than one job on a
node at once, list that node more than once.

Note that the order of the output is in no particular order and it may be
interleaved arbitrarily. In practice, most operating systems buffer whole lines
before sending them to stdout and so interleaving tends to only occur on a
line-by-line basis but this isn't guaranteed. If this matters to you, you
should make each job write its output to a file which you can later collect. If
you have a common network file store mounted on all remote machines you can do
something like:

    $ clstr --node e-c07kilf901 --node e-c07kilf902 \
            --job 'cd ~/experiment_dir; echo "Hello, world." > first'  \
            --job 'cd ~/experiment_dir; echo "Hello again." > second' \
            --job 'cd ~/experiment_dir; echo "What do you want?" > third'
    $ cd ~/experiment_dir
    $ ls
    first
    second
    third

Note that the jobs are run in whatever directory SSH defaults to (and not your
current working directory) so you must `cd` into your experiment directory as
part of your job specification.

If the SSH connection to a node exits with a status other than 0, `clstr` has
no way to know if it was the SSH connection, the node or the job which failed.
Instead, `clstr` presumes that jobs always succeed (return a status of 0) and
thus any failures are due to a broken node. When this occurs, the job is simply
placed back in the job queue and the node is removed from the pool of available
nodes. This feature can also be used to allow application-specific filtering of
nodes by making jobs check the suitability of the node and exit with an error
status if it is not appropriate. For example, you could make your jobs look
like this:

    [ -z "$(users)" ] && echo "running job..."

Which will fail immediately if anybody is logged into the remote machine
ensuring that the job will only run on idle machines.

### Listing jobs and nodes in files

In practice, specifying nodes and jobs as arguments is not practical for large
numbers of nodes or jobs. As a result the `--node-file` and `--job-file`
arguments can be used to specify a file where the list of jobs and nodes can be
read. Each file should contain a newline-delimited list of jobs and nodes
respectively. The first example could be rewritten in this fashion like so:

    $ cat nodes.txt
    e-c07kilf901
    e-c07kilf902
    
    $ cat jobs.txt
    echo "Hello, world."
    echo "Hello again."
    echo "What do you want?"
    
    $ clstr --node-file nodes.txt --job-file jobs.txt
    Hello, world.
    Hello again.
    What do you want?

Of course, in reality you will probably have a much larger list of nodes which
you'll reuse most of the time and a list of jobs for each application. You can
also generate jobs pragmatically on the shell. For example:

    $ clstr --node-file nodes.txt --job-file <(
          for i in `seq 10`; do echo "echo I am job $i"; done)
    I am job 1
    I am job 2
    I am job 3
    I am job 4
    I am job 5
    I am job 6
    I am job 7
    I am job 8
    I am job 9
    I am job 10

For the unfamiliar, the bash `<( ... )` syntax is substituted for a named pipe
from which the output of the shell expression between the brackets can be read.

### Running a job once on each node

Sometimes it is useful to run a command on every node in the cluster, for
example to determine what CPUs are available or to get a list of hosts matching
some criteria. The `--on-each-node` argument allows this and is used in place
of `--job` or `--job-file`.

For example, to find out what CPUs are available in the cluster we can run:

    $ clstr --node-file nodes.txt \
            --on-each-node 'grep "model name" /proc/cpuinfo' \
          | sort | uniq -c
    496 model name      : Intel(R) Core(TM) i5-2400 CPU @ 3.10GHz
    188 model name      : Intel(R) Core(TM) i5-3470 CPU @ 3.20GHz
     28 model name      : Intel(R) Core(TM) i5-4570 CPU @ 3.20GHz

### Monitoring execution

You can track the progress of your jobs by adding the `--status` option which
will print a status line giving realtime cluster utilisation and job completion
counts. For example, with some large set of nodes and jobs, you'll get a
status-line like the following:

    $ clstr --status --node-file nodes.txt --job-file jobs.txt
    110 of 556 jobs complete, 176 of 176 nodes running jobs.

You can also track what nodes are running which binaries if you need more
detailed diagnostic information using `--active-nodes FILE`. This argument
causes `clstr` to maintain a list of which nodes are currently running which
jobs:

    $ clstr --active-nodes /tmp/running_jobs --node-file nodes.txt --job-file jobs.txt

Looking at `/tmp/running_jobs` during the run will yield something like:

    $ head /tmp/running_jobs
    e-c07kilf3102: <'Job' 'my_very_exciting_experiment 1'>
    e-c07kig2314: <'Job' 'my_very_exciting_experiment 2'>
    e-c07kilf3143: <'Job' 'my_very_exciting_experiment 3'>
    e-c07kilf1614: <'Job' 'my_very_exciting_experiment 4'>
    e-c07kilf3132: <'Job' 'my_very_exciting_experiment 5'>
    e-c07kig2333: <'Job' 'my_very_exciting_experiment 6'>
    e-c07kilf901: <'Job' 'my_very_exciting_experiment 7'>
    e-c07kilf3136: <'Job' 'my_very_exciting_experiment 8'>


