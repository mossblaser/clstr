"""A quick'n'dirty cluster manager."""

import sys

import math

import argparse

import random

from threading import Thread, RLock, Event

from subprocess import Popen

from collections import deque

import logging


__version__ = "0.1.0"


class Node(Thread):
    """A compute node in the pool.
    
    This object is a :py:class:`~threading.Thread` which will attempt to
    continuously collect jobs from the cluster :py:class:`Manager` and execute
    them on a remote compute node (via SSH).
    
    Attributes
    ----------
    hostname : string
        The hostname of the machine this node is running on.
    node_uses_remaining : int or None
        The number of additional jobs this node is allowed to run before it
        terminates, or None if an unlimited number is allowed.
    """
    
    def __init__(self, manager, hostname, max_node_uses=None):
        """Instantiate a new compute node on the supplied host.
        
        Note the caller is responsible for calling :py:meth:`start` to start
        the node.
        """
        super(Node, self).__init__()
        
        self.manager = manager
        self.hostname = hostname
        self.node_uses_remaining = max_node_uses
    
    def ping_host(self):
        """A basic sanity check to ensure the host is currently
        up/reachable.
        
        Returns
        -------
        bool
            True if the host responds to ping, False otherwise.
        """
        # Just use the system 'ping' command since there is no standard library
        # routine for this. '-c 1' makes ping only try once, '-w 1' makes it
        # give up after waiting 1 second for a response. This is perhaps a bit
        # harsh but it enables bad hosts to get quickly eliminated from the
        # pool quickly.
        with open("/dev/null", "w") as f:
            return Popen(["ping", "-c", "1", "-w", "1", self.hostname],
                         stdout=f, stderr=f).wait() == 0
    
    def execute_job(self, job):
        """Execute a job on this host and return True if it succeeded."""
        ssh = Popen(["ssh",
                     "-o", "ConnectTimeout=10",
                     "-o", "ConnectionAttempts=1",
                     self.hostname,
                     job.command],
                    stdin=job.stdin,
                    stdout=job.stdout,
                    stderr=job.stderr,
                    close_fds=False)
        
        return ssh.wait() == 0
    
    def run(self):
        """The thread which manages the node."""
        # Initially make sure we can ping the host first
        if not self.ping_host():
            self.manager._node_failed(self)
            return
        
        while (self.node_uses_remaining is None
               or self.node_uses_remaining >= 1):
            # Wait until some work is available and do it
            job = self.manager._get_job(self)
            
            if self.node_uses_remaining is not None:
                self.node_uses_remaining -= 1
            
            if self.execute_job(job):
                self.manager._job_finished(self)
            else:
                self.manager._node_failed(self)
                # When a job fails we assume it is the fault of the host (not
                # the job) and so kill the node.
                return
        
        # Maximum number of node uses has been reached, terminate
        self.manager._node_failed(self)
        return
    
    
    def __repr__(self):
        return "<%s on %r>"%(
            self.__class__.__name__,
            self.hostname,
        )


class Job(object):
    """A job to execute on the cluster.
    
    Attributes
    ----------
    command : string
        A shell expression to execute.
    attempts : int
        The number of times this job has been executed.
    success : bool
        True if the job completed successfully, False otherwise.
    stdin : file or None
        The file to read stdin from.
    stdout : file or None
        The file to write stdout to.
    stderr : file or None
        The file to write stderr to.
    """
    
    def __init__(self, command, stdin=None, stdout=None, stderr=None):
        self.command = command
        self.attempts = 0
        self.success = False
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
    
    def __repr__(self):
        return "<%r %r>"%(self.__class__.__name__, self.command)


class Manager(object):
    """A cluster manager.
    
    The cluster manager is responsible for instantiating compute nodes and
    handling the scheduling of jobs.
    """
    
    def __init__(self, max_nodes=None, max_node_uses=None):
        """Create a new cluster manager.
        
        Parameters
        ----------
        max_nodes : int or None
            The maximum number of nodes which may be simultaneously used by the
            cluster. If None, an unlimited number of nodes may be used at once.
        max_node_uses : int or None
            The maximum number of times a node may be used to execute a job. If
            None, the number of uses is unlimited.
        """
        self.max_nodes = max_nodes
        self.max_node_uses = max_node_uses
        
        # A lock which must be held when accessing any of the data structures
        # in the cluster manager.
        self.lock = RLock()
        
        # A list of hostnames of machines a compute node may be started on if
        # required.
        self.unused_hostnames = []
        
        # The set of nodes which are currently idle.
        self.idle_nodes = set()
        
        # A dict {node: job, ...} giving the job being executed by any active
        # nodes.
        self.active_nodes = {}
        
        # A queue of jobs awaiting execution
        self.job_queue = deque()
        
        # An event set whenever the job_queue non-empty
        self.job_available = Event()
        
        # This event is set whenever the job queue changes. It is used to
        # trigger any UI waiting on :py:meth:`wait`.
        self.queue_changed = Event()
    
    def add_node(self, hostname):
        """Add a new machine to the pool of compute nodes.
        
        If a host is added multiple times, multiple jobs may be executed on it
        simultaneously.
        
        Parameters
        ----------
        hostname : string
            The hostname of the machine to use as a Node.
        """
        with self.lock:
            # List is maintained in a random order to avoid always using the
            # same machines.
            self.unused_hostnames.insert(
                0 if not self.unused_hostnames else
                random.randrange(len(self.unused_hostnames)),
                hostname
            )
            self._spawn_nodes_if_required()
    
    def add_job(self, command, stdin=None, stdout=None, stderr=None):
        """Add a new job to the job queue.
        
        Parameters
        ----------
        command : string
            The command to execute as a valid command line expression.
        stdin : file or None
            The stdin stream for this job
        
        Returns
        -------
        :py:class:`Job`
            The Job object representing this job.
        """
        with self.lock:
            job = Job(command, stdin, stdout, stderr)
            self.job_queue.append(job)
            self.job_available.set()
            
            self.queue_changed.set()
            
            
            self._spawn_nodes_if_required()
            return job
    
    def wait(self):
        """Wait for something to change.
        
        This method will block until any of the following events occur:
        
        * No nodes or no running or idle jobs are present
        * A job starts, completes or fails
        * A node starts or fails
        
        Only one thread may call this method at any time.
        
        Returns
        -------
        bool
            Returns True if the cluster is still running, False if all jobs are
            complete or no working nodes are available.
            
            Note: this may return True once when all jobs are finished or no
            nodes remain.
        """
        with self.lock:
            # If no nodes remain or no jobs remain, return immediately.
            if ((not self.unused_hostnames
                     and not self.idle_nodes
                     and not self.active_nodes) or
                    (not self.job_queue and not self.active_nodes)):
                return False
        
        # Otherwise, since at least one remaining job exists, *something* will
        # eventually set the queue_changed event.
        self.queue_changed.wait()
        self.queue_changed.clear()
        
        return True
    
    @property
    def num_queued_jobs(self):
        """The number of jobs waiting for a node to run on."""
        with self.lock:
            return len(self.job_queue)
    
    @property
    def num_running_jobs(self):
        """The number of jobs actually running."""
        with self.lock:
            return len(self.active_nodes)
    
    @property
    def num_active_nodes(self):
        """The number of hosts running jobs.
        
        The this method is same as num_running_jobs and here for consistency.
        """
        with self.lock:
            return len(self.active_nodes)
    
    @property
    def num_idle_nodes(self):
        """The number of hosts which are currently sitting idle."""
        with self.lock:
            return len(self.idle_nodes) + len(self.unused_hostnames)
    
    def _spawn_nodes_if_required(self):
        """Internal method.
        
        Spawns new nodes if there are more jobs in the queue than idle nodes
        (and the maximum number of nodes hasn't been reached).
        """
        with self.lock:
            # Start nodes if we're not over the maximum number allowed and there
            # are more jobs in the queue than idle nodes to process them.
            while ((self.max_nodes is None or
                    len(self.idle_nodes) + len(self.active_nodes) < self.max_nodes) and
                   len(self.job_queue) > len(self.idle_nodes) and
                   self.unused_hostnames):
                hostname = self.unused_hostnames.pop()
                n = Node(self, hostname, self.max_node_uses)
                self.idle_nodes.add(n)
                
                # The node is executed as a daemon thread so that it gets
                # terminated automatically when the main thread exits.
                n.daemon = True
                n.start()
            
            self.queue_changed.set()
    
    
    def _get_job(self, node):
        """To be called by Nodes. Get a job to execute.
        
        This method blocks until a new job is available for execution. When a
        job is available, it is returned. The Node must later call either
        :py:meth:`_job_finished` or :py:meth:`_node_failed` depending on the
        outcome.
        """
        while True:
            # Try and get a job now...
            with self.lock:
                if self.job_queue:
                    job = self.job_queue.popleft()
                    
                    # If we emptied the queue, clear the flag
                    if not self.job_queue:
                        self.job_available.clear()
                    
                    job.attempts += 1
                    
                    # Assign the job to the node
                    self.idle_nodes.remove(node)
                    self.active_nodes[node] = job
                    
                    self.queue_changed.set()
                    
                    return job
            
            # If no job was found, go to sleep until one becomes available and
            # try again...
            self.job_available.wait()
    
    def _job_finished(self, node):
        """Called by nodes when a job completes successfully."""
        with self.lock:
            job = self.active_nodes.pop(node)
            job.success = True
            self.idle_nodes.add(node)
            self.queue_changed.set()
    
    def _node_failed(self, node):
        """Called by nodes when the node has failed for some reason.
        
        Any jobs running on the node are re-tried. The node is then expected to
        shut itself down.
        """
        with self.lock:
            if node in self.idle_nodes:
                # The node failed while idle, just remove it from the list of
                # nodes
                self.idle_nodes.remove(node)
            else:
                # The node failed while running a job, reschedule it
                job = self.active_nodes.pop(node)
                self.job_queue.append(job)
                self.job_available.set()
            
            self.queue_changed.set()
            self._spawn_nodes_if_required()


def main():
    parser = argparse.ArgumentParser(
        description="A quick'n'dirty cluster manager.")
    
    parser.add_argument("--version", "-V", action="version",
                        version="%(prog)s v" + str(__version__))
    
    node_specs = parser.add_argument_group(
        title="node specification arguments",
        description="Arguments which specify the hostnames of the set of "
                    "computational nodes to run jobs on. List a hostname N "
                    "times to use up to N threads on that host.")
    node_specs.add_argument("--node", "-n",
                            metavar="HOSTNAME", type=str, nargs="+",
                            action="append", default=[],
                            help="hostnames of cluster nodes to use")
    node_specs.add_argument("--node-file", "-N",
                            metavar="FILE", type=str, nargs="+",
                            action="append", default=[],
                            help="file containing a list of hostnames, "
                                 "one per line")
    node_specs.add_argument("--max-nodes", "-m",
                            metavar="N", type=int, nargs=1, default=0,
                            help="the maximum number of nodes which may be "
                                 "used simultaneously, if 0 then unlimited "
                                 "(default=0)")
    node_specs.add_argument("--max-node-uses", "-u",
                            metavar="N", type=int, nargs=1, default=0,
                            help="the maximum number of jobs a node may "
                                 "execute before being retired, if 0 then "
                                 "unlimited (default=0)")
    
    job_specification = parser.add_argument_group(
        title="job specification arguments",
        description="Definition of the jobs to execute on the cluster. A "
                    "job is a valid command-line snippet "
                    "which will be executed via SSH on a compute node. If a "
                    "job exits with a non-zero status (it fails), it will be "
                    "retried on another node and the node it failed on will "
                    "removed from the pool of available nodes.")
    job_specification.add_argument("--job", "-j",
                                   metavar="COMMAND", type=str, nargs=1,
                                   action="append", default=[],
                                   help="specify a single command to execute")
    job_specification.add_argument("--job-file", "-J",
                                   metavar="FILE", type=str, nargs="+",
                                   action="append", default=[],
                                   help="specify a file containing jobs to "
                                        "execute, one per line (jobs listed "
                                        "in files will be queued after jobs "
                                        "listed with --job)")
    job_specification.add_argument("--on-each-node", "-E",
                                   metavar="COMMAND", type=str, nargs=1,
                                   help="execute this command once on every "
                                        "node")
    
    output_redirects = parser.add_argument_group(
        title="output redirection arguments",
        description="Files into which job outputs will be written. By "
                    "default this is stdout, if an option is given "
                    "without an argument, /dev/null is used, otherwise the "
                    "supplied file is used.")
    output_redirects.add_argument("--stdout", "-o",
                                  metavar="FILE", type=str, nargs='?',
                                  default=None, const="/dev/null",
                                  help="standard output file")
    output_redirects.add_argument("--stderr", "-e",
                                  metavar="FILE", type=str, nargs='?',
                                  default=None, const="/dev/null",
                                  help="standard error file")
    
    monitoring = parser.add_argument_group(
        title="status monitoring arguments",
        description="Controls optional status-monitoring features which allow "
                    "progress to be monitored.")
    monitoring.add_argument("--verbose", "-v", action="count", default=0,
                            help="show detailed execution status during "
                                 "the run (may be given multiple times)")
    monitoring.add_argument("--status", "-s", action="store_true",
                            help="show a summary of the status of the cluster "
                                 "in the terminal as the cluster runs")
    monitoring.add_argument("--active-nodes", "-A", type=str, nargs=1,
                            metavar="FILE",
                            help="maintain a list of which nodes are "
                                 "currently executing which jobs in the file "
                                 "supplied")
    
    args = parser.parse_args()
    
    m = Manager(max_nodes=args.max_nodes or None,
                max_node_uses=((args.max_node_uses or None)
                               if args.on_each_node is None else 1))
    
    job_stdout = None
    job_stderr = None
    try:
        # Open stdout/stderr files as requested
        if args.stdout is not None:
            job_stdout = open(args.stdout, "w")
        if args.stderr is not None:
            job_stderr = open(args.stderr, "w")
        
        # Add all nodes to the cluster
        some_nodes_specified = False
        for hostnames in args.node:
            for hostname in hostnames:
                m.add_node(hostname)
                some_nodes_specified = True
        for filenames in args.node_file:
            for filename in filenames:
                with open(filename, "r") as f:
                    for line in f:
                        # Skip blank lines
                        line = line.strip()
                        if line:
                            m.add_node(line)
                            some_nodes_specified = True
        
        if not some_nodes_specified:
            parser.error("At least one compute node must be specified "
                         "(see --node or --node-file)")
        
        node_count_digits = (int(math.log(m.num_idle_nodes, 10)) + 1
                             if m.num_idle_nodes > 0 else 1)
        
        # Add all jobs to the cluster
        if (args.job or args.job_file) and args.on_each_node:
            parser.error("--on-each-node may not be used with "
                         "--job or --job-file.")
        num_jobs = 0
        if args.on_each_node:
            # Add one job per node so that all nodes are sure to run the job
            # (note that max_node_uses is set to one when args.on_each_node is
            # used).
            for _ in range(m.num_idle_nodes):
                num_jobs += 1
                m.add_job(args.on_each_node[0],
                          stdin=None,
                          stdout=job_stdout,
                          stderr=job_stderr)
        else:
            # Add all the jobs supplied
            for jobs in args.job:
                for job in jobs:
                    num_jobs += 1
                    m.add_job(job,
                              stdin=None,
                              stdout=job_stdout,
                              stderr=job_stderr)
            for filenames in args.job_file:
                for filename in filenames:
                    with open(filename, "r") as f:
                        for job in f:
                            num_jobs += 1
                            m.add_job(job,
                                      stdin=None,
                                      stdout=job_stdout,
                                      stderr=job_stderr)
        if not some_nodes_specified:
            parser.error("At least one job must be specified "
                         "(see --job, --job-file or --on-each-node)")
        
        job_count_digits = int(math.log(num_jobs, 10)) + 1 if num_jobs > 0 else 1
        
        # Wait for all the jobs to finish
        with open("/dev/tty", "w") as tty:
            while True:
                # Display status, if required
                if args.status:
                    tty.write("\r\x1B[2K%*d of %*d jobs complete, "
                              "%*d of %*d nodes running jobs."%(
                        job_count_digits,
                        num_jobs - (m.num_running_jobs + m.num_queued_jobs),
                        job_count_digits,
                        num_jobs,
                        node_count_digits,
                        m.num_active_nodes,
                        node_count_digits,
                        m.num_idle_nodes + m.num_active_nodes,
                    ))
                    tty.flush()
                
                # Dump the list of active nodes, if required
                if args.active_nodes is not None:
                    with open(args.active_nodes[0], "w") as f:
                        with m.lock:
                            for node, job in m.active_nodes.items():
                                f.write("%s: %s\n"%(node.hostname, job))
                
                # Wait for the next change in state of the cluster manager
                if not m.wait():
                    break
            
            if args.status:
                tty.write("\n")
        
        # Exit
        if m.num_queued_jobs == 0:
            # All jobs completed successfuly!
            return 0
        else:
            # Some jobs didn't run
            if args.verbose > 1:
                for job in m.job_queue:
                    sys.stderr.write("ERROR: Job not executed: %s\n"%(
                        job.command
                    ))
            if args.verbose > 0:
                sys.stderr.write("%d of %d jobs were not completed successfuly\n"%(
                    m.num_queued_jobs, num_jobs
                ))
            return 1
    finally:
        # Close stdout/error files supplied
        if job_stdout is not None:
            job_stdout.close()
        if job_stderr is not None:
            job_stderr.close()


if __name__ == "__main__":
    sys.exit(main())
