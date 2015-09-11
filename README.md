`clstr`: A quick'n'dirty cluster manager.
=========================================

`clstr` is a simple program which spawns jobs in parallel on remote machines.
It is similar in spirit to [GNU
Parallel](https://www.gnu.org/software/parallel/) when primarily using it for
it's `ssh-hosts` feature but much less general in scope. The reason it exists
at all is that GNU Parallel becomes very flaky (and also quite slow) when
dealing with even a modest number of remote hosts (low tens). `clstr` targets
the truly quick'n'dirty clusters which simply consist of a load of (idle...)
desktop machines with SSH access on the order of low hundreds of nodes.

