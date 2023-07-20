Client code for Arboreal
=====

Refer to https://github.com/pfouto/edge-tree/ for details about Arboreal.

This repository contains the client code to benchmark Arboreal. It is based on the code
of YCSB (https://github.com/brianfrankcooper/YCSB).

The most important files are the client drivers:
- **EdgeClient.java**: contains a simple YCSB driver that interacts with
the ClientProxy of Arboreal to perform operations. Supports different persistence levels.
- **EdgeMigratingClient.java**: A variation of the client that supports migrating to a new replica
when the current one fails. Used for the fault tolerance experiments on the paper.
- **EdgeMobilityClient.java**: A variation of the client that receives a list of edge nodes and timestamp,
migrating to a new node at each timestamp. Used for the mobility experiments on the paper.

And the workloads:
- **site.ycsb.workloads.EdgeFixedWorkload**: The workload used for most of the experiments of the paper. Each client
receives a list of data partitions, and accesses objects randomly from these partitions.
- **site.ycsb.workloads.EdgeMobilityWorkload**: The workload used for the mobility experiments of the paper. Each client
receives a list of local partitions, remote partitions, and the intervals at which it should switch between
executing operations on local and remote partitions.

The scripts to run the
experiments in the paper, along with simple instructions to run Arboreal, can be found in https://github.com/pfouto/edge-exps.