# Programming Homework 2: A Tour of Apache Spark

## Part 1: Spark and Spark SQL

### Task1

- question 1

The default block size on HDFS is 128 MB. The default replication factor of HDFS is 2.

### Task2

- question 2


<img src="p1t2/q2.png" alt="q2" style="zoom:50%;" />

The completion time is 6 min 30 sec.

- question 3


<img src="p1t2/q3.png" alt="q3" style="zoom:50%;" />

The completion time is 4 min 12 sec, which is much shorter than running on a single node with 4 course. The performance gets better becuase it uses 2 worker nodes in which they distribute the workload and run faster.

- question 4


<img src="p1t2/q4.png" alt="q4" style="zoom:50%;" />

The completion time is 4 min 15 sec, which is very similar to the one under the default block size in HDFS. 

- question 5


<img src="p1t2/Q5_1.png" alt="Q5_1" style="zoom:50%;" />

<img src="p1t2/Q5_2.png" alt="Q5_2" style="zoom:50%;" />

The job still finishes, but the completion time gets longer. Since one of the worker nodes was killed, there is now only one worker node running on the program, so the job is still processing but it runs slower.

- question 6


<img src="p1t2/Q6.png" alt="Q6" style="zoom:50%;" />

The completion time is similar, because the loss of data is trivial for this program.

- question 7

<img src="p1t2/Q7.png" alt="Q7" style="zoom:50%;" />

The completion time is similar.

### Task3

- question 8

<img src="p1t3/Q8.png" alt="Q8" style="zoom:50%;" />

The completion time is 55 min 4 sec.

## Part 2: Spark Streamming

