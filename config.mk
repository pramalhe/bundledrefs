## Note: as a convenience hack, this file is included in Makefiles,
##       AND in bash scripts, to set some variables.
##       So, don't write anything that isn't valid in both make AND bash.

## Set the desired maximum thread count (maxthreads),
## an upper bound on the maximum thread count that is a power of 2 (maxthreads_powerof2),
## the number of threads to increment by in the graphs produced by experiments (threadincrement),
## and the CPU frequency in GHz (cpu_freq_ghz) used for timing measurements with RDTSC.
## Be sure that maxthreads_powerof2 is based on maxthreads + 1 to ensure that the bundle
## entry reclamation thread is included in the calculation.
## Then, configure the thread pinning/binding policy (see README.txt.old)
## Blank means no thread pinning. (Threads can run wherever they want.)

# maxthreads=64
# maxthreads_powerof2=128
# threadincrement=16
# cpu_freq_ghz=1.2
# pinning_policy="-bind 0-7,16-23,8-15,24-31"

## The following was used for our experiments.
#maxthreads=192
#maxthreads_powerof2=256
#threadincrement=24
maxthreads=64
maxthreads_powerof2=128
threadincrement=16
cpu_freq_ghz=2.1
pinning_policy=""
