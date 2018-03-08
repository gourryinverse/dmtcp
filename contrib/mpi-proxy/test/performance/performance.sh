#time mpirun -n 2 ./test/performance/send_only

#time mpirun -n 2 ./mpi_proxy ../../bin/dmtcp_launch -j --with-plugin $PWD/libdmtcp_mpi-proxy.so test/performance/send_only

time mpirun -n 2 ./test/performance/send_recv

time mpirun -n 2 ./mpi_proxy ../../bin/dmtcp_launch -j --with-plugin $PWD/libdmtcp_mpi-proxy.so ./test/performance/send_recv
