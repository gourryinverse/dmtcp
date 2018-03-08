EXECS=send_recv send_recv_many ping_pong ring typesize
MPICC?=mpicc

all: ${EXECS}

send_recv: send_recv.c
	${MPICC} -o send_recv send_recv.c

send_recv_many: send_recv_many.c
	${MPICC} -o send_recv_many send_recv_many.c

ping_pong: ping_pong.c
	${MPICC} -o ping_pong ping_pong.c

ring: ring.c
	${MPICC} -o ring ring.c

typesize: typesize.c
	${MPICC} -o typesize typesize.c

typesize: many_performance.c
	${MPICC} -o many_performance many_performance.c

clean:
	rm -f ${EXECS}
