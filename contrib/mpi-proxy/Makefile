# To demonstrate, do:  make check    [Checkpoints every 5 seconds]

MPICC = mpic++

# The name will be the same as the current directory name.
NAME=${shell basename $$PWD}

# By default, your resulting plugin library will have this name.
LIBNAME=libdmtcp_${NAME}

# As you add new files to your plugin library, add the object file names here.
LIBOBJS = mpi_plugin.o

# Modify if your DMTCP_ROOT is located elsewhere.
ifndef DMTCP_ROOT
  DMTCP_ROOT=../..
endif
DMTCP_INCLUDE=${DMTCP_ROOT}/include
JALIB_INCLUDE=${DMTCP_ROOT}/jalib

override CFLAGS += -fPIC -I${DMTCP_INCLUDE}
override CXXFLAGS += -fPIC -I${DMTCP_INCLUDE} -I${JALIB_INCLUDE}
LINK = ${MPICC}

DEMO_PORT=7781

default: ${LIBNAME}.so mpi_proxy

tests:
	cd test/hello; make; cd ../../
	cd test/send_recv; make; cd ../../
	cd test/performance; make; cd ../../

check: ${LIBNAME}.so tests mpi_proxy
	# Kill an old coordinator on this port if present, just in case.
	@ ${DMTCP_ROOT}/bin/dmtcp_command --quit --quiet \
	  --coord-port ${DEMO_PORT} 2>/dev/null || true
	mpirun -n 1 ./mpi_proxy ${DMTCP_ROOT}/bin/dmtcp_launch -i 4 -j \
	  --with-plugin $$PWD/${LIBNAME}.so test/hello/mpi_hello_world
	mpirun -n 1 ./mpi_proxy ${DMTCP_ROOT}/bin/dmtcp_restart -j \
	  ckpt_rank_0/ckpt_*.dmtcp

mpi_proxy.o: mpi_proxy.cpp
	${MPICC} -I${DMTCP_INCLUDE} -c -g3 -O0 -o $@ $<

mpi_proxy: mpi_proxy.o
	${MPICC} -g3 -O0 -o $@ $<

${LIBNAME}.so: ${LIBOBJS}
	${LINK} -shared -fPIC -g3 -O0 -o $@ $^

.c.o:
	${CC} ${CFLAGS} -g3 -O0 -c -o $@ $<

.cpp.o:
	${CXX} ${CXXFLAGS} -g3 -O0 -c -o $@ $<

tidy:
	rm -f *~ .*.swp dmtcp_restart_script*.sh ckpt_*.dmtcp

clean: tidy
	rm -f ${LIBOBJS} ${LIBNAME}.so
	rm -f mpi_proxy mpi_proxy.o
	rm -rf ckpt_rank_*
	cd test/hello; make clean; cd ../../
	cd test/send_recv; make clean; cd ../../

distclean: clean
	rm -f ${LIBNAME}.so *~ .*.swp dmtcp_restart_script*.sh ckpt_*.dmtcp

dist: distclean
	dir=`basename $$PWD`; cd ..; \
	  tar czvf $$dir.tar.gz --exclude-vcs ./$$dir
	dir=`basename $$PWD`; ls -l ../$$dir.tar.gz

.PHONY: default clean dist distclean
