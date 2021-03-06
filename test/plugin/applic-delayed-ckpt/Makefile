# To demonstrate, do:  make check    [Checkpoints every 5 seconds]

# The name will be the same as the current directory name.
NAME=${shell basename $$PWD}

# By default, your resulting plugin library will have this name.
LIBNAME=libdmtcp_plugin-to-announce-events

# As you add new files to your plugin library, add the object file names here.
LIBOBJS = plugin-to-announce-events.o

# Modify if your DMTCP_ROOT is located elsewhere.
ifndef DMTCP_ROOT
  DMTCP_ROOT=../../..
endif
DMTCP_INCLUDE=${DMTCP_ROOT}/include

override CFLAGS += -fPIC -I${DMTCP_INCLUDE}
override CXXFLAGS += -fPIC -I${DMTCP_INCLUDE}
LINK = ${CC}

DEMO_PORT=7781

default: ${LIBNAME}.so applic

# NOTE:  ${CFLAGS} expands to invoke '-fPIC -I${DMTCP_INCLUDE}'
#        This is required for use with DMTCP.
applic: applic.c
	${CC} ${CFLAGS} -o $@ $< -ldl

check: ${LIBNAME}.so applic
	@ echo ""
	@ echo "============ TESTING ./applic _WITHOUT_ DMTCP ================"
	- ./applic
	@ echo '(Application has exited.)'
	@ echo ""
	@ sleep 3
	@ echo "============ TESTING ./applic WITH DMTCP ================="
	# Kill an old coordinator on this port if present, just in case.
	@ ${DMTCP_ROOT}/bin/dmtcp_command --quit --quiet \
	  --coord-port ${DEMO_PORT} 2>/dev/null || true
	# --interval 2 flag will write checkpoint every two seconds.
	${DMTCP_ROOT}/bin/dmtcp_launch --quiet --coord-port ${DEMO_PORT} \
	  --interval 2 --with-plugin $$PWD/${LIBNAME}.so ./applic &
	@ sleep 7
	@ echo ""
	@ ${DMTCP_ROOT}/bin/dmtcp_command --coord-port ${DEMO_PORT} -s || echo "NO COORDINATOR"
	@ echo "===== RESTARTING USING ./dmtcp_restart_script.sh ============="
	./dmtcp_restart_script.sh
	@ echo ""
	@ echo "ALL TESTS SUCCEEDED"

${LIBNAME}.so: ${LIBOBJS}
	${LINK} -shared -fPIC -o $@ $^

.c.o:
	${CC} ${CFLAGS} -c -o $@ $<
.cpp.o:
	${CXX} ${CXXFLAGS} -c -o $@ $<

tidy:
	rm -f *~ .*.swp dmtcp_restart_script*.sh ckpt_*.dmtcp

clean: tidy
	rm -f ${LIBOBJS} ${LIBNAME}.so applic

distclean: clean
	rm -f ${LIBNAME}.so *~ .*.swp dmtcp_restart_script*.sh ckpt_*.dmtcp

dist: distclean
	dir=`basename $$PWD`; cd ..; \
	  tar czvf $$dir.tar.gz --exclude-vcs ./$$dir
	dir=`basename $$PWD`; ls -l ../$$dir.tar.gz

.PHONY: default clean dist distclean
