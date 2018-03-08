/* CopyLeft Gregory Price (2017) */

#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <mpi.h>
#include <map>
#include <vector>

#include "mpi_proxy.h"
#include "protectedfds.h"
#include "dmtcp.h"
#include "util.h"

// #define DEBUG_PRINTS

int g_commrank = 0;
int listfd = 0;

int serial_printf(const char * msg)
{
#ifdef DEBUG_PRINTS
  printf("%s\n", msg);
  fflush(stdout);
#endif
}

int MPIProxy_Receive_Arg_Int(int connfd)
{
  int retval;
  int status;
  status = read(connfd, &retval, sizeof(int));
  // TODO: error check
  return retval;
}

int MPIProxy_Send_Arg_Int(int connfd, int arg)
{
  int status = write(connfd, &arg, sizeof(int));
  // TODO: error check
  return status;
}

int MPIProxy_Send_Arg_Buf(int connfd, void *buf, int size)
{
  int status = write(connfd, buf, size);
  // TODO: error check
  return status;
}

void MPIProxy_Return_Answer(int connfd, int answer)
{
#ifdef DEBUG_PRINTS
  printf("Returned %08x\n", answer);
  fflush(stdout);
#endif
  write(connfd, &answer, 4);
  return;
}

void MPIProxy_Init(int connfd)
{
  // TODO: Get argc and argv
  serial_printf("PROXY: MPI_Init - ");
  MPIProxy_Return_Answer(connfd, MPI_Init(NULL, NULL));
}

void MPIProxy_Get_CommSize(int connfd)
{
  int group = 0;
  int commsize = 0;
  int retval = 0;
  group = MPIProxy_Receive_Arg_Int(connfd);
  retval = MPI_Comm_size(group, &commsize);
  MPIProxy_Return_Answer(connfd, retval);
  if (retval == MPI_SUCCESS)
    MPIProxy_Send_Arg_Int(connfd, commsize);
}

void MPIProxy_Get_CommRank(int connfd)
{
  int group = 0;
  int commrank = 0;
  int retval = 0;

  group = MPIProxy_Receive_Arg_Int(connfd);
  retval = MPI_Comm_rank(group, &commrank);
  g_commrank = commrank;
  MPIProxy_Return_Answer(connfd, retval);
  if (retval == MPI_SUCCESS)
    MPIProxy_Send_Arg_Int(connfd, commrank);
}

void MPIProxy_Type_size(int connfd)
{
  int retval = 0;
  int size = 0;
  MPI_Datatype datatype;
  datatype = (MPI_Datatype) MPIProxy_Receive_Arg_Int(connfd);

  // Do the actual Type_size call
  retval = MPI_Type_size(datatype, &size);

  MPIProxy_Return_Answer(connfd, retval);
  if (retval == MPI_SUCCESS)
    MPIProxy_Send_Arg_Int(connfd, size);
}


std::vector<MPI_Request *>grequest_queue;

void MPIProxy_Send(int connfd)
{
  int retval = 0;
  void * buf = NULL;
  int size = 0;
  int count, dest, tag;
  int msgid;  // used to verify it was sent
  int numread = 0;
  MPI_Datatype datatype;
  MPI_Comm comm;
  MPI_Request request;;
  memset(&request, 0x0, sizeof(MPI_Request));

  // Collect the arguments
  size = MPIProxy_Receive_Arg_Int(connfd);

  // Buffer read
  buf = malloc(size);
  while (numread < size)
    numread += read(connfd, ((char *)buf)+numread, size-numread);

  // rest of the arguments
  count = MPIProxy_Receive_Arg_Int(connfd);
  datatype = (MPI_Datatype) MPIProxy_Receive_Arg_Int(connfd);
  dest = MPIProxy_Receive_Arg_Int(connfd);
  tag = MPIProxy_Receive_Arg_Int(connfd);
  comm = (MPI_Comm) MPIProxy_Receive_Arg_Int(connfd);

  // Do the send
  // FIXME: do i need to keep track of these for any reason?
  retval = MPI_Isend(buf, count, datatype, dest, tag, comm, &request);

  if (retval != MPI_SUCCESS)
  {
    printf("Proxy - SEND FAILED\n");
    fflush(stdout);
  }

  free(buf); // TODO: Should I free here?
  MPIProxy_Return_Answer(connfd, retval);
}

void MPIProxy_Recv(int connfd)
{
  int status = 0;
  void * buf;
  int size = 0;
  int count, source, tag;
  MPI_Datatype datatype;
  MPI_Comm comm;
  int mpi_status_arg;
  MPI_Status *mpi_status;

  // Collect the arguments
  count = MPIProxy_Receive_Arg_Int(connfd);
  datatype = (MPI_Datatype) MPIProxy_Receive_Arg_Int(connfd);
  source = MPIProxy_Receive_Arg_Int(connfd);
  tag = MPIProxy_Receive_Arg_Int(connfd);
  comm = (MPI_Comm) MPIProxy_Receive_Arg_Int(connfd);

  mpi_status_arg = MPIProxy_Receive_Arg_Int(connfd);
  if (mpi_status_arg == 0xFFFFFFFF)
  {
    mpi_status = MPI_STATUS_IGNORE;
  }
  else
  {
    // TODO: ????
    // FIXME
    printf("NOT IGNORE STATUS!!!\n");
    fflush(stdout);
    mpi_status = NULL;
  }

  // Do the receive
  MPI_Type_size(datatype, &size);
  size = count * size;
  buf = malloc(size);

  // TODO: Check that mpi_status is correctly used here
  serial_printf("PROXY: RECIEVING - ");
  status = MPI_Recv(buf, count, datatype, source, tag, comm, mpi_status);
  serial_printf("\tDONE");

  // Return receive's status
  MPIProxy_Return_Answer(connfd, status);
  if (status == MPI_SUCCESS)
  {
    MPIProxy_Send_Arg_Buf(connfd, buf, size);
    // TODO: Check that mpi_status is correctly used here
    if (mpi_status != MPI_STATUS_IGNORE)
      MPIProxy_Send_Arg_Buf(connfd, &mpi_status, sizeof(mpi_status));
  }

  free(buf);
}


void MPIProxy_Iprobe(int connfd)
{
  int status = 0;
  int source = 0;
  int tag = 0;
  MPI_Comm comm;
  int flag = 0;
  MPI_Status mpi_status;

  // collect the arguments
  source = MPIProxy_Receive_Arg_Int(connfd);
  tag = MPIProxy_Receive_Arg_Int(connfd);
  comm = (MPI_Comm) MPIProxy_Receive_Arg_Int(connfd);

  // do the Iprobe
  status = MPI_Iprobe(source, tag, comm, &flag, &mpi_status);

  // return the info
  // Return receive's status
  MPIProxy_Return_Answer(connfd, status);
  if (status == MPI_SUCCESS)
  {
    MPIProxy_Send_Arg_Int(connfd, flag);
    MPIProxy_Send_Arg_Buf(connfd, &mpi_status, sizeof(mpi_status));
  }
}

void MPIProxy_Get_count(int connfd)
{
  int retval = 0;
  int count = 0;
  MPI_Status status;
  MPI_Datatype datatype;

  // Get the MPI_Status and Datatype
  read(connfd, &status, sizeof(MPI_Status));
  datatype = (MPI_Datatype) MPIProxy_Receive_Arg_Int(connfd);

  // Do the Get_count
  retval = MPI_Get_count(&status, datatype, &count);
  MPIProxy_Return_Answer(connfd, retval);
  MPIProxy_Send_Arg_Int(connfd, count);
}

void MPIProxy_Finalize(int connfd)
{
  serial_printf("PROXY: MPI_Finalize - ");
  MPIProxy_Return_Answer(connfd, MPI_Finalize());
}


void proxy(int connfd)
{
  int init = 0;
  int cmd = 0;
  while (1) {
    cmd = 0;
    int rc = read(connfd, &cmd, sizeof(cmd));
    if (rc < 0) {
      perror("PROXY: read");
      continue;
    }
    switch (cmd) {
    case MPIProxy_Cmd_Init:
      serial_printf("PROXY(INIT) ");
      MPIProxy_Init(connfd);
      break;
    case MPIProxy_Cmd_Get_CommSize:
      serial_printf("PROXY(Get_CommSize) - ");
      MPIProxy_Get_CommSize(connfd);
      break;
    case MPIProxy_Cmd_Get_CommRank:
      serial_printf("PROXY(Get_CommRank) - ");
      MPIProxy_Get_CommRank(connfd);
      break;
    case MPIProxy_Cmd_Send:
      serial_printf("PROXY(Send) - ");
      MPIProxy_Send(connfd);
      break;
    case MPIProxy_Cmd_Recv:
      serial_printf("PROXY(Recv) - ");
      MPIProxy_Recv(connfd);
      break;
    case MPIProxy_Cmd_Type_size:
      serial_printf("PROXY(Type_size) - ");
      MPIProxy_Type_size(connfd);
      break;
    case MPIProxy_Cmd_Iprobe:
      serial_printf("PROXY(Iprobe) - ");
      MPIProxy_Iprobe(connfd);
      break;
    case MPIProxy_Cmd_Get_count:
      serial_printf("PROXY(Get_count)");
      MPIProxy_Get_count(connfd);
      break;
    case MPIProxy_Cmd_Finalize:
      serial_printf("PROXY(Finalize)");
      MPIProxy_Finalize(connfd);
      break;
    case MPIProxy_Cmd_Shutdown_Proxy:
      serial_printf("PROXY: Shutdown - ");
      MPIProxy_Return_Answer(connfd, 0);
      goto DONE;
    default:
      printf("PROXY: Unknown Command: %d. Exiting.\n", cmd);
      goto DONE;
      break;
    }
  }
DONE:
  return;
}

void launch_or_restart(pid_t pid, int rank, int argc, char *argv[])
{
  int i = 0;
  if (pid == 0) {
    // child:
    if (strstr(argv[1], "dmtcp_launch")) {
      // go ahead and exec into provided arglist
      serial_printf("Starting");
      std::vector<char*> s;
#ifdef DEBUGGING
      s.push_back(const_cast<char*>("xterm"));
      s.push_back(const_cast<char*>("-e"));
      s.push_back(const_cast<char*>("gdb"));
      s.push_back(const_cast<char*>("--args"));
#endif
      for (int i = 1; i < argc; i++) {
        s.push_back(argv[i]);
      }
      execvp(s[0], &s[0]);
    } else if (strstr(argv[1], "dmtcp_restart")) {
      serial_printf("Restarting");
      // TODO: Select correct image from arglist, re-form arglist
      char * newargv[5] = {NULL, NULL, NULL, NULL, NULL};
      newargv[0] = argv[1];
      newargv[1] = argv[2];
      newargv[2] = argv[3 + rank];
      // newargv[2] = argv[3];
      // newargv[3] = argv[4+rank];
      execvp(newargv[0], (char* const*) &newargv);
    } else {
      printf("ERROR - NOT A LAUNCH OR RESUME\n");
    }
    exit(1);
  }
  return;
}

int main(int argc, char *argv[])
{
  // 0 is read
  // 1 is write
  int debugPipe[2];
  int rank = 0;
  int restart_rank = -1;

  socketpair(AF_UNIX, SOCK_STREAM, 0, debugPipe);

  pid_t pid = fork();
  if (pid > 0) {
    int status;
    if (strstr(argv[1], "dmtcp_restart")) {
      MPI_Init(NULL, NULL);
      MPI_Comm_rank(MPI_COMM_WORLD, &rank);
      write(debugPipe[0], &rank, sizeof(int));
      // FIXME: wait a second to let child get the rank
      sleep(1);
    }
    proxy(debugPipe[0]);
    waitpid(pid, &status, 0);
  } else if (pid == 0) {
    assert(dup2(debugPipe[1], PROTECTED_MPI_PROXY_FD) ==
           PROTECTED_MPI_PROXY_FD);
    close(debugPipe[1]);
    if (strstr(argv[1], "dmtcp_restart")) {
      read(PROTECTED_MPI_PROXY_FD, &restart_rank, sizeof(int));
      assert(restart_rank != -1);
    }
    launch_or_restart(pid, restart_rank, argc, argv);
  } else {
    assert(0);
  }

  return 0;
}
