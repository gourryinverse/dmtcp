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
#include <sys/types.h>
#include <fcntl.h>
#include <mpi.h>
#include <vector>
#include <map>

#include "mpi_proxy.h"
#include "config.h"
#include "dmtcp.h"
#include "jassert.h"
#include "jfilesystem.h"
#include "protectedfds.h"

// #define DEBUG

int gworld_rank = 0;
int gworld_size = 0;

int gworld_sent = 0;
int gworld_recv = 0;
int glocal_sent = 0;
int glocal_recv = 0;

typedef struct Message
{
  void * buf;
  int count;
  MPI_Datatype datatype;
  MPI_Comm comm;
  MPI_Status status;
  int size;
} Message;

std::vector<Message *>gmessage_queue;

#define MPI_PLUGIN_PROXY_PACKET_WAITING 0
#define MPI_PLUGIN_BUFFERED_PACKET_WAITING 1
#define MPI_PLUGIN_NO_PACKET_WAITING 2

void mpi_proxy_wait_for_instructions();

int Receive_Int_From_Proxy(int connfd)
{
    int retval;
    int status;
    status = read(connfd, &retval, sizeof(int));
    // TODO: error check
    return retval;
}

bool g_restart_receive;
bool g_restart_retval;
int Complete_Blocking_Call_Safely(int connfd)
{
  int flags = 0;
  int status = EWOULDBLOCK;
  int retval = 0;
  while (status == EWOULDBLOCK && !g_restart_receive)
  {
    DMTCP_PLUGIN_DISABLE_CKPT();
    if (g_restart_receive)
    {
      DMTCP_PLUGIN_ENABLE_CKPT();
      break;
    }
    flags = fcntl(connfd, F_GETFL, 0);
    fcntl(connfd, F_SETFL, flags | O_NONBLOCK);
    status = read(connfd, &retval, sizeof(int));
    fcntl(connfd, F_SETFL, flags);
    DMTCP_PLUGIN_ENABLE_CKPT();
  }

  if (g_restart_receive)
    retval = g_restart_retval;
  return retval;
}

int Receive_Buf_From_Proxy(int connfd, void* buf, int size)
{
  int status = 0;
  int numread = 0;
  while (numread < size)
  {
    numread = read(connfd, ((char *)buf)+numread, size-numread);
  }
  return numread > 0;
}

int Send_Int_To_Proxy(int connfd, int arg)
{
    int status = write(connfd, &arg, sizeof(int));
    // TODO: error check
    return status;
}

int Send_Buf_To_Proxy(int connfd, const void* buf, int size)
{
    int status = write(connfd, buf, size);
    // TODO: error check
    return status;
}

int exec_proxy_cmd(int pcmd)
{
  int answer = 0;
  JTRACE("PLUGIN: Sending Proxy Command");
  if (write(PROTECTED_MPI_PROXY_FD, &pcmd, 4) < 0) {
    JNOTE("ERROR WRITING TO SOCKET")(JASSERT_ERRNO);
  }

  JTRACE("PLUGIN: Receiving Proxy Answer - ");
  if (read(PROTECTED_MPI_PROXY_FD, &answer, 4) < 0) {
    JTRACE("ERROR READING FROM SOCKET")(JASSERT_ERRNO);
  } else {
    JTRACE("Answer Received");
  }
  return answer;
}

void close_proxy(void)
{
  JTRACE("PLUGIN: Close Proxy Connection\n");
  exec_proxy_cmd(MPIProxy_Cmd_Shutdown_Proxy);
}


bool matching_buffered_packet(int source, int tag, MPI_Comm comm)
{
  std::vector<Message *>::iterator itt;
  for(itt = gmessage_queue.begin(); itt != gmessage_queue.end(); itt++)
  {
    Message * msg = *itt;
    if (((msg->status.MPI_SOURCE == source) | (source == MPI_ANY_SOURCE))
        && ((msg->status.MPI_TAG == tag) | (tag == MPI_ANY_TAG))
        && ((msg->comm == comm)))
    {
      return true;
    }
  }
  return false;
}

int mpi_plugin_is_packet_waiting(int source, int tag, MPI_Comm comm, int *flag,
                                  MPI_Status *mpi_status, int *wait_type)
{
  int status = 0;
  MPI_Status ignore;
  if (matching_buffered_packet(source, tag, comm))
  {
    *flag = true;
    *wait_type = MPI_PLUGIN_BUFFERED_PACKET_WAITING;
  }
  else
  {
    // MPI_Proxy_Iprobe
    // send command and arguments
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Iprobe);
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, source);
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, tag);
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, comm);

    // receive answer
    status = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
    if (status == 0)
    {
      *flag = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
      Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD, mpi_status,
                              sizeof(MPI_Status));
    }
    // Return the correct wait type (either proxy packet waiting, or no packet)
    if (*flag)
      *wait_type = MPI_PLUGIN_PROXY_PACKET_WAITING;
    else
      *wait_type = MPI_PLUGIN_NO_PACKET_WAITING;
  }

  return status;
}

int mpi_plugin_return_buffered_packet(void *buf, int count, int datatype,
                                      int source, int tag, MPI_Comm comm,
                                      MPI_Status *mpi_status, int size)
{
  int status = -1; // FIXME
  int cpysize;
  int element = 0;
  std::vector<Message *>::iterator itt;
  Message * msg;
  for(itt = gmessage_queue.begin(); itt != gmessage_queue.end(); itt++)
  {
    msg = *itt;
    if (((msg->status.MPI_SOURCE == source) | (source == MPI_ANY_SOURCE))
        && ((msg->status.MPI_TAG == tag) | (tag == MPI_ANY_TAG))
        && ((msg->comm == comm)))
    {
      break;
    }
    element++;
  }
  if (itt == gmessage_queue.end())
  {
    // this should never happen!
    return -1;
  }

  cpysize = (size < msg->size) ? size: msg->size;
  memcpy(buf, msg->buf, cpysize);
  gmessage_queue.erase(gmessage_queue.begin()+element);
  free(msg->buf);
  free(msg);
  // TODO: Actually handle MPI_Status that isn't MPI_STATUS_IGNORE

  return MPI_SUCCESS;
}


/* hello-world */
EXTERNC int
MPI_Init(int *argc, char ***argv)
{
  int status = 0;
  DMTCP_PLUGIN_DISABLE_CKPT();
  JTRACE("PLUGIN: MPI_Init!\n");
  status = exec_proxy_cmd(MPIProxy_Cmd_Init);
  DMTCP_PLUGIN_ENABLE_CKPT();
  // get our rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &gworld_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &gworld_size);
  return status;
}

EXTERNC int
MPI_Comm_size(int group, int *world_size)
{
  int status = 0;
  DMTCP_PLUGIN_DISABLE_CKPT();
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Get_CommSize);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, group);
  status = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
  if (!status) // success
    *world_size = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);

  DMTCP_PLUGIN_ENABLE_CKPT();
  return status;
}

EXTERNC int
MPI_Comm_rank(int group, int *world_rank)
{
  int status = 0;
  DMTCP_PLUGIN_DISABLE_CKPT();
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Get_CommRank);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, group);
  status = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
  if (status == MPI_SUCCESS) {
    *world_rank = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
    JTRACE("*** GOT RANK\n");
    gworld_rank = *world_rank;
  }
  DMTCP_PLUGIN_ENABLE_CKPT();
  return status;
}

EXTERNC int
MPI_Type_size(MPI_Datatype datatype, int *size)
{
  int status = 0;
  DMTCP_PLUGIN_DISABLE_CKPT();
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Type_size);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, datatype);

  // get the status
  status = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
  if (status == MPI_SUCCESS) // if successful, ge the size
    *size = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
  DMTCP_PLUGIN_ENABLE_CKPT();

  return status;
}

EXTERNC int
MPI_Get_count(const MPI_Status *status, MPI_Datatype datatype, int *count)
{
  DMTCP_PLUGIN_DISABLE_CKPT();
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Get_count);
  Send_Buf_To_Proxy(PROTECTED_MPI_PROXY_FD, &status, sizeof(MPI_Status));
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPI_BYTE);
  Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD); // status
  *count = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
  DMTCP_PLUGIN_ENABLE_CKPT();
}

// Blocking Call must be handled safely
bool g_pending_send;
EXTERNC int
MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag,
          MPI_Comm comm)
{
  int status = 0xFFFFFFFF;
  int size = 0;

  status = MPI_Type_size(datatype, &size);

  DMTCP_PLUGIN_DISABLE_CKPT();
  g_pending_send = true;
  g_restart_receive = false;
  g_restart_retval = 0;
  if (status == MPI_SUCCESS)
  {
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Send);

    // Buf part
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, count * size);
    write(PROTECTED_MPI_PROXY_FD, buf, count*size);

    // rest of stuff
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, count);
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, (int)datatype);
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, dest);
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, tag);
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, (int)comm);

    // Get the status
  }
  glocal_sent++;
  DMTCP_PLUGIN_ENABLE_CKPT();

  // Block *safely* until we receive the status back
  // this _Safely call handles the situation where a ckpt/restart
  // occurs during the process of waiting for the Recv to occur
  status = Complete_Blocking_Call_Safely(PROTECTED_MPI_PROXY_FD);

  return status;
}

std::map<MPI_Request*, bool> g_request_map;
std::map<MPI_Request*, MPI_Status*> g_test_status_map;

EXTERNC int
MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag,
          MPI_Comm comm, MPI_Request* request)
{
  int status = 0xFFFFFFFF;
  int size = 0;

  status = MPI_Type_size(datatype, &size);

  DMTCP_PLUGIN_DISABLE_CKPT();
  if (status == MPI_SUCCESS)
  {
    // TODO: Only need to do this if it's a stack pointer?
    // if this is a heap pointer, we can share the malloc'd page
    g_request_map[request] = false;
    g_test_status_map[request] = NULL;

    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Send);

    // Buf part
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, count * size);
    write(PROTECTED_MPI_PROXY_FD, buf, count*size);

    // rest of stuff
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, count);
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, (int)datatype);
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, dest);
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, tag);
    Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, (int)comm);
    Send_Buf_To_Proxy(PROTECTED_MPI_PROXY_FD, request, sizeof(MPI_Request));

    // Get the status
    status = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);

    // Get the Request Info
    Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD,
                            request,
                            sizeof(MPI_Request));
  }
  glocal_sent++;
  DMTCP_PLUGIN_ENABLE_CKPT();

  return status;
}

bool g_pending_wait;
MPI_Request* g_pending_wait_request;
MPI_Status* g_pending_wait_status;

// Blocking Call must be handled safely
EXTERNC int
MPI_Wait(MPI_Request* request, MPI_Status* status)
{
  int flags = 0;
  int sockstat = EWOULDBLOCK;
  int retval = 0;

  // Send Wait request
  DMTCP_PLUGIN_DISABLE_CKPT();
  g_pending_wait = true;
  g_restart_receive = false;
  g_restart_retval = 0;
  g_pending_wait_request = request;
  g_pending_wait_status = status;
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Wait);
  Send_Buf_To_Proxy(PROTECTED_MPI_PROXY_FD, request, sizeof(MPI_Request));
  Send_Buf_To_Proxy(PROTECTED_MPI_PROXY_FD, status, sizeof(MPI_Status));
  DMTCP_PLUGIN_ENABLE_CKPT();

  // Block *safely* until we receive the status back
  // handle the ckpt/restart case during this blocking call gracefully
  while (sockstat == EWOULDBLOCK && !g_restart_receive)
  {
    DMTCP_PLUGIN_DISABLE_CKPT();
    if (g_restart_receive)
    {
      DMTCP_PLUGIN_ENABLE_CKPT();
      break;
    }
    flags = fcntl(PROTECTED_MPI_PROXY_FD, F_GETFL, 0);
    fcntl(PROTECTED_MPI_PROXY_FD, F_SETFL, flags | O_NONBLOCK);
    sockstat = read(PROTECTED_MPI_PROXY_FD, &retval, sizeof(int));
    fcntl(PROTECTED_MPI_PROXY_FD, F_SETFL, flags);
    if (sockstat != EWOULDBLOCK)
    {
      // we waited successfully, get the rest
      // this must be done before re-enabling checkpoint
      Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD,
                              request,
                              sizeof(MPI_Request));
      Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD,
                              status,
                              sizeof(MPI_Status));

      // TODO: if this was a Wait for an Irecv, update the receive buffer
      // void * buf = g_irecv_buffers[request]
      // unsigned int bufsize = g_buffer_size[request]
      // Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD, buf, bufsize);
      // g_irecv_buffers.delete(request)
      // g_buffer_size.delete(request)
    }
    DMTCP_PLUGIN_ENABLE_CKPT();
  }

  if (g_restart_receive)
    retval = g_restart_retval;

  return retval;
}

/*
  FIXME: https://stackoverflow.com/questions/22410827/mpi-reuse-mpi-request

  It's just fine to reuse your MPI_Request objects as long as they're completed
  before you use them again (either by completing the request or freeing the
  request object manually using MPI_REQUEST_FREE).

  Variables of type MPI_Request are not request objects themselves but rather
  just opaque handles (something like an abstract pointer) to the real MPI
  request objects. Assigning a new value to such a variable in no way affects
  the MPI object and only breaks the association to it. Therefore the object
  might become inaccessible in the sense that if no handle to it exists in your
  program, it can no longer be passed to MPI calls. This is the same as losing a
  pointer to a dynamically allocated memory block, thus leaking it.

  When it comes to asynchronous request handles, once the operation is completed
  MPI destroys the request object and MPI_Wait* / MPI_Test* set the passed
  handle variable to MPI_REQUEST_NULL on return. Also, a call to
  MPI_Request_free will mark the request for deletion and set the handle to
  MPI_REQUEST_NULL on return. At that point you can reuse the variable and store
  a different request handle in it.

  The same applies to handles to communicators (of type MPI_Comm), handles to
  datatypes (of type MPI_Datatype), handles to reduce operations (of type
  MPI_Op), and so on.
*/

EXTERNC int
MPI_Iprobe(int source, int tag, MPI_Comm comm, int *flag,
            MPI_Status *mpi_status)
{
  int status = 0;
  int wait_type = 0;
  DMTCP_PLUGIN_DISABLE_CKPT();
  status = mpi_plugin_is_packet_waiting(source, tag, comm, flag,
                                        mpi_status, &wait_type);
  DMTCP_PLUGIN_ENABLE_CKPT();
  return status;
}

EXTERNC int
MPI_Probe(int source, int tag, MPI_Comm comm, MPI_Status *mpi_status)
{
  int status = 0;
  while (true)
  {
    int flag = 0;
    status = MPI_Iprobe(source, tag, comm, &flag, mpi_status);
    if (flag)
      break;
    // sleep(1);
  }
  return status;
}

EXTERNC int
MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag,
          MPI_Comm comm, MPI_Status *mpi_status)
{
  int status = 0xFFFFFFFF;
  int size = 0;
  bool done = false;
  MPI_Status iprobe_mstat;

  // calculate total size of expected message before we do anything
  status = MPI_Type_size(datatype, &size);
  size = size * count;

  while (true)  // loop until we receive a packet
  {
    int flag = 0;
    int wait_type = MPI_PLUGIN_NO_PACKET_WAITING;

    // during this critical section we must disable checkpointing
    DMTCP_PLUGIN_DISABLE_CKPT();
    status = mpi_plugin_is_packet_waiting(source, tag, comm, &flag,
                                          &iprobe_mstat, &wait_type);
    if (flag && wait_type == MPI_PLUGIN_BUFFERED_PACKET_WAITING)
    {
      // drain from plugin to rank buffer
      status = mpi_plugin_return_buffered_packet(buf, count, datatype, source,
                                                tag, comm, mpi_status, size);
      done = true;
    }
    else if (flag && wait_type == MPI_PLUGIN_PROXY_PACKET_WAITING)
    {
      // drain from proxy to rank buffer
      Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Recv);
      Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, count);
      Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, (int)datatype);
      Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, source);
      Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, tag);
      Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, (int)comm);
      if (mpi_status == MPI_STATUS_IGNORE)
        Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, 0xFFFFFFFF);
      else
        Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, 0x0);

      status = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
      if (status == 0)
      {
        Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD, buf, size);
        if (mpi_status != MPI_STATUS_IGNORE)
          Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD,
                                  mpi_status,
                                  sizeof(MPI_Status));
      }
      done = true;
      gworld_recv++;
    }
    // no packet waiting, allow a moment to sleep in case we need to checkpoint
    DMTCP_PLUGIN_ENABLE_CKPT();
    if (done)
      break;
  }
  return status;
}


std::map<MPI_Request*, void *> g_irecv_buffer;
std::map<MPI_Request*, int> g_irecv_buffer_size;
EXTERNC int
MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag,
          MPI_Comm comm, MPI_Request *request)
{
  int status = 0xFFFFFFFF;
  int size = 0;
  bool done = false;

  // calculate total size of expected message before we do anything
  status = MPI_Type_size(datatype, &size);
  size = size * count;

  // during this critical section we must disable checkpointing
  DMTCP_PLUGIN_DISABLE_CKPT();
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Irecv);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, count);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, (int)datatype);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, source);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, tag);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, (int)comm);
  Send_Buf_To_Proxy(PROTECTED_MPI_PROXY_FD, request, sizeof(MPI_Request));

  status = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
  if (status == 0)
  {
    Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD, &request,
                            sizeof(MPI_Request));
    g_irecv_buffer[*request] = buf;
    g_irecv_buffer_size[*request] = size;
  }
  DMTCP_PLUGIN_ENABLE_CKPT();

  return status;
}



EXTERNC int
MPI_Finalize(void)
{
  return exec_proxy_cmd(MPIProxy_Cmd_Finalize);
}

static void
mpi_plugin_event_hook(DmtcpEvent_t event, DmtcpEventData_t *data)
{
  switch (event) {
  case DMTCP_EVENT_INIT:
    JTRACE("*** DMTCP_EVENT_INIT\n");
    break;
  case DMTCP_EVENT_EXIT:
    JTRACE("*** DMTCP_EVENT_EXIT\n");
    close_proxy();
    break;
  default:
    break;
  }
}


static void
pre_ckpt_update_ckpt_dir()
{
  const char *ckptDir = dmtcp_get_ckpt_dir();
  dmtcp::string baseDir;

  if (strstr(ckptDir, dmtcp_get_computation_id_str()) != NULL) {
    baseDir = jalib::Filesystem::DirName(ckptDir);
  } else {
    baseDir = ckptDir;
  }
  dmtcp::ostringstream o;
  o << baseDir << "/ckpt_rank_" << gworld_rank;
  dmtcp_set_ckpt_dir(o.str().c_str());
}

static bool drain_packet()
{
  void *buf;
  int size = 0;
  int count = 0;
  int flag = 0;
  int source = 0;
  int tag = 0;
  MPI_Datatype datatype;
  MPI_Comm comm = MPI_COMM_WORLD; // FIXME - other comms?
  MPI_Status status;
  Message *message;

  // Probe for waiting packet
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Iprobe);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPI_ANY_SOURCE);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPI_ANY_TAG);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, (int) MPI_COMM_WORLD);

  // get probe resules
  Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
  flag = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
  Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD, &status, sizeof(MPI_Status));
  if (!flag)
    return false;

  // There's a packet waiting for us
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Get_count);
  Send_Buf_To_Proxy(PROTECTED_MPI_PROXY_FD, &status, sizeof(MPI_Status));
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPI_BYTE);
  Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD); // status
  count = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);

  // Get Type_size info
  // FIXME: get actual type size
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Type_size);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, (int)MPI_BYTE);
  Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
  size = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);

  // allocate our receive buffer
  size = count * size;
  buf = malloc(size); // maximum of 65535 ints

  // drain from proxy to plugin buffer
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPIProxy_Cmd_Recv);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, count);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, MPI_BYTE); // FIXME: actual datatype
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, status.MPI_SOURCE);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, status.MPI_TAG);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, (int)comm);
  Send_Int_To_Proxy(PROTECTED_MPI_PROXY_FD, 0xFFFFFFFF); // Ignore FIXME

  Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
  Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD, buf, size);

  // copy all data into local message
  message = (Message *)malloc(sizeof(Message));
  message->buf        = buf;
  message->count      = count;
  message->datatype   = MPI_BYTE;
  message->comm       = comm;
  message->status.count_lo    = status.count_lo;
  message->status.count_hi_and_cancelled = status.count_hi_and_cancelled;
  message->status.MPI_SOURCE  = status.MPI_SOURCE;
  message->status.MPI_TAG     = status.MPI_TAG;
  message->status.MPI_ERROR   = status.MPI_ERROR;
  message->size       = size;

  // queue it
  gmessage_queue.push_back(message);
  glocal_recv++;
  return true;
}

struct keyVal {
  int typerank;
  uint32_t value;
} mystruct, mystruct_other;

int HIGHBIT = 1 << ((sizeof(mystruct.typerank)*8)-1);
uint32_t sizeofval = sizeof(mystruct_other.value);

static void
pre_ckpt_register_data()
{
  // publish my keys and values
  mystruct.typerank = gworld_rank;
  mystruct.value = glocal_sent;
  dmtcp_send_key_val_pair_to_coordinator("mpi-proxy",
                                          &(mystruct.typerank),
                                          sizeof(mystruct.typerank),
                                          &(mystruct.value),
                                          sizeof(mystruct.value));
  mystruct.typerank |= HIGHBIT;
  mystruct.value = glocal_recv;
  dmtcp_send_key_val_pair_to_coordinator("mpi-proxy",
                                          &(mystruct.typerank),
                                          sizeof(mystruct.typerank),
                                          &(mystruct.value),
                                          sizeof(mystruct.value));
  return;
}

static void
get_packets_sent()
{
  int i = 0;
  gworld_sent = glocal_sent;
  for (i = 0; i < gworld_size; i++)
  {
    if (i == gworld_rank)
      continue;
    // get number of sent packets by this proxy
    mystruct_other.typerank = i;
    mystruct_other.value = 0;
    dmtcp_send_query_to_coordinator("mpi-proxy",
                                      &(mystruct_other.typerank),
                                      sizeof(mystruct_other.typerank),
                                      &(mystruct_other.value),
                                      &sizeofval);
    gworld_sent += mystruct_other.value;
  }
}

static void
get_packets_recv()
{
  int i = 0;
  gworld_recv = 0;
  // get everyone elses keys and values
  for (i = 0; i < gworld_size; i++)
  {
    if (i == gworld_rank)
    {
      gworld_recv += glocal_recv;
      continue;
    }

    // get number of received packets by this proxy
    mystruct_other.typerank = (i | HIGHBIT);
    mystruct_other.value = 0;
    dmtcp_send_query_to_coordinator("mpi-proxy",
                                      &(mystruct_other.typerank),
                                      sizeof(mystruct_other.typerank),
                                      &(mystruct_other.value),
                                      &sizeofval);
    gworld_recv += mystruct_other.value;
  }
}

static void
publish_packets_recv()
{
  mystruct.typerank = gworld_rank | HIGHBIT;
  mystruct.value = glocal_recv;
  dmtcp_send_key_val_pair_to_coordinator("mpi-proxy",
                                          &(mystruct.typerank),
                                          sizeof(mystruct.typerank),
                                          &(mystruct.value),
                                          sizeof(mystruct.value));
}

static void
complete_blocking_call()
{
  if (g_pending_send)
  {
    g_restart_retval = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
    g_restart_receive = true;
    g_pending_send = false;
  }
  else if (g_pending_wait)
  {
    g_restart_retval = Receive_Int_From_Proxy(PROTECTED_MPI_PROXY_FD);
    g_restart_receive = true;
    Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD, g_pending_wait_request,
                            sizeof(MPI_Request));
    Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD, g_pending_wait_status,
                            sizeof(MPI_Status));
    g_pending_wait = false;
  }
}

static void
drain_irecvs()
{
  // TODO: if this was a Wait for an Irecv, update the receive buffer
  // for (queued_irecv)
    // MPI_Test(Request, Status)
    // if (ready)
      // void * buf = g_irecv_buffers[request]
      // unsigned int bufsize = g_buffer_size[request]
      // Receive_Buf_From_Proxy(PROTECTED_MPI_PROXY_FD, buf, bufsize);
      // g_irecv_buffers.delete(request)
      // g_buffer_size.delete(request)
      // g_replay_commands.delete(request)
      // TODO: update cached irecv Requests
      // received_msgs++;
}

static void
pre_ckpt_drain_data_from_proxy()
{
  get_packets_sent();
  get_packets_recv();

  complete_blocking_call();

  while (gworld_sent != gworld_recv)
  {
    drain_irecvs();
    drain_packet();
    // we have to call this every time to get up to date numbers
    // of all the proxies, since we're not the only one waiting
    publish_packets_recv();
    get_packets_recv();
  }

  // TODO: set all g_request_map[x] to true and *x = MPI_REQUEST_NULL

  // on restart, we want to start totally fresh, since everything
  // is sent and received, these numbers are now totally irrelevant
  gworld_sent = 0;
  gworld_recv = 0;
  glocal_sent = 0;
  glocal_recv = 0;
}

static DmtcpBarrier mpiPluginBarriers[] = {
  { DMTCP_GLOBAL_BARRIER_PRE_CKPT, pre_ckpt_register_data,
    "Drain-Data-From-Proxy" },
  { DMTCP_GLOBAL_BARRIER_PRE_CKPT, pre_ckpt_drain_data_from_proxy,
    "Drain-Data-From-Proxy" },
  { DMTCP_GLOBAL_BARRIER_PRE_CKPT, pre_ckpt_update_ckpt_dir,
    "update-ckpt-dir-by-rank" }
};

DmtcpPluginDescriptor_t mpi_plugin = {
  DMTCP_PLUGIN_API_VERSION,
  PACKAGE_VERSION,
  "mpi_plugin",
  "DMTCP",
  "dmtcp@ccs.neu.edu",
  "MPI Proxy Plugin",
  DMTCP_DECL_BARRIERS(mpiPluginBarriers),
  mpi_plugin_event_hook
};

DMTCP_DECL_PLUGIN(mpi_plugin);
