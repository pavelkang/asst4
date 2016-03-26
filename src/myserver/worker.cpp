#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include "tools/work_queue.h"

#define MAX_NUM_THREADS 24
WorkQueue<Request_msg> work_queue;
WorkQueue<Request_msg> tellme_queue;
using namespace std;

static void *routine(void *arg) {
  while (1) {
    Request_msg req = work_queue.get_work(); // will block if not available
    Response_msg res(req.get_tag());
    execute_work(req, res);
    worker_send_response(res);
  }
  // compiler don't yell
  arg = arg;
}

static void* tellme_routine(void *arg) {
  while (1) {
    Request_msg req = tellme_queue.get_work();
    Response_msg res(req.get_tag());
    execute_work(req, res);
    worker_send_response(res);
  }
  arg = arg;
}

void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";
  for (int i = 0; i < MAX_NUM_THREADS-1; i++) {
    pthread_t thread;
    pthread_create(&thread, NULL, &routine, NULL);
  }
  pthread_t thr;
  pthread_create(&thr, NULL, &tellme_routine, NULL);
}

void worker_handle_request(const Request_msg& req) {
  // Make the tag of the reponse match the tag of the request.  This
  // is a way for your master to match worker responses to requests.
  Response_msg resp(req.get_tag());

  // Output debugging help to the logs (in a single worker node
  // configuration, this would be in the log logs/worker.INFO)
  DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";
  if (req.get_arg("cmd") == "tellmenow") {
    tellme_queue.put_work(req);
    return;
  }
  work_queue.put_work(req);
}
