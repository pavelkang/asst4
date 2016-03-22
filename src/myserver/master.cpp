#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <queue>
#include <assert.h>
#include <unordered_map>
#include <vector>
#include "server/messages.h"
#include "server/master.h"
#include <stack>
#include <iostream>

using namespace std;

#define MAX_INT 9999999
#define MAX_NUM_THREAD 24

typedef struct Work {
  Client_handle client_handle;
  Request_msg client_req;
  Work() {};
  Work(Client_handle ch, Request_msg cr) : client_handle(ch), client_req(cr) {};
} Work;

typedef struct Worker {
  Worker_handle handle;
  int load;
  Worker() {};
  Worker(Worker_handle h) : handle(h), load(0) {};
} Worker;

typedef struct CPRequest {
  Client_handle client_handle;
  int count;
  int params[4];
  int counts[4];
  int tag_start;
  CPRequest() {};
  CPRequest(Client_handle ch) : client_handle(ch), count(0) {};
} CPRequest;

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.
  bool server_ready;
  int max_num_workers;
  int num_pending_client_requests;
  int next_tag;

  Worker_handle my_worker;
  Client_handle waiting_client;

  queue<Work> pending_work; // ?? need this ??
  Work wait_table[MAX_INT];
  // I use vector because I might need to loop through to find the best worker
  vector<Worker> my_workers;
  unordered_map<int, Response_msg>cache;
  unordered_map<int, CPRequest *>cp_map;
} mstate;

Worker find_worker() {
  for (auto worker : mstate.my_workers) {
    if (worker.load < MAX_NUM_THREAD) { // good choice
      return worker;
    }
  }
  assert(false);
}

void assign_work() {
  Work work = mstate.pending_work.front();
  mstate.pending_work.pop();
  int tag = mstate.next_tag++;
  mstate.wait_table[tag] = work;
  Request_msg worker_req(tag, work.client_req);
  Worker worker = find_worker();
  send_request_to_worker(worker.handle, worker_req);
}

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_pending_client_requests = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire off a request for a new worker

  int tag = random();
  Request_msg req(tag);
  req.set_arg("name", "my worker 0");
  request_new_worker_node(req);

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.

  //1 mstate.my_worker = worker_handle;
  Worker w(worker_handle);
  mstate.my_workers.push_back(w);

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }

  tag = tag;
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {
  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;
  int tag = resp.get_tag();
  Work waiting_work = mstate.wait_table[tag];
  Client_handle waiting_client = waiting_work.client_handle;
  Request_msg waiting_req = waiting_work.client_req;
  // todo: change wait_table to store Work
  if (waiting_req.get_arg("cmd") == "countprimes") { // cache
    DLOG(INFO) << "Ya" << endl;
    if (mstate.cp_map.find(tag) != mstate.cp_map.end()) { // compare primes
      CPRequest *cpreq = mstate.cp_map[tag];
      DLOG(INFO) << "Yo; " << cpreq << "; " << cpreq->count << endl;
      int i = tag - cpreq->tag_start;
      cpreq->counts[i] = atoi(resp.get_response().c_str());
      cpreq->count++;
      DLOG(INFO) << cpreq->count << endl;
      mstate.cache[cpreq->params[i]] = cpreq->counts[i];
      if (cpreq->count == 4) {
        Response_msg resp(cpreq->tag_start);
        if (cpreq->counts[1]-cpreq->counts[0] > cpreq->counts[3]-cpreq->counts[2])
          resp.set_response("There are more primes in first range.");
        else
          resp.set_response("There are more primes in second range.");
        DLOG(INFO) << "Master sent response" << endl;
        DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;
        send_client_response(waiting_client, resp);
      }
      return ;
    } else { // count primes
      int n = atoi(waiting_req.get_arg("n").c_str());
      mstate.cache[n] = resp;
    }
  }
  send_client_response(waiting_client, resp);
  mstate.num_pending_client_requests--;
  worker_handle = worker_handle;
  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.
  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {
  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

  /* Cache for countprimes */
  if (client_req.get_arg("cmd") == "countprimes") {
    int n = atoi(client_req.get_arg("n").c_str());
    if (mstate.cache.find(n) != mstate.cache.end()) {
      send_client_response(client_handle, mstate.cache[n]);
      return ;
    }
  }

  /* compareprimes */
  if (client_req.get_arg("cmd") == "compareprimes") {
    CPRequest *cpreq = (CPRequest *)malloc(sizeof(CPRequest)); // TODO free
    cpreq->client_handle = client_handle;
    cpreq->count = 0;

    cpreq->params[0] = atoi(client_req.get_arg("n1").c_str());
    cpreq->params[1] = atoi(client_req.get_arg("n2").c_str());
    cpreq->params[2] = atoi(client_req.get_arg("n3").c_str());
    cpreq->params[3] = atoi(client_req.get_arg("n4").c_str());
    cpreq->tag_start = mstate.next_tag;
    // see cache
    for (int i = 0; i < 4; i++) {
      if (mstate.cache.find(cpreq->params[i]) != mstate.cache.end()) {
        mstate.next_tag++;
        cpreq->counts[i] = atoi(mstate.cache[cpreq->params[i]].get_response().c_str());
        cpreq->count++;
      } else { // not found in cache
        int tag = mstate.next_tag++;
        mstate.cp_map[tag] = cpreq;
        Request_msg r(tag);
        create_computeprimes_req(r, cpreq->params[i]);
        Work work(client_handle, r);
        mstate.wait_table[tag] = work;
        Worker worker = find_worker();
        send_request_to_worker(worker.handle, r);
      }
    }
    if (cpreq->count == 4) { // all in cache
      Response_msg resp(mstate.next_tag++);
      if (cpreq->counts[1]-cpreq->counts[0] > cpreq->counts[3]-cpreq->counts[2])
        resp.set_response("There are more primes in first range.");
      else
        resp.set_response("There are more primes in second range.");
      send_client_response(client_handle, resp);
      return ;
    }
    return;
  }

  Work w(client_handle, client_req);
  mstate.pending_work.push(w);
  assign_work();

  // original
  // // The provided starter code cannot handle multiple pending client
  // // requests.  The server returns an error message, and the checker
  // // will mark the response as "incorrect"
  // if (mstate.num_pending_client_requests > 0) {
  //   Response_msg resp(0);
  //   resp.set_response("Oh no! This server cannot handle multiple outstanding requests!");
  //   send_client_response(client_handle, resp);
  //   return;
  // }

  // // Save off the handle to the client that is expecting a response.
  // // The master needs to do this it can response to this client later
  // // when 'handle_worker_response' is called.
  // mstate.waiting_client = client_handle;
  // mstate.num_pending_client_requests++;

  // // Fire off the request to the worker.  Eventually the worker will
  // // respond, and your 'handle_worker_response' event handler will be
  // // called to forward the worker's response back to the server.
  // int tag = mstate.next_tag++;
  // Request_msg worker_req(tag, client_req);
  // send_request_to_worker(mstate.my_worker, worker_req);

  // // We're done!  This event handler now returns, and the master
  // // process calls another one of your handlers when action is
  // // required.

}


void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.
  if (mstate.pending_work.size() > 0) {
    assign_work();
  }
}
