// TODO maintain load, mem_load
// TODO tick frequence
// TODO delete when pop
// TODO inefficient find_tag_by_work
// TODO cache everything?
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
#define MAX_NUM_THREAD 48
#define MAX_CPU_POW (MAX_NUM_THREAD)
#define OK_CPU_POW (MAX_NUM_THREAD + 5)
#define MAX_MEM_POW 2
#define BANDWIDTH_TH 2
#define CPU_TH 3

typedef struct Work {
  Client_handle client_handle;
  Request_msg client_req;
  int cpu_pow;
  int mem_pow;
  Work() {};
  Work(Client_handle ch, Request_msg cr) : client_handle(ch), client_req(cr) {
    string cmd = cr.get_arg("cmd");
    cpu_pow = 0;
    mem_pow = 0;
    if (cmd.compare("projectidea") == 0) {
      // has an L3-cache sized working set
      cpu_pow = 1;
      mem_pow = 1;
    } else {
      cpu_pow = 1;
      mem_pow = 0;
    }
  };
} Work;

typedef struct Worker {
  Worker_handle handle;
  int load;
  int mem_load;
  Worker() {};
  Worker(Worker_handle h) : handle(h), load(0), mem_load(0) {};
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
  bool requesting_worker;
  int max_num_workers;
  int num_pending_client_requests;
  int next_tag;

  queue<Work *> pending_work;
  queue<Work *> pending_bandwidth_work;
  Work *wait_table[MAX_INT];
  // I use vector because I might need to loop through to find the best worker
  vector<Worker *> my_workers;
  unordered_map<int, Response_msg>cache;
  unordered_map<int, CPRequest *>cp_map;
} mstate;

Worker *find_worker(int required_cpu, int required_mem) {
  for (auto worker : mstate.my_workers) {
    if ((worker->load + required_cpu < MAX_CPU_POW) &&
        (worker->mem_load + required_mem < MAX_MEM_POW))
      return worker;
  }
  return NULL;
}

Worker *find_ok_worker(int required_cpu) {
  for (auto worker : mstate.my_workers) {
    if (worker->load + required_cpu < OK_CPU_POW)
      return worker;
  }
  return NULL;
}

Worker *find_least_busy_worker() {
  int min_load = MAX_INT;
  Worker *least_busy = NULL;
  for (auto worker : mstate.my_workers) {
    if (worker->load < min_load) {
      min_load = worker->load;
      least_busy = worker;
    }
  }
  assert(least_busy != NULL);
  return least_busy;
}


Worker *locate_worker(Worker_handle handle) {
  for (auto worker: mstate.my_workers) {
    if (worker->handle == handle) {
      return worker;
    }
  }
  assert(false);
}

void Request_Worker() {
  DLOG(INFO) << "Requesting a new worker" << endl;
  Request_msg req(0);
  request_new_worker_node(req);
  mstate.requesting_worker = true;
}

void Send_Request_To_Worker(Worker_handle handle, const Request_msg &req,
                            bool isMem = false) {
  DLOG(INFO) << "Sending request " << req.get_tag() << "to worker." << endl;
  Worker *worker = locate_worker(handle);
  worker->load++;
  if (isMem)
    worker->mem_load++;
  send_request_to_worker(handle, req);
}

int find_tag_by_work(Work *work) {
  for (int i = 0; i < MAX_INT; i++) {
    if (mstate.wait_table[i] == work) {
      DLOG(INFO) << "tag is " << i << endl;
      return i;
    }
  }
  assert(false);
}

void assign_work(Work *work) {
  int tag = mstate.next_tag++;
  mstate.wait_table[tag] = work;
  DLOG(INFO) << "assign tag " << tag << " to work " << work << endl;
  if (work->client_req.get_arg("cmd") == "tellmenow") { // assign to a random worker immediately
    Request_msg worker_req(tag, work->client_req);
    int rand_worker = rand() % (mstate.my_workers.size());
    Send_Request_To_Worker(mstate.my_workers[rand_worker]->handle, worker_req);
    return ;
  } else if (work->client_req.get_arg("cmd") == "projectidea") { // bandwidth job
    // Worker *best_worker = find_worker(work.cpu_pow, work.mem_pow);
    // if (best_worker == NULL) {
    //   // no one can server it right now, push to pending_bandwidth_works
    //   mstate.pending_bandwidth_work.push(work);
    //   // be careful requestingnew worker
    //   if (mstate.pending_bandwidth_work.size() > BANDWIDTH_TH &&
    //       mstate.requesting_worker == false &&
    //       mstate.my_workers.size() < (size_t) mstate.max_num_workers) {
    //     Request_Worker();
    //   }
    //   return;
    // } else { // found a worker who can serve a mem job
    //   mstate.pending_bandwidth_work.push(work);
    //   Work mem_job = mstate.pending_bandwidth_work.front();
    //   Send_Request_To_Worker(best_worker->handle, mem_job.client_req, true); // PROBLEM HERE!!!
    //   mstate.pending_bandwidth_work.pop();
    //   return;
    // }
  } else { // schedule a normal cpu job
    Worker *best_worker = find_worker(work->cpu_pow, work->mem_pow);
    if (best_worker == NULL) {
      DLOG(INFO) << "cannot find best worker" << endl;

      // time to request a new worker
      if (mstate.requesting_worker == false)
        Request_Worker();

      Worker *ok_worker = find_ok_worker(work->cpu_pow);
      if (ok_worker == NULL) {
        // not even a ok worker
        mstate.pending_work.push(work);
        if (mstate.pending_work.size() > 25) {
          // make this a function TODO
          Work *this_job = mstate.pending_work.front();
          int this_tag = find_tag_by_work(this_job);
          Request_msg worker_req(this_tag, this_job->client_req);
          Worker *least_worker = find_least_busy_worker();
          Send_Request_To_Worker(least_worker->handle, worker_req);
          mstate.pending_work.pop();
        }
      } else {
        // there is a ok worker
        if (mstate.pending_work.size() == 0) {
          Request_msg worker_req(tag, work->client_req);
          Send_Request_To_Worker(ok_worker->handle, worker_req);
        } else {
          mstate.pending_work.push(work);
          while (ok_worker != NULL && mstate.pending_work.size() > 0) {
            Work *this_job = mstate.pending_work.front();
            int this_tag = find_tag_by_work(this_job);
            Request_msg worker_req(this_tag, this_job->client_req);
            Worker *least_worker = find_least_busy_worker();
            Send_Request_To_Worker(least_worker->handle, worker_req);
            mstate.pending_work.pop();
            ok_worker = find_ok_worker(1);
          }
        }
      }


      // be careful requesting new worker
      // if (mstate.requesting_worker == false &&
      //     mstate.my_workers.size() < (size_t) mstate.max_num_workers) {
      //   Request_Worker();
      // } else {
      //   if (mstate.pending_work.size() <= CPU_TH) {
      //     DLOG(INFO) << "Not requesting because <= CPU_TH" << endl;
      //   } else if (mstate.requesting_worker) {
      //     DLOG(INFO) << "Not requesting because requesting" << endl;
      //   } else {
      //     DLOG(INFO) << "Not requesting because size" << endl;
      //   }
      // }
      // return;
    } else { // there is a worker who can do this job right now
      // if no other cpu jobs waiting
      if (mstate.pending_work.size() == 0) {
        Request_msg worker_req(tag, work->client_req);
        Send_Request_To_Worker(best_worker->handle, worker_req);
      } else {
        mstate.pending_work.push(work);
        while (best_worker != NULL && mstate.pending_work.size() > 0) {
          Work *this_job = mstate.pending_work.front();
          int this_tag = find_tag_by_work(this_job);
          Request_msg worker_req(this_tag, this_job->client_req);
          Send_Request_To_Worker(best_worker->handle, worker_req);
          mstate.pending_work.pop();
          best_worker = find_worker(1, 0);
        }
      }
    }
  }
}

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 1 seconds.
  tick_period = 1;

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_pending_client_requests = 0;
  mstate.requesting_worker = false;
  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire off a request for a new worker
  Request_Worker();
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {
  mstate.requesting_worker = false;

  Worker *w = new Worker(worker_handle);
  mstate.my_workers.push_back(w);

  // assign me jobs!
  while (mstate.pending_work.size() > 0) {
    Work *work = mstate.pending_work.front();
    int tag = find_tag_by_work(work);
    Request_msg req(tag, work->client_req);
    Send_Request_To_Worker(worker_handle, req);
    mstate.pending_work.pop();
  }

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
  tag = tag; // compiler don't yell
}


void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {
  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;
  int tag = resp.get_tag();
  Work waiting_work = *mstate.wait_table[tag];
  DLOG(INFO) << "Found tag " << tag << " work " << mstate.wait_table[tag] << endl;
  Client_handle waiting_client = waiting_work.client_handle;
  Request_msg waiting_req = waiting_work.client_req;
  // maintain the load, mem_load fields
  Worker *worker = locate_worker(worker_handle);
  worker->load--;
  if (waiting_req.get_arg("cmd") == "projectidea")
    worker->mem_load--;

  // todo: change wait_table to store Work
  if (waiting_req.get_arg("cmd") == "countprimes") { // cache
    if (mstate.cp_map.find(tag) != mstate.cp_map.end()) { // compare primes
      CPRequest *cpreq = mstate.cp_map[tag];
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
        send_client_response(waiting_client, resp);
      }
      return ;
    } else { // count primes
      int n = atoi(waiting_req.get_arg("n").c_str());
      mstate.cache[n] = resp;
    }
  }
  send_client_response(waiting_client, resp);
  //remove_load(worker_handle);
  mstate.num_pending_client_requests--;
  worker_handle = worker_handle;
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
        Work *work = new Work(client_handle, r);
        mstate.wait_table[tag] = work;
        // TODO here
        Worker *worker = find_worker(1, 0);
        if (worker == NULL) {
          mstate.pending_work.push(work);
        } else {
          Send_Request_To_Worker(worker->handle, r);
        }
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
  Work *work = new Work(client_handle, client_req);
  assign_work(work);
}


void handle_tick() {
  // // kill TODO
  if (mstate.my_workers.size() >= 2) {
    for (auto worker : mstate.my_workers) {
      if (worker->load == 0 && worker->mem_load == 0) {
        kill_worker_node(worker->handle);
      }
    }
  }
  // // see if there is pending work
  // if (mstate.pending_work.size() > 0) {
  //   Work *work = mstate.pending_work.front();
  //   int tag = find_tag_by_work(work);
  //   mstate.pending_work.pop();
  //   Request_msg worker_req(tag, work->client_req);
  //   DLOG(INFO) << "ticker sending request" << endl;
  //   Send_Request_To_Worker(mstate.my_workers[0]->handle, worker_req);
  // }
}
