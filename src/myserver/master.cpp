// change to priority queue
// primes
// refactor

#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <assert.h>
#include <unordered_map>
#include <sstream>
#include <queue>
#include <stack>
#include <iterator>

#include "server/messages.h"
#include "server/master.h"

#define STRETCH_CPU_POW 27
#define MAX_CPU_POW 23
#define MAX_QUEUE_LENGTH 25
#define MAX_MEM_POW 1

using namespace std;

typedef struct Work {
  Client_handle client_handle;
  Request_msg client_req;
  int cpu_pow;
  int mem_pow;
  int tag;
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
  unsigned int max_num_workers;
  int next_tag;

  bool is_requesting_worker;

  unordered_map<int, Work *> wait_table;

  // queue of holding requests
  queue<Request_msg> pending_cpu;
  stack<Request_msg> pending_mem;

  // count of pending requests
  unordered_map<Worker_handle, Worker *> my_workers;
  unordered_map<int, Response_msg> prime_cache;
  unordered_map<int, CPRequest *>cp_map;
} mstate;


void assign_request(const Request_msg&);

// wrappers
void Request_Worker() {
  Request_msg req(0);
  request_new_worker_node(req);
  mstate.is_requesting_worker = true;
}

void Send_Request_To_Worker(Worker_handle handle, const Request_msg &req,
                            bool isMem = false) {
  DLOG(INFO) << "Sending request " << req.get_tag() << "to worker." << endl;
  Worker *worker = mstate.my_workers[handle];
  worker->load++;
  if (isMem)
    worker->mem_load++;
  send_request_to_worker(handle, req);
}

int count_idle_threads() {
  int count = 0;
  for (auto w: mstate.my_workers) {
    if (w.second->load < MAX_CPU_POW) {
      count += MAX_CPU_POW - w.second->load;
    }
  }
  return count;
}

int count_avalible_queue() {
  int count = 0;
  for (auto w: mstate.my_workers) {
    if (w.second->load < STRETCH_CPU_POW) {
      count += STRETCH_CPU_POW - w.second->load;
    }
  }
  return count;
}

int count_no_mem() {
  // count the number of workers that are not running projectidea
  int count = 0;
  for (auto& w: mstate.my_workers) {
    if (!w.second->mem_load) {
      count++;
    }
  }
  return count;
}


Worker *find_worker(int cpu_cost, int mem_cost=0) {
  for (auto worker : mstate.my_workers) {
    if (worker.second->load + cpu_cost <= MAX_CPU_POW &&
        worker.second->mem_load + mem_cost <= MAX_MEM_POW)
      return worker.second;
  }
  return NULL;
}

Worker *find_ok_worker(int cpu_cost, int mem_cost=0) {
  for (auto worker : mstate.my_workers) {
    if (worker.second->load + cpu_cost <= STRETCH_CPU_POW &&
        worker.second->mem_load + mem_cost <= MAX_MEM_POW)
      return worker.second;
  }
  return NULL;
}

Worker *find_least_busy_worker() {
  int min_load = INT_MAX;
  Worker *least_worker = NULL;
  for (auto worker : mstate.my_workers) {
    if (worker.second->load < min_load) {
      min_load = worker.second->load;
      least_worker = worker.second;
    }
  }
  assert(least_worker != NULL);
  return least_worker;
}

void count_resource(int &a, int &b, int &c) {
  a = 0;
  b = 0;
  c = 0;
  for (auto w : mstate.my_workers) {
    if (w.second->load < MAX_CPU_POW)
      a += MAX_CPU_POW - w.second->load;
    if (w.second->load < STRETCH_CPU_POW)
      b += STRETCH_CPU_POW - w.second->load;
    if (w.second->mem_load == 0)
      c += 1;
  }
}

// -----------------------------------------------------------------------

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 1;

  mstate.next_tag = 0;
  mstate.is_requesting_worker = false;
  mstate.max_num_workers = max_workers;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;
  Request_Worker();
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  mstate.is_requesting_worker = false;

  Worker *worker = new Worker(worker_handle);

  mstate.my_workers[worker_handle] = worker;

  while(mstate.pending_cpu.size() > 0) {
    Send_Request_To_Worker(worker_handle, mstate.pending_cpu.front());
    mstate.pending_cpu.pop();
  }

  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }

  tag = tag; // compiler don't yell
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  int tag = resp.get_tag();
  Client_handle waiting_client = mstate.wait_table[tag]->client_handle;
  Request_msg waiting_req = mstate.wait_table[tag]->client_req;

  // Master node has received a response from one of its workers.
  mstate.my_workers[worker_handle]->load--;

  if (waiting_req.get_arg("cmd") == "projectidea") {
    mstate.my_workers[worker_handle]->mem_load--;
  }

  if (waiting_req.get_arg("cmd") == "countprimes") { // cache
    if (mstate.cp_map.find(tag) != mstate.cp_map.end()) { // compare primes
      CPRequest *cpreq = mstate.cp_map[tag];
      int i = tag - cpreq->tag_start;
      cpreq->counts[i] = atoi(resp.get_response().c_str());
      cpreq->count++;
      mstate.prime_cache[cpreq->params[i]] = cpreq->counts[i];
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
      mstate.prime_cache[n] = resp;
    }
  }
  send_client_response(waiting_client, resp);

  worker_handle = worker_handle; // compiler don't yell
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

  if (client_req.get_arg("cmd") == "countprimes") {
    int n = atoi(client_req.get_arg("n").c_str());
    if (mstate.prime_cache.find(n) != mstate.prime_cache.end()) {
      send_client_response(client_handle, mstate.prime_cache[n]);
      return ;
    }
  }

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
      if (mstate.prime_cache.find(cpreq->params[i]) != mstate.prime_cache.end()) {
        mstate.next_tag++;
        cpreq->counts[i] = atoi(mstate.prime_cache[cpreq->params[i]].get_response().c_str());
        cpreq->count++;
      } else { // not found in cache
        int tag = mstate.next_tag++;
        mstate.cp_map[tag] = cpreq;
        Request_msg r(tag);
        create_computeprimes_req(r, cpreq->params[i]);
        Work *work = new Work(client_handle, r);
        mstate.wait_table[tag] = work;
        work->tag = tag;
        assign_request(r);
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
  int tag = mstate.next_tag++;
  Work *work = new Work(client_handle, client_req);
  mstate.wait_table[tag] = work;
  work->tag = tag;
  Request_msg worker_req(tag, client_req);
  assign_request(worker_req);
}

void send_request_to_best_worker(const Request_msg& req) {
  int idle_count = count_idle_threads();
  if (idle_count > 0) { // there is a idle threads
    Worker_handle worker_handle = mstate.my_workers.begin()->first;
    int t_num = 0;
    for (auto w: mstate.my_workers) {
      if (w.second->load < MAX_CPU_POW && t_num < w.second->load) {
        // send request to the worker with least idle threads
        worker_handle = w.first;
        t_num = w.second->load;
      }
    }
    Send_Request_To_Worker(worker_handle, req);
  } else { // there is no idle threads
    // send request to the least busy worker
    Worker_handle worker_handle = mstate.my_workers.begin()->first;
    int t_num = INT_MAX; // big number
    for (auto& w: mstate.my_workers) {
      if (w.second->load < t_num) {
        // send request to the worker with least idle threads
        worker_handle = w.first;
        t_num = w.second->load;
      }
    }
    Send_Request_To_Worker(worker_handle, req);
  }
}


void send_memjob() {
  int count = count_no_mem();
  while (count > 0 && mstate.pending_mem.size() > 0) {
    count--;
    int t_num = INT_MAX; // big number
    Worker_handle worker_handle = mstate.my_workers.begin()->first;
    // find the least busy worker
    for (auto& w: mstate.my_workers) {
      if (!w.second->mem_load && w.second->load < t_num) {
        worker_handle = w.first;
        t_num = w.second->load;
      }
    }
    Send_Request_To_Worker(worker_handle, mstate.pending_mem.top(), 1);
    mstate.pending_mem.pop();
  }
}

void assign_request(const Request_msg& req) {
  if (req.get_arg("cmd") == "tellmenow") { // fire instantly
    auto w = mstate.my_workers.begin();
    Send_Request_To_Worker(w->first, req);
  } else if (req.get_arg("cmd") == "projectidea"){
    mstate.pending_mem.push(req);
    send_memjob();
    if (mstate.pending_mem.size() > 3
        && mstate.my_workers.size() < mstate.max_num_workers
        && !mstate.is_requesting_worker) {
      Request_Worker();
    }
  } else {
    // if workers' number is at max
    if (mstate.my_workers.size() == mstate.max_num_workers) {
      // fire request
      send_request_to_best_worker(req);
    } else {
      // decide whether to queue or fire
      int idle_count = count_idle_threads();
      // have avalible workers
      if (idle_count > 0) {
        // check if there is request in the holding queue
        if (mstate.pending_cpu.size() > 0) {
          // push the request into the back of the queue
          mstate.pending_cpu.push(req);
          while(idle_count > 0 && mstate.pending_cpu.size() > 0) {
            // send queued reqs
            send_request_to_best_worker(mstate.pending_cpu.front());
            mstate.pending_cpu.pop();
            idle_count--;
          }
        } else {
          // fire instantly
          send_request_to_best_worker(req);
        }
      } else {
        // !!!!! if overload too much should allow init more than one worker
        // fire off a request for a new worker
        if (!mstate.is_requesting_worker) { // init a new worker
          Request_Worker();
        }

        int avalible_queue = count_avalible_queue();
        if (avalible_queue > 0) {
          // check if there is request in the holding queue
          if (mstate.pending_cpu.size() > 0) {
            // push the request into the back of the queue
            mstate.pending_cpu.push(req);
            while(avalible_queue > 0 && mstate.pending_cpu.size() > 0) {
              // send queued reqs
              send_request_to_best_worker(mstate.pending_cpu.front());
              mstate.pending_cpu.pop();
              avalible_queue--;
            }
          } else {
            // fire instantly
            send_request_to_best_worker(req);
          }
        } else { // workers are all overload
          mstate.pending_cpu.push(req); // cache the request in queue
          if (mstate.pending_cpu.size() > MAX_QUEUE_LENGTH) {
            // queue too long
            send_request_to_best_worker(mstate.pending_cpu.front());
            mstate.pending_cpu.pop();
          }
        }
      }
    }
  }
}

// void assign_request(const Request_msg &req) {
//   if (req.get_arg("cmd") == "tellmenow") { // assign to a random worker immediately
//     auto w = mstate.my_workers.begin();
//     Send_Request_To_Worker(w->first, req);
//     return ;
//   } else if (req.get_arg("cmd") == "projectidea") { // bandwidth job
//     mstate.pending_mem.push(req);
//     send_memjob();
//     if (mstate.pending_mem.size() > 3
//         && mstate.my_workers.size() < (size_t) mstate.max_num_workers
//         && mstate.is_requesting_worker == false) {
//       Request_Worker();
//     }
//   } else { // schedule a normal cpu job
//     if (mstate.my_workers.size() == (size_t) mstate.max_num_workers) {
//       send_request_to_best_worker(req);
//       return;
//     }
//     Worker *best_worker = find_worker(1, 0);
//     if (best_worker == NULL) {
//       DLOG(INFO) << "cannot find best worker" << endl;

//       // time to request a new worker
//       if (mstate.is_requesting_worker == false)
//         Request_Worker();

//       Worker *ok_worker = find_ok_worker(1);
//       if (ok_worker == NULL) {
//         // not even a ok worker
//         mstate.pending_cpu.push(req);
//         if (mstate.pending_cpu.size() > 25) {
//           send_request_to_best_worker(req);
//           mstate.pending_cpu.pop();
//         }
//       } else {
//         // there is a ok worker
//         if (mstate.pending_cpu.size() == 0) {
//           send_request_to_best_worker(req);
//         } else {
//           mstate.pending_cpu.push(req);
//           while (ok_worker != NULL && mstate.pending_cpu.size() > 0) {
//             send_request_to_best_worker(req);
//             mstate.pending_cpu.pop();
//             ok_worker = find_ok_worker(1);
//           }
//         }
//       }
//     } else { // there is a worker who can do this job right now
//       // if no other cpu jobs waiting
//       if (mstate.pending_cpu.size() == 0) {
//         send_request_to_best_worker(req);
//       } else {
//         mstate.pending_cpu.push(req);
//         while (best_worker != NULL && mstate.pending_cpu.size() > 0) {
//           send_request_to_best_worker(mstate.pending_cpu.front());
//           best_worker = find_worker(1, 0);
//           mstate.pending_cpu.pop();
//         }
//       }
//     }
//   }
// }

void handle_tick() {
  send_memjob();
  if (mstate.my_workers.size() > 1) {
    for (auto& w: mstate.my_workers) {
      if (w.second->load == 0) {
        kill_worker_node(w.first);
        Worker_handle wh = w.first;
        mstate.my_workers.erase(wh);
        break;
      }
    }
  }
}
