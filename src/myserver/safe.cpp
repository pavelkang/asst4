#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>
#include <sstream>
#include <queue>
#include <stack>
#include <iterator>

#include "server/messages.h"
#include "server/master.h"

#define MAX_REQUESTS 27
#define MAX_THREADS 23
#define MAX_QUEUE_LENGTH 25
#define BIG_INT 999999

typedef struct {
  int first_tag;
  int finished_count;
  int n[4];
} Crequest;

typedef struct {
  int request_num;
  bool pending_projectidea;
} Wstate;

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

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

void assign_request(const Request_msg&);

// -----------------------------------------------------------------------

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

  // fire off a request for a new worker
  Request_Worker();
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.
  // decrease pending worker num
  mstate.is_requesting_worker = false;

  Worker *worker = new Worker(worker_handle);
  mstate.my_workers[worker_handle] = worker;

  // empty holding queue
  while(mstate.pending_cpu.size() > 0) {
    Send_Request_To_Worker(worker_handle, mstate.holding_requests.front());
    mstate.pending_cpu.pop();
  }

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
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

  if (mstate.compareprimes_requests.count(tag)) {
    // TODO
  } else {
    send_client_response(waiting_client, resp);
  }
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

  // if request is compareprimes, split into four requests
  if (client_req.get_arg("cmd").compare("compareprimes") == 0) {
    // TODO
  } else {
    // New tag
    int tag = mstate.next_tag++;
    Work *work = new Work(client_handle, client_req);
    mstate.wait_table[tag] = work;
    work->tag = tag;
    Request_msg worker_req(tag, client_req);
    assign_request(worker_req);
  }
}

int count_idle_threads() {
  int count = 0;
  for (auto& w: mstate.my_workers) {
    if (w.second.request_num < MAX_THREADS) {
      count += MAX_THREADS - w.second.request_num;
    }
  }
  return count;
}

int count_avalible_queue() {
  int count = 0;
  for (auto& w: mstate.my_workers) {
    if (w.second.request_num < MAX_REQUESTS) {
      count += MAX_REQUESTS - w.second.request_num;
    }
  }
  return count;
}

void send_request_to_best_worker(const Request_msg& req) {
  int idle_count = count_idle_threads();
  if (idle_count > 0) { // there is a idle threads
    Worker_handle worker_handle = mstate.my_workers.begin()->first;
    int t_num = 0;
    for (auto& w: mstate.my_workers) {
      if (w.second.request_num < MAX_THREADS && t_num < w.second.request_num) {
        // send request to the worker with least idle threads
        worker_handle = w.first;
        t_num = w.second.request_num;
      }
    }
    send_request_to_worker(worker_handle, req);
    mstate.my_workers[worker_handle].request_num++;
  } else { // there is no idle threads
    // send request to the least busy worker
    Worker_handle worker_handle = mstate.my_workers.begin()->first;
    int t_num = BIG_INT; // big number
    for (auto& w: mstate.my_workers) {
      if (w.second.request_num < t_num) {
        // send request to the worker with least idle threads
        worker_handle = w.first;
        t_num = w.second.request_num;
      }
    }
    send_request_to_worker(worker_handle, req);
    mstate.my_workers[worker_handle].request_num++;
  }
}

int count_no_projectidea() {
  // count the number of workers that are not running projectidea
  int count = 0;
  for (auto& w: mstate.my_workers) {
    if (!w.second.pending_projectidea) {
      count++;
    }
  }
  return count;
}

void send_projectidea() {
  int count = count_no_projectidea();
  while (count > 0 && mstate.projectidea_requests.size() > 0) {
    count--;
    int t_num = BIG_INT; // big number
    Worker_handle worker_handle = mstate.my_workers.begin()->first;
    // find the least busy worker
    for (auto& w: mstate.my_workers) {
      if (!w.second.pending_projectidea && w.second.request_num < t_num) {
        worker_handle = w.first;
        t_num = w.second.request_num;
      }
    }
    send_request_to_worker(worker_handle, mstate.projectidea_requests.top());
    mstate.projectidea_requests.pop();
    mstate.my_workers[worker_handle].request_num++;
    mstate.my_workers[worker_handle].pending_projectidea = true;
  }
}

void assign_request(const Request_msg& req) {
  if (req.get_arg("cmd") == "tellmenow") { // fire instantly
    auto w = mstate.my_workers.begin();
    send_request_to_worker(w->first, req);
    w->second.request_num++;
  } else if (req.get_arg("cmd") == "projectidea"){
    mstate.projectidea_requests.push(req);
    send_projectidea();
    if (mstate.projectidea_requests.size() > 3
        && mstate.my_workers.size() < mstate.max_num_workers
        && mstate.pending_worker_num == 0) {
      int tag = random();
      Request_msg req(tag);
      req.set_arg("name", "my worker 0");
      request_new_worker_node(req);
      // increase number of pending worker
      mstate.pending_worker_num++;
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
        if (mstate.holding_requests.size() > 0) {
          // push the request into the back of the queue
          mstate.holding_requests.push(req);
          while(idle_count > 0 && mstate.holding_requests.size() > 0) {
            // send queued reqs
            send_request_to_best_worker(mstate.holding_requests.front());
            mstate.holding_requests.pop();
            idle_count--;
          }
        } else {
          // fire instantly
          send_request_to_best_worker(req);
        }
      } else {
        // !!!!! if overload too much should allow init more than one worker
        // fire off a request for a new worker
        if (mstate.pending_worker_num == 0) { // init a new worker
          int tag = random();
          Request_msg req(tag);
          req.set_arg("name", "my worker 0");
          request_new_worker_node(req);
          // increase number of pending worker
          mstate.pending_worker_num++;
        }

        int avalible_queue = count_avalible_queue();
        if (avalible_queue > 0) {
          // check if there is request in the holding queue
          if (mstate.holding_requests.size() > 0) {
            // push the request into the back of the queue
            mstate.holding_requests.push(req);
            while(avalible_queue > 0 && mstate.holding_requests.size() > 0) {
              // send queued reqs
              send_request_to_best_worker(mstate.holding_requests.front());
              mstate.holding_requests.pop();
              avalible_queue--;
            }
          } else {
            // fire instantly
            send_request_to_best_worker(req);
          }
        } else { // workers are all overload
          mstate.holding_requests.push(req); // cache the request in queue
          if (mstate.holding_requests.size() > MAX_QUEUE_LENGTH) {
            // queue too long
            send_request_to_best_worker(mstate.holding_requests.front());
            mstate.holding_requests.pop();
          }
        }
      }
    }
  }
}

void handle_tick() {

  // This method is called at fixed time intervals,
  // according to how you set 'tick_period' in 'master_node_init'.

  // send projectidea requests
  send_projectidea();

  // kill idle workers
  if (mstate.my_workers.size() > 1) {
    for (auto& w: mstate.my_workers) {
      if (w.second.request_num == 0) {
        kill_worker_node(w.first);
        Worker_handle wh = w.first;
       mstate.my_workers.erase(wh);
        break;
      }
    }
  }
}
