// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017  Mellanox Technologies Ltd. All rights reserved.
 *
 *
 * Author: Alex Mikheev <alexm@mellanox.com> 
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

extern "C" {
#include <ucp/api/ucp.h>
};

#include "UCXStack.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "UCXStack "


bool test_func() 
{
    ucs_status_t err;
    ucp_config_t *ucp_config;

    err = ucp_config_read("CEPH", NULL, &ucp_config);
    if (UCS_OK != err) {
        return false;
    }
    return true;
}

UCXWorker::UCXWorker(CephContext *c, unsigned i) :
  Worker(c, i)
{

}

UCXWorker::~UCXWorker()
{
}

int UCXWorker::listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *sock)
{
  return -1;
}

int UCXWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *sock)
{
  return -1;
}

void UCXWorker::initialize()
{
} 

UCXStack::UCXStack(CephContext *cct, const string &t) :
   NetworkStack(cct, t) 
{
    ldout(cct, 0) << __func__ << " constructing UCX stack " << t <<
      " with " << get_num_worker() << " workers " << dendl;

}

UCXStack::~UCXStack() 
{
}

void UCXStack::spawn_worker(unsigned i, std::function<void ()> &&func)
{
  threads.resize(i+1);
  threads[i] = std::thread(func);
}

void UCXStack::join_worker(unsigned i)
{
  assert(threads.size() > i && threads[i].joinable());
  threads[i].join();
}
