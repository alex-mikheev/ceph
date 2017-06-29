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

#ifndef CEPH_MSG_UCXSTACK_H
#define CEPH_MSG_UCXSTACK_H

#include <vector>
#include <thread>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "msg/async/Stack.h"

class UCXWorker : public Worker {

 public:
  explicit UCXWorker(CephContext *c, unsigned i);
  virtual ~UCXWorker();
  virtual int listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  virtual void initialize() override;
};

class UCXConnectedSocketImpl : public ConnectedSocketImpl {
  public:
    UCXConnectedSocketImpl(UCXWorker *w);
    virtual ~UCXConnectedSocketImpl();

    virtual int is_connected();
    virtual ssize_t read(char*, size_t);
    virtual ssize_t zero_copy_read(bufferptr&);
    virtual ssize_t send(bufferlist &bl, bool more);
    virtual void shutdown();
    virtual void close();
    virtual int fd();
};

class UCXServerSocketImpl : public ServerSocketImpl {

  public:
    UCXServerSocketImpl(UCXWorker *w, entity_addr_t& a);

    virtual int accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w) = 0;
    virtual void abort_accept() = 0;
    // Get file descriptor
    virtual int fd() const = 0;
};

class UCXStack : public NetworkStack {
  vector<std::thread> threads;
 public:
  explicit UCXStack(CephContext *cct, const string &t);
  virtual ~UCXStack();
  virtual bool support_zero_copy_read() const override { return false; }
  virtual bool nonblock_connect_need_writable_event() const { return false; }

  virtual void spawn_worker(unsigned i, std::function<void ()> &&func) override;
  virtual void join_worker(unsigned i) override;

};

#endif
