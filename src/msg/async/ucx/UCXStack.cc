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

#include "UCXStack.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "UCXStack "


UCXConnectedSocketImpl::UCXConnectedSocketImpl(UCXWorker *w) :
  worker(w)
{
}

UCXConnectedSocketImpl::~UCXConnectedSocketImpl()
{
}

int UCXConnectedSocketImpl::is_connected()
{
  lderr(cct()) << __func__ << " is " << (tcp_fd > 0) << dendl;
  return tcp_fd > 0;
}

// TODO: consider doing a completely non blockig connect/accept
//do blocking connect
int UCXConnectedSocketImpl::connect(const entity_addr_t& peer_addr, const SocketOptions &opts)
{
  NetHandler net(cct());
  int ret;

  lderr(cct()) << __func__ << dendl;

  tcp_fd = net.connect(peer_addr, opts.connect_bind_addr);
  if (tcp_fd < 0) {
    return -errno;
  }

  ldout(cct(), 20) << __func__ << " tcp_fd: " << tcp_fd << dendl;
  net.set_close_on_exec(tcp_fd);

  ret = net.set_socket_options(tcp_fd, opts.nodelay, opts.rcbuf_size);
  if (ret < 0) {
    lderr(cct()) << __func__ << " failed to set socket options" << dendl;
    goto err;
  }

  net.set_priority(tcp_fd, opts.priority, peer_addr.get_family());
  
  ret = worker->send_addr(tcp_fd, reinterpret_cast<uint64_t>(this));
  if (ret != 0) 
    goto err;

  ret = worker->recv_addr(tcp_fd, &ucp_ep, &dst_tag);
  if (ret != 0)
    goto err;

  return ret; 
err:
  ::close(tcp_fd);
  tcp_fd = -1;
  return ret;
}

// do blocking accept()
int UCXConnectedSocketImpl::accept(int server_sock, entity_addr_t *out, const SocketOptions &opt)
{
  NetHandler net(cct());
  int ret = 0;

  sockaddr_storage ss;
  socklen_t slen = sizeof(ss);
  lderr(cct()) << __func__ << " 3 " << dendl;
  tcp_fd = ::accept(server_sock, (sockaddr*)&ss, &slen);
  if (tcp_fd < 0) {
    return -errno;
  }

  net.set_close_on_exec(tcp_fd);

  lderr(cct()) << __func__ << " 4 " << dendl;
  ret = net.set_socket_options(tcp_fd, opt.nodelay, opt.rcbuf_size);
  if (ret < 0) {
    ret = -errno;
    goto err;
  }

  lderr(cct()) << __func__ << " 2 " << dendl;
  assert(NULL != out); //out should not be NULL in accept connection
  out->set_sockaddr((sockaddr*)&ss);
  net.set_priority(tcp_fd, opt.priority, out->get_family());

  lderr(cct()) << __func__ << " 1 " << dendl;
  ret = worker->recv_addr(tcp_fd, &ucp_ep, &dst_tag);
  if (ret != 0)
    goto err;

  lderr(cct()) << __func__ << "ADDRESS RECVD" << dendl;
  ret = worker->send_addr(tcp_fd, reinterpret_cast<uint64_t>(this));
  if (ret != 0) 
    goto err;

  lderr(cct()) << __func__ << "ADDRESS SENT" << dendl;
  return 0;

err:
  ::close(tcp_fd);
  tcp_fd = -1;
  lderr(cct()) << __func__ << " failed accept " << ret << dendl;
  return ret;
}
    

ssize_t UCXConnectedSocketImpl::read(char*, size_t)
{
  lderr(cct()) << __func__ << dendl;
  return 0;
}

ssize_t UCXConnectedSocketImpl::zero_copy_read(bufferptr&)
{
  lderr(cct()) << __func__ << dendl;
  return 0;
}

ssize_t UCXConnectedSocketImpl::send(bufferlist &bl, bool more)
{
  lderr(cct()) << __func__ << dendl;
  return 0;
}

void UCXConnectedSocketImpl::shutdown()
{
  lderr(cct()) << __func__ << dendl;
}

void UCXConnectedSocketImpl::close()
{
  lderr(cct()) << __func__ << dendl;
}

UCXServerSocketImpl::UCXServerSocketImpl(UCXWorker *w) :
  worker(w) 
{
}

UCXServerSocketImpl::~UCXServerSocketImpl()
{
  if (tcp_fd >= 0)
    ::close(tcp_fd);
}

int UCXServerSocketImpl::listen(entity_addr_t &sa, const SocketOptions &opt)
{
  NetHandler net(cct()); 
  int rc;

  tcp_fd = net.create_socket(sa.get_family(), true);
  if (tcp_fd < 0) {
    lderr(cct()) << __func__ << " failed to create server socket: "
               << cpp_strerror(errno) << dendl;
    return -errno;
  }

  rc = net.set_nonblock(tcp_fd);
  if (rc < 0) {
    goto err;
  }

  net.set_close_on_exec(tcp_fd);

  rc = ::bind(tcp_fd, sa.get_sockaddr(), sa.get_sockaddr_len());
  if (rc < 0) {
    ldout(cct(), 10) << __func__ << " unable to bind to " << sa.get_sockaddr()
                   << " on port " << sa.get_port() << ": " << cpp_strerror(errno) << dendl;
    goto err;
  }

  rc = ::listen(tcp_fd, 128);
  if (rc < 0) {
    lderr(cct()) << __func__ << " unable to listen on " << sa << ": " << cpp_strerror(errno) << dendl;
    goto err;
  }

  ldout(cct(), 20) << __func__ << " bind to " << sa.get_sockaddr() << " on port " << sa.get_port()  << dendl;
  return 0;

err:
  ::close(tcp_fd);
  tcp_fd = -1;
  return -errno;
}

int UCXServerSocketImpl::accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w)
{
  UCXConnectedSocketImpl *p = new UCXConnectedSocketImpl(dynamic_cast<UCXWorker *>(w));

  int r = p->accept(tcp_fd, out, opt);
  if (r < 0) {
    ldout(cct(), 1) << __func__ << " accept failed. ret = " << r << dendl;
    delete p;
    return r;
  }
  std::unique_ptr<UCXConnectedSocketImpl> csi(p);
  *sock = ConnectedSocket(std::move(csi));
  return 0;
}

void UCXServerSocketImpl::abort_accept()
{
  if (tcp_fd >= 0)
    return;
  ::close(tcp_fd);
  tcp_fd = -1;
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
  UCXServerSocketImpl *p = new UCXServerSocketImpl(this);

  int r = p->listen(addr, opts);
  if (r < 0) {
    delete p;
    return r;
  }

  *sock = ServerSocket(std::unique_ptr<ServerSocketImpl>(p));
  return 0;
}


int UCXWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *sock)
{
  UCXConnectedSocketImpl *p = new UCXConnectedSocketImpl(this);

  int r = p->connect(addr, opts);
  if (r < 0) {
    ldout(cct, 1) << __func__ << " try connecting failed." << dendl;
    delete p;
    return r;
  }
  std::unique_ptr<UCXConnectedSocketImpl> csi(p);
  *sock = ConnectedSocket(std::move(csi));
  return 0;
}

void UCXWorker::initialize()
{
  ucs_status_t status;
  ucp_worker_params_t params;

  params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  // TODO: check if we need a multi threaded mode
  params.thread_mode = UCS_THREAD_MODE_SINGLE;

  status = ucp_worker_create(get_stack()->get_ucp_context(), &params, &ucp_worker);
  if (status != UCS_OK) {
    lderr(cct) << __func__ << " failed to create UCP worker " << dendl;
    ceph_abort();
  }
  if (id == 0) 
    ucp_worker_print_info(ucp_worker, stdout);

  status = ucp_worker_get_address(ucp_worker, &ucp_addr, &ucp_addr_len);
  if (status != UCS_OK) {
    lderr(cct) << __func__ << " failed to obtain worker address " << dendl;
    ceph_abort();
  }
} 

void UCXWorker::destroy()
{
  ucp_worker_release_address(ucp_worker, ucp_addr);
  ucp_worker_destroy(ucp_worker);
} 

void UCXWorker::set_stack(UCXStack *s)
{ 
  stack = s;
}

int UCXWorker::send_addr(int sock, uint64_t tag)
{
  ucx_connect_message msg;
  int rc;

  // send connected message
  msg.addr_len = ucp_addr_len;
  msg.tag      = tag; 

  rc = ::write(sock, &msg, sizeof(msg));
  if (rc != sizeof(msg)) {
    lderr(cct) << __func__ << " failed to send connect msg header" << dendl;
    return -errno;
  }

  rc = ::write(sock, ucp_addr, ucp_addr_len);
  if (rc != (int)ucp_addr_len) {
    lderr(cct) << __func__ << " failed to send worker address " << dendl;
    return -errno;
  }

  return 0;
}

int UCXWorker::recv_addr(int sock, ucp_ep_h *ep, uint64_t *dst_tag)
{
  ucx_connect_message msg;
  int rc, ret = 0;
  char *addr_buf;
  ucs_status_t status;
  ucp_ep_params_t params;

  // get our peer address 
  rc = ::read(sock, &msg, sizeof(msg));
  if (rc != sizeof(msg)) {
    lderr(cct) << __func__ << " failed to recv connect msg header" << dendl;
    return -errno;
  }

  ldout(cct, 1) << __func__ << " received tag: " << msg.tag <<
                " addr len: " << msg.addr_len << dendl;

  *dst_tag = msg.tag;
  addr_buf = new char [msg.addr_len];
  rc = ::read(sock, addr_buf, ucp_addr_len);
  if (rc != (int)ucp_addr_len) {
    lderr(cct) << __func__ << " failed to recv worker address " << dendl;
    ret = -errno;
    goto out;
  }

  params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
  params.address    = reinterpret_cast<ucp_address_t *>(addr_buf);
  status = ucp_ep_create(ucp_worker, &params, ep);
  if (status != UCS_OK) {
    lderr(cct) << __func__ << " failed to create UCP endpoint " << dendl;
    ret = -EINVAL;
  }

out:
  delete [] addr_buf;
  return ret;
}

UCXStack::UCXStack(CephContext *cct, const string &t) :
   NetworkStack(cct, t) 
{

    ucs_status_t status;
    ucp_config_t *ucp_config;
    ucp_params_t params;

    ldout(cct, 0) << __func__ << " constructing UCX stack " << t <<
      " with " << get_num_worker() << " workers " << dendl;

    status = ucp_config_read("CEPH", NULL, &ucp_config);
    if (UCS_OK != status) {
      lderr(cct) << __func__ << "failed to read UCP config" << dendl;
      ceph_abort();
    }

    memset(&params, 0, sizeof(params));
    params.field_mask = UCP_PARAM_FIELD_FEATURES|
                        UCP_PARAM_FIELD_TAG_SENDER_MASK|
                        UCP_PARAM_FIELD_MT_WORKERS_SHARED;
    params.features   = UCP_FEATURE_TAG|UCP_FEATURE_WAKEUP;
    params.mt_workers_shared = 1;
    params.tag_sender_mask = -1;

    status = ucp_init(&params, ucp_config, &ucp_context);
    ucp_config_release(ucp_config);
    if (UCS_OK != status) {
      lderr(cct) << __func__ << "failed to init UCP context" << dendl;
      ceph_abort();
    }
    ucp_context_print_info(ucp_context, stdout);

    for (unsigned i = 0; i < get_num_worker(); i++) {
      UCXWorker *w = dynamic_cast<UCXWorker *>(get_worker(i));
      w->set_stack(this);
    }
}

UCXStack::~UCXStack() 
{
  ucp_cleanup(ucp_context);
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
