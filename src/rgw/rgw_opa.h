// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_OPA_H
#define RGW_OPA_H

#include "rgw_common.h"
#include "rgw_op.h"

/* authorize request using OPA */
int rgw_opa_authorize(RGWOp*& op,
                      req_state* s);

/* Send bucket policy to opa */
int rgw_send_bucket_policy_to_opa(RGWOp* op,
                      req_state* s,
                      std::string policy);

#endif /* RGW_OPA_H */
