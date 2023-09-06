// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string.h>

#include "rgw_extern_iam.h"
#include "rgw_http_client.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw


int RGWExternIAMAuthorize::eval(const DoutPrefixProvider *dpp,
                                const rgw::IAM::Environment& env,
                                const rgw::ARN& resource, uint64_t op,
                                boost::optional<const rgw::auth::Identity&> ida,
                                rgw::IAM::Effect& effect)
{
  bufferlist bl;
  RGWHTTPTransceiver req(cct, "POST", addr(), &bl);

  req.set_headers(headers);
  req.set_verify_ssl(verify_ssl());

  JSONFormatter jf;
  jf.open_object_section("");
  jf.open_object_section("env");
  for (auto &e : env) {
    jf.dump_string(e.first, e.second);
  }
  jf.close_section();
  jf.dump_string("arn", resource.to_string());
  jf.dump_string("op", rgw::IAM::action_bit_string(op));
  if (likely(ida != boost::none)) {
    jf.dump_string("subuser", ida->get_subuser());
    ida->get_rgw_user().dump(&jf);
  }
  jf.close_section();

  std::stringstream ss;
  jf.flush(ss);
  req.set_post_data(ss.str());
  req.set_send_length(ss.str().length());

  int ret = req.process(null_yield);
  if (unlikely(ret < 0)) {
    ldpp_dout(dpp, 0) << "ERROR: External IAM process error:" << bl.c_str() << " ret:" << ret << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  JSONParser parser;
  if (unlikely(!parser.parse(bl.c_str(), bl.length()))) {
    ldpp_dout(dpp, 0) << "ERROR: External IAM parse error: malformed json: " << bl.c_str() << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  JSONObj::data_val val;
  if (likely(parser.get_data("Effect", &val))) {
    auto res = val.str.c_str();
    if (strcmp(res, "Allow") == 0) {
      effect = rgw::IAM::Effect::Allow;
      return 0;
    } else if (strcmp(res, "Pass") == 0) {
      effect = rgw::IAM::Effect::Pass;
      return 0;
    } else if (strcmp(res, "Deny") == 0) {
      effect = rgw::IAM::Effect::Deny;
      return 0;
    }
  }

  ldpp_dout(dpp, 0) << "ERROR: External IAM unknown json response:" << bl.c_str() << dendl;
  return -ERR_INTERNAL_ERROR;
}
