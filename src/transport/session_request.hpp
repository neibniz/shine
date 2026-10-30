#pragma once

#include "core/common.hpp"
#include "core/auth.hpp"
#include "transport/address.hpp"
#include "transport/session_stream.hpp"

#include <memory>
#include <string>

namespace shine {

// A routing-visible request: "inbound X wants to send bytes to target".
// The actual bytestream is an ISessionStream (either a wrapped tcp::socket or
// a shine Session).
struct SessionRequest {
    enum class Protocol { TCP, UDP };

    std::string                       inbound_tag;
    std::string                       inbound_protocol;
    Protocol                          protocol = Protocol::TCP;
    Address                           target;
    std::optional<UserIdentity>       authenticated_user;
    std::shared_ptr<ISessionStream>   client_stream;
    std::shared_ptr<IDatagramStream>  client_datagram_stream;
};

} // namespace shine
