#pragma once

#include "core/auth.hpp"
#include "core/buffer.hpp"
#include "core/common.hpp"
#include "proto/resp2_parser.hpp"
#include "proto/shine_frame.hpp"
#include "transport/address.hpp"
#include "transport/session.hpp"

#include <absl/container/flat_hash_map.h>
#include <boost/asio/experimental/concurrent_channel.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <random>
#include <string>

namespace shine {

struct LinkOptions {
    std::string peer_label;            // for logs/metrics
    std::string password;              // single legacy credential or client secret
    std::vector<AuthUser> users;       // server credentials
    u32         init_window  = Session::kDefaultInitWindow;
    std::chrono::seconds ping_interval{15};
    std::chrono::seconds pong_timeout{10};
    std::chrono::seconds handshake_timeout{5};
    std::size_t max_payload = proto::Resp2Parser::kMaxBulkLen;
    std::size_t max_data_frame = 256 * 1024;  // chunk DATA into <= this many bytes
    bool        is_server   = true;           // server receives AUTH; client sends it
};

// Callback invoked on the inbound-side of a shine link when peer opens a new
// session with NEW. Handler will receive a fully-constructed Session reflecting
// the stream to the peer, plus the target Address.
using NewSessionHandler =
    std::function<awaitable<void>(SessionPtr, Address target)>;

class Link final : public std::enable_shared_from_this<Link> {
public:
    Link(tcp::socket sock, LinkOptions opts);
    Link(std::shared_ptr<ISessionStream> stream, asio::any_io_executor executor,
         LinkOptions opts);
    ~Link();

    // Start reader/writer coroutines. On server links, opens handler is set;
    // on client links, leave it empty (client only opens sessions locally).
    void start(NewSessionHandler on_new_session);

    // Locally open a new session (client-side), returning the Session.
    awaitable<StatusOr<SessionPtr>> openSession(Address target, bool is_udp = false);

    // Wait until handshake_done_ or the link is closed.
    awaitable<bool> waitHandshake();

    // Close gracefully: broadcast CLOSE to all sessions, drain, then close TCP.
    awaitable<void> gracefulShutdown(std::chrono::seconds drain_timeout);

    // Send a fully-encoded frame (writer serialises them to the TCP socket).
    awaitable<void> enqueueFrame(std::string frame_bytes);

    bool isClosed() const noexcept { return closed_.load(std::memory_order_acquire); }

    // Immediately close; used during inbound shutdown.
    void closeNow();

    const LinkOptions& options() const noexcept { return opts_; }
    std::string        peer() const { return opts_.peer_label; }
    std::optional<UserIdentity> authenticatedUser() const { return identity_; }

    asio::any_io_executor executor() noexcept { return strand_; }

private:
    awaitable<void> readerLoop();
    awaitable<void> writerLoop();
    awaitable<void> heartbeatLoop();
    awaitable<void> handshakeServer();
    awaitable<void> handshakeClient();

    awaitable<void> dispatchFrame(proto::ShineFrame frame);
    awaitable<void> failLink(absl::string_view reason);
    void            markClosed();

    SessionPtr findSession(u64 sid);
    void       eraseSession(u64 sid);

    LinkOptions                  opts_;
    asio::strand<asio::any_io_executor> strand_;
    std::shared_ptr<ISessionStream> stream_;
    std::optional<UserIdentity> identity_;

    proto::Resp2Parser parser_;
    ReadBuffer         read_buf_;

    // Writer queue: string of RESP2 frame bytes.
    using WriteChan = asio::experimental::concurrent_channel<void(boost::system::error_code, std::string)>;
    std::unique_ptr<WriteChan> write_chan_;

    absl::flat_hash_map<u64, SessionPtr> sessions_;
    absl::Mutex                          sessions_mu_;

    std::atomic<bool> closed_{false};
    std::atomic<bool> handshake_done_{false};
    using HandshakeSignal = asio::experimental::concurrent_channel<void(boost::system::error_code)>;
    std::unique_ptr<HandshakeSignal> handshake_signal_;

    NewSessionHandler on_new_session_;

    // PING tracking.
    std::atomic<u64> last_pong_ns_{0};
    std::atomic<u64> pending_ping_nonce_{0};
    std::atomic<u64> last_ping_sent_ns_{0};

    std::mt19937_64 rng_;
    absl::Mutex     rng_mu_;

    friend class Session;
};

using LinkPtr = std::shared_ptr<Link>;

// Utility: current steady ns.
inline u64 nowNs() {
    using namespace std::chrono;
    return static_cast<u64>(duration_cast<nanoseconds>(
        steady_clock::now().time_since_epoch()).count());
}

} // namespace shine
