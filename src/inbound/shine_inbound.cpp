#include "inbound/shine_inbound.hpp"

#include "core/logging.hpp"

#include <boost/asio/co_spawn.hpp>

#include <cstring>

namespace shine {

Status ShineInbound::start(SessionRequestHandler on_req) {
    on_req_ = std::move(on_req);
    auto ep = Address::parseHostPort(cfg_.listen);
    if (!ep.ok()) return ep.status();
    if (ep->type() == Address::Type::Domain)
        return absl::InvalidArgumentError("shine listen must be IP");
    boost::system::error_code ec;
    tcp::endpoint real;
    if (ep->type() == Address::Type::IPv4) {
        boost::asio::ip::address_v4::bytes_type b;
        std::memcpy(b.data(), ep->bytes().data(), 4);
        real = tcp::endpoint(boost::asio::ip::make_address_v4(b), ep->port());
    } else {
        boost::asio::ip::address_v6::bytes_type b;
        std::memcpy(b.data(), ep->bytes().data(), 16);
        real = tcp::endpoint(boost::asio::ip::make_address_v6(b), ep->port());
    }
    acceptor_.open(real.protocol(), ec);
    if (ec) return absl::UnavailableError(ec.message());
    acceptor_.set_option(tcp::acceptor::reuse_address(true), ec);
    acceptor_.bind(real, ec);
    if (ec) return absl::UnavailableError(ec.message());
    acceptor_.listen(asio::socket_base::max_listen_connections, ec);
    if (ec) return absl::UnavailableError(ec.message());

    if (cfg_.transport == "redis")
        redis_acceptor_ = std::make_shared<redis::Acceptor>(ex_, cfg_.password, cfg_.users, cfg_.compression, cfg_.handshake_timeout);
    auto self = shared_from_this();
    asio::co_spawn(ex_, [self]() { return self->acceptLoop(); }, asio::detached);
    SHINE_INFO("shine inbound '{}' listening on {}", cfg_.tag, cfg_.listen);
    return absl::OkStatus();
}

void ShineInbound::stop() {
    stopping_.store(true, std::memory_order_release);
    boost::system::error_code ec;
    acceptor_.close(ec);
    if (redis_acceptor_) redis_acceptor_->stop();
    absl::MutexLock lock(&links_mu_);
    for (auto& l : links_) l->closeNow();
    links_.clear();
}

awaitable<void> ShineInbound::acceptLoop() {
    while (!stopping_.load(std::memory_order_acquire)) {
        boost::system::error_code ec;
        auto sock = co_await acceptor_.async_accept(asio::redirect_error(use_awaitable, ec));
        if (ec) { if (!stopping_) SHINE_WARN("shine accept err: {}", ec.message()); co_return; }

        tcp::endpoint rep = sock.remote_endpoint(ec);
        LinkOptions lo;
        lo.peer_label        = rep.address().to_string() + ":" + std::to_string(rep.port());
        lo.password          = cfg_.password;
        lo.users             = cfg_.users;
        lo.init_window       = cfg_.init_window;
        lo.ping_interval     = cfg_.ping_interval;
        lo.pong_timeout      = cfg_.pong_timeout;
        lo.handshake_timeout = cfg_.handshake_timeout;
        lo.max_payload       = cfg_.max_payload;
        lo.is_server         = true;

        if (redis_acceptor_) {
            auto self = shared_from_this();
            asio::co_spawn(redis_acceptor_->executor(), [self, sock = std::move(sock), lo = std::move(lo)]() mutable {
                return self->acceptRedis(std::move(sock), std::move(lo));
            }, asio::detached);
        } else {
            startLink(std::make_shared<Link>(std::move(sock), std::move(lo)));
        }
    }
}

awaitable<void> ShineInbound::acceptRedis(tcp::socket sock, LinkOptions options) {
    auto stream = co_await redis_acceptor_->accept(std::move(sock));
    if (!stream.ok()) co_return;
    if (*stream && !stopping_) startLink(std::make_shared<Link>(*stream, redis_acceptor_->executor(), std::move(options)));
    else if (*stream) (*stream)->close();
}

void ShineInbound::startLink(LinkPtr link) {
        std::string inbound_tag = cfg_.tag;
        std::string inbound_proto = cfg_.protocol;
        auto on_req = on_req_;
        std::weak_ptr<Link> weak_link = link;
        link->start([on_req, inbound_tag, inbound_proto, weak_link](SessionPtr s, Address t) -> awaitable<void> {
            SessionRequest req;
            if (auto owner = weak_link.lock()) req.authenticated_user = owner->authenticatedUser();
            req.inbound_tag      = inbound_tag;
            req.inbound_protocol = inbound_proto;
            req.protocol         = s->protocol();
            req.target           = std::move(t);
            if (req.protocol == SessionRequest::Protocol::UDP) {
                auto ds = std::static_pointer_cast<IDatagramStream>(s);
                auto first = co_await ds->receiveFrom();
                if (!first.ok()) co_return;
                req.target = first->second;
                
                class InjectedStream : public IDatagramStream {
                public:
                    InjectedStream(std::shared_ptr<IDatagramStream> inner, std::shared_ptr<std::string> p, Address t)
                        : inner_(std::move(inner)), p_(std::move(p)), t_(std::move(t)) {}
                    awaitable<Status> sendTo(std::span<const u8> data, class Address target) override { return inner_->sendTo(data, target); }
                    awaitable<StatusOr<std::pair<std::shared_ptr<std::string>, class Address>>> receiveFrom() override {
                        if (p_) {
                            auto ret = std::make_pair(std::move(p_), std::move(t_));
                            p_.reset();
                            co_return ret;
                        }
                        co_return co_await inner_->receiveFrom();
                    }
                    void close() override { inner_->close(); }
                private:
                    std::shared_ptr<IDatagramStream> inner_;
                    std::shared_ptr<std::string> p_;
                    Address t_;
                };
                req.client_datagram_stream = std::make_shared<InjectedStream>(ds, first->first, first->second);
            } else {
                req.client_stream    = std::static_pointer_cast<ISessionStream>(s);
            }
            if (on_req) co_await on_req(std::move(req));
            co_return;
        });
        {
            absl::MutexLock lock(&links_mu_);
            // Remove closed links to prevent unbounded growth.
            links_.erase(
                std::remove_if(links_.begin(), links_.end(),
                    [](const LinkPtr& l) { return !l || l->isClosed(); }),
                links_.end());
            if (stopping_) link->closeNow();
            else links_.push_back(std::move(link));
        }
}

} // namespace shine
