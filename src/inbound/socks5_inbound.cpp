#include "inbound/socks5_inbound.hpp"

#include "core/logging.hpp"
#include "transport/address.hpp"
#include "transport/tcp_stream.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <cstring>

namespace shine {

Status Socks5Inbound::start(SessionRequestHandler on_req) {
    on_req_ = std::move(on_req);
    auto ep = Address::parseHostPort(cfg_.listen);
    if (!ep.ok()) return ep.status();
    boost::system::error_code ec;
    tcp::endpoint real;
    if (ep->type() == Address::Type::IPv4) {
        boost::asio::ip::address_v4::bytes_type b;
        std::memcpy(b.data(), ep->bytes().data(), 4);
        real = tcp::endpoint(boost::asio::ip::make_address_v4(b), ep->port());
    } else if (ep->type() == Address::Type::IPv6) {
        boost::asio::ip::address_v6::bytes_type b;
        std::memcpy(b.data(), ep->bytes().data(), 16);
        real = tcp::endpoint(boost::asio::ip::make_address_v6(b), ep->port());
    } else {
        return absl::InvalidArgumentError("socks5 listen must be IP");
    }
    acceptor_.open(real.protocol(), ec);
    if (ec) return absl::UnavailableError(ec.message());
    acceptor_.set_option(tcp::acceptor::reuse_address(true), ec);
    acceptor_.bind(real, ec);
    if (ec) return absl::UnavailableError(ec.message());
    acceptor_.listen(asio::socket_base::max_listen_connections, ec);
    if (ec) return absl::UnavailableError(ec.message());

    auto self = shared_from_this();
    asio::co_spawn(ex_, [self]() { return self->acceptLoop(); }, asio::detached);
    SHINE_INFO("socks5 inbound '{}' listening on {}", cfg_.tag, cfg_.listen);
    return absl::OkStatus();
}

void Socks5Inbound::stop() {
    stopping_.store(true, std::memory_order_release);
    boost::system::error_code ec;
    acceptor_.close(ec);
}

awaitable<void> Socks5Inbound::acceptLoop() {
    while (!stopping_.load(std::memory_order_acquire)) {
        boost::system::error_code ec;
        auto sock = co_await acceptor_.async_accept(asio::redirect_error(use_awaitable, ec));
        if (ec) {
            if (!stopping_) SHINE_WARN("socks5 accept error: {}", ec.message());
            co_return;
        }
        auto self = shared_from_this();
        asio::co_spawn(ex_,
            [self, s = std::move(sock)]() mutable {
                return self->handleClient(std::move(s));
            },
            asio::detached);
    }
}

namespace {

awaitable<Status> readExact(tcp::socket& s, u8* buf, std::size_t n) {
    boost::system::error_code ec;
    co_await asio::async_read(s, asio::buffer(buf, n),
                              asio::redirect_error(use_awaitable, ec));
    if (ec) co_return absl::UnavailableError(ec.message());
    co_return absl::OkStatus();
}

awaitable<Status> writeAll(tcp::socket& s, const u8* buf, std::size_t n) {
    boost::system::error_code ec;
    co_await asio::async_write(s, asio::buffer(buf, n),
                               asio::redirect_error(use_awaitable, ec));
    if (ec) co_return absl::UnavailableError(ec.message());
    co_return absl::OkStatus();
}

using boost::asio::ip::udp;

class Socks5UdpStream : public IDatagramStream {
public:
    Socks5UdpStream(std::shared_ptr<tcp::socket> ts, udp::socket us)
        : ts_(std::move(ts)), us_(std::move(us)) {}

    awaitable<Status> sendTo(std::span<const u8> data, class Address target) override {
        if (!us_.is_open()) co_return absl::UnavailableError("closed");
        std::string buf;
        buf.push_back(0); buf.push_back(0); buf.push_back(0);
        if (target.type() == Address::Type::IPv4) {
            buf.push_back(0x01);
            buf.append(reinterpret_cast<const char*>(target.bytes().data()), 4);
        } else if (target.type() == Address::Type::IPv6) {
            buf.push_back(0x04);
            buf.append(reinterpret_cast<const char*>(target.bytes().data()), 16);
        } else {
            if (target.domain().empty() || target.domain().size() > 255)
                co_return absl::InvalidArgumentError("invalid SOCKS5 UDP domain");
            buf.push_back(0x03);
            buf.push_back(static_cast<char>(target.domain().size()));
            buf.append(target.domain());
        }
        buf.push_back(static_cast<char>(target.port() >> 8));
        buf.push_back(static_cast<char>(target.port() & 0xff));
        buf.append(reinterpret_cast<const char*>(data.data()), data.size());

        boost::system::error_code ec;
        co_await us_.async_send_to(asio::buffer(buf), client_ep_, asio::redirect_error(use_awaitable, ec));
        if (ec) co_return absl::UnavailableError(ec.message());
        co_return absl::OkStatus();
    }

    awaitable<StatusOr<std::pair<std::shared_ptr<std::string>, class Address>>> receiveFrom() override {
        while (us_.is_open()) {
            std::array<u8, 65536> buf;
            udp::endpoint sender;
            boost::system::error_code ec;
            std::size_t n = co_await us_.async_receive_from(asio::buffer(buf), sender, asio::redirect_error(use_awaitable, ec));
            if (ec) co_return absl::UnavailableError(ec.message());

            if (client_ep_.port() == 0) {
                client_ep_ = sender;
            } else if (sender != client_ep_) {
                continue; // Security: drop packets from unauthenticated sources to prevent hijacking/reflection
            }

            if (n < 4 || buf[0] != 0 || buf[1] != 0 || buf[2] != 0) continue;
            
            Address target;
            std::size_t off = 3;
            u8 atyp = buf[off++];
            if (atyp == 1) { // IPv4
                if (n < off + 4 + 2) continue;
                std::array<u8, 4> ip; std::memcpy(ip.data(), &buf[off], 4); off += 4;
                u16 port = (buf[off] << 8) | buf[off+1]; off += 2;
                target = Address::fromIPv4(ip, port);
            } else if (atyp == 3) { // Domain
                if (n < off + 1) continue;
                u8 dlen = buf[off++];
                if (n < off + dlen + 2) continue;
                std::string d(reinterpret_cast<char*>(&buf[off]), dlen); off += dlen;
                u16 port = (buf[off] << 8) | buf[off+1]; off += 2;
                target = Address::fromDomain(std::move(d), port);
            } else if (atyp == 4) { // IPv6
                if (n < off + 16 + 2) continue;
                std::array<u8, 16> ip; std::memcpy(ip.data(), &buf[off], 16); off += 16;
                u16 port = (buf[off] << 8) | buf[off+1]; off += 2;
                target = Address::fromIPv6(ip, port);
            } else { continue; }

            auto payload = std::make_shared<std::string>(reinterpret_cast<char*>(&buf[off]), n - off);
            
            // Only set actual target on the very first packet if needed, but we return target with every packet
            co_return std::make_pair(std::move(payload), std::move(target));
        }
        co_return absl::UnavailableError("closed");
    }

    void close() override {
        boost::system::error_code ec;
        us_.close(ec);
        ts_->close(ec);
    }

private:
    std::shared_ptr<tcp::socket> ts_;
    udp::socket us_;
    udp::endpoint client_ep_;
};

} // namespace

awaitable<void> Socks5Inbound::handleClient(tcp::socket sock) {
    // --- method select ---
    u8 hdr[2];
    if (!(co_await readExact(sock, hdr, 2)).ok()) co_return;
    if (hdr[0] != 0x05) co_return;
    u8 nm = hdr[1];
    if (nm == 0) co_return;
    std::vector<u8> methods(nm);
    if (!(co_await readExact(sock, methods.data(), nm)).ok()) co_return;

    const bool require_auth = !cfg_.user.empty() || !cfg_.pass.empty();
    u8 chosen = 0xFF;
    for (u8 m : methods) {
        if (!require_auth && m == 0x00) { chosen = 0x00; break; }
        if (require_auth  && m == 0x02) { chosen = 0x02; break; }
    }
    {
        u8 resp[2] = {0x05, chosen};
        if (!(co_await writeAll(sock, resp, 2)).ok()) co_return;
        if (chosen == 0xFF) co_return;
    }

    if (require_auth) {
        // RFC 1929
        u8 v; if (!(co_await readExact(sock, &v, 1)).ok()) co_return;
        if (v != 0x01) co_return;
        u8 ulen; if (!(co_await readExact(sock, &ulen, 1)).ok()) co_return;
        std::string u(ulen, '\0');
        if (ulen && !(co_await readExact(sock, reinterpret_cast<u8*>(u.data()), ulen)).ok()) co_return;
        u8 plen; if (!(co_await readExact(sock, &plen, 1)).ok()) co_return;
        std::string p(plen, '\0');
        if (plen && !(co_await readExact(sock, reinterpret_cast<u8*>(p.data()), plen)).ok()) co_return;
        u8 status = (u == cfg_.user && p == cfg_.pass) ? 0x00 : 0x01;
        u8 resp[2] = {0x01, status};
        if (!(co_await writeAll(sock, resp, 2)).ok()) co_return;
        if (status != 0) co_return;
    }

    // --- request ---
    u8 req_hdr[4];
    if (!(co_await readExact(sock, req_hdr, 4)).ok()) co_return;
    u8 cmd = req_hdr[1];
    if (req_hdr[0] != 0x05 || (cmd != 0x01 && cmd != 0x03)) {  // CONNECT or UDP ASSOCIATE
        u8 rep[10] = {0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0};
        (void)co_await writeAll(sock, rep, 10);
        co_return;
    }
    Address target;
    switch (req_hdr[3]) {
        case 0x01: {
            u8 buf[6];
            if (!(co_await readExact(sock, buf, 6)).ok()) co_return;
            std::array<u8, 4> b{buf[0], buf[1], buf[2], buf[3]};
            u16 port = (static_cast<u16>(buf[4]) << 8) | buf[5];
            target = Address::fromIPv4(b, port);
        } break;
        case 0x03: {
            u8 dlen; if (!(co_await readExact(sock, &dlen, 1)).ok()) co_return;
            std::string d(dlen, '\0');
            if (dlen && !(co_await readExact(sock, reinterpret_cast<u8*>(d.data()), dlen)).ok()) co_return;
            u8 pb[2];
            if (!(co_await readExact(sock, pb, 2)).ok()) co_return;
            u16 port = (static_cast<u16>(pb[0]) << 8) | pb[1];
            target = Address::fromDomain(std::move(d), port);
        } break;
        case 0x04: {
            u8 buf[18];
            if (!(co_await readExact(sock, buf, 18)).ok()) co_return;
            std::array<u8, 16> b;
            std::memcpy(b.data(), buf, 16);
            u16 port = (static_cast<u16>(buf[16]) << 8) | buf[17];
            target = Address::fromIPv6(b, port);
        } break;
        default:
            co_return;
    }

    if (cmd == 0x03) {
        // UDP ASSOCIATE
        boost::system::error_code ec;
        udp::socket us(ex_, udp::v4());
        us.bind(udp::endpoint(udp::v4(), 0), ec);
        if (ec) {
            u8 rep[10] = {0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0};
            (void)co_await writeAll(sock, rep, 10);
            co_return;
        }
        auto lep = us.local_endpoint(ec);
        std::vector<u8> rep;
        rep.push_back(0x05); rep.push_back(0x00); rep.push_back(0x00); rep.push_back(0x01);
        auto bytes = lep.address().to_v4().to_bytes();
        rep.insert(rep.end(), bytes.begin(), bytes.end());
        rep.push_back((lep.port() >> 8) & 0xFF);
        rep.push_back(lep.port() & 0xFF);
        if (!(co_await writeAll(sock, rep.data(), rep.size())).ok()) co_return;

        auto ts = std::make_shared<tcp::socket>(std::move(sock));
        auto udp_stream = std::make_shared<Socks5UdpStream>(ts, std::move(us));

        // Wait for first UDP packet to determine routing target
        auto first = co_await udp_stream->receiveFrom();
        if (!first.ok()) co_return;

        // Monitor TCP connection to terminate UDP association
        asio::co_spawn(ex_, [ts, udp_stream]() -> awaitable<void> {
            char b; boost::system::error_code e;
            co_await asio::async_read(*ts, asio::buffer(&b, 1), asio::redirect_error(use_awaitable, e));
            udp_stream->close();
        }, asio::detached);

        SessionRequest r;
        r.inbound_tag            = cfg_.tag;
        r.inbound_protocol       = cfg_.protocol;
        r.protocol               = SessionRequest::Protocol::UDP;
        r.target                 = first->second; // Route based on first packet destination
        r.client_datagram_stream = udp_stream;
        if (on_req_) {
            // Forward the first packet because receiveFrom() consumed it
            auto forwarder = [udp_stream, payload = first->first, target = first->second](SessionRequest req, SessionRequestHandler handler) -> awaitable<void> {
                // Wrapper to inject first packet before returning to receive loop
                class InjectedStream : public IDatagramStream {
                public:
                    InjectedStream(std::shared_ptr<IDatagramStream> inner, std::shared_ptr<std::string> p, Address t)
                        : inner_(inner), p_(p), t_(t) {}
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
                req.client_datagram_stream = std::make_shared<InjectedStream>(udp_stream, payload, target);
                co_await handler(std::move(req));
            };
            co_await forwarder(std::move(r), on_req_);
        }
        
        // Ensure the control socket is closed when the session terminates
        // This will trigger the monitor coroutine to abort and clean up.
        boost::system::error_code ignore_ec;
        ts->close(ignore_ec);
    } else {
        // Reply "succeeded" with local-bound dummy (0.0.0.0:0)
        u8 rep[10] = {0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0};
        if (!(co_await writeAll(sock, rep, 10)).ok()) co_return;

        SessionRequest r;
        r.inbound_tag      = cfg_.tag;
        r.inbound_protocol = cfg_.protocol;
        r.protocol         = SessionRequest::Protocol::TCP;
        r.target           = std::move(target);
        r.client_stream    = std::make_shared<TcpSocketStream>(std::move(sock));
        if (on_req_) co_await on_req_(std::move(r));
    }
}

} // namespace shine
