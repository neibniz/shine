#pragma once

#include "core/common.hpp"
#include "core/auth.hpp"

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace shine::config {

struct InboundConfig {
    std::string tag;
    std::string protocol;                // socks5 | http-connect | shine
    std::string listen;                  // "host:port"
    std::string password;                // legacy single Shine credential
    std::vector<AuthUser> users;
    std::string transport = "redis";     // redis | tcp
    bool compression = false;
    std::size_t max_payload     = 512ull * 1024 * 1024;
    std::chrono::seconds ping_interval{15};
    std::chrono::seconds pong_timeout{10};
    std::chrono::seconds handshake_timeout{5};
    u32 init_window = 256 * 1024;
    // socks5 optional user-pass
    std::string user;
    std::string pass;
};

struct OutboundConfig {
    std::string tag;
    std::string protocol;                // direct | shine-client | socks5 | http-connect
    std::string server;                  // "host:port" for client-mode
    std::string password;
    std::vector<AuthUser> users;
    std::string transport = "redis";     // redis | tcp
    bool compression = false;
    std::size_t pool_size   = 1;
    std::chrono::seconds ping_interval{15};
    std::chrono::seconds pong_timeout{10};
    std::chrono::seconds handshake_timeout{5};
    u32 init_window = 256 * 1024;
    std::string user;
    std::string pass;
};

struct RuleConfig {
    std::optional<std::string> inbound_tag;
    std::vector<std::string>   domain_exact;
    std::vector<std::string>   domain_suffix;
    std::vector<std::string>   cidrs;
    std::vector<std::string>   geoip;
    std::vector<std::string>   geosite;
    std::string                outbound_tag;
};

struct RouteConfig {
    std::string             geoip_path;
    std::string             geosite_path;
    std::vector<RuleConfig> rules;
    std::string             default_outbound;
};

struct LogConfig {
    std::string level  = "info";
    std::string format = "text";
};

struct MetricsConfig {
    std::string listen = "127.0.0.1:9100";
    std::string path   = "/metrics";
};

struct ServerConfig {
    std::size_t io_threads = 0;
    std::chrono::seconds graceful_timeout{30};
};

struct Config {
    LogConfig                   log;
    ServerConfig                server;
    MetricsConfig               metrics;
    std::vector<InboundConfig>  inbounds;
    std::vector<OutboundConfig> outbounds;
    RouteConfig                 route;

    // Validate: at least one inbound, at least one direct outbound, unique
    // tags, default outbound exists.
    Status validate() const;
};

} // namespace shine::config
