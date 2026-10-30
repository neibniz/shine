#include "redis/fake.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <limits>

namespace shine::redis {
namespace {
std::string upper(std::string s) {
  for (auto &c : s)
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  return s;
}
std::string lower(std::string s) {
  for (auto &c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}
std::string safe(std::string s) {
  if (s.size() > 64)
    s.resize(64);
  for (auto &c : s)
    if (c < 33 || c > 126 || c == '\'' || c == '\\')
      c = '?';
  return s;
}
std::string num(i64 n) { return ":" + std::to_string(n) + "\r\n"; }
std::string err(const std::string &s) { return "-ERR " + s + "\r\n"; }
std::string wrong(const std::string &s) {
  return err("wrong number of arguments for '" + lower(s) + "' command");
}
bool integer(const std::string &s, i64 &n) {
  const char *begin = s.data();
  const char *end = begin + s.size();
  if (begin != end && *begin == '+') {
    ++begin;
    if (begin != end && (*begin == '+' || *begin == '-'))
      return false;
  }
  if (begin == end)
    return false;
  auto [p, e] = std::from_chars(begin, end, n);
  return e == std::errc{} && p == end;
}

std::chrono::steady_clock::time_point expiration(i64 milliseconds) {
  auto now = std::chrono::steady_clock::now();
  auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::time_point::max() - now)
                       .count();
  if (milliseconds >= remaining)
    return std::chrono::steady_clock::time_point::max();
  return now + std::chrono::milliseconds(milliseconds);
}
const std::string noauth = "-NOAUTH Authentication required.\r\n";
const std::string wrongpass =
    "-WRONGPASS invalid username-password pair or user is disabled.\r\n";
const std::string no_config =
    err("AUTH <password> called without any password configured for the "
        "default user. Are you sure your configuration is correct?");
} // namespace
Fake::Fake(std::string password, std::vector<AuthUser> users)
    : password_(std::move(password)), users_(std::move(users)) {}
bool Fake::authenticate(const std::string &p) const {
  if (!password_.empty() && authEquals(password_, p))
    return true;
  for (auto &u : users_)
    if (authEquals(u.auth, p))
      return true;
  return false;
}
void Fake::purge() {
  auto now = std::chrono::steady_clock::now();
  for (auto it = data_.begin(); it != data_.end();)
    if (it->second.expires != decltype(it->second.expires){} &&
        it->second.expires <= now)
      it = data_.erase(it);
    else
      ++it;
}
FakeReply Fake::reply(const Value &cmd, FakeSession &s) {
  if (!bulkArray(cmd, cmd.array.size()) || cmd.array.empty())
    return {true, false, err("Protocol error: expected bulk arguments")};
  std::vector<std::string> p;
  for (auto &v : cmd.array)
    p.push_back(v.text);
  auto name = upper(p[0]);
  auto n = p.size();
  auto result = [](std::string v) {
    return FakeReply{true, false, std::move(v)};
  };
  if (name == "AUTH") {
    if (n != 2 && n != 3)
      return result(wrong(name));
    if (!required())
      return result(no_config);
    if ((n == 3 && p[1] != "default") || !authenticate(p.back()))
      return result(wrongpass);
    s.authenticated = true;
    s.secret = p.back();
    return result("+OK\r\n");
  }
  if (name == "HELLO") {
    if (n > 1 && p[1] != "2")
      return result("-NOPROTO unsupported protocol version\r\n");
    for (size_t i = 2; i < n; ++i) {
      auto opt = upper(p[i]);
      if (opt == "AUTH") {
        if (i + 2 >= n)
          return result(err("Syntax error in HELLO option 'AUTH'"));
        if (!required())
          return result(no_config);
        if (p[i + 1] != "default" || !authenticate(p[i + 2]))
          return result(wrongpass);
        s.authenticated = true;
        s.secret = p[i + 2];
        i += 2;
      } else if (opt == "SETNAME") {
        if (i + 1 >= n)
          return result(err("Syntax error in HELLO option 'SETNAME'"));
        const auto &proposed = p[++i];
        for (unsigned char c : proposed)
          if (c <= 32 || c >= 127)
            return result(err("Client names cannot contain spaces, newlines or "
                              "special characters."));
        s.name = proposed;
      } else
        return result(err("Syntax error in HELLO option '" + safe(opt) + "'"));
    }
    if (!s.authenticated)
      return result("-NOAUTH HELLO must be called with the client already "
                    "authenticated, otherwise the HELLO AUTH <user> <pass> "
                    "option can be used to authenticate the client and select "
                    "the RESP protocol version at the same time\r\n");
    return result("*14\r\n" + bulk("server") + bulk("redis") + bulk("version") +
                  bulk("7.2.10") + bulk("proto") + num(2) + bulk("id") +
                  num(s.id) + bulk("mode") + bulk("standalone") + bulk("role") +
                  bulk("master") + bulk("modules") + "*0\r\n");
  }
  if (!s.authenticated)
    return result(noauth);
  if (name == "SUBSCRIBE" || name == "PUBLISH")
    return {false, false, {}};
  if (name == "PING")
    return result(n == 1 ? "+PONG\r\n" : n == 2 ? bulk(p[1]) : wrong(name));
  if (name == "ECHO")
    return result(n == 2 ? bulk(p[1]) : wrong(name));
  if (name == "QUIT")
    return n == 1 ? FakeReply{true, true, "+OK\r\n"} : result(wrong(name));
  if (name == "SELECT") {
    i64 db;
    if (n != 2)
      return result(wrong(name));
    if (!integer(p[1], db))
      return result(err("invalid DB index"));
    if (db < 0 || db >= 16)
      return result(err("DB index is out of range"));
    s.db = static_cast<int>(db);
    return result("+OK\r\n");
  }
  if (name == "CLIENT") {
    if (n < 2)
      return result(wrong(name));
    auto sub = upper(p[1]);
    if (sub == "ID")
      return result(n == 2 ? num(s.id) : wrong("client|id"));
    if (sub == "GETNAME")
      return result(n == 2 ? (s.name.empty() ? "$-1\r\n" : bulk(s.name))
                           : wrong("client|getname"));
    if (sub == "SETNAME") {
      if (n != 3)
        return result(wrong("client|setname"));
      for (unsigned char c : p[2])
        if (c <= 32 || c >= 127)
          return result(err("Client names cannot contain spaces, newlines or "
                            "special characters."));
      s.name = p[2];
      return result("+OK\r\n");
    }
    if (sub == "SETINFO") {
      if (n != 4)
        return result(wrong("client|setinfo"));
      auto field = upper(p[2]);
      return result(field == "LIB-NAME" || field == "LIB-VER"
                        ? "+OK\r\n"
                        : err("syntax error"));
    }
    return result(
        err("unknown subcommand '" + safe(sub) + "'. Try CLIENT HELP."));
  }
  purge();
  if (name == "INFO") {
    if (n > 2)
      return result(wrong(name));
    auto section = n == 2 ? lower(p[1]) : "default";
    std::string info;
    if (section == "default" || section == "all" || section == "server")
      info = "# "
             "Server\r\nredis_version:7.2.10\r\nredis_mode:"
             "standalone\r\nuptime_in_seconds:" +
             std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                                std::chrono::steady_clock::now() - started_)
                                .count()) +
             "\r\n\r\n";
    if (section == "default" || section == "all" || section == "replication")
      info += "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n\r\n";
    if (section == "default" || section == "all" || section == "keyspace") {
      info += "# Keyspace\r\n";
      for (int db = 0; db < 16; ++db) {
        i64 count = 0, ex = 0, total = 0;
        for (auto &[k, v] : data_)
          if (k.first == db) {
            ++count;
            if (v.expires != decltype(v.expires){}) {
              ++ex;
              total += std::chrono::duration_cast<std::chrono::milliseconds>(
                           v.expires - std::chrono::steady_clock::now())
                           .count();
            }
          }
        if (count)
          info += "db" + std::to_string(db) + ":keys=" + std::to_string(count) +
                  ",expires=" + std::to_string(ex) +
                  ",avg_ttl=" + std::to_string(ex ? total / ex : 0) + "\r\n";
      }
      info += "\r\n";
    }
    return result(bulk(info));
  }
  if (name == "DBSIZE") {
    if (n != 1)
      return result(wrong(name));
    i64 count = 0;
    for (auto &[k, v] : data_)
      if (k.first == s.db)
        ++count;
    return result(num(count));
  }
  if (name == "GET" || name == "TYPE" || name == "TTL" || name == "PTTL") {
    if (n != 2)
      return result(wrong(name));
    auto it = data_.find({s.db, p[1]});
    if (name == "GET")
      return result(it == data_.end() ? "$-1\r\n" : bulk(it->second.value));
    if (name == "TYPE")
      return result(it == data_.end() ? "+none\r\n" : "+string\r\n");
    if (it == data_.end())
      return result(num(-2));
    if (it->second.expires == decltype(it->second.expires){})
      return result(num(-1));
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  it->second.expires - std::chrono::steady_clock::now())
                  .count();
    return result(num(name == "PTTL" ? ms : (ms + 500) / 1000));
  }
  if (name == "MGET" || name == "DEL" || name == "EXISTS") {
    if (n < 2)
      return result(wrong(name));
    std::string out = "*" + std::to_string(n - 1) + "\r\n";
    i64 count = 0;
    size_t total = 0;
    for (size_t i = 1; i < n; ++i) {
      auto it = data_.find({s.db, p[i]});
      if (it != data_.end()) {
        ++count;
        if (name == "MGET") {
          total += it->second.value.size();
          if (total > 4u * 1024u * 1024u)
            return result(err("simulated response exceeds limit"));
          out += bulk(it->second.value);
        }
        if (name == "DEL")
          data_.erase(it);
      } else if (name == "MGET")
        out += "$-1\r\n";
    }
    return result(name == "MGET" ? out : num(count));
  }
  if (name == "EXPIRE" || name == "PEXPIRE") {
    if (n != 3)
      return result(wrong(name));
    i64 t;
    if (!integer(p[2], t))
      return result(err("value is not an integer or out of range"));
    i64 unit = name == "EXPIRE" ? 1000 : 1;
    if (t > std::numeric_limits<i64>::max() / unit / 1000000 ||
        t < std::numeric_limits<i64>::min() / unit / 1000000)
      return result(err("invalid expire time"));
    auto it = data_.find({s.db, p[1]});
    if (it == data_.end())
      return result(num(0));
    if (t <= 0)
      data_.erase(it);
    else
      it->second.expires = expiration(t * unit);
    return result(num(1));
  }
  if (name == "SET") {
    if (n < 3)
      return result(wrong(name));
    std::string mode;
    auto expiry = std::chrono::steady_clock::time_point{};
    bool has_exp = false;
    for (size_t i = 3; i < n; ++i) {
      auto opt = upper(p[i]);
      if (opt == "NX" || opt == "XX") {
        if (!mode.empty())
          return result(err("syntax error"));
        mode = opt;
      } else if (opt == "EX" || opt == "PX") {
        if (has_exp || i + 1 >= n)
          return result(err("syntax error"));
        i64 t;
        if (!integer(p[++i], t))
          return result(err("value is not an integer or out of range"));
        i64 unit = opt == "EX" ? 1000 : 1;
        if (t <= 0 || t > std::numeric_limits<i64>::max() / unit / 1000000)
          return result(err("invalid expire time in 'set' command"));
        expiry = expiration(t * unit);
        has_exp = true;
      } else
        return result(err("syntax error"));
    }
    Key key{s.db, p[1]};
    auto it = data_.find(key);
    if ((mode == "NX" && it != data_.end()) ||
        (mode == "XX" && it == data_.end()))
      return result("$-1\r\n");
    size_t bytes = 0;
    for (auto &[k, v] : data_)
      bytes += k.second.size() + v.value.size();
    if (it != data_.end())
      bytes -= it->first.second.size() + it->second.value.size();
    if (bytes + p[1].size() + p[2].size() > 4u * 1024u * 1024u ||
        (it == data_.end() && data_.size() >= 1024))
      return result(
          "-OOM command not allowed when used memory > 'maxmemory'.\r\n");
    data_[key] = {p[2], expiry};
    return result("+OK\r\n");
  }
  return result(err("unknown command '" + safe(name) + "'"));
}
} // namespace shine::redis
