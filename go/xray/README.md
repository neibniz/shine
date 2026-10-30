# Shine 的 Xray-core 适配层

本模块将 [独立 Shine 协议库](../shine/README.md)接入 Xray-core：

- `Inbound` 接收 Shine 连接，将每个 TCP / UDP 会话交给 Xray 的路由器。
- `Outbound` 使用 Xray 的 dialer 建立上游连接，并在多个会话间复用；链路断开后，下一个请求重新连接。
- Protobuf 配置与 Xray 对象工厂在包初始化时注册。`conf` 包提供 JSON 配置构建器。

适配层针对 Xray-core 提交 `0afec66085a85191e9dd2a4a6646c7c9a94688d1`（2026-10-29）编译并测试，要求 Go 1.27。其他版本的接口和注册代码可能不同，需要重新适配和测试。

## 加入 Xray-core 并构建

Xray 的 JSON 协议名称表目前是内部静态表，所以仅导入包不能注册 `"shine"`。下面的脚本会对指定检出应用 [配置注册补丁](patches/xray-core.patch)，并在该检出的 `go.mod` 中添加本地模块引用。执行前请确认该检出没有与这些文件冲突的修改；脚本只接受上面的 Xray 提交。

```sh
/path/to/shine/go/xray/scripts/install-into-xray.sh /path/to/Xray-core
cd /path/to/Xray-core
GOWORK=off go build -mod=mod -o xray ./main
```

脚本使用绝对路径引用本地 Shine 和适配层目录；移动这些目录后需更新 Xray 检出中的 `go.mod`。这是源码集成，不能把适配包动态加载到现成的官方二进制中。

## 配置示例

服务端，收到的 Shine 会话交由 Xray 路由：

```json
{
  "inbounds": [{
    "tag": "shine-in",
    "listen": "0.0.0.0",
    "port": 7011,
    "protocol": "shine",
    "settings": {"users": [{"auth": "replace-me", "email": "alice@example.com"}]}
  }],
  "outbounds": [{"protocol": "freedom", "tag": "direct"}]
}
```

客户端，可将 Xray 其他入口的流量转发到 Shine 上游：

```json
{
  "inbounds": [{
    "listen": "127.0.0.1",
    "port": 1080,
    "protocol": "socks",
    "settings": {"udp": true}
  }],
  "outbounds": [{
    "tag": "shine-out",
    "protocol": "shine",
    "settings": {
      "address": "127.0.0.1",
      "port": 7011,
      "users": [{"auth": "replace-me", "email": "alice@example.com"}]
    }
  }]
}
```

### 用户认证

服务端在 `settings.users` 中配置用户数组，每个用户使用一个 `auth`：

```json
{
  "users": [
    {"auth": "alice-secret", "email": "alice@example.com"},
    {"auth": "bob-secret", "email": "bob@example.com"}
  ]
}
```

`auth` 是该用户的认证密码，`email` 是可选的用户标识，可用于 Xray 的用户路由和统计。也可填写可选的 `level`，该值会写入 Xray 用户元数据；本适配层自身不按 level 设置会话超时。`auth` 和非空的 `email` 均不能在用户之间重复；`auth` 不能为空，也不能是数组。

客户端的 `settings.users` 应配置**一个**用户，例如：

```json
{
  "address": "127.0.0.1",
  "port": 7011,
  "users": [{"auth": "alice-secret", "email": "alice@example.com"}]
}
```

一个 outbound 复用的 Shine 连接使用该用户的 `auth`；需要多个用户时，可分别配置 outbound。Shine v1 握手只发送 `auth`，服务端根据该值匹配用户并填充服务端配置的 email；客户端填写的 email 不会传给服务端。

原有顶层 `password` 字符串配置继续可用，但不能与 `users` 混用；顶层 `password` 为空或未设置且没有用户时不启用认证。显式的空用户数组或 `null` 会被拒绝。

`settings` 支持以下可选字段，数值为 0 或未设置时使用协议库默认值：

| 字段 | 单位 | 默认值 |
| --- | --- | --- |
| `initialWindow` | 字节 | 262144 |
| `maxFrameSize` | 字节 | 536870912 |
| `maxDataSize` | 字节 | 262144 |
| `handshakeTimeoutSeconds` | 秒 | 5 |
| `pingIntervalSeconds` | 秒 | 15 |
| `pongTimeoutSeconds` | 秒 | 10 |

`maxFrameSize` 至少为 `maxDataSize + 9`，并且不能大于 512 MiB。与当前 C++ 实现互通时，两端的 `initialWindow` 必须一致。可以调低帧上限，限制对端单帧触发的内存分配。

UDP 会话按首个数据报的目标交给 Xray 路由；后续数据报保留各自的目标地址，但沿用该会话已选择的路由。Xray 的缓冲通道会忽略空 UDP 数据报。Xray 出口通过其 dialer 建立 Shine 连接，因此 Xray 的出口传输设置由 Xray 处理。Shine 自带复用，额外启用 Xray mux 会叠加一层复用，应先评估是否需要。

密码在 Shine 协议中用于认证，**本身不加密连接**。跨不可信网络时应通过配置好的安全传输保护链路。

## 测试

```sh
cd /path/to/shine/go/xray
go test -race ./...
# 用按上面方法构建的 Xray 验证真实双端配置、并发复用和 TCP/UDP：
SHINE_XRAY_BIN=/absolute/path/to/xray go test -race ./...
```

端到端测试启动两个 Xray 进程和本地回显服务；没有 `SHINE_XRAY_BIN` 时自动跳过。仓库内的协议库和适配层尚未发布，因此独立引用时需使用本地 `replace`，或发布后改为对应的模块版本。
