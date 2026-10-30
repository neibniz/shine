# Shine 的 Xray-core 适配层

本模块将 [独立 Shine 协议库](../shine/README.md)接入 Xray-core：

- `Inbound` 接收 Shine 连接，将每个 TCP / UDP 会话交给 Xray 的路由器。
- `Outbound` 使用 Xray 的 dialer 建立上游连接，并在多个会话间复用；链路断开后，下一个请求重新连接。
- Protobuf 配置与 Xray 对象工厂在包初始化时注册。`conf` 包提供 JSON 配置构建器。

适配层以 Xray-core `v26.10.29`（提交 `0afec66085a85191e9dd2a4a6646c7c9a94688d1`）为默认版本，要求 Go 1.27 或更高。其他版本可以尝试构建，但需要通过注册补丁检查、编译和测试才能确认兼容。

## 加入 Xray-core 并构建

Xray 的 JSON 协议名称表目前是内部静态表，所以仅导入包不能注册 `"shine"`。下面的脚本会对指定检出应用 [配置注册补丁](patches/xray-core.patch)，并在该检出的 `go.mod` 中添加本地模块引用。脚本接受任意检出的版本，会先检查需要修改的文件是否干净以及补丁是否能应用；不同 Xray 版本的接口仍可能不兼容，应继续执行编译和测试。

```sh
/path/to/shine/go/xray/scripts/install-into-xray.sh /path/to/Xray-core
cd /path/to/Xray-core
GOWORK=off go build -mod=mod -o xray ./main
```

脚本使用绝对路径引用本地 Shine 和适配层目录；移动这些目录后需更新 Xray 检出中的 `go.mod`。这是源码集成，不能把适配包动态加载到现成的官方二进制中。

针对其他 Xray 版本，可以像工作流一样为适配层准备一个临时副本，使用所选 Xray 自己的依赖版本：

```sh
/path/to/shine/go/xray/scripts/prepare-adapter.sh /path/to/Xray-core /tmp/shine-xray-adapter
SHINE_XRAY_ADAPTER_DIR=/tmp/shine-xray-adapter \
  /path/to/shine/go/xray/scripts/install-into-xray.sh /path/to/Xray-core
cd /path/to/Xray-core
GOWORK=off go build -mod=mod -o xray ./main
```

临时副本目录必须事先不存在；它和本地 Shine 库的路径应在构建期间保持不变。该步骤不能保证所有 Xray 版本的代码接口兼容。

## GitHub Actions 构建与镜像

工作流 [Build Xray with Shine](../../.github/workflows/xray-shine.yml) 可在 Actions 页面通过 **Run workflow** 手动启动，在 `xray_ref` 中填写 Xray-core 的标签、分支或提交，例如 `v26.10.29`。工作流会先将所选引用解析为一个固定提交，所有测试、交叉编译和镜像都使用这个提交。默认的 `v26.10.29` 和较早的 `v26.9.9` 已在本地完成集成测试。推送到 `main`、`master` 或 `v*` 标签以及向主分支提交 pull request 时，也会使用默认版本自动构建。

构建前会把本仓库的 Shine 适配层和注册补丁合并进临时检出，运行协议库和适配层测试以及真实 Xray 双端测试。补丁不适用、接口不兼容或测试不通过时，该次构建会失败，不会发布镜像。选择版本不代表该版本一定兼容。

构建完成后，从工作流的 Artifacts 下载 Linux（amd64、arm64、armv7、386）、macOS（amd64、arm64）和 Windows（amd64、arm64）的归档，以及 `SHA256SUMS-xray-shine.txt`。每个归档包含程序、双方许可证和记录两个项目提交的 `BUILD-INFO.txt`。推送本仓库的 `v*` 标签时，这些文件也会附加到相应 GitHub Release。

非 pull request 的构建还会将 Linux amd64/arm64 多架构镜像推送到 `ghcr.io/<仓库所有者>/<仓库名>-xray`，与 C++ 镜像分开。镜像包含分支或标签名，以及 `sha-<Shine提交>-xray-<Xray提交前12位>` 形式的标签；不会自动覆盖 `latest`。首次推送需要仓库允许 Actions 使用 `GITHUB_TOKEN` 写入 Packages，镜像的公开或私有权限由 GitHub Packages 设置决定。

容器默认以非 root 用户运行，读取 `/etc/xray/config.json`。例如，替换镜像标签和配置后运行：

```sh
docker run --rm -p 7011:7011 \
  -v "$PWD/config.json:/etc/xray/config.json:ro" \
  ghcr.io/hhhflow2020/shine-xray:main
```

镜像不包含 `geoip.dat` 和 `geosite.dat`；如果配置使用它们，请将文件挂载到 `/usr/local/share/xray`，并确保配置与数据文件对容器内非 root 用户可读。可用 `docker run --rm -v "$PWD/config.json:/etc/xray/config.json:ro" <镜像:标签> run -test -config /etc/xray/config.json` 检查配置。

## 配置示例

服务端，收到的 Shine 会话交由 Xray 路由：

```json
{
  "inbounds": [{
    "tag": "shine-in",
    "listen": "0.0.0.0",
    "port": 7011,
    "protocol": "shine",
    "settings": {"compression": true, "users": [{"auth": "replace-me", "email": "alice@example.com"}]}
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
      "compression": true,
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

## Redis 风格传输与压缩

Redis 风格传输是 Xray 适配层的默认模式，不需要显式填写 `transport`。例如：

```json
{
  "compression": true,
  "users": [{"auth": "replace-me", "email": "alice@example.com"}]
}
```

客户端还需要配置 `address` 和 `port`。省略 `transport` 或显式设置为 `"redis"` 都使用 Redis 风格传输；压缩默认关闭，可通过 `"compression": true` 开启。压缩可以只在一端开启：两端都能接收压缩和未压缩的数据。

需要与原有 Shine TCP 协议互通时，Xray 端必须在 Shine `settings` 中显式设置 `"transport": "tcp"`，并且不能启用 `compression`；如果两端都是 Xray，则两端都要设置。如果另一端是本分支的 C++ 程序，双方都必须设置 `transport: tcp`。**此前省略 `transport` 的旧 Xray 配置现在会选择 Redis 风格传输**，升级时请检查两端配置。

新模式使用两条物理连接，分别承载 RESP2 的发布和订阅交互；内部仍运行原有的 Shine 会话和流控。只有 zlib 压缩结果比原数据至少小 16 字节时才发送压缩结果。每次发布需要等待响应，因此远距离链路的性能应实测。小数据、已经压缩的数据和额外的协议封装可能使总流量不降反升。

该模式是 **Redis 风格的专用传输**，不是完整 Redis 服务，也不经过现有 Redis 服务器。配对前会模拟常见 RESP2 命令，包括 `AUTH`、`HELLO 2`、`PING`、`INFO`、`CLIENT`、`GET`、`SET`、`DEL`、`EXISTS` 及部分过期命令。模拟键值区在进程内临时保存，最多 1,024 个键、4 MiB，重启后丢失，不应用于真实业务存储；部分命令和选项以及 RESP3 尚未实现。

配对流程和消息内容仍是 Shine 专用格式，不能保证逃过协议识别。没有新增加密，明文连接上的 `AUTH` 和数据仍可能被读取；压缩不提供保密性。本分支的 C++ 端也支持该模式。更多命令、测试方式与限制见 [传输说明](../shine/redis/README.md)。

## 测试

```sh
cd /path/to/shine/go/xray
go test -race ./...
# 用按上面方法构建的 Xray 验证真实双端配置、并发复用和 TCP/UDP：
SHINE_XRAY_BIN=/absolute/path/to/xray go test -race ./...
```

端到端测试启动两个 Xray 进程和本地回显服务；没有 `SHINE_XRAY_BIN` 时自动跳过。仓库内的协议库和适配层尚未发布，因此独立引用时需使用本地 `replace`，或发布后改为对应的模块版本。
