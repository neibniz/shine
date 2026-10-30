# C++ Shine 配置

[服务端示例](shine-server.example.yaml)和[客户端示例](shine-client.example.yaml)使用 Redis 风格传输。服务端可配置多个用户，客户端只配置一个用户；每个用户的 `auth` 是该用户唯一的密码，`email` 是可选的用户标识，`level` 也是可选字段。客户端的 `auth` 必须与服务端其中一个用户一致。C++ 入站会将认证用户的信息附加到会话请求；当前 C++ 路由规则尚不支持按 `email` 或 `level` 匹配。

省略 `transport` 时默认使用 `redis`，它与本分支 Go/Xray 的默认传输互通。设置 `compression: true` 可启用发送方向的 zlib 压缩；接收端无论是否启用压缩都可以接收压缩数据。压缩仅在能减少数据量时使用，HTTPS 等已经压缩或加密的数据通常难以进一步压缩。

需要与旧版直接使用 Shine RESP2 的程序互通时，在双方的 Shine 配置中设置 `transport: tcp`，并关闭或省略 `compression`。旧配置中的单一 `password` 仍可使用，但不能与 `users` 同时配置。

Redis 命令模拟仅提供有限的内存数据库，不是完整 Redis 服务；数据重启后不会保留。这个传输没有提供加密：`auth` 和传输内容不具备机密性，应根据实际环境另行保护连接。

示例配置可分别通过 `shine -c configs/shine-server.example.yaml` 和 `shine -c configs/shine-client.example.yaml` 启动。实际部署前请修改监听地址、远端地址和密码。
