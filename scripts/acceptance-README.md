# 项目验收

从 `IM-learn` 目录执行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/project-acceptance.ps1
```

前置条件：Docker Desktop 已运行、Go 和 Node.js 24 可用、前端 `node_modules` 已安装、本机已安装 Microsoft Edge。脚本使用 Node 调用 TypeScript、Vite 和 Playwright，不要求安装 Bun。

如果上述代码检查已通过、只因基础设施未启动而中断，可以加 `-SkipChecks` 跳过重复的测试和前端构建。此时必须已有 `dist/demo`；八个后端服务仍会从当前代码重新编译。

脚本完成 Go 测试与构建、前端类型检查与生产构建，然后创建独立 MySQL 库、Kafka Topic/消费组、用户和会话，启动八个业务进程。通过 `:10010` 网关验证 HTTP、WebSocket、生产页面，以及同步游标与实时推送的交接。Vite 只用于加载前端源码执行游标断言，生产页面测试直接访问网关。

## 故障注入与隔离

- 所有业务 SQL 只访问 `im_acceptance_<时间>`，不修改原业务库；退款测试会修改测试红包的过期时间，并在测试库短暂创建延迟触发器。
- Redis 使用独立测试用户/红包的 Key；Kafka 使用独立 Topic 和消费组；RPC 使用独立 etcd 注册 Key。
- Kafka 和 Redis 故障测试会短暂停止本机共享容器，因此执行期间不要运行其他依赖这些容器的业务。
- 脚本结束或失败时会恢复 Kafka/Redis，并停止本次业务进程；`-KeepRunning` 可保留本次进程用于人工查看。基础设施容器与数据卷会保留。
- 测试库、Topic 和 `%TEMP%/im-acceptance-<时间>` 留作证据。该临时目录的 YAML 含本机连接凭据，不应上传；`results.json`、`browser-ui.json`、`browser-handoff.json` 与截图不包含登录 Token。

## 文件

| 文件 | 用途 |
|---|---|
| `project-acceptance.ps1` | 构建、隔离数据、启动、浏览器验收与退出清理 |
| `project-acceptance.mjs` | HTTP/WS、资金并发、SQL 回滚、缓存回源、Kafka/Redis 故障与对账断言 |
| `acceptance-tools/main.go` | 调用真实只读红包对账服务，以及两个退款 Service 实例的并发验证 |
| `../../webark-im-template/scripts/imlearn-ui-acceptance.mjs` | 生产页面双浏览器登录、收发与刷新恢复 |
| `../../webark-im-template/scripts/imlearn-live-handoff.mjs` | 实时推送不推进同步游标、拉取合并去重、断线后补齐 |
| `../../webark-im-template/scripts/imlearn-redpacket-ui-acceptance.mjs` | 红包 UI：群/单聊发送领取、详情刷新、页面恢复、响应丢失后的原请求重试 |

每项后端断言完成立即写入 `results.json`；失败时保留错误与已完成结果。浏览器 UI 失败时保存截图和页面文字。本套件验证当前单 Realtime、单有效 Relay 的部署，不是压测或全故障穷举。

红包 UI 验收是独立脚本：先以 `-KeepRunning` 保留验收服务，在前端目录设置 `IM_ACCEPTANCE_RUN_DIR` 为该次临时目录，再执行 `node scripts/imlearn-redpacket-ui-acceptance.mjs`。结果写入 `redpacket-ui.json`，并保存桌面与移动端详情截图。脚本关闭自己的浏览器，保留业务服务供人工体验。
