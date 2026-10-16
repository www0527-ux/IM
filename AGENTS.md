# AGENTS.md — 项目协作说明

> 给 Codex 看。打开这个项目先读本文件，再读 `redpacket-design.md`。

## 项目定位

`IM-learn` 是我的后端面试项目，也是我学习 Go 后端的主线项目。目标不是堆功能，而是把几个核心机制写对、讲清楚：

- 消息主线：旧版链路已完成并有端到端验证；目标链路改为统一 `MessageWriteService` + MySQL事务内Seq + Outbox，机械骨架已完成，核心事务待开发者实现
- 红包模块：MySQL 事务 + Redis 候选池 + 两段 Outbox + 幂等消费（设计已冻结，机械骨架已完成，核心事务待实现）
- 网关模块（后续）：服务发现、负载均衡、认证与限流

## 唯一事实源

红包模块的一切实现以 `redpacket-design.md` 为准：第 18 节是产品规则，第 20 节是评审决议。动手前必须先读，不要按自己的理解改动设计。

## 我的学习目标（重点）

我想通过这个项目重点掌握四块，并且希望核心代码自己写，而不是全部交给 AI：

1. Redis Lua 脚本编写与原子性
2. MySQL 事务与资金写入的边界
3. SQL 锁：行锁、条件更新、`FOR UPDATE` / `SKIP LOCKED`
4. Kafka：offset 语义、至少一次、幂等消费、Outbox、DLQ

## 我们怎么协作

- 动手前先给计划：改哪些文件、为什么、怎么验证。
- 核心逻辑我自己写：Lua 脚本、发送/领取/退款事务、关键 SQL、幂等键设计、错误分类。
- AI 负责机械部分：goctl 脚手架、model 层、DTO、config/wiring、日志、测试骨架、文档和图。
- 生成脚手架时把核心逻辑留成 TODO，并列出函数签名、前置/后置条件、失败语义；不要一次生成整个模块。
- 消息链路只保留 Outbox 目标实现；旧版 Accepted/Transfer/Sequenced/Offline Store 已按用户 2026-09-23 的明确要求移除。未完成组件必须明确报错，不得对外返回假成功。现有数据库启用前要完成 Seq 回填与删列迁移。
- 每个阶段先验收，再进入下一阶段。
- 每阶段结束补一段「做了什么决策 / 为什么 / 失败边界是什么」，这是我面试的素材。
- 不要改 `note/readme.md` 和未提交的实验代码，不要顺手重构无关代码。

## 表达偏好

中文回答，先结论后理由，尽量短。涉及事实时区分观察、推断和假设；不确定就直说不确定。

## 技术栈与运行环境

- Go + go-zero；MySQL 8.4、Redis 7、etcd、Kafka 3.9、MinIO 由 `IM-learn/compose.yaml` 提供。
- 端口：user.rpc 8080、user-api 8104、offline 8105、realtime 9101；红包服务规划 8103。
- 本地配置在 `service/*/etc/*.yaml`，不入库；密钥通过环境变量提供。
- 启动服务需要 `IM_JWT_SECRET`；user-api 还需要 SMTP 相关变量。

## 常用命令

- 起基础设施：`cd IM-learn && docker compose up -d`
- 编译：`cd IM-learn && go build ./...`
- 测试：`cd IM-learn && go test ./...`
- 后端联调：`IM-learn/scripts/chat-demo.ps1`
- 前端：`cd IM-frontend && npm run dev`
