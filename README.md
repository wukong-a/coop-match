# 游戏搭子 · 随机匹配

免登录找人一起玩双人合作游戏：双人成行、双影奇境、胡闹厨房、逃出生天等。随机匹配、预约组队、联邦节点互通，不存数据，打开就能玩。

## 一键部署你自己的节点

点一下，30 秒后你就有自己的节点网址，拿去分享给朋友：

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/new/template?template=https://github.com/wukong-a/coop-match)
[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/wukong-a/coop-match)

部署完记得在环境变量里改个节点名（`NODE_NAME`），不改也行，默认会随机生成一个。

> Railway 每月送 5 美元额度，小应用 24 小时跑着基本够用。Render 免费档 15 分钟没人访问会休眠，下个人打开要等 30 秒唤醒。

## 自己跑

```bash
npm install
npm start
```

打开 http://127.0.0.1:3000

## 环境变量

| 变量 | 说明 | 默认 |
|---|---|---|
| `PORT` | 监听端口 | 3000 |
| `BIND` | 监听地址 | 0.0.0.0 |
| `NODE_NAME` | 节点名字（显示在荣誉榜上） | 随机生成 |
| `NODE_MOTD` | 节点公告 | 空 |
| `HUB_URL` | 枢纽地址（连上后跟全网互通，不填就是独立节点） | 空 |
| `RELAY_TOKEN` | 枢纽令牌（连私有枢纽时用） | 空 |
| `ADMIN_TOKEN` | 管理令牌（收录提名游戏用，别外发） | 空 |
| `DATA_DIR` | 数据目录（留言/预约/统计落盘处） | ./data |
| `SIGNING` | 签名存证开关（1=开） | 0 |

## 联邦网络

设了 `HUB_URL` 的节点会连到枢纽，跟全网共用一个匹配池。枢纽默认是 https://yx.niuniu94.xyz。

## 许可

MIT
