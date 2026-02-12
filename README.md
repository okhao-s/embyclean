# EmbyCleaner Pro

<div align="center">

![EmbyCleaner](https://img.shields.io/badge/Emby-Cleaner-38bdf8?style=for-the-badge&logo=emby)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ed?style=for-the-badge&logo=docker)
![Python](https://img.shields.io/badge/Built%20with-FastAPI-009688?style=for-the-badge&logo=fastapi)

**一个现代化、高性能的 Emby 媒体库洗版与清理工具**
<br/>
*告别重复文件，让你的媒体库井井有条*

</div>

## 📖 简介 | Introduction

**EmbyCleaner** 是一个专为 Emby 媒体服务器设计的轻量级管理工具。它能通过多种智能算法（如视频指纹、番号匹配、时长对比）快速发现媒体库中的重复资源、低质量版本或垃圾文件，并提供现代化的 Web 界面进行一键清理。

**v1.2 更新亮点：**
* ✨ **全新 UI 设计**：Obsidian Pro 暗黑主题，16:9 影院级海报展示，细节打磨。
* 🚀 **无限滚动 (Infinite Scroll)**：告别分页，丝滑浏览数千条资源，支持动态加载。
* 🔍 **精准路径对比**：在“同大查重”与“时长查重”模式下，自动高亮显示文件路径差异，一眼识别重复位置。
* ⚡ **高性能交互**：优化组选逻辑，点击标题栏即可全选；状态栏实时呼吸灯显示连接状态。
* 📊 **全量日志**：日志中心集成系统任务与人工操作记录，一切尽在掌握。

## ✨ 核心功能 | Features

### 🧹 六大扫描模式
1.  **🔞 番号查重**：自动识别 AV 番号，找出不同路径下的重复收录。
2.  **🧠 智能洗版**：按分辨率 (4K/1080P)、大小排序，助你保留最佳画质版本。
3.  **⚖️ 同大查重**：基于文件大小的精准匹配，发现完全一致的冗余文件（支持路径差异高亮）。
4.  **⏳ 时长查重**：基于精确时长的查重算法，识别不同文件名但内容相同的视频。
5.  **🖼️ 缺失封面**：快速找出没有海报的媒体项，治愈强迫症。
6.  **🐜 极小文件**：一键扫描并清理非正片的残留文件（如花絮、样本）。

### 🛡️ 安全与自动化
* **物理删除确认**：所有删除操作均需二次确认，防止手滑。
* **黑名单/白名单**：支持将特定影片加入忽略列表，永久跳过扫描。
* **计划任务**：内置 Crontab，支持自定义定时扫描与全量同步。
* **Webhook 通知**：支持对接消息推送插件，清理进度实时通知。
* ⚠️ 免责声明 | Disclaimer
本工具涉及 物理文件删除 操作，文件删除后不可恢复。

虽然程序设有黑名单和确认机制，但在执行批量删除前，请务必仔细核对。

安全警示：本程序未做任何加密鉴权，切勿将端口直接暴露在公网！建议仅在内网或通过 VPN 使用。

开发者不对因使用本工具导致的数据丢失负责。

## 🐳 部署指南 | Docker Deployment

> 默认端口：`19898`

version: '3'
services:
  embyclean:
    image: okhao/embyclean:latest
    container_name: embyclean
    ports:
      - "19898:19898"
    volumes:
      - ./data:/app/data
    restart: always
