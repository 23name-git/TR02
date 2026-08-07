#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TrendRadar 周/月/季/年汇总报告生成脚本
用法: python scripts/generate_periodic_summary.py --period weekly|monthly|quarterly|yearly

核心改进：直接复用 TrendRadar 官方 RemoteStorage 类，
与 Get Hot News 爬虫使用完全相同的存储访问代码路径，
零配置差异，自动处理 SigV2/SigV4、virtual-hosted style 等细节。
"""

import os
import sys
import argparse
import sqlite3
import tempfile
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any
from collections import Counter
import json

try:
    import requests
    import jieba
    # 直接复用官方存储模块
    from trendradar.storage.remote import RemoteStorageBackend
    from botocore.exceptions import ClientError
except ImportError as e:
    print(f"❌ 缺少依赖: {e}")
    print("请运行: pip install requests jieba")
    sys.exit(1)

# 北京时区（UTC+8）
BEIJING_TZ = timezone(timedelta(hours=8))

# ============ 配置区（从环境变量读取） ============
COS_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
COS_BUCKET = os.getenv("S3_BUCKET_NAME")
COS_AK = os.getenv("S3_ACCESS_KEY_ID")
COS_SK = os.getenv("S3_SECRET_ACCESS_KEY")
COS_REGION = os.getenv("S3_REGION", "ap-guangzhou")

AI_API_KEY = os.getenv("AI_API_KEY")
AI_MODEL = os.getenv("AI_MODEL", "deepseek/deepseek-v4-flash")
AI_BASE = os.getenv("AI_API_BASE", "https://api.deepseek.com/v1")

FEISHU_WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL")

# 校验必需环境变量
required_env = {
    "S3_ENDPOINT_URL": COS_ENDPOINT,
    "S3_BUCKET_NAME": COS_BUCKET,
    "S3_ACCESS_KEY_ID": COS_AK,
    "S3_SECRET_ACCESS_KEY": COS_SK,
    "AI_API_KEY": AI_API_KEY,
    "FEISHU_WEBHOOK_URL": FEISHU_WEBHOOK_URL,
}
missing = [k for k, v in required_env.items() if not v]
if missing:
    print(f"❌ 缺少必需环境变量: {', '.join(missing)}")
    sys.exit(1)

# 初始化 jieba
jieba.initialize()


def parse_period(period: str) -> tuple[datetime, datetime]:
    """
    返回 (start_date, end_date) 闭区间，北京时间。
    统计「上一个完整周期」：
    - weekly: 上周一 00:00 ~ 上周日 23:59
    - monthly: 上月 1 号 ~ 上月最后一天
    - quarterly: 上个自然季度
    - yearly: 去年全年
    """
    # 用北京时间计算周期边界（COS 文件名、数据时间戳均为北京时间）
    now = datetime.now(BEIJING_TZ)

    if period == "weekly":
        # 上周一
        last_monday = now - timedelta(days=now.weekday() + 7)
        start = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59)

    elif period == "monthly":
        # 上月 1 号 ~ 上月最后一天
        first_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_month_end = first_this_month - timedelta(seconds=1)
        start = last_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = last_month_end.replace(hour=23, minute=59, second=59, microsecond=999999)

    elif period == "quarterly":
        # 上个自然季度
        quarter = (now.month - 1) // 3 + 1
        if quarter == 1:
            # 当前 Q1，上季度是去年 Q4
            last_q_end = now.replace(month=1, day=1) - timedelta(seconds=1)
        else:
            last_q_end = now.replace(month=(quarter - 1) * 3, day=1) - timedelta(seconds=1)
        last_q_start = last_q_end.replace(month=((quarter - 2) % 4) * 3 + 1, day=1,
                                           hour=0, minute=0, second=0, microsecond=0)
        start, end = last_q_start, last_q_end.replace(hour=23, minute=59, second=59, microsecond=999999)

    elif period == "yearly":
        # 去年全年
        start = now.replace(year=now.year - 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(year=now.year - 1, month=12, day=31, hour=23, minute=59, second=59, microsecond=999999)

    else:
        raise ValueError(f"Unknown period: {period}")

    return start, end


def get_remote_storage() -> RemoteStorageBackend:
    """创建官方 RemoteStorageBackend 实例，完全复用爬虫的存储配置逻辑"""
    print(f"[INFO] 初始化 RemoteStorageBackend: bucket={COS_BUCKET}, endpoint={COS_ENDPOINT}, region={COS_REGION}")
    return RemoteStorageBackend(
        bucket_name=COS_BUCKET,
        access_key_id=COS_AK,
        secret_access_key=COS_SK,
        endpoint_url=COS_ENDPOINT,
        region=COS_REGION,
        # 以下使用默认值，与官方保持一致
        enable_txt=False,
        enable_html=False,
        temp_dir=None,
    )


def _object_exists(storage: RemoteStorageBackend, key: str) -> bool:
    """
    轻量检查对象是否存在：用 get_object 只读 1 字节（不带 Range，避免 SigV2 签名问题）
    """
    try:
        response = storage.s3_client.get_object(Bucket=COS_BUCKET, Key=key)
        # 只读 1 字节即可确认存在，然后关闭流
        response['Body'].read(1)
        response['Body'].close()
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            return False
        # 其他错误（如 403）当作不存在处理，避免误判
        print(f"[DEBUG] 检查 {key} 出错: {code} - {e}")
        return False
    except Exception as e:
        print(f"[DEBUG] 检查 {key} 异常: {type(e).__name__}: {e}")
        return False


def list_latest_db_keys(storage: RemoteStorageBackend, prefix: str) -> List[str]:
    """
    找最新的一个 .db 文件（COS 上只有最新日期的文件，数据全在里面）
    用 get_object + Range 探测：从今天(北京时间)往前找，最多找 365 天
    """
    # 用北京时间找文件（COS 文件名用北京时间）
    today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
    key = f"{prefix}{today}.db"
    print(f"[DEBUG] 尝试 Key: {key}")
    if _object_exists(storage, key):
        print(f"[DEBUG] 找到文件: {key}")
        return [key]

    # 往前找最多 365 天（年报最长）
    for i in range(1, 366):
        date_str = (datetime.now(BEIJING_TZ) - timedelta(days=i)).strftime("%Y-%m-%d")
        key = f"{prefix}{date_str}.db"
        print(f"[DEBUG] 尝试 Key: {key}")
        if _object_exists(storage, key):
            print(f"[DEBUG] 找到文件: {key}")
            return [key]
    print(f"[DEBUG] {prefix} 前缀下 365 天内均未找到 .db 文件")
    return []


def list_date_keys(storage: RemoteStorageBackend, prefix: str, start: datetime, end: datetime) -> List[str]:
    """
    统一入口：返回最新的一个 .db key（数据全在里面，后续在 SQLite 里按日期过滤）
    """
    return list_latest_db_keys(storage, prefix)


def download_db_files(storage: RemoteStorageBackend, keys: List[str], local_dir: Path) -> List[Path]:
    """下载数据库文件到本地临时目录"""
    local_dir.mkdir(parents=True, exist_ok=True)
    downloaded = []
    for key in keys:
        local_path = local_dir / Path(key).name
        # 使用官方的下载方法（支持 chunked encoding）
        response = storage.s3_client.get_object(Bucket=COS_BUCKET, Key=key)
        with open(local_path, 'wb') as f:
            for chunk in response['Body'].iter_chunks(chunk_size=1024*1024):
                f.write(chunk)
        downloaded.append(local_path)
    return downloaded


def merge_news_databases(db_paths: List[Path], start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """合并多个 SQLite 数据库的 news_items 表，并按日期过滤"""
    all_items = []
    for db_path in db_paths:
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            # 假设表中有 crawl_time 或 created_at 字段（按实际表结构调整）
            # 先查表结构
            cursor.execute("PRAGMA table_info(news_items)")
            cols = [row[1] for row in cursor.fetchall()]
            
            # 找时间字段
            time_col = None
            for col in ['crawl_time', 'created_at', 'timestamp', 'date', 'pub_time']:
                if col in cols:
                    time_col = col
                    break
            
            if time_col:
                start_ts = start.strftime("%Y-%m-%d %H:%M:%S")
                end_ts = end.strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(f"SELECT * FROM news_items WHERE {time_col} BETWEEN ? AND ?", (start_ts, end_ts))
            else:
                # 没时间字段，全取
                cursor.execute("SELECT * FROM news_items")
            
            rows = cursor.fetchall()
            for row in rows:
                all_items.append(dict(row))
            conn.close()
        except Exception as e:
            print(f"⚠️ 读取 {db_path} 失败: {e}")
    return all_items


def merge_rss_databases(db_paths: List[Path], start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """合并 RSS 数据库，并按日期过滤"""
    all_items = []
    for db_path in db_paths:
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            # 先查表结构
            cursor.execute("PRAGMA table_info(rss_items)")
            cols = [row[1] for row in cursor.fetchall()]

            # 找时间字段
            time_col = None
            for col in ['crawl_time', 'created_at', 'timestamp', 'date', 'pub_time', 'published_at']:
                if col in cols:
                    time_col = col
                    break

            if time_col:
                start_ts = start.strftime("%Y-%m-%d %H:%M:%S")
                end_ts = end.strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(f"SELECT * FROM rss_items WHERE {time_col} BETWEEN ? AND ?", (start_ts, end_ts))
            else:
                # 没时间字段，全取
                cursor.execute("SELECT * FROM rss_items")

            rows = cursor.fetchall()
            for row in rows:
                all_items.append(dict(row))
            conn.close()
        except Exception as e:
            print(f"⚠️ 读取 RSS {db_path} 失败: {e}")
    return all_items


def extract_keywords(items: List[Dict], top_n: int = 30) -> List[tuple]:
    """从标题中提取高频关键词"""
    all_titles = [item.get("title", "") for item in items if item.get("title")]
    words = []
    for t in all_titles:
        words.extend([w for w in jieba.cut(t) if len(w) >= 2])
    return Counter(words).most_common(top_n)


def build_ai_prompt(period: str, news_items: List[Dict], rss_items: List[Dict],
                    start: datetime, end: datetime) -> str:
    """构建发给 AI 的分析提示词"""
    period_names = {"weekly": "周报", "monthly": "月报", "quarterly": "季报", "yearly": "年报"}
    pname = period_names.get(period, period)

    # 统计
    news_count = len(news_items)
    rss_count = len(rss_items)

    # 平台分布
    platform_dist = Counter([item.get("platform", "unknown") for item in news_items])

    # 高频关键词
    all_items = news_items + rss_items
    top_keywords = extract_keywords(all_items, 30)

    # 头部标题样本（用于 AI 理解上下文）
    sample_titles = [item.get("title", "") for item in news_items[:50] if item.get("title")]
    sample_titles += [item.get("title", "") for item in rss_items[:20] if item.get("title")]

    prompt = f"""请生成一份 **{pname}**（{start.strftime('%Y-%m-%d')} 至 {end.strftime('%Y-%m-%d')}）的 AI 深度分析报告。

【数据概览】
- 统计周期：{start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}
- 热榜新闻条数：{news_count}
- RSS 文章条数：{rss_count}
- 覆盖平台分布：{dict(platform_dist)}
- 高频关键词 TOP30：{top_keywords}
- 标题样本（前 70 条）：{sample_titles[:70]}

【重点关注领域】（请在分析中体现）
- 手作饰品/耳环/项链/戒指/DIY 首饰趋势
- 韩系/日系服饰选品、流行款式、供应链动态
- 日本电商/时尚/手作市场资讯（用于日语学习与选品参考）
- AI 产品运营、AIGC 内容运营、AI 工具运营、AI 项目助理、AI 讲师等岗位招聘动态、薪资行情

【分析要求】
请按以下结构输出中文报告，使用 Markdown 格式：

## 1. 核心热点回顾
本周期最重要的 3-5 个热点事件，每个给出：
- 事件脉络（时间线、关键节点）
- 跨平台热度对比（哪个平台最火、持续多久）
- 舆论风向（正面/负面/中性、主要观点分歧）

## 2. 趋势演变分析
- 哪些话题持续发酵、热度上升
- 哪些是新爆发点（从零到热）
- 哪些已降温/退出视野
- 关键词热度变化轨迹

## 3. 异动信号捕捉
- 排名剧烈波动的新闻（如从 50 名冲上前 10）
- 突然冲榜的新话题
- 长尾持续上榜的「隐形赢家」
- 跨平台同步爆发的事件

## 4. RSS 专业源洞察
- 专业媒体/垂直博客/行业 KOL 的深度观点摘要
- 与热榜舆论的差异对比

## 5. 策略研判建议（针对「手作饰品创业+韩日选品+AI 求职」场景）
- **选品方向**：基于热点推荐的 3-5 个具体品类/风格/价格带
- **内容运营**：建议跟进的热点话题、切入角度、发布节奏
- **招聘/求职**：AI 非技术岗市场行情、关键技能要求、薪资区间参考
- **风险提示**：需规避的伪热点、合规风险、供应链不确定性

【输出格式要求】
- 使用 Markdown，标题层级清晰（## / ###）
- 关键数据用 **粗体** 标注
- 避免空泛套话，给具体案例、数字、平台名支撑
- 篇幅控制在 1000-2000 字
- 语言专业、可读性强，适合直接发到飞书群/作为决策参考
"""
    return prompt


def call_ai(prompt: str) -> str:
    """调用 AI 生成报告"""
    headers = {
        "Authorization": f"Bearer {AI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": AI_MODEL,
        "messages": [
            {"role": "system", "content": "你是专业的舆情分析师，擅长从海量新闻中提炼趋势洞察，输出可执行的策略建议。"},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 4000
    }
    resp = requests.post(f"{AI_BASE}/chat/completions", headers=headers, json=data, timeout=120)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def send_feishu(text: str):
    """发送到飞书群（Markdown 格式）"""
    # 飞书单条消息限制约 4000 字符，需分片
    max_len = 3500
    chunks = [text[i:i+max_len] for i in range(0, len(text), max_len)]

    for idx, chunk in enumerate(chunks):
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"📊 TrendRadar 汇总报告 ({idx+1}/{len(chunks)})"},
                    "template": "blue"
                },
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content": chunk}}
                ]
            }
        }
        try:
            r = requests.post(FEISHU_WEBHOOK_URL, json=payload, timeout=30)
            r.raise_for_status()
            print(f"✅ 飞书推送成功 (片段 {idx+1}/{len(chunks)})")
        except Exception as e:
            print(f"❌ 飞书推送失败: {e}")
            # 降级：尝试纯文本
            try:
                fallback = {"msg_type": "text", "content": {"text": chunk[:2000]}}
                requests.post(FEISHU_WEBHOOK_URL, json=fallback, timeout=30)
            except Exception:
                pass


def upload_html_to_cos(storage: RemoteStorageBackend, html_content: str, period: str, start: datetime, end: datetime):
    """上传 HTML 版报告到 COS summary/ 目录"""
    try:
        key = f"summary/{period}/{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}.html"
        storage.s3_client.put_object(
            Bucket=COS_BUCKET,
            Key=key,
            Body=html_content.encode("utf-8"),
            ContentType="text/html; charset=utf-8"
        )
        print(f"✅ HTML 报告已上传: s3://{COS_BUCKET}/{key}")
    except Exception as e:
        print(f"⚠️ 上传 HTML 失败: {e}")


def markdown_to_html(md: str) -> str:
    """简单的 Markdown 转 HTML（用于 COS 归档）"""
    html = md.replace("## ", "<h2>").replace("##", "</h2>") \
             .replace("### ", "<h3>").replace("###", "</h3>") \
             .replace("**", "<strong>").replace("**", "</strong>") \
             .replace("\n", "<br>")
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>TrendRadar 汇总报告</title>
<style>
body{{font-family:system-ui,-apple-system,sans-serif;padding:2rem;max-width:900px;margin:auto;line-height:1.8;color:#333}}
h2{{color:#1a73e8;border-bottom:2px solid #e8f0fe;padding-bottom:0.3rem}}
h3{{color:#34a853}}
strong{{color:#d93025}}
code{{background:#f5f5f5;padding:0.1rem 0.3rem;border-radius:3px}}
pre{{background:#f8f9fa;padding:1rem;overflow:auto;border-radius:6px}}
</style></head><body>{html}</body></html>"""


def main():
    parser = argparse.ArgumentParser(description="TrendRadar 周期汇总报告生成器")
    parser.add_argument("--period", required=True, choices=["weekly", "monthly", "quarterly", "yearly"],
                        help="汇总周期: weekly|monthly|quarterly|yearly")
    args = parser.parse_args()

    print(f"🚀 开始生成 {args.period} 汇总报告...")
    start, end = parse_period(args.period)
    print(f"📅 统计范围: {start.strftime('%Y-%m-%d %H:%M')} ~ {end.strftime('%Y-%m-%d %H:%M')}")

    # 使用官方 RemoteStorage，完全复用爬虫的存储访问逻辑
    storage = get_remote_storage()

    # 1. 列出并下载 news/ 和 rss/ 下的数据库
    print("🔍 扫描 COS 数据...")
    news_keys = list_date_keys(storage, "news/", start, end)
    rss_keys = list_date_keys(storage, "rss/", start, end)
    print(f"📦 找到 news 数据库: {len(news_keys)} 个, rss 数据库: {len(rss_keys)} 个")

    if not news_keys and not rss_keys:
        print("⚠️ 该周期无数据，跳过生成")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        news_dbs = download_db_files(storage, news_keys, tmp / "news")
        rss_dbs = download_db_files(storage, rss_keys, tmp / "rss")

        # 2. 合并数据
        print("🔄 合并数据库...")
        news_items = merge_news_databases(news_dbs, start, end)
        rss_items = merge_rss_databases(rss_dbs, start, end)
        print(f"📊 合并后: 热榜 {len(news_items)} 条, RSS {len(rss_items)} 条")

        if not news_items and not rss_items:
            print("⚠️ 合并后无有效数据，跳过")
            return

        # 3. 构建提示词并调用 AI
        prompt = build_ai_prompt(args.period, news_items, rss_items, start, end)
        print("🤖 正在调用 AI 生成报告...")
        report_md = call_ai(prompt)

        # 4. 生成 HTML 版本（用于 COS 归档）
        html_content = markdown_to_html(report_md)

        # 5. 推送到飞书
        period_names = {"weekly": "周报", "monthly": "月报", "quarterly": "季报", "yearly": "年报"}
        header = f"📊 **TrendRadar {period_names[args.period]}** ({start.strftime('%m/%d')} - {end.strftime('%m/%d')})"
        full_text = header + "\n\n" + report_md
        send_feishu(full_text)

        # 6. 上传 HTML 到 COS（归档）
        upload_html_to_cos(storage, html_content, args.period, start, end)

    print("✅ 汇总报告生成完成！")


if __name__ == "__main__":
    main()