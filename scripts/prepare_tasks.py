# scripts/prepare_tasks.py (测试版 - 前100只)

import baostock as bs
import pandas as pd
import json
import random
import os
from datetime import datetime, timedelta

# --- 配置 ---
TASK_COUNT = 20
OUTPUT_DIR = "task_slices"
# (这是唯一的、关键的修正)
# 设置一个测试时处理的股票数量上限
TEST_STOCK_LIMIT = 100 
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_recent_trade_day():
    """智能获取最近的交易日"""
    for i in range(1, 7):
        day = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        rs = bs.query_trade_dates(start_date=day, end_date=day)
        if rs.error_code == '0' and rs.next() and rs.get_row_data()[1] == '1':
            print(f"📅 自动获取到最近交易日: {day}")
            return day
    raise Exception("一周内未找到有效交易日。")

def main():
    print("🚀 开始从 Baostock 准备并行下载任务 (测试模式)...")
    
    lg = bs.login()
    if lg.error_code != '0':
        raise Exception(f"登录失败：{lg.error_msg}")
    print("✅ 登录成功")

    try:
        trade_day = get_recent_trade_day()
        rs_stock = bs.query_all_stock(day=trade_day)
        if rs_stock.error_code != '0':
            raise Exception(f"获取股票列表失败: {rs_stock.error_msg}")
        
        stock_df = rs_stock.get_data()
        if stock_df.empty:
            raise Exception("获取到的股票列表为空。")

        stock_list = []
        for index, row in stock_df.iterrows():
            code, name = row['code'], row['code_name']
            if str(code).startswith(('sh.', 'sz.')) and 'ST' not in name and '退' not in name:
                stock_list.append({'code': code, 'name': name})
        print(f"  -> 成功获取并筛选出 {len(stock_list)} 支股票。")

        # --- (这是唯一的、关键的修正) ---
        print(f"  -> ⚠️ 警告: 当前为测试模式，仅使用前 {TEST_STOCK_LIMIT} 支股票进行处理。")
        # 直接对列表进行切片，只保留前100个元素
        stock_list = stock_list[:TEST_STOCK_LIMIT]
        # ------------------------------------

        random.shuffle(stock_list)
        print(f"  -> 🃏 已将 {len(stock_list)} 支测试股票随机打乱。")

        chunk_size = (len(stock_list) + TASK_COUNT - 1) // TASK_COUNT
        print(f"  -> 每个任务分片包含约 {chunk_size} 支股票。")
        
        for i in range(TASK_COUNT):
            subset = stock_list[i * chunk_size : (i + 1) * chunk_size]
            slice_filepath = os.path.join(OUTPUT_DIR, f"task_slice_{i}.json")
            with open(slice_filepath, "w", encoding="utf-8") as f:
                json.dump(subset, f, ensure_ascii=False)
                
        print(f"\n✅ 成功为 {len(stock_list)} 支测试股票生成了 {TASK_COUNT} 个任务分片。")

    finally:
        bs.logout()
        print("✅ 已登出。")

if __name__ == "__main__":
    main()
