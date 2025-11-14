# scripts/download_parallel.py (仅下载资金流测试版)

import os
import json
# import baostock as bs  # <-- 已注释
import requests
import pandas as pd
from tqdm import tqdm
import time
import sys

# --- 配置 ---
# KDATA_OUTPUT_DIR = "data_slice/kdata" # <-- 已注释
MONEYFLOW_OUTPUT_DIR = "data_slice/moneyflow"
# KDATA_START_DATE = "2005-01-01" # <-- 已注释
SINA_API_HISTORY = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page={page}&num=50&sort=opendate&asc=0&daima={code}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}

TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
# os.makedirs(KDATA_OUTPUT_DIR, exist_ok=True) # <-- 已注释
os.makedirs(MONEYFLOW_OUTPUT_DIR, exist_ok=True)

# def download_kdata(code): # <-- 整个函数已注释
#     ...

def download_fundflow(code):
    """从新浪财经获取资金流数据"""
    all_data_list = []
    page = 1
    code_for_api = code.replace('.', '')
    # 增加一个最大页数限制，防止在某些异常情况下无限循环
    while page <= 200: 
        try:
            target_url = SINA_API_HISTORY.format(page=page, num=50, code=code_for_api)
            response = requests.get(target_url, headers=HEADERS, timeout=45)
            response.raise_for_status()
            response.encoding = 'gbk'
            data = response.json()
            if not data: break
            all_data_list.extend(data)
            if len(data) < 50: break
            page += 1
            time.sleep(0.3) # 保持友好暂停
        except Exception as e:
            # 如果出错，清晰地打印错误并返回 False
            print(f"\n  -> ❌ Sina Fund Flow API Error for {code} on page {page}: {e}")
            return False
            
    if all_data_list:
        df = pd.DataFrame(all_data_list)
        df.to_parquet(f"{MONEYFLOW_OUTPUT_DIR}/{code}.parquet", index=False)
    # 无论有无数据（例如新股），只要没出错就算成功
    return True

def main():
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    try:
        with open(task_file, "r", encoding="utf-8") as f:
            subset = json.load(f)
    except FileNotFoundError:
        print(f"❌ 致命错误: 未找到任务分片文件 {task_file}！"); sys.exit(1)
    if not subset: print("🟡 本分区任务列表为空。"); return
    
    print(f"📦 分区 {TASK_INDEX + 1}，负责 {len(subset)} 支股票 (仅下载资金流)。")
    
    # Baostock 登录/登出全部注释掉
    # lg = bs.login()
    # if lg.error_code != '0': ...
    
    successful_stocks = 0
    for s in tqdm(subset, desc=f"分区 {TASK_INDEX + 1} 资金流下载进度"):
        code = s["code"]
        name = s.get("name", "")
        
        try:
            # --- (这是唯一的、关键的修正) ---
            # 只调用资金流下载函数
            if download_fundflow(code):
                successful_stocks += 1
            # --------------------------------

        except Exception as e:
            print(f"\n  -> ❌ 在处理 {name} ({code}) 时发生未知严重错误: {e}")
            
    print(f"\n✅ 分区 {TASK_INDEX + 1} 任务完成。")
    print(f"   - 负责股票数: {len(subset)}")
    print(f"   - 资金流下载成功（或无数据）的股票数: {successful_stocks}")
    
    if successful_stocks == 0 and len(subset) > 0:
        print("\n❌ 致命错误: 本分区没有成功下载任何一只股票的资金流数据！")
        sys.exit(1)

if __name__ == "__main__":
    main()
