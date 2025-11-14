# scripts/download_parallel.py (最终侦察版)

import os
import json
# import baostock as bs # 我们先不导入，专注于资金流
import requests
import pandas as pd
from tqdm import tqdm
import time
import sys

# --- 配置 ---
MONEYFLOW_OUTPUT_DIR = "data_slice/moneyflow"
SINA_API_HISTORY = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page={page}&num=50&sort=opendate&asc=0&daima={code}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(MONEYFLOW_OUTPUT_DIR, exist_ok=True)


def download_fundflow(code):
    """(引擎B) 从新浪财经获取资金流数据 - 严格模式"""
    all_data_list = []
    page = 1
    code_for_api = code.replace('.', '')
    print(f"\n  -> Attempting to download fund flow for {code}...")
    
    # --- (这是唯一的、关键的修正) ---
    # 我们将循环放在 try 块内部，并且让失败传递出去
    try:
        while page <= 150:
            target_url = SINA_API_HISTORY.format(page=page, num=50, code=code_for_api)
            
            # 增加打印，看看我们请求的 URL 是什么
            if page <= 2: # 只打印前两页的URL
                print(f"     Requesting page {page}: {target_url}")

            response = requests.get(target_url, headers=HEADERS, timeout=45)
            response.raise_for_status() # 这是关键！如果状态码不是2xx，直接抛出异常
            response.encoding = 'gbk'
            data = response.json()
            
            if not data:
                print(f"     Page {page} returned empty data. Ending pagination.")
                break
            
            all_data_list.extend(data)
            
            if len(data) < 50:
                print(f"     Page {page} is the last page ({len(data)} records).")
                break
                
            page += 1
            time.sleep(0.3)
            
    except Exception as e:
        print(f"\n  -> ❌ CRITICAL FAILURE during download for {code} on page {page}: {e}")
        # 重新抛出异常，让主循环的 except 块能捕获到，并让整个脚本失败
        raise e
    # ------------------------------------------

    if all_data_list:
        df = pd.DataFrame(all_data_list)
        df.to_parquet(f"{MONEYFLOW_OUTPUT_DIR}/{code}.parquet", index=False)
        print(f"  -> ✅ Success for {code}, saved {len(df)} records.")

def main():
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    try:
        with open(task_file, "r", encoding="utf-8") as f:
            subset = json.load(f)
    except FileNotFoundError:
        print(f"❌ 致命错误: 未找到任务分片文件 {task_file}！"); sys.exit(1)
        
    if not subset: print("🟡 本分区任务列表为空。"); return
    
    print(f"📦 分区 {TASK_INDEX + 1}，负责 {len(subset)} 支股票 (仅下载资金流)。")
    
    for s in tqdm(subset, desc=f"分区 {TASK_INDEX + 1} 总体进度"):
        code = s["code"]
        name = s.get("name", "")
        
        try:
            download_fundflow(code)
        except Exception as e:
            # 捕获从 download_fundflow 抛出的异常
            print(f"\n" + "="*60)
            print(f"❌ 脚本因处理 {name} ({code}) 时发生致命错误而终止。")
            print(f"   根本原因: {e}")
            print("="*60)
            sys.exit(1) # <--- 让整个 job 失败！

    print(f"\n✅ 分区 {TASK_INDEX + 1} 任务完成。")

if __name__ == "__main__":
    main()
