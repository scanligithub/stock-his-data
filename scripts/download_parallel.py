# scripts/download_parallel.py

import os
import json
import baostock as bs
import requests
import pandas as pd
from tqdm import tqdm
import time

# --- 配置 ---
KDATA_OUTPUT_DIR = "data_slice/kdata"
MONEYFLOW_OUTPUT_DIR = "data_slice/moneyflow"
KDATA_START_DATE = "2005-01-01"
SINA_API_HISTORY = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page={page}&num=50&sort=opendate&asc=0&daima={code}"
HEADERS = { 'User-Agent': 'Mozilla/5.0 ...', 'Referer': 'https://vip.stock.finance.sina.com.cn/' }

TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(KDATA_OUTPUT_DIR, exist_ok=True)
os.makedirs(MONEYFLOW_OUTPUT_DIR, exist_ok=True)

def download_kdata(code):
    try:
        rs = bs.query_history_k_data_plus(
            code, "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,isST",
            start_date=KDATA_START_DATE, end_date="", frequency="d", adjustflag="3"
        )
        if rs.error_code != '0': return
        data_list = [rs.get_row_data() for _ in iter(rs.next, False)]
        if data_list:
            df = pd.DataFrame(data_list, columns=rs.fields)
            df.to_parquet(f"{KDATA_OUTPUT_DIR}/{code}.parquet", index=False)
    except Exception as e:
        print(f"\n  -> ❌ Baostock K-Data download CRASHED for {code}: {e}")

def download_fundflow(code):
    all_data_list = []
    page = 1
    code_for_api = code.replace('.', '')
    while page <= 100:
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
            time.sleep(0.3)
        except Exception as e:
            print(f"\n  -> ❌ Sina Fund Flow API Error for {code} on page {page}: {e}")
            break
    if all_data_list:
        df = pd.DataFrame(all_data_list)
        df.to_parquet(f"{MONEYFLOW_OUTPUT_DIR}/{code}.parquet", index=False)

def main():
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    try:
        with open(task_file, "r", encoding="utf-8") as f:
            subset = json.load(f)
    except FileNotFoundError:
        print(f"❌ 致命错误: 未找到任务分片文件 {task_file}！"); exit(1)
    if not subset: print("🟡 本分区任务列表为空。"); return
    
    print(f"📦 分区 {TASK_INDEX + 1}，负责 {len(subset)} 支股票。")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ 分区 {TASK_INDEX + 1} Baostock 登录失败: {lg.error_msg}"); exit(1)

    try:
        for s in tqdm(subset, desc=f"分区 {TASK_INDEX + 1} 总体进度"):
            code = s["code"]
            
            # --- (这是唯一的、关键的修正) ---
            # 串行执行两个下载任务
            # download_kdata(code)
            download_fundflow(code)
            # --------------------------------

    finally:
        bs.logout()
    print(f"\n✅ 分区 {TASK_INDEX + 1} 任务完成。")

if __name__ == "__main__":
    main()
