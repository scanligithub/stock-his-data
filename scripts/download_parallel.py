# scripts/download_parallel.py

import os
import json
import baostock as bs
import requests
import pandas as pd
from tqdm import tqdm
import time
import sys

# --- 配置 ---
KDATA_OUTPUT_DIR = "data_slice/kdata"
MONEYFLOW_OUTPUT_DIR = "data_slice/moneyflow"
KDATA_START_DATE = "2005-01-01"
SINA_API_HISTORY = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page={page}&num=50&sort=opendate&asc=0&daima={code}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}

TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(KDATA_OUTPUT_DIR, exist_ok=True)
os.makedirs(MONEYFLOW_OUTPUT_DIR, exist_ok=True)

def download_kdata(code):
    """从 Baostock 获取K线数据"""
    try:
        rs = bs.query_history_k_data_plus(
            code, "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,isST",
            start_date=KDATA_START_DATE, end_date="", frequency="d", adjustflag="3"
        )
        if rs.error_code != '0':
            print(f"\n  -> 🟡 Baostock K-Data API Warning for {code}: {rs.error_msg}")
            return False
        
        data_list = [rs.get_row_data() for _ in iter(rs.next, False)]
        if data_list:
            df = pd.DataFrame(data_list, columns=rs.fields)
            df.to_parquet(f"{KDATA_OUTPUT_DIR}/{code}.parquet", index=False)
            return True
        return True # 没有历史数据也算成功完成
    except Exception as e:
        print(f"\n  -> ❌ Baostock K-Data download CRASHED for {code}: {e}")
        return False

def download_fundflow(code):
    """从新浪财经获取资金流数据"""
    all_data_list = []
    page = 1
    code_for_api = code.replace('.', '')
    while page <= 100: # 最大页数限制，防止无限循环
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
            return False # 下载失败
            
    if all_data_list:
        df = pd.DataFrame(all_data_list)
        df.to_parquet(f"{MONEYFLOW_OUTPUT_DIR}/{code}.parquet", index=False)
    return True # 即使没数据也算成功完成

def main():
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    try:
        with open(task_file, "r", encoding="utf-8") as f:
            subset = json.load(f)
    except FileNotFoundError:
        print(f"❌ 致命错误: 未找到任务分片文件 {task_file}！")
        sys.exit(1)
    
    if not subset:
        print("🟡 本分区任务列表为空。")
        return

    print(f"📦 分区 {TASK_INDEX + 1}，负责 {len(subset)} 支股票。")

    # --- 阶段 1: 下载所有资金流数据 (无需登录 Baostock) ---
    print("\n--- 开始下载资金流数据 (Sina) ---")
    fundflow_success_count = 0
    for s in tqdm(subset, desc=f"分区 {TASK_INDEX + 1} 资金流下载"):
        code = s["code"]
        name = s.get("name", "")
        try:
            if download_fundflow(code):
                fundflow_success_count += 1
        except Exception as e:
            print(f"\n  -> ❌ 在处理 {name} ({code}) 的资金流时发生严重错误: {e}")
    print(f"✅ 资金流下载完成，成功 {fundflow_success_count} / {len(subset)}。")


    # --- 阶段 2: 下载所有K线数据 (需要登录 Baostock) ---
    print("\n--- 开始下载K线数据 (Baostock) ---")
    kdata_success_count = 0
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Baostock 登录失败: {lg.error_msg}，将跳过K线数据下载。")
    else:
        print("✅ Baostock 登录成功")
        try:
            for s in tqdm(subset, desc=f"分区 {TASK_INDEX + 1} K线下载"):
                code = s["code"]
                name = s.get("name", "")
                try:
                    if download_kdata(code):
                        kdata_success_count += 1
                except Exception as e:
                    print(f"\n  -> ❌ 在处理 {name} ({code}) 的K线时发生严重错误: {e}")
        finally:
            bs.logout()
            print("✅ Baostock 登出成功")
    print(f"✅ K线下载完成，成功 {kdata_success_count} / {len(subset)}。")

    # --- 总结 ---
    print(f"\n🏁 分区 {TASK_INDEX + 1} 所有任务完成。")
    
    # 只有当两种数据都完全没下载下来时，才让 job 失败
    if fundflow_success_count == 0 and kdata_success_count == 0 and len(subset) > 0:
        print("\n❌ 致命错误: 本分区没有成功下载任何数据！")
        sys.exit(1)

if __name__ == "__main__":
    main()
