# scripts/download_parallel.py (最终正确版 - 双引擎)

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

# Baostock 配置
KDATA_START_DATE = "2005-01-01"

# 新浪财经配置
SINA_API_HISTORY = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page={page}&num=50&sort=opendate&asc=0&daima={code}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}

# --- 获取环境变量 & 准备目录 ---
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(KDATA_OUTPUT_DIR, exist_ok=True)
os.makedirs(MONEYFLOW_OUTPUT_DIR, exist_ok=True)


def download_kdata(code):
    """(引擎A) 从 Baostock 获取K线数据"""
    try:
        rs = bs.query_history_k_data_plus(
            code, "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,isST",
            start_date=KDATA_START_DATE, end_date="", frequency="d", adjustflag="3"
        )
        if rs.error_code != '0':
            print(f"\n  -> 🟡 Baostock K-Data API Warning for {code}: {rs.error_msg}")
            return False # 表示本次下载操作未成功
        
        data_list = [rs.get_row_data() for _ in iter(rs.next, False)]
        if data_list:
            df = pd.DataFrame(data_list, columns=rs.fields)
            df.to_parquet(f"{KDATA_OUTPUT_DIR}/{code}.parquet", index=False)
            return True # 成功下载并保存
        return True # 没有历史数据也算成功完成
    except Exception as e:
        print(f"\n  -> ❌ Baostock K-Data download CRASHED for {code}: {e}")
        return False

def download_fundflow(code):
    """(引擎B) 从新浪财经获取资金流数据 (高容错版)"""
    all_data_list = []
    page = 1
    code_for_api = code.replace('.', '')
    
    while True: # 无限循环，由内部逻辑 break
        try:
            target_url = SINA_API_HISTORY.format(page=page, num=50, code=code_for_api)
            response = requests.get(target_url, headers=HEADERS, timeout=45)
            response.raise_for_status()
            response.encoding = 'gbk'
            data = response.json()
            
            if not data or len(data) == 0:
                break # 正常结束
            
            all_data_list.extend(data)

            if len(data) < 50:
                break # 正常结束

            page += 1
            time.sleep(0.3)
        except Exception as e:
            # (关键) 捕获异常，打印信息，然后跳出循环，不抛出异常
            print(f"\n  -> ❌ 在请求新浪资金流 {code} 的第 {page} 页时出错: {e}")
            break
            
    if all_data_list:
        df_full = pd.DataFrame(all_data_list)
        # --- 数据清洗 ---
        try:
            columns_to_keep = {
                'opendate': 'date', 'trade': 'close', 'changeratio': 'pct_change',
                'turnover': 'turnover_rate', 'netamount': 'net_flow_amount',
                'r0_net': 'main_net_flow', 'r1_net': 'super_large_net_flow',
                'r2_net': 'large_net_flow', 'r3_net': 'medium_small_net_flow'
            }
            if all(col in df_full.columns for col in columns_to_keep.keys()):
                df_selected = df_full[list(columns_to_keep.keys())]
                df_renamed = df_selected.rename(columns=columns_to_keep)
                df_renamed['date'] = pd.to_datetime(df_renamed['date'])
                numeric_cols = df_renamed.columns.drop('date')
                df_renamed[numeric_cols] = df_renamed[numeric_cols].apply(pd.to_numeric, errors='coerce')
                df_renamed['code'] = code
                df_final = df_renamed.sort_values(by='date', ascending=True).reset_index(drop=True)
                df_final.to_parquet(f"{MONEYFLOW_OUTPUT_DIR}/{code}.parquet", index=False)
                return True # 成功下载并保存
        except Exception as e:
            print(f"\n  -> ❌ 在处理新浪资金流 {code} 的数据时出错: {e}")
            return False
            
    return True # 即使没数据也算成功完成

def main():
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    try:
        with open(task_file, "r", encoding="utf-8") as f:
            subset = json.load(f)
    except FileNotFoundError:
        print(f"❌ 致命错误: 未找到任务分片文件 {task_file}！"); sys.exit(1)
        
    if not subset:
        print("🟡 本分区任务列表为空。"); return
        
    print(f"📦 分区 {TASK_INDEX + 1}，负责 {len(subset)} 支股票。")
    
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Baostock 登录失败: {lg.error_msg}"); sys.exit(1)
    print("✅ Baostock 登录成功")

    successful_stocks_count = 0
    try:
        for s in tqdm(subset, desc=f"分区 {TASK_INDEX + 1} 总体进度"):
            code = s["code"]
            name = s.get("name", "")
            
            try:
                # 串行执行两个下载任务
                kdata_ok = download_kdata(code)
                fundflow_ok = download_fundflow(code)
                
                # 只要其中任意一个数据成功下载，就算这次处理是成功的
                if kdata_ok or fundflow_ok:
                    successful_stocks_count += 1

            except Exception as e:
                # 捕获意料之外的、更严重的错误
                print(f"\n  -> ❌ 在主循环中处理 {name} ({code}) 时发生严重错误: {e}")

    finally:
        bs.logout()
        print("✅ Baostock 登出成功")

    print(f"\n✅ 分区 {TASK_INDEX + 1} 任务完成。")
    print(f"   - 负责股票数: {len(subset)}")
    print(f"   - 至少一种数据下载成功的股票数: {successful_stocks_count}")
    
    # 只有当一个文件都未成功下载时，才让 job 失败
    if successful_stocks_count == 0 and len(subset) > 0:
        print("\n❌ 致命错误: 本分区没有成功下载任何一只股票的任何数据！")
        sys.exit(1)

if __name__ == "__main__":
    main()
