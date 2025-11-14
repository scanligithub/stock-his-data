# scripts/download_parallel.py (交换顺序 + 严格模式)

import os
import json
import baostock as bs
import requests
import pandas as pd
from tqdm import tqdm
import time
import sys # 引入 sys 模块

# ... 配置部分保持不变 ...

def download_kdata(code):
    """从 Baostock 获取K线数据"""
    rs = bs.query_history_k_data_plus(
        code, "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,isST",
        start_date=KDATA_START_DATE, end_date="", frequency="d", adjustflag="3"
    )
    if rs.error_code != '0':
        # (关键) 如果API明确返回错误，打印并返回 False
        print(f"\n  -> 🟡 Baostock K-Data API Warning for {code}: {rs.error_msg}")
        return False
    
    data_list = [rs.get_row_data() for _ in iter(rs.next, False)]
    if data_list:
        df = pd.DataFrame(data_list, columns=rs.fields)
        df.to_parquet(f"{KDATA_OUTPUT_DIR}/{code}.parquet", index=False)
        return True # 成功下载并保存
    return True # 没有数据也算成功完成

def download_fundflow(code):
    """从新浪财经获取资金流数据"""
    all_data_list = []
    page = 1
    code_for_api = code.replace('.', '')
    while page <= 100:
        target_url = SINA_API_HISTORY.format(page=page, num=50, code=code_for_api)
        response = requests.get(target_url, headers=HEADERS, timeout=45)
        response.raise_for_status() # 请求失败直接抛异常
        response.encoding = 'gbk'
        data = response.json()
        if not data: break
        all_data_list.extend(data)
        if len(data) < 50: break
        page += 1
        time.sleep(0.3)
            
    if all_data_list:
        df = pd.DataFrame(all_data_list)
        df.to_parquet(f"{MONEYFLOW_OUTPUT_DIR}/{code}.parquet", index=False)
        return True # 成功下载并保存
    return True # 没有数据也算成功完成

def main():
    # ... 读取和切分任务的逻辑不变 ...

    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Baostock 登录失败: {lg.error_msg}"); sys.exit(1)
    print("✅ Baostock 登录成功")

    try:
        success_stocks = 0
        for s in tqdm(subset, desc=f"分区 {TASK_INDEX + 1} 总体进度"):
            code = s["code"]
            name = s.get("name", "")
            
            try:
                # --- (关键修正: 交换顺序) ---
                # 1. 先下载资金流
                fundflow_success = download_fundflow(code)
                
                # 2. 再下载K线
                kdata_success = download_kdata(code)
                
                if kdata_success and fundflow_success:
                    success_stocks += 1

            except Exception as e:
                # (关键) 捕获任何下载失败，并清晰地打印
                print(f"\n  -> ❌ 在处理 {name} ({code}) 时发生严重错误，已跳过: {e}")
                # 我们可以不让整个 job 失败，而是继续处理下一只
                
    finally:
        bs.logout()

    print(f"\n✅ 分区 {TASK_INDEX + 1} 任务完成。")
    print(f"   - 负责股票数: {len(subset)}")
    print(f"   - 至少一种数据下载成功的股票数: {success_stocks}")
    
    # (关键) 如果一个文件都没下载成功，就让 job 失败
    if success_stocks == 0 and len(subset) > 0:
        print("\n❌ 致命错误: 本分区没有成功下载任何一只股票的数据！")
        sys.exit(1)


if __name__ == "__main__":
    main()
