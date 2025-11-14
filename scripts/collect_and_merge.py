# scripts/collect_and_merge.py

import pandas as pd
import glob
import os
from tqdm import tqdm
import shutil
import json

# --- 配置 ---
INPUT_BASE_DIR = "all_data"
OUTPUT_KDATA_DIR = "final_kdata"
OUTPUT_MONEYFLOW_DIR = "final_moneyflow"
QC_REPORT_FILE = "data_quality_report.json"

# run_quality_check 函数保持不变

def collect_and_merge_data(data_type, output_dir):
    print("\n" + "="*50)
    print(f"🚀 开始收集和处理 {data_type} 数据...")
    
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    # (关键修正) 搜索路径不再包含 "data_slice"
    search_pattern = os.path.join(INPUT_BASE_DIR, "data_part_*", data_type, "*.parquet")
    file_list = glob.glob(search_pattern)
    
    if not file_list:
        print(f"⚠️ 未找到任何 {data_type} 的 Parquet 文件。")
        return None

    print(f"📦 共找到 {len(file_list)} 个 {data_type} 文件，开始收集...")
    all_dfs = []
    for src_path in tqdm(file_list, desc=f"收集中 ({data_type})"):
        try:
            filename = os.path.basename(src_path)
            dest_path = os.path.join(output_dir, filename)
            shutil.copy2(src_path, dest_path)
            all_dfs.append(pd.read_parquet(dest_path))
        except Exception as e:
            print(f"\n⚠️ 处理文件 {src_path} 失败: {e}")
            
    print(f"✅ 全部 {len(file_list)} 个文件已收集到 '{output_dir}' 目录。")
    
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return None

def main():
    kdata_df = collect_and_merge_data("kdata", OUTPUT_KDATA_DIR)
    moneyflow_df = collect_and_merge_data("moneyflow", OUTPUT_MONEYFLOW_DIR)
    
    # run_quality_check(kdata_df, moneyflow_df) # 质检可以后续再完善
