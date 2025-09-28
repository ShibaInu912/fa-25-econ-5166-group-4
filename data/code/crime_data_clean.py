import os
import pandas as pd
from datetime import datetime
import numpy as np

# 資料夾設定
base_dir = os.path.dirname(__file__)
raw_dir = os.path.join(base_dir, "..", "raw")
processed_dir = os.path.join(base_dir, "..", "processed")
os.makedirs(processed_dir, exist_ok=True)

# 讀取所有 crime CSV 檔
crime_files = [f for f in os.listdir(raw_dir) if f.startswith("crime_") and f.endswith(".csv")]

all_data = []

for file in crime_files:
    file_path = os.path.join(raw_dir, file)
    
    # 讀 CSV，先全部當字串讀
    df = pd.read_csv(file_path, dtype=str)
    
    # wide → long
    # 指標名稱從檔名自動抓：crime_XXX_total.csv → XXX
    indicator = file.replace("crime_", "").replace("_total.csv", "")
    df_long = pd.melt(df, id_vars=["date"], var_name="city", value_name=indicator)
    
    # 將 "-" 轉成 NaN，再轉成 float
    df_long[indicator] = pd.to_numeric(df_long[indicator].replace("－", np.nan), errors='coerce')
    
    all_data.append(df_long)

# 合併所有 crime 指標
merged = all_data[0]
for df in all_data[1:]:
    merged = pd.merge(merged, df, on=["date", "city"], how="outer")

# 日期轉西元 datetime
def convert_minguo_to_gregorian(date_str):
    date_str = date_str.replace(" ", "")  # 去掉空格
    year_part, month_part = date_str.split("年")
    year = int(year_part) + 1911
    month = int(month_part.replace("月",""))
    return pd.Timestamp(f"{year}-{month:02d}-01")

merged["date"] = merged["date"].apply(convert_minguo_to_gregorian)

# 從 date 取出 year / month
merged["year"] = merged["date"].dt.year
merged["month"] = merged["date"].dt.month

# 調整欄位順序
cols = ["date", "year", "month", "city"] + [file.replace("crime_","").replace("_total.csv","") for file in crime_files]
merged = merged[cols]

# 重新命名欄位成英文
rename_dict = {
    "date": "date",
    "year": "year",
    "month": "month",
    "city": "city",
    "竊盜發生件數": "theft_cases",
    "駕駛過失發生數": "traffic_accident_cases",
    "一般傷害發生數": "general_injury_cases",
    "刑案總數(不含駕駛過失)": "total_crime_exclude_traffic",
    "性侵害發生件數": "sexual_assault_cases",
    "暴力犯罪發生數": "violent_crime_cases",
    "毒品查獲件數": "drug_cases"
}

merged = merged.rename(columns=rename_dict)


# 輸出 Excel
today = datetime.today().strftime("%Y-%m-%d")
output_filename = f"crime_long_{today}.xlsx"
output_path = os.path.join(processed_dir, output_filename)

merged.to_excel(output_path, index=False)

print(f"輸出完成：{output_path}")
