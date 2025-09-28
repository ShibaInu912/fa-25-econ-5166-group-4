import os
import pandas as pd
from datetime import datetime

# 找到當前程式所在目錄
base_dir = os.path.dirname(__file__)

# 指定 raw 資料夾路徑
raw_dir = os.path.join(base_dir, "..", "raw")

# 指標對照表
files = {
    "日照時數.xls": "sunshine_hours",
    "平均相對濕度.xls": "avg_humidity",
    "平均氣溫.xls": "avg_temp",
    "降水日數.xls": "rainy_days",
    "降水量.xls": "precipitation"
}

# 測站對應縣市（跟前面一樣）
station_city = {
    "淡水": "新北市",
    "鞍部": "臺北市",
    "臺北": "臺北市",
    "竹子湖": "臺北市",
    "基隆": "基隆市",
    "彭佳嶼": "基隆市",
    "花蓮": "花蓮縣",
    "蘇澳": "宜蘭縣",
    "宜蘭": "宜蘭縣",
    "東吉島": "澎湖縣",
    "澎湖": "澎湖縣",
    "臺南": "臺南市",
    "高雄": "高雄市",
    "嘉義": "嘉義市",
    "臺中": "臺中市",
    "阿里山": "嘉義縣",
    "大武": "臺東縣",
    "玉山": "南投縣",
    "新竹": "新竹市",
    "恆春": "屏東縣",
    "成功": "臺東縣",
    "蘭嶼": "臺東縣",
    "日月潭": "南投縣",
    "臺東": "臺東縣",
    "梧棲": "臺中市",
    "金門": "金門縣",
    "馬祖": "連江縣"
}

all_data = []

# 逐檔讀取並轉換
for file, indicator in files.items():
    file_path = os.path.join(raw_dir, file)  # ← 這裡指定 ../raw
    df = pd.read_excel(file_path)
    df_long = pd.melt(df, id_vars=["日期"], var_name="測站", value_name=indicator)
    all_data.append(df_long)

# 合併
merged = all_data[0]
for df in all_data[1:]:
    merged = pd.merge(merged, df, on=["日期", "測站"], how="outer")

# 日期轉西元
def convert_minguo_to_gregorian(date_str):
    minguo_year = int(date_str[:3])
    month = int(date_str[4:6])
    gregorian_year = minguo_year + 1911
    return pd.Timestamp(f"{gregorian_year}-{month:02d}-01")

merged["日期"] = merged["日期"].apply(convert_minguo_to_gregorian)

# 加入縣市欄位

merged["縣市"] = merged["測站"].map(station_city)

# 調整欄位順序
cols = ["日期", "縣市", "測站"] + list(files.values())
merged = merged[cols]

# 輸出 Excel，檔名加上今天日期
processed_dir = os.path.join(base_dir, "..", "processed")
today = datetime.today().strftime("%Y-%m-%d")
output_filename = f"氣象資料_long_{today}.xlsx"
output_path = os.path.join(processed_dir, output_filename)
merged.to_excel(output_path, index=False)

print(f"輸出完成：{output_path}")