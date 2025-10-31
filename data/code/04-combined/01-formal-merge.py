import pandas as pd
import numpy as np
from pathlib import Path
import os
from datetime import datetime
from time import sleep
import itertools

# ====================================================================================================================
# 【主控台】

# 檔案路徑
PATH = "/Users/windkuo1017/Desktop/NTU_local/fa-25-econ-5166-group-4/data/raw"
OUTPUT_PATH = "/Users/windkuo1017/Desktop/NTU_local/fa-25-econ-5166-group-4/data/processed"

# 是否要檢測天氣資料重複值
is_check_weather_duplicates = True

# 是否要檢攝天氣資料缺值
is_check_weather_missing = True

# 是否輸出天氣資料
is_output_weather = True

# 匯出天氣+犯罪
is_output_weather_crime = True

# 匯出天氣+犯罪+人口
is_output_weather_crime_pop = True



df_weather1 = pd.read_excel(PATH+"/01a-weather-big-station-only.xlsx")
df_weather2 = pd.read_csv(PATH+"/01b-pivoted_stations.csv")

#  匯入 crime 資料
crime_cols = [
    'date', 
    'city',
    'theft_cases',
    'traffic_accident_cases',
    'alcohol_drive_cases',
    'general_injury_cases',
    'total_crime_exclude_traffic',
    'sexual_assault_cases',
    'violent_crime_cases',
    'drug_cases'
]
df_crime = pd.read_excel(PATH+"/02-crime.xlsx", usecols=crime_cols)

pop_cols = ['date', 'city', 'pop']
df_pop = pd.read_csv(PATH+"/03-population.csv", usecols=pop_cols)

# ====================================================================================================================

city_station_dict = {
    "基隆市": ["基隆"],
    "新北市": ["板橋","新北"],
    "臺北市": ["臺北"],
    "桃園市": ["新屋","新屋1","新屋2","新屋3"],
    "新竹地區": ["新竹"],
    "新竹市": ["新竹"],
    "宜蘭縣": ["宜蘭"],
    "苗栗縣": ["三義","三義1","三義2"],
    "臺中市": ["臺中"],
    "彰化縣": ["員林"],
    "南投縣": ["日月潭"],
    "雲林縣": ["虎尾"],
    "嘉義市": ["嘉義"],
    "嘉義縣": ["奮起湖"],
    "臺南市": ["臺南"],
    "高雄市": ["高雄"],
    "屏東縣": ["恆春"],
    "臺東縣": ["臺東"],
    "花蓮縣": ["花蓮"],
    "澎湖縣": ["澎湖"],
    "金門縣": ["金門"],
    "連江縣": ["馬祖"]
}



# 統一欄位名稱
df_weather2 = df_weather2.rename(columns={"station_name": "station"})

# 新增缺少欄位讓兩個表格一致
for col in ["date", "city"]:
    if col not in df_weather2.columns:
        df_weather2[col] = pd.NA

# 重新排序欄位，與 df_weather1 一致
df_weather2 = df_weather2[df_weather1.columns]

df_weather1['city'] = df_weather1['city'].replace({
    '新竹市': '新竹地區',
    '新竹縣': '新竹地區'
})

# station -> city 反向查表
station_to_city = {s: c for c, stations in city_station_dict.items() for s in stations}

df_weather2["city"] = df_weather2["station"].map(station_to_city)

# 合併
df_weather = pd.concat([df_weather1, df_weather2], ignore_index=True)

# -----------------------------
# Step 1：用 year + month 生成 date（每月第一天）
df_weather['date'] = pd.to_datetime(df_weather[['year', 'month']].assign(day=1))

# Step 2：把 ["--", "X", ""] 轉成缺失值
df_weather.replace(["--", "X", ""], np.nan, inplace=True)

# Step 3（可選）：把數值型欄位轉成 float
num_cols = ['sunshine_hours', 'avg_humidity', 'avg_temp', 'rainy_days', 'precipitation']
for col in num_cols:
    df_weather[col] = pd.to_numeric(df_weather[col], errors='coerce')



# -----------------------------

# 1. 刪掉 year <= 2001
df_weather = df_weather[df_weather['year'] > 2001]
df_weather = df_weather[df_weather['year'] < 2025]

# 2. 只保留在 city_station_dict 的測站
valid_stations = [s for stations in city_station_dict.values() for s in stations]
df_weather = df_weather[df_weather['station'].isin(valid_stations)]

# -----------------------------

# 依 city_station_dict 產生 city_code
city_list = list(city_station_dict.keys())
city_code_map = {city: i+1 for i, city in enumerate(city_list)}  # city_code 從 1 開始

# 新增 city_code 欄位
df_weather['city_code'] = df_weather['city'].map(city_code_map)

# -----------------------------

# 移動 city_code 到第四欄
col = df_weather.pop('city_code')      # 先把 city_code 拿出來
df_weather.insert(3, 'city_code', col) # 插入到 index 3，也就是第四欄

# -----------------------------
# 檢查
# 找出重複的組合


if(is_check_weather_duplicates):
    duplicates = df_weather[df_weather.duplicated(subset=['year', 'month', 'city_code'], keep=False)]

    if len(duplicates) == 0:
        print("✅ (year, month, city_code) 唯一")
    else:
        print("⚠️ 有重複的組合：")
        print(duplicates)

    df_weather = df_weather.drop_duplicates(subset=['year', 'month', 'city_code'], keep='last')

    # 再檢測一次
    duplicates = df_weather[df_weather.duplicated(subset=['year', 'month', 'city_code'], keep=False)]
    if len(duplicates) == 0:
        print("✅ 刪除重複值後，(year, month, city_code) 唯一")
    else:
        print("⚠️ 刪除重複值後，依然有重複的組合：")
        print(duplicates)


# -----------------------------
# 排序
df_weather = df_weather.sort_values(by=['city_code', 'date'], ascending=[True, True])
# 重設索引
df_weather = df_weather.reset_index(drop=True)

if(is_output_weather):
    # 輸出
    df_weather.to_csv(OUTPUT_PATH+"/01c-merged-weather-clean.csv", index=False, encoding="utf-8-sig")

    print("合併完成，資料已輸出")

# -----------------------------

if(is_check_weather_missing):
    # 檢查是否有缺值
    # 假設 df 已經有 year, month, city_code
    # 1️⃣ 定義範圍
    years = list(range(2002, 2025))        # 2002~2024
    months = list(range(1, 13))           # 1~12
    city_codes = [i for i in range(1, 23) if i != 6]  # 1~22 except 6

    # 2️⃣ 生成所有可能組合
    all_combinations = pd.DataFrame(
        list(itertools.product(years, months, city_codes)),
        columns=['year', 'month', 'city_code']
    )

    # 3️⃣ 標示哪些已存在
    all_combinations['exists'] = all_combinations.apply(
        lambda row: ((df_weather['year'] == row['year']) & 
                    (df_weather['month'] == row['month']) & 
                    (df_weather['city_code'] == row['city_code'])).any(),
        axis=1
    )

    # 4️⃣ 只保留缺失的組合
    missing = all_combinations[~all_combinations['exists']].drop(columns='exists')

    print(missing)


# ===========================================================================================
# 【二、合併犯罪資料】

# df_weather['date'] = pd.to_datetime(df_weather['date'])
# df_crime['date'] = pd.to_datetime(df_crime['date'])
# df_weather['city'] = df_weather['city'].str.strip()
# df_crime['city'] = df_crime['city'].str.strip()

# # Step 1：檢查欄位
# print("df_weather columns:", df_weather.columns.tolist())
# print("df_crime columns:", df_crime.columns.tolist())

# # Step 2：檢查前五筆資料
# print("df_weather head:")
# print(df_weather.head())
# print("df_crime head:")
# print(df_crime.head())

# # Step 3：檢查 date 型態
print("df_weather date dtype:", df_weather['date'].dtype)
print("df_crime date dtype:", df_crime['date'].dtype)

# merged_check = df_weather.merge(
#     df_crime[['date','city']],
#     on=['date','city'],
#     how='left',
#     indicator=True
# )
# print(merged_check['_merge'].value_counts())
# # "_merge" 會顯示 left_only / right_only / both
# # left_only 就是 df_weather 沒對應到 df_crime 的資料

# Step 7：只先測試 merge 前幾筆
# print("測試 merge 前五筆:")
# print(df_weather.head())
# print(df_crime.head())


# 處理新竹問題
def normalize_city(city):
    if city in ['新竹市', '新竹縣']:
        return '新竹地區'
    else:
        return city
    
df_crime['city'] = df_crime['city'].apply(normalize_city)

df_crime = (
    df_crime
    .groupby(['date', 'city'], as_index=False)
    .sum(numeric_only=True)
)

# 合併 weather + crime
df_merged = pd.merge(
    df_weather,
    df_crime,
    left_on=['date','city'],
    right_on=['date','city'],
    how='left'   # left join，保留 df_weather 全部資料
)

if(is_output_weather_crime):
    # 輸出
    df_merged.to_csv(OUTPUT_PATH+"/02z-merged-weather-crime.csv", index=False, encoding="utf-8-sig")

    print("02：合併完成，資料已輸出")

# ===========================================================================================
# 【三、合併人口資料】

df_pop['city'] = df_pop['city'].apply(normalize_city)
df_pop = (
    df_pop
    .groupby(['date', 'city'], as_index=False)
    .sum(numeric_only=True)
)

# ----------------------------
# 合併
df_pop['date'] = pd.to_datetime(df_pop['date'], errors='coerce')

df_merged = pd.merge(
    df_merged,
    df_pop,
    on=['date', 'city']
)


# --------------------------------
# 調整欄位
# 先把 pop 欄拿出來
col_pop = df_merged.pop('pop')

# 插入到第7欄（index 6）
df_merged.insert(6, 'pop', col_pop)


if(is_output_weather_crime_pop):
    # 輸出
    df_merged.to_csv(OUTPUT_PATH+"/04-weather-crime-pop.csv", index=False, encoding="utf-8-sig")

    print("03：合併完成，資料已輸出")

# print(df_weather['date'].dtype)
# print(df_crime['date'].dtype)
# print(df_pop['date'].dtype)  # <- population CSV 的 date
# print(df_merged['date'].dtype)  # 如果你已經做過一次 merge
