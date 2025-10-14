import pandas as pd
from pathlib import Path

data_path = Path("/Users/windkuo1017/Desktop/NTU_local/fa-25-econ-5166-group-4/data/processed")

crime_df = pd.read_excel(data_path / "crime_long_2025-10-06_21-17-02.xlsx")
pop_df = pd.read_csv(data_path / "pop_long_2025-10-06_21-10-07.csv")
weather_df = pd.read_excel(data_path / "weather_long_2025-09-28.xlsx")

# 統一 date 欄位為 datetime
crime_df['date'] = pd.to_datetime(crime_df['date'])
pop_df['date'] = pd.to_datetime(pop_df['date'])
weather_df['date'] = pd.to_datetime(weather_df['date'])

# city -> station 對應
city_station_map = {
    "基隆市": "基隆",
    "新北市": "板橋",
    "臺北市": "臺北",
    "桃園市": "新屋",
    "新竹市": "新竹",
    "新竹縣": "新竹",
    "宜蘭縣": "宜蘭",
    "苗栗縣": "三義",
    "臺中市": "臺中",
    "彰化縣": "員林",
    "南投縣": "日月潭",
    "雲林縣": "虎尾",
    "嘉義市": "嘉義",
    "嘉義縣": "奮起湖",
    "臺南市": "臺南",
    "高雄市": "高雄",
    "屏東縣": "恆春",
    "臺東縣": "臺東",
    "花蓮縣": "花蓮",
    "澎湖縣": "澎湖",
    "金門縣": "金門",
    "連江縣": "馬祖"
}

# 建立 city-station pair 的 set
valid_city_station = set(city_station_map.items())  # (city, station)

# 只保留 city-station 在對應表的資料
weather_df = weather_df[weather_df.apply(lambda row: (row['city'], row['station']) in valid_city_station, axis=1)].copy()


# 合併
merged_df = crime_df.merge(pop_df, on=['city', 'date'], how='left') \
                    .merge(weather_df, on=['city', 'date'], how='left')

# 刪掉 rainy_days 為 NaN 的列
merged_df = merged_df.dropna(subset=['rainy_days'])


# 儲存
merged_df.to_csv(data_path / "merged_data.csv", index=False, encoding="utf-8-sig")

print("資料合併完成")
