import pandas as pd

# 讀取 CSV
df = pd.read_csv('/Users/windkuo1017/Desktop/NTU_local/fa-25-econ-5166-group-4/data/processed/01c-merged-weather-clean.csv')

# 對 city 分組，計算缺失值數量
missing_by_city = df.groupby('city').apply(lambda x: x.isnull().sum())
print("各城市缺失值數量:")
print(missing_by_city)

# # 對 city 分組，做數值型欄位敘述統計
# desc_by_city = df.groupby('city').describe()
# # print("\n各城市數值型欄位敘述統計:")
# # print(desc_by_city)

# # 如果想要每個城市的缺失值比例，可以這樣：
# missing_count_by_city = df.groupby('city').apply(lambda x: x.isnull().sum())
# print("\n各城市缺失值數量:")
# print(missing_count_by_city)
