import pandas as pd

# 讀入資料
df = pd.read_csv('/Users/windkuo1017/Desktop/NTU_local/fa-25-econ-5166-group-4/data/processed/03-population.csv')

# 把臺中市和臺中縣合併為一個統一名稱
df['city'] = df['city'].replace({'臺中市': '臺中市', '臺中縣': '臺中市'})

# 依照其他欄位 groupby，加總人口
df_grouped = df.groupby(['date', 'year', 'month', 'city'], as_index=False)['pop'].sum()

# 查看結果
df_grouped.to_csv('/Users/windkuo1017/Desktop/NTU_local/fa-25-econ-5166-group-4/data/processed/臺中-data_merged.csv', index=False, encoding='utf-8-sig')

