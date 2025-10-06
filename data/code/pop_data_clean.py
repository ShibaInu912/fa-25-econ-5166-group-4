import pandas as pd
import os
from datetime import datetime
from time import sleep
# 檔案名稱

base_dir = os.path.dirname(__file__)
raw_dir = os.path.join(base_dir, "..", "raw")
processed_dir = os.path.join(base_dir, "..", "processed")
os.makedirs(processed_dir, exist_ok=True)

file1 = 'pop_104前各縣市人口（月資料）.xls'
file_name1 = os.path.join(raw_dir, file1)



file2 = 'pop_105後各縣市人口（月資料）.xlsx'
file_name2 = os.path.join(raw_dir, file2)

# 數據起始行：縣市名稱（區域別）和數據從 Excel 的第 4 行開始 (Python 索引 3)
data_start_row = 3

files_list = [file_name1, file_name2]

# 儲存所有長格式數據的列表
all_data_long = []

for file_name in files_list:
    print(f"\n正在處理檔案: {os.path.basename(file_name)}")
    
    # 讀取 Excel 中所有工作表的名稱
    try:
        excel_file = pd.ExcelFile(file_name)
        sheet_names = excel_file.sheet_names
    except FileNotFoundError:
        print(f"錯誤：找不到檔案 {file_name}，請確認檔案路徑。")
        exit()

    for sheet_name in sheet_names:
        # 檢查工作表名稱是否為有效的年份（整數）
        try:
            current_year = int(sheet_name)
            if not (2002 <= current_year <= 2024): # 設定一個合理的年份範圍檢查
                raise ValueError
            print(f"--- 正在處理年份工作表: {current_year} ---")
        except ValueError:
            print(f"工作表名稱 '{sheet_name}' 不是有效的年份數字，跳過。")
            continue

        # 1. 讀取數據：縣市名稱和 12 個月人口數
        try:
            # 讀取數據部分，不使用原始複雜標頭
            df = pd.read_excel(
                file_name,
                sheet_name=sheet_name,
                header=None,  # 不使用原始標頭
                skiprows=data_start_row, 
                usecols='A:M' # 讀取 A 欄 (區域別) 到 M 欄 (12月)
            )
        except Exception as e:
            print(f"讀取工作表 {current_year} 時發生錯誤: {e}，跳過。")
            continue

        # 2. 設定標頭：手動定義「區域別」和 12 個月份的欄位名稱
        # 由於 Excel 標頭是多行的，我們手動創建一個簡潔的月份名稱
        month_headers = [f'{current_year}年{i}月底' for i in range(1, 13)]
        df.columns = ['區域別'] + month_headers
        
        # 處理可能讀取到的空行
        df.dropna(subset=['區域別'], inplace=True)

        # 3. 清理數據：移除總計、地區或省份的加總行
        # 只保留縣市名稱（假設這些加總行的名稱包含特定關鍵字）
        df_clean = df[
            ~df['區域別'].astype(str).str.contains(
                '總計|Total|臺灣地區|Taiwan Area|臺灣省|Taiwan Prov.|福建省|Fuchien Province|區域|the|Locality|更新日期', 
                na=False
            )
        ].copy()

        # print(df_clean.head())
        # sleep(10)  # 暫停 1 秒，方便查看輸出

        # 4. 執行資料融化 (Melt)
        # 將 12 個月份的寬欄位堆疊成兩個長欄位：'月份' 和 '人口數'
        month_cols_to_melt = df_clean.columns[1:13] # 從第 2 欄到第 13 欄

        df_long = pd.melt(
            df_clean,
            id_vars=['區域別'],
            value_vars=month_cols_to_melt,
            var_name='月份標籤',       
            value_name='人口數'     
        )

        df_long['區域別'] = df_long['區域別'].str.replace(r'\s+', '', regex=True)
        df_long['區域別'] = df_long['區域別'].str.slice(0, 3) 
        
        # 6. 執行縣市合併與更名 (新增的邏輯)
        
        # 確保「人口數」為數值，非數字值設為 NaN，才能進行加總
        df_long['人口數'] = pd.to_numeric(df_long['人口數'], errors='coerce')

        # 6.1. 桃園縣改名為桃園市
        df_long['區域別'] = df_long['區域別'].str.replace('桃園縣', '桃園市')
        
        # 6.2. 臺北縣改名為新北市
        df_long['區域別'] = df_long['區域別'].str.replace('臺北縣', '新北市')
        
        # 6.3. 合併/加總 臺南縣/市
        # 標記需要合併的縣市，並將其名稱統一為合併後的名稱（如：臺南市）
        df_long.loc[df_long['區域別'] == '臺南縣', '區域別'] = '臺南市'
        
        # 6.4. 合併/加總 高雄縣/市
        df_long.loc[df_long['區域別'] == '高雄縣', '區域別'] = '高雄市'
        
        # 6.5. 進行分組加總 (groupby)
        # 必須根據 '區域別' 和 '月份標籤' 進行加總，才能完成合併
        df_long = df_long.groupby(['區域別', '月份標籤'], as_index=False)['人口數'].sum()
        
        # 7. 提取月份和建立日期欄位
        # 由於 groupby 會丟棄其他欄位，我們需要重新計算
        df_long['年份'] = current_year
        df_long['月份數字'] = df_long['月份標籤'].str.extract(r'(\d+)月').astype(int)

        # 設置為該月的第一天
        df_long['日期'] = pd.to_datetime(
            df_long['年份'].astype(str) + '-' + df_long['月份數字'].astype(str) + '-01'
        ) 

        # 重新整理欄位順序
        df_long = df_long[['日期', '年份', '月份數字', '區域別', '人口數']].copy()
        
        all_data_long.append(df_long)

# 6. 合併所有工作表的結果
if not all_data_long:
    print("\n沒有找到有效的年份工作表進行處理。")
    exit()

final_df_long = pd.concat(all_data_long, ignore_index=True)




rename_dict = {
    "日期": "date",
    "年份": "year",
    "月份數字": "month",
    "區域別": "city",
    "人口數": "pop",
}

				

final_df_long = final_df_long.rename(columns=rename_dict)

today = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
output_filename = f"pop_long_{today}.csv"
output_path = os.path.join(processed_dir, output_filename)


# 7. 儲存結果 (可選)
final_df_long.to_csv(output_path, index=False, encoding='utf-8-sig')

print("--- 轉換完成 ---")
print(f"成功處理 {len(all_data_long)} 個年份工作表。")
print("最終長格式數據的前 5 行：")
print(final_df_long.head())
print(f"\n最終數據總行數：{len(final_df_long)}")