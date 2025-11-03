import pandas as pd
import numpy as np
import os # 引入 os 模組來檢查檔案是否存在

def clean_numeric(column):
    """
    [FIX v17.2] 輔助函式：清理數字字串 (移除逗號) 並轉換為數值。
    
    這個版本更穩健，它會先將所有值 (包含數字 218) 
    強制轉換為字串，然後再清理並轉回數字，
    以避免混合型態 (mixed-type) 導致的加總失敗。
    """
    
    # 1. 強制將整欄轉換為字串型態
    column_str = column.astype(str)
    
    # 2. 清理逗號 (現在 100% 安全，因為它們都是字串了)
    #    同時清理 'nan' 字串，以防萬一
    column_cleaned = column_str.str.replace(',', '', regex=False).str.replace('nan', '', regex=False)
    
    # 3. 將清理過的字串轉回數字
    return pd.to_numeric(column_cleaned, errors='coerce')

def process_population_sheet(df, gregorian_year, month, sheet_name):
    """
    [v17.1 + DEBUG] 
    處理單一月份的工作表 (DataFrame)。
    此版本包含詳細的 print() 步驟，以便除錯。
    """
    
    # ------------------------------------------------------------------
    # --- [DEBUG] 開關：設為 False 即可關閉所有 print 訊息 ---
    DEBUG_MODE = False 
    # ------------------------------------------------------------------

    if DEBUG_MODE:
        print(f"\n==================== [DEBUG] {sheet_name}: 開始處理 ====================")
        print("--- [DEBUG] 步驟 0: 讀取到的原始資料 (前 10 列) ---")
        # 為了印出 MultiIndex，我們需要轉換
        with pd.option_context('display.max_columns', 15):
            print(df.head(10))

    try:
        # --- 步驟 1: (已移除 v16) ---

        # --- 步驟 2: 自動清理所有欄位名稱 ---
        try:
            new_columns = []
            for col_tuple in df.columns:
                new_tuple = []
                for item in col_tuple:
                    if isinstance(item, str):
                        new_tuple.append(item.strip())
                    else:
                        new_tuple.append(item) # (這會保留 np.nan)
                new_columns.append(tuple(new_tuple))
            df.columns = pd.MultiIndex.from_tuples(new_columns)
            
            if DEBUG_MODE:
                print(f"--- [DEBUG] 步驟 2: 清理欄位名稱 (空格已移除) ---")
                print(df.columns[:20]) # 印出前 20 個清理過的欄位名稱
                
        except Exception as e:
            print(f"    > 警告: 在 {sheet_name} 自動清理欄位名稱時發生錯誤: {e}")

        # --- 步驟 3: 定義欄位名稱 (使用 "清理後" 的名稱) ---
        col_region = ('區 域 別', 'Unnamed: 0_level_1')
        col_gender = ('性別', 'Unnamed: 1_level_1')
        col_total = ('總　計', 'Unnamed: 2_level_1')
        
        sum_col_full = '合　計' # 預設 (全形空格)
        sum_col_half = '合計'   # 備用 (無空格)
        
        # --- 步驟 3.1: 修正 0-14 歲 (幼年人口) 欄位定義 ---
        col_10_14_tuple = ('１０　～　１４', sum_col_half) # 優先嘗試 '合計'
        
        if col_10_14_tuple not in df.columns:
            col_10_14_tuple_alt = ('１０　～　１４', sum_col_full)
            if col_10_14_tuple_alt in df.columns:
                col_10_14_tuple = col_10_14_tuple_alt
            else:
                pass 

        cols_0_14 = [
            ('０　～　４', sum_col_full),
            ('５　～　９', sum_col_full),
            col_10_14_tuple 
        ]

        # 15-64 歲 (v11 已修正打字錯誤)
        cols_15_64 = [
            ('１５　～　１９', sum_col_full), ('２０　～　２４', sum_col_full),
            ('２５　～　２９', sum_col_full), ('３０　～　３４', sum_col_full),
            ('３５　～　３９', sum_col_full), ('４０　～　４４', sum_col_full),
            ('４５　～　４９', sum_col_full), ('５０　～　５４', sum_col_full), # 已修正 'S'
            ('５５　～　５９', sum_col_full), ('６０　～　６４', sum_col_full)
        ]

        # --- [NEW v17] 步驟 3.2: 動態定義 65+ 歲欄位 ---
        cols_65_99 = [
            ('６５　～　６９', sum_col_full), ('７０　～　７４', sum_col_full),
            ('７５　～　７９', sum_col_full), ('８０　～　８４', sum_col_full),
            ('８５　～　８９', sum_col_full), ('９０　～　９４', sum_col_full),
            ('９５　～　９９', sum_col_full)
        ]

        dynamic_100_plus_cols = []
        found_100_plus = False
        
        for col_tuple in df.columns:
            if str(col_tuple[0]).strip() == '100+' or str(col_tuple[0]).strip() == '１００+':
                dynamic_100_plus_cols.append(col_tuple)
                if DEBUG_MODE:
                    print(f"    > [DEBUG] 偵測到 100+ 欄位: {col_tuple}")
                found_100_plus = True

        if not found_100_plus and DEBUG_MODE:
             print(f"    > [DEBUG] 警告: 找不到任何 100+ 相關欄位。")

        cols_65_plus = cols_65_99 + dynamic_100_plus_cols
        
        if DEBUG_MODE:
            print(f"--- [DEBUG] 步驟 3: 定義加總欄位 (65+) ---")
            print(f"    > {cols_65_plus}")


        # --- 步驟 4: 檢查欄位是否存在 ---
        all_cols_to_check = [col_region, col_gender, col_total] + cols_0_14 + cols_15_64 + cols_65_plus
        missing_cols = [col for col in all_cols_to_check if col not in df.columns]

        if missing_cols:
            print(f"    > 錯誤: 工作表 {sheet_name} 欄位不符，可能格式不同。跳過。")
            print(f"    > (提示: 找不到 {missing_cols[0]} 等欄位)")
            return None

        # --- 步驟 5: 清理數值欄位 ---
        all_cols_to_clean = [col_total] + cols_0_14 + cols_15_64 + cols_65_plus
        for col in all_cols_to_clean:
            df[col] = clean_numeric(df[col])

        # --- 步驟 7: 加總年齡群組 ---
        col_0_14_new = ('0-14歲人口', 'sum')
        col_15_64_new = ('15-64歲人口', 'sum')
        col_65_plus_new = ('65歲以上人口', 'sum')
        
        df[col_0_14_new] = df[cols_0_14].sum(axis=1)
        df[col_15_64_new] = df[cols_15_64].sum(axis=1)
        df[col_65_plus_new] = df[cols_65_plus].sum(axis=1) 

        # --- 步驟 8: 建立初步摘要 ---
        df_summary = pd.DataFrame()
        df_summary['區 域 別'] = df[col_region] # 此時包含 NaN
        df_summary['性別'] = df[col_gender]
        df_summary['總計'] = df[col_total]
        df_summary['0-14歲人口'] = df[col_0_14_new]
        df_summary['15-64歲人口'] = df[col_15_64_new]
        df_summary['65歲以上人口'] = df[col_65_plus_new]
        
        if DEBUG_MODE:
            print(f"--- [DEBUG] 步驟 8: 建立摘要表 (尚未填補) ---")
            print(df_summary.head(10))

        # --- 步驟 9: [v9 FIX] 修正篩選邏輯 ---
        
        # 9.1 清理 '性別' 和 '區 域 別' 欄位 "內部" 的資料
        if '性別' in df_summary.columns:
            df_summary['性別'] = df_summary['性別'].astype(str).str.strip()
        if '區 域 別' in df_summary.columns:
            df_summary['區 域 別'] = df_summary['區 域 別'].astype(str).str.strip()
            df_summary['區 域 別'] = df_summary['區 域 別'].replace(['nan', 'Unknown'], np.nan)
        
        if DEBUG_MODE:
            print(f"--- [DEBUG] 步驟 9.1: 清理內容 (NaN 仍在) ---")
            print(df_summary.head(10))

        # 9.2 使用 bfill (向後填補) 和 ffill (向前填補)
        df_summary['區 域 別'] = df_summary['區 域 別'].bfill() # 向上填補到 '計'
        df_summary['區 域 別'] = df_summary['區 域 別'].ffill() # 向下填補到 '女'
        
        if DEBUG_MODE:
            print(f"--- [DEBUG] 步驟 9.2: 執行 bfill/ffill (關鍵步驟) ---")
            print(df_summary.head(10))

        # 9.3 丟棄 ffill/bfill 後仍然是 NaN 的資料列 (移除頂端的總計)
        df_summary = df_summary.dropna(subset=['區 域 別'])
        
        if DEBUG_MODE:
            print(f"--- [DEBUG] 步驟 9.3: 丟棄頂端 NaN ---")
            print(df_summary.head(10))

        # 9.4 定義要排除的摘要列 (清理過的名稱)
        regions_to_exclude = ['總　　計', '臺灣地區', '臺 灣 省', '福 建 省', '臺閩地區']
        
        # 9.5 篩選 '性別' 為 '計'
        df_filtered = df_summary[df_summary['性別'] == '計'].copy()
        
        if DEBUG_MODE:
            print(f"--- [DEBUG] 步驟 9.5: 篩選 性別 == '計' ---")
            print(df_filtered.head(10))
        
        # 9.6 排除摘要列
        df_filtered = df_filtered[~df_filtered['區 域 別'].isin(regions_to_exclude)]
        
        if DEBUG_MODE:
            print(f"--- [DEBUG] 步驟 9.6: 排除摘要列 (總計, 臺灣省...) ---")
            print(df_filtered.head(10))

        # --- 步驟 10: 清理並回傳 ---
        df_filtered = df_filtered.drop(columns=['性別'])
        df_filtered['Year'] = gregorian_year
        df_filtered['Month'] = month

        if DEBUG_MODE:
            print(f"==================== [DEBUG] {sheet_name}: 處理完畢 ====================\n")

        return df_filtered

    except Exception as e:
        print(f"    > 嚴重錯誤: 處理工作表 {sheet_name} 時發生未知錯誤: {e}")
        return None

def post_process_data(df):
    """
    [NEW v12] 對合併後的 long_data_df 進行最終的清理、更名、合併與特徵工程。
    """
    print("...[後處理] 步驟 1: 移除縣市名稱中的所有空格...")
    # 移除 '區 域 別' 欄位中的所有半形與全形空格
    df['區 域 別'] = df['區 域 別'].str.replace(' ', '', regex=False).str.replace('　', '', regex=False)

    print("...[後處理] 步驟 2/3/4: 標準化所有歷史名稱 (無條件替換)...")
    
    # [NEW v12] 移除所有年份判斷，無條件替換所有舊名稱
    replacements_all = {
        # 升格
        "臺北縣": "新北市",
        "臺中縣": "臺中市",
        "臺南縣": "臺南市",
        "高雄縣": "高雄市",
        "桃園縣": "桃園市",
        # 合併
        "新竹縣": "新竹地區",
        "新竹市": "新竹地區"
    }
    df['區 域 別'] = df['區 域 別'].replace(replacements_all)

    print("...[後處理] 步驟 3/4 (續): 依據新名稱加總人口資料...")
    # 透過 GroupBy，自動將 "臺中市"+"臺中縣" (已同名) 或 "新竹地區" 的資料加總
    numeric_cols = ['總計', '0-14歲人口', '15-64歲人口', '65歲以上人口']
    # GroupBy 年、月、新區域名稱，並加總所有數值欄位
    df_agg = df.groupby(['Year', 'Month', '區 域 別'])[numeric_cols].sum().reset_index()

    # --- 這裡是您要新增的程式碼 ---
    print("...[後處理] 步驟4.5: 新增 Date 欄位 (該月第一天)...")
    # 根據 Year 和 Month 欄位建立一個新的 'Date' 欄位
    df_agg['Date'] = pd.to_datetime(df_agg[['Year', 'Month']].assign(Day=1))
    # --- 新增結束 ---

    print("...[後處理] 步驟 5: 新增 city_code...")
    city_order_dict = {
        "基隆市": 1, "新北市": 2, "臺北市": 3, "桃園市": 4, "新竹地區": 5, 
        "宜蘭縣": 7, "苗栗縣": 8, "臺中市": 9, "彰化縣": 10, "南投縣": 11, 
        "雲林縣": 12, "嘉義市": 13, "嘉義縣": 14, "臺南市": 15, "高雄市": 16, 
        "屏東縣": 17, "臺東縣": 18, "花蓮縣": 19, "澎湖縣": 20, "金門縣": 21, 
        "連江縣": 22
    }
    
    df_agg['city_code'] = df_agg['區 域 別'].map(city_order_dict)
    
    # [NEW v12] 警告邏輯簡化
    unmapped_cities = df_agg[df_agg['city_code'].isna()]['區 域 別'].unique()
    if len(unmapped_cities) > 0:
        print(f"    > 警告: 發現未對應的城市名稱: {unmapped_cities}。 這些縣市的 city_code 將為 NaN。")

    return df_agg


def main():
    """
    主執行函式：
    """
    
    # --- 1. 設定年份範圍 ---
    start_year_gregorian = 2002 # 民國 91 年
    end_year_gregorian = 2024   # 民國 113 年
    
    all_dataframes = [] 

    print(f"開始處理 {start_year_gregorian} 至 {end_year_gregorian} 年的資料...")

    # --- 2. 外層迴圈: 遍歷年份 ---
    for year in range(start_year_gregorian, end_year_gregorian + 1):
        roc_year = year - 1911
        input_filename = f"縣市人口數按性別及年齡-{roc_year}年.xls"
        
        print(f"\n--- G正在讀取年份檔案: {input_filename} ---")

        if not os.path.exists(input_filename):
            print(f"  > 警告: 找不到檔案 {input_filename}，跳過此年份。")
            continue

        # --- 3. 內層迴圈: 遍歷月份 (1-12月) ---
        for month in range(1, 13):
            
            # 根據年份決定工作表名稱
            if year <= 2013: # 102年 (含) 以前
                sheet_name = f"{roc_year}{month:02d}"
            else: # 103年 (含) 以後
                sheet_name = f"{month:02d}"
            
            try:
                print(f"  > G正在處理工作表: {sheet_name}")
                df_sheet = pd.read_excel(
                    input_filename, 
                    sheet_name=sheet_name, 
                    header=[2, 3], 
                    engine='xlrd'
                )
                
                # --- 4. 呼叫處理函式 ---
                df_processed = process_population_sheet(df_sheet, year, month, sheet_name)
                
                if df_processed is not None and not df_processed.empty:
                    all_dataframes.append(df_processed)

            except Exception as e:
                print(f"  > G資訊: 工作表 {sheet_name} 不存在或讀取失敗，跳過。")
                pass 

    # --- 5. 合併 ---
    if not all_dataframes:
        print("\n處理完成，但沒有找到任何可處理的資料。")
        return

    print("\n...正在合併所有年份和月份的資料...")
    long_data_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"...合併完成，共 {len(long_data_df)} 筆原始資料。")

    # --- [MODIFIED] 步驟 6: 執行 v12 後處理 (清理, 合併, city_code) ---
    print("\n...開始執行資料後處理 (Post-Processing)...")
    final_df = post_process_data(long_data_df)
    print("...後處理完成。")

    # --- 步驟 7: 排序並儲存 ---
    final_df = final_df.sort_values(by=['city_code', 'Year', 'Month'])

    # 重新排列欄位，將 'Date' 放在最前面
    cols_order = [
        'Date', 'Year', 'Month', 'city_code', '區 域 別', '總計', 
        '0-14歲人口', '15-64歲人口', '65歲以上人口'
    ]

    # # 重新排列欄位
    # cols_order = [
    #     'Year', 'Month', 'city_code', '區 域 別', '總計', 
    #     '0-14歲人口', '15-64歲人口', '65歲以上人口'
    # ]
    
    cols_present = [col for col in cols_order if col in final_df.columns]
    other_cols = [col for col in final_df.columns if col not in cols_order]
    final_df = final_df[cols_present + other_cols]
# --- 這裡是您要新增的程式碼 ---
    print("...[最終步驟] 欄位名稱中文化 (Rename Columns)...")
    
    # 在這裡一次定義所有您想要的 1:1 英文名稱
    rename_dict = {
        'Date': 'date',
        'Year': 'year',
        'Month': 'month',
        'city_code': 'city_code',
        '區 域 別': 'city',
        '總計': 'pop_total',
        '0-14歲人口': 'pop_0-14',
        '15-64歲人口': 'pop_15-64',
        '65歲以上人口': 'pop_65up'
    }
    
    final_df = final_df.rename(columns=rename_dict)
    # --- 新增結束 ---


    output_filename = f"population_long_data_CLEANED_{start_year_gregorian}-{end_year_gregorian}.csv"
    final_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    print(f"\n========================================================")
    print(f"成功！所有資料已清理、合併並儲存至: {output_filename}")
    print(f"總共處理了 {len(final_df)} 筆資料 (縣市-月份)。")
    print("========================================================")
    print("\nLong Data (頭 5 筆):")
    print(final_df.head())
    print("\nLong Data (末 5 筆):")
    print(final_df.tail())

# S執行主函式
if __name__ == "__main__":
    main()