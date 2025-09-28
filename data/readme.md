# 資料說明

- Author: 郭秉豐
- Last Updated: 2025-09-28 21:25

同步發布於Discord群組

## 犯罪資料（2002-2024）
- 主要變數欄位（以下單位都是「件數」）
  - `theft_cases`：竊盜發生件數
  - `traffic_accident_cases`：駕駛過失發生件數
  - `general_injury_cases`：一般傷害發生件數
  - `total_crime_exclude_traffic`：刑案總件數(不含駕駛過失)
  - `sexual_assault_cases`：性侵害發生件數
  - `violent_crime_cases`：暴力犯罪發生件數
  - `drug_cases`：毒品查獲件數
    - 如你所見，我覺得之後可以再調整column的順序

- 資料來源：[警政統計查詢網](https://ba.npa.gov.tw/statis/webMain.aspx?k=defjsp)

- 注意事項
  - 我國於100年與103年進行縣市改制，此處資料已將舊行政區劃整併進去
    - e.g. 95年的「高雄市」欄位是舊高雄市與舊高雄縣的加總
  - 性侵害案件包括強制性交、共同強制性交、對幼性交及性交猥褻等4案類。
  - 為符合實際治安現況，自113年9月起，刑案相關數據由受理報案e化系統自動產出刑案紀錄表統計，部分刑案發生數的計算方式由原本從「1案1發生數」改為員警受理報案「1被害人1發生數」，
    - e.g. 以詐騙集團詐騙3名被害人報案為例，原先計算方式為發生數1件，刑案統計方式變更後發生數計算為3件。

## 天氣資料（2002-2024）
- 主要變數欄位
  - `sunshine_hours`：日照時數
  - `avg_humidity`：月平均相對濕度
  - `avg_temp`：月平均氣溫
  - `rainy_days`：月降水日數
  - `precipitation`：月降水量

- 資料來源：[交通統計查詢網](https://statis.motc.gov.tw/motc/Statistics/Display?Seq=333&Start=113-08-00&End=114-08-00&ShowYear=true&ShowMonth=true&ShowQuarter=false&ShowHalfYear=false&Mode=0&ColumnValues=4371&CodeListValues=6128_6129_6130_6131_6132_6133_6134_6135_6136_6137_6138_6139_6140_6141_6142_6143_6144_6145_6146_6147_6148_6149_6150_6151_6152_6153_6154)

- 注意事項
  - 裡面的資料是以「氣象測站」為紀錄標的，並不是以縣市區分。因此，我們需要討論各縣市要取哪個測站作為評估標準。