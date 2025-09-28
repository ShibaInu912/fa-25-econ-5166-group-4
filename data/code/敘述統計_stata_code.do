clear all
cd "/Users/windkuo1017/Desktop/NTU_local/fa-25-econ-5166-group-4/data"

import excel "processed/氣象資料_long_2025-09-28.xlsx", sheet("Sheet1") firstrow clear

/* rename 測站 station
rename 縣市 city
rename 日期 date */

* 建立一個欄位列表
local varlist sunshine_hours avg_humidity avg_temp rainy_days precipitation

/* foreach var of local varlist {
    * 將 "-" 先改成空字串
    replace `var' = "" if `var' == "-"
    
    * 將字串轉成數字，空字串會變成 missing
    destring `var', replace
} */


// sort station
// by station: summarize sunshine_hours avg_humidity avg_temp rainy_days precipitation, detail

tabstat sunshine_hours avg_humidity avg_temp rainy_days precipitation, ///
    by(station) ///
    statistics(n mean sd min max median)

tabstat sunshine_hours avg_humidity avg_temp rainy_days precipitation, ///
    by(year) ///
    statistics(n mean sd min max median)

preserve
	collapse (mean) avg_temp, by(year)
	twoway line avg_temp year
restore
