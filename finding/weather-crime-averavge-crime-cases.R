# 載入套件
library(tidyverse)
library(lubridate)
library(tseries)

# 讀取資料
dat <- read_csv("04-weather-crime-pop.csv")

# 檢查欄位名稱
names(dat)

# 確保時間欄位是日期格式
dat <- dat %>%
  mutate(date = ym(paste(year, month, sep = "-")))

# ---（1）全台平均暴力犯罪的時間趨勢 ---
ts_all <- dat %>%
  group_by(date) %>%
  summarise(violent_cases_mean = mean(violent_crime_cases, na.rm = TRUE))

# 視覺化
ggplot(ts_all, aes(x = date, y = violent_cases_mean)) +
  geom_line(color = "#D1495B", linewidth = 1) +
  labs(
    title = "Trend of Violent Crime Cases in Taiwan (2002–2024)",
    x = "Year",
    y = "Average Violent Crime Cases"
  ) +
  theme_minimal()
render("weather_crime_average_crime_cases.R", output_format = "html_document")
knitr::spin("weather_crime_average_crime_case.R", knit = FALSE)
rmarkdown::render("1103 敘述統計分析.Rmd")
