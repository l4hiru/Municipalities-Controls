# Piketty Municipalities Control Variables

#0) Packages 

library(httr)
library(jsonlite)
library(dplyr)
library(readr)
library(tidyr)
library(arrow)
library(stargazer)
library(plm)
library(summarytools)
library(haven)

#I) Data 

age_data <- read_dta("Age Data/agesexcommunes.dta")


#II) Variables 

#A) Age

age_data <- age_data %>%
  select(
    codecommune, nomcommune,
    prop0141973, prop15391973, prop40591973, prop60p1973,
    prop0141980, prop15391980, prop40591980, prop60p1980, 
    prop0141987, prop15391987, prop40591987, prop60p1987
  ) %>%
  pivot_longer(
    cols = starts_with("prop"),
    names_to = c("AgeGroup", "Year"),
    names_pattern = "prop(\\d+p?)(\\d{4})",
    values_to = "value"
  ) %>%
  mutate(
    AgeGroup = recode(AgeGroup,
                      "014" = "part014",
                      "1539" = "part1539",
                      "4059" = "part4059",
                      "60p" = "part60"),
    Year = factor(Year, levels = c("1973", "1980", "1987")),
    value = value * 100  # conversion en pourcentage
  ) %>%
  pivot_wider(
    names_from = AgeGroup,
    values_from = value
  ) %>%
  arrange(codecommune, Year)

#B) Diploma 