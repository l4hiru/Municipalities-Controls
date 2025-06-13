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
library(stringr)

#I) Data 

age_data <- read_dta("Age Data/agesexcommunes.dta")
diploma_data <- read_dta("Diploma Data/diplomescommunes.dta")
citysize_data <- read_dta("City Size/popcommunes.dta")
csp_data <- read_dta("CSP Data/cspcommunes.dta")
income_data <- read_dta("Income Data/revcommunes.dta")
pop_data <- read_dta("Pop Data/popcommuneselecteurs.dta")


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

diploma_data <- diploma_data %>%
  select(
    codecommune, nomcommune,
    pbac1973,
    pbac1980, 
    pbac1987
  ) %>%
  pivot_longer(
    cols = starts_with("pbac"),
    names_to = "Year",
    names_pattern = "pbac(\\d{4})",
    values_to = "partbac"
  ) %>%
  mutate(
    Year = factor(Year, levels = c("1973", "1980", "1987")),
    partbac = partbac * 100  # convertir en pourcentage si nécessaire
  ) %>%
  arrange(codecommune, Year)

#C) Rural (not time-varying variable ! based on 2022 data)

citysize_data <- citysize_data %>%
  select(codecommune, nomcommune, codeagglo) %>%
  mutate(
    Rural = case_when(
      is.na(codeagglo) ~ NA_real_,                     
      str_starts(codeagglo, "C") ~ 1,              
      TRUE ~ 0                        
    )
  ) %>%
  crossing(Year = factor(c("1973", "1980", "1987"))) %>%
  arrange(codecommune, Year)

#D) CSP 

csp_data <- csp_data %>%
  select(
    codecommune, nomcommune,
    pagri1973, pagri1980, pagri1987,
    pindp1973, pindp1980, pindp1987,
    pcadr1973, pcadr1980, pcadr1987,
    ppint1973, ppint1980, ppint1987,
    pempl1973, pempl1980, pempl1987,
    pouvr1973, pouvr1980, pouvr1987,
    pchom1973, pchom1980, pchom1987
  ) %>%
  pivot_longer(
    cols = -c(codecommune, nomcommune),
    names_to = c("CSP", "Year"),
    names_pattern = "p(\\w+)(\\d{4})",
    values_to = "value"
  ) %>%
  mutate(
    CSP = recode(CSP,
                 "agri" = "partagri",
                 "indp" = "partindp",
                 "cadr" = "partcadr",
                 "pint" = "partpint",
                 "empl" = "partempl",
                 "ouvr" = "partouvr",
                 "chom" = "partchom"),
    Year = factor(Year, levels = c("1973", "1980", "1987")),
    value = value * 100
  ) %>%
  pivot_wider(
    names_from = CSP,
    values_from = value
  ) %>%
  arrange(codecommune, Year)

#E) Income 

income_data <- income_data %>%
  select(codecommune, nomcommune,
         revratio1973, revratio1980, revratio1987) %>%
  pivot_longer(
    cols = starts_with("revratio"),
    names_to = "Year",
    names_pattern = "revratio(\\d{4})",
    values_to = "RatioRev"
  ) %>%
  mutate(
    Year = factor(Year, levels = c("1973", "1980", "1987")),
    RatioRev = RatioRev * 100 
  ) %>%
  arrange(codecommune, Year)

# F) Population 

pop_data <- pop_data %>%      
 dplyr::select(
   codecommune, nomcommune, voters1973 = electeurs1973, voters1985 = electeurs1985      
 ) %>%
 mutate(
   log_voters1973 = log(voters1973),
   log_voters1985 = log(voters1985)
 )

summary(pop_data$voters1973)
summary(pop_data$voters1985)


#III) Parquet exportation 

write_parquet(age_data, "age_data.parquet")
write_parquet(citysize_data, "citysize_data.parquet")
write_parquet(csp_data, "csp_data.parquet")
write_parquet(diploma_data, "diploma_data.parquet")
write_parquet(income_data, "income_data.parquet")
write_parquet(pop_data, "pop_data.parquet")