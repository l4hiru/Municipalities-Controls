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

age_data <- read_dta("Age Data/agesexdepartements.dta")
diploma_data <- read_dta("Diploma Data/diplomesdepartements.dta")
citysize_data <- read_dta("City Size/popcommunes.dta")
csp_data <- read_dta("CSP Data/cspcommunes.dta")
income_data <- read_dta("Income Data/revdepartements.dta")
pop_data <- read_dta("Pop Data/popcommuneselecteurs.dta")
immi_data <- read_dta("Immi Data/natidepartements.dta")


#II) Variables 

#A) Age

age_data <- age_data %>%   
  select(     
    dep, nomdep,     
    # Sélectionner toutes les colonnes de proportion de 1978 à 2022
    starts_with("prop014"), starts_with("prop1539"), 
    starts_with("prop4059"), starts_with("prop60p")
  ) %>%   
  # Filtrer seulement les colonnes des années 1978-2022
  select(dep, nomdep, matches("prop.*(197[8-9]|19[8-9]\\d|20[0-2]\\d|2022)$")) %>%
  pivot_longer(     
    cols = starts_with("prop"),     
    names_to = c("AgeGroup", "Year"),     
    names_pattern = "prop(\\d+p?)(\\d{4})",     
    values_to = "value"   
  ) %>%   
  # Filtrer les années 1978-2022
  filter(Year >= 1978 & Year <= 2022) %>%
  mutate(     
    AgeGroup = recode(AgeGroup,                       
                      "014" = "part014",                       
                      "1539" = "part1539",                       
                      "4059" = "part4059",                       
                      "60p" = "part60"),     
    Year = factor(Year),     
    value = value * 100  # conversion en pourcentage   
  ) %>%   
  pivot_wider(     
    names_from = AgeGroup,     
    values_from = value   
  ) %>%   
  arrange(dep, Year)

#B) Diploma 

diploma_data <- diploma_data %>%   
  select(     
    dep, nomdep,     
    # Sélectionner toutes les colonnes pbac de 1978 à 2022
    starts_with("pbac")
  ) %>%   
  # Filtrer seulement les colonnes des années 1978-2022
  select(dep, nomdep, matches("pbac(197[8-9]|19[8-9]\\d|20[0-2]\\d|2022)$")) %>%
  pivot_longer(     
    cols = starts_with("pbac"),     
    names_to = "Year",     
    names_pattern = "pbac(\\d{4})",     
    values_to = "partbac"   
  ) %>%   
  # Filtrer les années 1978-2022
  filter(Year >= 1978 & Year <= 2022) %>%
  mutate(     
    Year = factor(Year),     
    partbac = partbac * 100  # convertir en pourcentage si nécessaire   
  ) %>%   
  arrange(dep, Year)

#C) CSP 

csp_data <- csp_data %>%
  # Filtrer les lignes avec nomdep vide
  filter(nomdep != "") %>%
  # Sélectionner seulement les colonnes CSP de 1978 à 2022
  select(
    dep, nomdep,
    matches("^(agri|indp|cadr|pint|empl|ouvr|chom)(197[8-9]|19[8-9]\\d|20[0-2]\\d|2022)$")
  ) %>%
  # Agréger au niveau départemental
  group_by(dep, nomdep) %>%
  summarise(across(where(is.numeric), \(x) mean(x, na.rm = TRUE)), .groups = "drop") %>%
  # Restructurer les données
  pivot_longer(
    cols = -c(dep, nomdep),
    names_to = c("CSP", "Year"),
    names_pattern = "(agri|indp|cadr|pint|empl|ouvr|chom)(\\d{4})",
    values_to = "value"
  ) %>%
  filter(Year >= 1978 & Year <= 2022) %>%
  # Recoder les noms CSP
  mutate(
    CSP = paste0("part", CSP),
    Year = factor(Year)
  ) %>%
  pivot_wider(
    names_from = CSP,
    values_from = value
  ) %>%
  arrange(dep, Year)

#E) Income 

income_data <- income_data %>%   
  select(
    dep, nomdep,          
    # Sélectionner toutes les colonnes revmoy de 1980 à 2022
    starts_with("revmoy")
  ) %>%   
  # Filtrer seulement les colonnes des années 1980-2022
  select(dep, nomdep, matches("revmoy(198\\d|199\\d|20[0-2]\\d|2022)$")) %>%
  pivot_longer(     
    cols = starts_with("revmoy"),     
    names_to = "Year",     
    names_pattern = "revmoy(\\d{4})",     
    values_to = "RevMoy"   
  ) %>%   
  # Filtrer les années 1980-2022
  filter(Year >= 1980 & Year <= 2022) %>%
  mutate(     
    Year = factor(Year)   
  ) %>%   
  arrange(dep, Year)

# F) Population 

pop_data <- pop_data %>%      
 dplyr::select(
   dep, codecommune, nomcommune, 
   voters1973 = electeurs1973, 
   voters1975 = electeurs1975,
   voters1985 = electeurs1985      
 ) %>%
 mutate(
   log_voters1973 = log(voters1973),
   log_voters1975 = log(voters1975),
   log_voters1985 = log(voters1985)
 )

summary(pop_data$voters1973)
summary(pop_data$voters1985)

#G) Immigration 

years <- 1980:2022
petranger_vars <- paste0("petranger", years)

immi_data <- immi_data %>%
  dplyr::select(dep, nomdep, any_of(petranger_vars)) %>%
  dplyr::mutate(across(all_of(petranger_vars), ~ .x * 100))

#III) Parquet exportation 

write_parquet(age_data, "age_data_dep.parquet")
write_parquet(csp_data, "csp_data_dep.parquet")
write_parquet(diploma_data, "diploma_data_dep.parquet")
write_parquet(income_data, "income_data_dep.parquet")
write_parquet(immi_data, "immi_data_dep.parquet")