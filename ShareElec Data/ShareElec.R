# Share of ind in electrity econ sector (municipality lvl)

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
library(COGugaison)

#I) Data 

share_75 <- read_parquet("ShareElec Data/share_elec75.parquet")
share_82 <- read_parquet("ShareElec Data/share_elec82.parquet")
share_90 <- read_parquet("ShareElec Data/share_elec90.parquet")

#II) Variables 

#A) Year and renaming variables

share_75$Year <- as.factor(1975)

share_82$Year <- as.factor(1982)
share_82$pop_total <- share_82$pop_totale

share_90$Year <- as.factor(1990)
share_90$pop_total <- share_90$pop_totale

#B) Harmonizing municipality codes

share_75_harmo <- changement_COG_varNum(table_entree= share_75, annees=c(1975:2022), agregation = FALSE, libgeo = TRUE, donnees_insee = TRUE) # It works but lose factor Year variable bc of aggregation !
share_82_harmo <- changement_COG_varNum(table_entree= share_82, annees=c(1982:2022), agregation = FALSE, libgeo = TRUE, donnees_insee = TRUE) 
share_90_harmo <- changement_COG_varNum(table_entree= share_90, annees=c(1990:2022), agregation = FALSE, libgeo = TRUE, donnees_insee = TRUE) 

length(unique(share_75$code_commune))
length(unique(share_75_harmo$code_commune))

length(unique(share_82$code_commune))
length(unique(share_82_harmo$code_commune))

length(unique(share_90$code_commune))
length(unique(share_90_harmo$code_commune))

#C) Sum numeric variables and get unique municipality code (for merging municipalities)

share_75_harmo <- share_75_harmo %>%
  group_by(code_commune, nom_commune, Year) %>% 
  summarise(
    pop_total = sum(pop_total, na.rm = TRUE),
    pop_elec = sum(pop_elec, na.rm = TRUE),
    pop_employment = sum(pop_employment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(share_elec = pop_elec / pop_employment)

share_82_harmo <- share_82_harmo %>%
  group_by(code_commune, nom_commune, Year) %>% 
  summarise(
    pop_total = sum(pop_total, na.rm = TRUE),
    pop_elec = sum(pop_elec, na.rm = TRUE),
    pop_employment = sum(pop_employment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(share_elec = pop_elec / pop_employment)

share_90_harmo <- share_90_harmo %>%
  group_by(code_commune, nom_commune, Year) %>% 
  summarise(
    pop_total = sum(pop_total, na.rm = TRUE),
    pop_elec = sum(pop_elec, na.rm = TRUE),
    pop_employment = sum(pop_employment, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(share_elec = pop_elec / pop_employment)

rm(share_75, share_75_harmo2, share_82, share_90)

#III) Panel dataset construction

#A) Binding rows

panel_data <- bind_rows(share_75_harmo, share_82_harmo, share_90_harmo)

#B) Linear interpolation

colnames(panel_data)
str(panel_data)

panel_data <- panel_data %>%
  # Créer un dataframe complet avec toutes les années pour chaque commune
  complete(nesting(code_commune, nom_commune), Year = c(1975, 1982, 1988, 1990)) %>%
  # Grouper par commune pour les calculs d'interpolation
  group_by(code_commune, nom_commune) %>%
  # Interpolation linéaire pour share_elec (retourne NA si impossible)
  mutate(share_elec = ifelse(Year == 1988 & is.na(share_elec),
                           tryCatch({
                             approx(Year, share_elec, xout = 1988, rule = 1)$y
                           }, error = function(e) NA),
                           share_elec)) %>%
  # Réorganiser les lignes
  arrange(code_commune, Year) %>%
  ungroup()