library(purrr)
library(readr)
library(dplyr)
library(tidyverse)
library(tidytext)
library(stringr)
library(lubridate)
library(broom)

load_data = function(data_dir) {
  fs::dir_ls(data_dir, regexp = "\\.csv$") %>% 
    map_dfr(read_csv)
}

df_2018 = load_data('data/2018/boxscores')
df_2019 = load_data('data/2019/boxscores')
df = bind_rows(df_2018, df_2019)

expected_fgp = sum(df$fgm, na.rm = T)/sum(df$fga, na.rm = T)
expected_tpp = sum(df$tpm, na.rm = T)/sum(df$tpa, na.rm = T)
expected_ftp = sum(df$ftm, na.rm = T)/sum(df$fta, na.rm = T)

schedule = load_data('data/schedule/')

player_avg = df %>% 
  group_by(personId) %>%
  summarise(games = n(),
            fgp = sum(fgm, na.rm = T)/sum(fga, na.rm = T),
            ftp = sum(ftm, na.rm = T)/sum(fta, na.rm = T),
            tpp = sum(tpm, na.rm = T)/sum(tpa, na.rm = T)
            )

this_date = '2020-03-10'

s = schedule %>% filter(startDateEastern == gsub('-', '', this_date)) %>% select(hTeam.teamId, vTeam.teamId)
teams = c(s$hTeam.teamId, s$vTeam.teamId)

recent_game_ids = df_2019 %>% 
  left_join(., schedule %>% select(startDateEastern, gameId), by = "gameId") %>%
  mutate(date = as.Date(paste0(substring(startDateEastern, 1, 4), '-', substring(startDateEastern, 5, 6), '-', substring(startDateEastern, 7, 8)))) %>%
  filter(teamId %in% teams) %>%
  filter(date < this_date) %>%
  group_by(teamId) %>%
  filter(row_number() == n()) %>%
  pull(gameId)

player_avg = bs_df %>% 
  group_by(personId) %>%
  summarise(games = n(),
            pts = mean(points, na.rm = T),
            fgp = sum(fgm, na.rm = T)/sum(fga, na.rm = T),
            ftp = sum(ftm, na.rm = T)/sum(fta, na.rm = T),
            tpp = sum(tpm, na.rm = T)/sum(tpa, na.rm = T),
            reb = mean(totReb, na.rm = T),
            ass = mean(assists, na.rm = T),
            to = mean(turnovers, na.rm = T),
            stl = mean(steals, na.rm = T),
            blk = mean(blocks, na.rm = T))

final_df = df_2019 %>% 
       filter(teamId %in% teams) %>%
       filter(gameId %in% recent_game_ids) %>%
       left_join(., player_avg, by = c("personId")) %>%
       mutate(name = paste(firstName, lastName),
              fp = points + assists * 1.5 + totReb * 1.25 - turnovers + blocks * 3 + steals * 3,
              exp_pts = round((fga * fgp) * 2 + fta * ftp + (tpa * tpp) * 3, 1),
              exp_fp = round(exp_pts + reb * 1.25 + ass * 1.5 - to + stl * 3 + blk * 3, 1),
              diff = fp - exp_fp) %>%
       select(name, min, points, exp_pts, fga, tpa, fta, assists, totReb, turnovers, steals, blocks, exp_fp, fp, diff) %>%
       arrange(-exp_fp)
  
