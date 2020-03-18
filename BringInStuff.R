library(purrr)
library(readr)
library(dplyr)
library(stringr)
library(lubridate)
library(broom)

load_data <- function(path) {
  csv_files <- fs::dir_ls(path, regexp = "\\.csv$") %>%
    map_dfr(read_csv, col_types = cols(visitor_start_time = col_character()))
}

players_df = load_data(path = '/Users/acgrose/Documents/nbadata/data/players/') %>%
  mutate(position = ifelse(pos == "G" | pos == "PG", "Guard",
                           ifelse(pos == "G-F" | pos == "F-G" | pos == "GF", "Wing", "Big"))) %>%
  filter(!is.na(pos))

players_df = players_df[!duplicated(players_df[, c("personId")], fromLast=T),] %>%
  mutate(name = paste(firstName, lastName))

bs_df = load_data(path = '/Users/acgrose/Documents/nbadata/data/2018/boxscores/') %>% 
  bind_rows(., load_data('/Users/acgrose/Documents/nbadata/data/2019/boxscores/')) %>%
  mutate(fp = points + totReb * 1.25 + assists * 1.5 - turnovers + steals * 3 + blocks * 3)

games_df_2019 = load_data(path = '/Users/acgrose/Documents/nbadata/data/2019/games/') %>% dplyr::select(season_id, id, date, visitor.id, home.id)
games_df_2018 = load_data(path = '/Users/acgrose/Documents/nbadata/data/2018/games/') %>% dplyr::select(season_id, id, date, visitor.id, home.id)
games_df = bind_rows(games_df_2018, games_df_2019) %>%
  mutate(season_type = substring(season_id, 1, 1)) %>%
  filter(season_type == 2)

game.no_df = games_df %>%
  dplyr::select(season_id, id, visitor.id) %>%
  rename(team.id=visitor.id) %>%                                                                                                                                                                                                                                                
  bind_rows(., games_df %>% dplyr::select(season_id, id, home.id) %>% rename(team.id=home.id)) %>%
  group_by(season_id, team.id) %>%
  arrange(id) %>%
  mutate(game.no = 1:n()) %>%
  ungroup()

bs_df.game = bs_df %>% 
  left_join(., game.no_df, by = c("gameId"='id', "teamId"="team.id")) %>%
  dplyr::select(-firstName, -lastName, -teamId) %>%
  mutate(game_minus_1 = game.no - 1) %>%
  dplyr::select(-gameId) %>%
  mutate(fga.min = fga/(min + 0.001),
         pts.min = points/(min + 0.001),
         fp.min = fp/(min + 0.001))

bs_df_gg = bs_df.game %>%
  left_join(., bs_df.game, by = c("personId", "season_id", "game.no"="game_minus_1")) %>%
  setNames(gsub(pattern = ".x", "", names(.))) %>%
  setNames(gsub(pattern = ".y", "_nplus1", names(.))) %>%
  dplyr::select(-game_minus_1) %>%
  filter(!is.na(fp_nplus1))

cor_matrix = bs_df_gg %>% cor(use = 'complete.obs') %>% round(3)





