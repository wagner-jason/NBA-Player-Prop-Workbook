#!/usr/bin/env python
# coding: utf-8

# Import necessary libraries
import pandas as pd
from datetime import datetime
from nba_api.stats.endpoints import PlayerGameLog, CommonAllPlayers
import time

# Function to fetch game logs for today's date
def fetch_player_logs_today():
    today = datetime.now().date()
    
    all_logs = []
    all_players = CommonAllPlayers().get_data_frames()[0]
    
    # Filter players who have played during the specified seasons
    players_data = all_players[(all_players['TO_YEAR'].astype(int) >= 2020) & (all_players['FROM_YEAR'].astype(int) <= 2023)]
    
    batch_size = 50
    
    for i in range(0, len(players_data), batch_size):
        batch_players = players_data.iloc[i:i+batch_size]
        
        for index, player in batch_players.iterrows():
            player_id = str(player['PERSON_ID'])
            player_name = player['DISPLAY_FIRST_LAST']
            
            player_logs = []
            
            # Example: Query game logs for today's date
            try:
                print(f"Fetching logs for player {player_name} (ID: {player_id}) for {today}...")
                game_log = PlayerGameLog(player_id=player_id, date_from_utc=today, date_to_utc=today, timeout=60)
                logs = game_log.get_data_frames()[0]
                
                # Add player name to logs DataFrame
                logs['PlayerName'] = player_name
                
                player_logs.append(logs)
            except Exception as e:
                print(f"Error fetching logs for player {player_name} (ID: {player_id}) for {today}: {e}")
            
            # Concatenate logs for current player across all seasons
            if player_logs:
                player_logs_concat = pd.concat(player_logs, ignore_index=True)
                all_logs.append(player_logs_concat)
    
    # Concatenate all player logs into a single DataFrame
    df_today_logs = pd.concat(all_logs, ignore_index=True)
    return df_today_logs

# Function to update game logs with today's data and save to CSV
def update_game_logs():
    # Load existing game logs
    try:
        df_existing_logs = pd.read_csv('C:\\Users\\jwag1\\NBA Stats Python\\player_game_logs.csv')
    except FileNotFoundError:
        df_existing_logs = pd.DataFrame()
    
    # Fetch game logs for today's date
    df_today_logs = fetch_player_logs_today()
    
    # Append today's logs to existing logs
    df_updated_logs = pd.concat([df_existing_logs, df_today_logs], ignore_index=True)
    
    # Save the updated logs to a CSV file
    save_path = 'C:\\Users\\jwag1\\NBA Stats Python\\player_game_logs.csv'
    df_updated_logs.to_csv(save_path, index=False)
    print(f"Updated game logs saved to {save_path}")

# Main execution
if __name__ == "__main__":
    # Call the function to update game logs
    update_game_logs()