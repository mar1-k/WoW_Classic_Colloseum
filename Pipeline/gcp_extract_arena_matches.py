import os
import re
import hashlib
from google.cloud import storage
from datetime import datetime

class ExtractArenaGames:
    def __init__(self):
        self.current_arena = None
        self.current_events = []
        self.player_ids = set()
        self.player_id_pattern = re.compile(r'Player-\d+-\w+')  # Regex for player IDs

    def extract_player_ids(self, element):
        regex_matches = self.player_id_pattern.findall(element)
        for regex_match in regex_matches:
            self.player_ids.add(regex_match)

    def process(self, element):
        parts = element.split(',')
        if 'ZONE_CHANGE' in parts[0]:
            if self.current_arena and self.current_events:
                self.current_events.append(element)
                yield self.current_arena, self.current_events, self.player_ids
                self.current_events = []  # Reset events for the new arena
                self.current_arena = None
                self.player_ids = set()  # Reset player IDs for the new arena
            zone_name = parts[2].strip('"').replace('\'','') #Remove double quotes and apostrophes
            # Only take logs for Arena zones
            valid_zones = ["Nagrand Arena", "Ruins of Lordaeron", "Dalaran Arena", "Blades Edge Arena"]
            if zone_name in valid_zones:
                # Add the zone change event to the beginning of events list
                self.current_events.append(element)
                self.current_arena_event = element  # Store the current zone change event
                timestamp_parts = parts[0].split(' ')
                date_parts = timestamp_parts[0].split('/')
                time_parts = timestamp_parts[1].split(':')
                year = datetime.now().year
                month = date_parts[0]
                day = date_parts[1]
                time = time_parts[0] + '.' + time_parts[1]
                timestamp = f"{year}-{month}-{day}_{time}"
                map_name = zone_name.replace(' ', '_').lower()
                self.current_arena = f"{timestamp}_{map_name}"

        else:
            if self.current_arena:
                if "SPELL_CAST_FAILED" not in element:
                    self.current_events.append(element)
                    self.extract_player_ids(element)

    def finish_bundle(self):
        if self.current_arena and self.current_events:
            yield self.current_arena, self.current_events, self.player_ids

def generate_unique_match_id(arena, player_ids):
    combined_string = (arena + ''.join(sorted(player_ids))).lower()
    unique_id = hashlib.sha256(combined_string.encode()).hexdigest()
    return unique_id

def save_game_to_gcs(match_data, output_bucket):
    arena, events, player_ids = match_data
    if len(events) < 25 or len(player_ids) < 2:  # Check for sufficient events and unique players
        return
    unique_match_id = generate_unique_match_id(arena, player_ids)
    output_file_name = f"{arena}_{unique_match_id}.txt"

    # Create a Blob object for the file
    output_blob = output_bucket.blob(output_file_name)
    # Check if the file already exists, if it does, skip.
    if output_blob.exists():
        print(f"File {output_file_name} already exists. Skipping upload.")
        return

    # Upload the new file
    output_blob.upload_from_string('\n'.join(events) + '\n')
    print(f"File {output_file_name} uploaded successfully.")

def extract_arena_games(event, context):
    client = storage.Client()
    bucket_name = event['bucket']
    file_name = event['name']
    
    input_bucket = client.bucket(bucket_name)
    output_bucket = client.bucket('cata-colosseum-arena-matches')
    
    blob = input_bucket.blob(file_name)
    content = blob.download_as_text().splitlines()
    
    extractor = ExtractArenaGames()
    games = []

    for line in content:
        for game in extractor.process(line):
            games.append(game)
    
    for game in extractor.finish_bundle():
        games.append(game)

    for game in games:
        save_game_to_gcs(game, output_bucket)
