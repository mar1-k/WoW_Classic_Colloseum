from datetime import datetime
import apache_beam as beam
import hashlib
import re
import os

class ExtractArenaGames(beam.DoFn):
    def __init__(self):
        self.current_arena = None
        self.current_events = []
        self.player_ids = set()
        self.player_id_pattern = re.compile(r'Player-\d+-\w+') 

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
            zone_name = parts[2].strip('"')
            #Only take logs for Arena zones
            valid_zones = ["Nagrand Arena", "Ruins of Lordaeron", "Dalaran Arena", "Blade's Edge Arena"]
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
                if "SPELL_CAST_FAILED" not in element:  # Omit SPELL_CAST_FAILED events
                    self.current_events.append(element)
                    self.extract_player_ids(element)

    def finish_bundle(self):
        if self.current_arena and self.current_events and self.player_ids:
            yield self.current_arena, self.current_events, self.player_ids

def generate_unique_match_id(arena, player_ids):
    combined_string = (arena + ''.join(sorted(player_ids))).lower()
    unique_id = hashlib.sha256(combined_string.encode()).hexdigest()
    return unique_id

def write_output_file(match_data, output_dir):
    if len(match_data[2]) < 2:    #Invalidate logs with less than 2 unique players
        return
    arena, events, player_ids = match_data
    if len(events) < 25:
        return
    unique_match_id = generate_unique_match_id(arena, player_ids)
    output_file = os.path.join(output_dir, f"{arena + '_' + unique_match_id}.txt")
    with open(output_file, 'a') as f:  # Use 'a' mode to append events to existing files
        f.write('\n'.join(events))
        f.write('\n')  # Add a newline after appending events = f"{unique_match_id}.txt"

def run(input_file, output_dir):
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'Read from File' >> beam.io.ReadFromText(input_file)
            | 'Extract Arena Events' >> beam.ParDo(ExtractArenaGames())
            | 'Write to Files' >> beam.ParDo(write_output_file, output_dir)
        )

if __name__ == '__main__':
    input_file = './WoWCombatLog-060224_040929.txt'
    output_dir = './output/'
    os.makedirs(output_dir, exist_ok=True)
    run(input_file, output_dir)
