import os
import re
from datetime import datetime, timedelta
from google.cloud import storage, bigquery


#Spell dictionary for spec matching
#This is a really bad way of doing this, but itll do for now
spell_dict = {
        "Blood Shield": ("Death Knight", "Blood"),
        "Rune Tap": ("Death Knight", "Blood"),
        "Vampiric Blood": ("Death Knight", "Blood"),
        "Dancing Rune Weapon": ("Death Knight", "Blood"),
        "Frost Strike": ("Death Knight", "Frost"),
        "Improved Icy Talons" : ("Death Knight", "Frost"),
        "Howling Blast": ("Death Knight", "Frost"),
        "Pillar of Frost": ("Death Knight", "Frost"),
        "Hungering Cold": ("Death Knight", "Frost"),
        "Scourge Strike": ("Death Knight", "Unholy"),
        "Ebon Plague": ("Death Knight, Unholy"),
        "Festering Strike": ("Death Knight", "Unholy"),
        "Unholy Blight": ("Death Knight", "Unholy"),
        "Summon Gargoyle": ("Death Knight", "Unholy"),
        "Anti-Magic Zone": ("Death Knight", "Unholy"),
        "Dark Transformation": ("Death Knight", "Unholy"),
        "Moonkin Aura": ("Druid", "Balance"),
        "Moonkin Form": ("Druid", "Balance"),
        "Starfall": ("Druid", "Balance"),
        "Eclipse (Solar)" : ("Druid", "Balance"),
        "Eclipse (Lunar)" : ("Druid", "Balance"),
        "Solar Beam": ("Druid", "Balance"),
        "Mangle": ("Druid", "Feral"),
        "Rake": ("Druid", "Feral"),
        "Shred": ("Druid", "Feral"),
        "Ferocious Bite": ("Druid", "Feral"),
        "Rip": ("Druid", "Feral"),
        "Maul": ("Druid", "Feral"),
        "Swipe": ("Druid", "Feral"),
        "Savage Defense": ("Druid", "Feral"),
        "Frenzied Regeneration": ("Druid", "Feral"),
        "Harmony": ("Druid", "Restoration"),
        "Lifebloom": ("Druid", "Restoration"),
        "Tree of Life": ("Druid", "Restoration"),
        "Swiftmend": ("Druid", "Restoration"),
        "Nature's Cure": ("Druid", "Restoration"),
        "Kill Command": ("Hunter", "Beast Mastery"),
        "Bestial Wrath": ("Hunter", "Beast Mastery"),
        "Intimidation": ("Hunter", "Beast Mastery"),
        "Ferocious Inspiration": ("Hunter", "Beast Mastery"),
        "Marked for Death": ("Hunter", "Marksmanship"),
        "Readiness": ("Hunter", "Marksmanship"),
        "Chimera Shot": ("Hunter", "Marksmanship"),
        "Trueshot Aura": ("Hunter", "Marksmanship"),
        "Explosive Shot": ("Hunter", "Survival"),
        "Black Arrow": ("Hunter", "Survival"),
        "Lock and Load": ("Hunter", "Survival"),
        "Wyvern Sting": ("Hunter", "Survival"),
        "Hunting Party": ("Hunter", "Survival"),
        "Arcane Blast": ("Mage", "Arcane"),
        "Slow": ("Mage", "Arcane"),
        "Arcane Tactics": ("Mage", "Arcane"),
        "Arcane Barrage": ("Mage", "Arcane"),
        "Presence of Mind": ("Mage", "Arcane"),
        "Arcane Power": ("Mage", "Arcane"),
        "Pyroblast": ("Mage", "Fire"),
        "Combustion": ("Mage", "Fire"),
        "Living Bomb": ("Mage", "Fire"),
        "Dragon's Breath": ("Mage", "Fire"),
        "Cauterize": ("Mage", "Fire"),
        "Blast Wave": ("Mage", "Fire"),
        "Deep Freeze": ("Mage", "Frost"),
        "Ice Barrier": ("Mage", "Frost"),
        "Fingers of Frost": ("Mage", "Frost"),
        "Holy Shock": ("Paladin", "Holy"),
        "Beacon of Light": ("Paladin", "Holy"),
        "Divine Light": ("Paladin", "Holy"),
        "Illuminated Healing": ("Paladin", "Holy"),
        "Hammer of the Righteous": ("Paladin", "Protection"),
        "Shield of the Righteous": ("Paladin", "Protection"),
        "Avenger's Shield": ("Paladin", "Protection"),
        "Ardent Defender": ("Paladin", "Protection"),
        "Divine Guardian": ("Paladin", "Protection"),
        "Crusader Strike": ("Paladin", "Retribution"),
        "Repentance": ("Paladin", "Retribution"),
        "Sacred Shield": ("Paladin", "Retribution"),
        "Templar's Verdict": ("Paladin", "Retribution"),
        "Divine Storm": ("Paladin", "Retribution"),
        "The Art of War": ("Paladin", "Retribution"),
        "Penance": ("Priest", "Discipline"),
        "Power Word: Shield": ("Priest", "Discipline"),
        "Divine Aegis": ("Priest", "Discipline"),
        "Borrowed Time": ("Priest", "Discipline"),
        "Grace": ("Priest", "Discipline"),
        "Pain Suppression": ("Priest", "Discipline"),
        "Power Infusion": ("Priest", "Discipline"),
        "Shadow Word: Pain": ("Priest", "Shadow"),
        "Vampiric Touch": ("Priest", "Shadow"),
        "Vampiric Embrace": ("Priest", "Shadow"),
        "Shadowform": ("Priest", "Shadow"),
        "Dispersion": ("Priest", "Shadow"),
        "Circle of Healing": ("Priest", "Holy"),
        "Spirit of Redemption": ("Priest", "Holy"),
        "Chakra": ("Priest", "Holy"),
        "Guardian Spirit": ("Priest", "Holy"),
        "Holy Word: Chastise": ("Priest", "Holy"),
        "Shadowstep": ("Rogue", "Subtlety"),
        "Honor Among Thieves": ("Rogue", "Subtlety"),
        "Hemorrhage": ("Rogue", "Subtlety"),
        "Premeditation": ("Rogue", "Subtlety"),
        "Shadow Dance": ("Rogue", "Subtlety"),
        "Sinister Strike": ("Rogue", "Combat"),
        "Blade Flurry": ("Rogue", "Combat"),
        "Savage Combat": ("Rogue", "Combat"),
        "Killing Spree": ("Rogue", "Combat"),
        "Adrenaline Rush": ("Rogue", "Combat"),
        "Mutilate": ("Rogue", "Assassination"),
        "Envenom": ("Rogue", "Assassination"),
        "Cold Blood": ("Rogue", "Assassination"),
        "Vendetta": ("Rogue", "Assassination"),
        "Elemental Oath": ("Shaman", "Elemental"),
        "Thunderstorm": ("Shaman", "Elemental"),
        "Elemental Mastery": ("Shaman", "Elemental"),
        "Earth Shield": ("Shaman", "Restoration"),
        "Riptide": ("Shaman", "Restoration"),
        "Spirit Link Totem": ("Shaman", "Restoration"),
        "Mana Tide Totem": ("Shaman", "Restoration"),
        "Stormstrike": ("Shaman", "Enhancement"),
        "Lava Lash": ("Shaman", "Enhancement"),
        "Feral Spirit": ("Shaman", "Enhancement"),
        "Maelstrom Weapon": ("Shaman", "Enhancement"),
        "Nightfall": ("Warlock", "Affliction"),
        "Unstable Affliction": ("Warlock", "Affliction"),
        "Haunt": ("Warlock", "Affliction"),
        "Soul Swap": ("Warlock", "Affliction"),
        "Conflagrate": ("Warlock", "Destruction"),
        "Chaos Bolt": ("Warlock", "Destruction"),
        "Shadowfury": ("Warlock", "Destruction"),
        "Shadowburn": ("Warlock", "Destruction"),
        "Nether Protection": ("Warlock", "Destruction"),
        "Demonic Pact": ("Warlock", "Demonology"),
        "Hand of Gul'dan": ("Warlock", "Demonology"),
        "Metamorphosis": ("Warlock", "Demonology"),
        "Mortal Strike": ("Warrior", "Arms"),
        "Throwdown": ("Warrior", "Arms"),
        "Bladestorm": ("Warrior", "Arms"),
        "Sudden Death": ("Warrior", "Arms"),
        "Taste for Blood": ("Warrior", "Arms"),
        "Bloodthirst": ("Warrior", "Fury"),
        "Raging Blow": ("Warrior", "Fury"),
        "Rampage": ("Warrior", "Fury"),
        "Death Wish": ("Warrior", "Fury"),
        "Shield Slam": ("Warrior", "Protection"),
        "Revenge": ("Warrior", "Protection"),
        "Devastate": ("Warrior", "Protection"),
        "Vigilance": ("Warrior", "Protection"),
        "Shockwave": ("Warrior", "Protection"),
        "Last Stand": ("Warrior", "Protection")
        }

#Crowd Control Lookup list
crowd_control_spells = [
    'Polymorph',
    'Freezing Trap',
    'Hex',
    'Fear',
    'Howl of Terror'
    'Sap',
    'Gouge',
    'Blind',
    'Repentance',
    'Wyvern Sting',
    'Hibernate',
    'Entangling Roots',
    'Cyclone',
    'Mind Control',
    'Death Coil',
    'Piercing Howl',
    'Solar Beam',
    'Seduction',
    'Scatter Shot',
    'Shackle Undead',
    'Intimidating Shout',
    'Psychic Scream',
    'Banish',
    'Frost Nova',
    'Hungering Cold',
    'Ring of Frost',
    'Kidney Shot',
    'Cheap Shot',
    'Hammer of Justice',
    'Charge Stun',
    'Intercept Stun',
    'Concussion Blow',
    'Blackout',
    'Shadowfury',
    'Maim',
    'Pounce',
    'Skull Bash',
    'War Stomp',
    'Sonic Blast',
    'Paralysis',
    'Gnaw',
    'Bash',
    'Intimidation',
    'Inferno Effect',
    'Silenced - Improved Counterspell',
    'Silencing Shot',
    'Asphyxiate',
    'Strangulate',
    'Improved Cone of Cold',
    'Dragon\'s Breath',
    'Shockwave',
    'Deep Freeze',
    'Holy Word: Chastise',
    'Blinding Light',
    'Psychic Horror',
    'Axe Toss',
    'Mortal Coil',
    'Storm Bolt',
    'Fist of Justice',
    'Sleep',
    'Impale',
    'Bursting Shot',
    'Explosive Trap',
    'Snake Trap',
    'Bear Trap',
    'Smite Demon',
    'Sin and Punishment',
    'Subjugate Demon',
    'Demon Leap',
    'Net-o-Matic'
]

#Class definition for match object
class match:
    def __init__(self,file_name,log_path):
        self.file_name = file_name
        self.match_id = re.search(r'([a-f0-9]{64})\.txt$', file_name).group(1)  #Unique match_id for this arena game, found in file name, generated by the log parser that generated the arena log
        self.arena_map = re.search(r'_(\w+_\w+)_([a-f0-9]{64})\.txt$', file_name).group(1) #The zone of that the arena match was fought in [[Nagrand Arena", "Ruins of Lordaeron", "Dalaran Arena", "Blade's Edge Arena"]
        self.combat_log = log_path
        self.log_process_timestamp = None #The timestamp that the log for this game was processed at
        self.last_death_event = None #The last death logline, keeping in memory to determine winner once teams are determined
        self.match_start_time = None #Timestamp determined based on first event in the log + 45 seconds
        self.match_end_time = None #Timestamp determined based on the last death in the log
        self.match_length = 0 #Length of the arena game (in seconds)
        self.players = {} #The players in this game, can be 4 (2v2), 6(3v3), or 10(5v5) players. This is a dict where the key is the player id and value is a player object
        self.team_1 = set() #The set of players on team 1, typically the team of the uploader (probably), can be 2, 3, or 5 players, uses player objects
        self.team_2 = set()   #The set of players on team 2, typically the team of the uploader (probably), can be 2, 3, or 5 players 
        self.game_type = None #The format of the arena game (2v2, 3v3, or 5v5)
        self.winner = None #The team that won the match, this is determined based on the team of the last person to die in the log

    def to_dict(self):
        team_1_player_list = []
        team_2_player_list = []

        team_1_player_details = []
        team_2_player_details = []

        # Sort the team members by name
        sorted_team_1 = sorted(self.team_1, key=lambda player: player.name)
        sorted_team_2 = sorted(self.team_2, key=lambda player: player.name)

        for team_member in sorted_team_1:
            team_1_player_list.append(team_member.name + " | " + team_member.played_spec + " " + team_member.player_class)
            team_1_player_details.append(team_member.to_dict())

        for team_member in sorted_team_2:
            team_2_player_list.append(team_member.name + " | " + team_member.played_spec + " " + team_member.player_class)
            team_2_player_details.append(team_member.to_dict())

        return {
            'match_id': self.match_id,
            'match_log' : self.combat_log,
            'arena_map': self.arena_map,
            'game_type': self.game_type,
            'match_start_time': self.match_start_time.strftime('%m-%d %H:%M:%S') + ' in timezone of uploader',
            'match_end_time': self.match_end_time.strftime('%m-%d %H:%M:%S') + ' in timezone of uploader',
            'match_length' : self.match_length,
            'log_process_timestamp': self.log_process_timestamp,
            'team_1' :  team_1_player_list,
            'team_2' : team_2_player_list,
            'winner' : self.winner,
            'team_1_player_details' : team_1_player_details,
            'team_2_player_details' : team_2_player_details
        }
    
#Class definition for player object
class player:
    def __init__(self,player_id, player_name):
        self.name = player_name #Name of the player e.g 'Pushbackk-Benediction'
        self.player_id = player_id #ID of the player e.g 'Player-4728-0561A69A'
        self.flags = set() #Set of flags ascribed to this player this game, used to determine team
        self.player_class = None #Class of the player e.g 'Rogue'
        self.played_spec = None #Spec of the player e.g 'Subtlety'
        self.team = None #Team of the player for this game e.g 'team_1'
        self.winner = False #Flag for whether this player's team won this match
        self.damage = {} #Player's damage done this match, its a dictionary of the spell/ability as the key and the value being the integer damage. 
        self.damage_taken = {} #Player damage taken this match, its a dictionary of the spell/ability as the key and the value being the integer damage. 
        self.pet_damage = {} #Player's pet damage this match, its a dictionary of the spell/ability as the key and the value being the integer damage. 
        self.interrupts = {} #Player's number of succesful interrupts this match
        self.crowd_control = {} #Player's number of crowd_controls this match
        self.crowd_controlled_duration = 0 #Duration this player was CC'd by the other team this game (in seconds)
        self.purges = {} #Number of buffs this player has removed from the other team this game
        self.healing = {} #Player's healing this match        
        self.mitigation = {} #Player's mitigation this match
        self.dispels = {} #Number of debuffs this player has removed from their team this game
        self.time_to_dispel = {} #EXPERIMENTAL/NOT YET IMPLEMENTED
        self.interrupted = 0 #Number of times this player was interrupted this game
        self.interrupted_duration = 0 #Duration this player was interrupted for their main spell school this match (in seconds)
        self.fake_casts = 0 #Number of times the player has succesfully fake casted an opponent this game
        self.misses = {} #Number of times the player missed an attack, spell or had it resisted. This is experimental at this time
        self.pets = {} #A dictionary of pets, key is pet id, value is a player object

    def to_dict(self):
        return {
            'player_name' : self.name,
            'player_id' : self.player_id,
            'player_class' : self.player_class,
            'played_spec' : self.played_spec,
            'player_damage_done' : self.damage,
            'player_damage_taken' :self.damage_taken,
            'pet_damage' : self.pet_damage,
            'interrupts' : self.interrupts,
            'crowd_control' : self.crowd_control,
            'crowd_control_duration' : self.crowd_control_duration,
            'purges' : self.purges,
            'healing' : self.healing,
            'mitigation' : self.mitigation,
            'dispels' : self.dispels,
            'interrupted_duration' : self.interrupted_duration,
            'fake_casts' : self.fake_casts,
            'crowd_controlled_duration' : self.crowd_controlled_duration,
            'misses' : self.misses
        }
    
"""
Calculates time interrupted out of main school
    spell_interrupted (str): The spell that was interrupted, e.g: Polymorph
    target_spec (str): The spec of the interrupted play, e.g: Frost
    interrupt_used (str): The interrupt ability used, e.g: Kick
    Returns:
    Time in seconds the target player's main school was interrupted for, 0 if not the target's main school. 
    
"""
def get_interrupt_duration(spell_interrupted, target_spec, interrupt_used):   
    interrupt_durations = {
        'Kick': 5,
        'Pummel': 4,
        'Shield Bash': 6,
        'Counterspell': 7,
        'Wind Shear': 2,
        'Skull Bash': 4,
        'Rebuke': 4,
        'Silencing Shot': 3,
        'Spell Lock': 6,
        'Mind Freeze': 4
    }

    spec_school_mapping = {
        'Arcane': 'Arcane',
        'Fire': 'Fire',
        'Frost': 'Frost',
        'Holy': 'Holy',
        'Discipline': 'Holy',
        'Shadow': 'Shadow',
        'Affliction': 'Shadow',
        'Demonology': 'Shadow',
        'Destruction': 'Fire',
        'Elemental': 'Nature',
        'Enhancement': 'Nature',
        'Restoration': 'Nature',
        'Balance': 'Nature',
        'Feral': 'Nature',
        'Guardian': 'Nature',
        'Restoration': 'Nature',
        'Retribution': 'Holy',
        'Protection': 'Holy',
    }

    spell_school_dict = {
    'Arcane Blast': 'Arcane',
    'Arcane Missiles': 'Arcane',
    'Polymorph' : 'Arcane',
    'Evocation': 'Arcane',
    'Fireball': 'Fire',
    'Flamestrike': 'Fire',
    'Pyroblast': 'Fire',
    'Scorch': 'Fire',
    'Frostbolt': 'Frost',
    'Frostfire Bolt': 'Frost',
    'Blizzard': 'Frost',
    'Divine Hymn': 'Holy',
    'Greater Heal': 'Holy',
    'Heal': 'Holy',
    'Flash Heal' : 'Holy',
    'Holy Fire': 'Holy',
    'Hymn of Hope': 'Holy',
    'Prayer of Healing': 'Holy',
    'Penance': 'Holy',
    'Mind Blast': 'Shadow',
    'Mind Control': 'Shadow',
    'Mana Burn': 'Shadow',
    'Mind Flay': 'Shadow',
    'Vampiric Touch': 'Shadow',
    'Haunt': 'Shadow',
    'Seed of Corruption': 'Shadow',
    'Unstable Affliction': 'Shadow',
    'Hand of Gul\'dan': 'Shadow',
    'Incinerate': 'Fire',
    'Rain of Fire': 'Fire',
    'Searing Pain': 'Fire',
    'Soul Fire': 'Fire',
    'Chain Heal': 'Nature',
    'Chain Lightning': 'Nature',
    'Greater Healing Wave': 'Nature',
    'Healing Rain': 'Nature',
    'Healing Wave': 'Nature',
    'Lightning Bolt': 'Nature',
    'Healing Surge': 'Nature',
    'Hurricane': 'Nature',
    'Starsurge': 'Nature',
    'Wrath': 'Nature',
    'Cyclone': 'Nature',
    'Entangling Roots': 'Nature',
    'Healing Touch': 'Nature',
    'Nourish': 'Nature',
    'Regrowth': 'Nature',
    'Rejuvenation': 'Nature',
    'Tranquility': 'Nature',
    'Divine Light': 'Holy',
    'Exorcism': 'Holy',
    'Flash of Light': 'Holy',
    'Holy Light': 'Holy',
    'Holy Radiance': 'Holy',
    'Holy Wrath': 'Holy'
    }

    interrupt_duration = interrupt_durations.get(interrupt_used, 0)
    target_spec_school = spec_school_mapping.get(target_spec, None)
    interrupted_spell_school = spell_school_dict.get(spell_interrupted, 0)

    if (interrupted_spell_school == target_spec_school):
        return interrupt_duration
    else:
        return 0

"""
Processes a match object out of a game log 
    Parameters:
    game_log (str): The raw game combat log
    file_name (str): The filename of the game log (used for extracting game_id and map
    log_path (str): The Google cloud storage location of the game log
    Returns:
    A match object for the arena game
    
"""
def process_match_from_arena_log(game_log, file_name, log_path):
        
    '''
    Infers player class and specialization from the game log.
    Parameters:
    player object to find class and spec
    Returns:
    The same player object with class and spec
    '''
    def get_player_class_and_spec(player):
        for log_line in game_log:
            parts = log_line.split(',')
            if ('SPELL_AURA_APPLIED' in parts[0] or 'SPELL_CAST_SUCCESS' in parts[0]) and parts[1] == player.player_id:
                lookup = spell_dict.get(parts[10].strip('"'))
                if lookup:
                    inferred_class, inferred_spec = lookup
                    player.player_class = inferred_class
                    player.played_spec = inferred_spec
                    return player


    '''
    Gets the time in seconds for a crowd control
    Parameters:
    The log line of the crowd control
    The index in the game log
    Returns:
    CC duration in seconds
    '''
    def get_crowd_control_duration(current_line, current_index):
        duration = 0
        split_log = current_line.split(',')
        cc_timestamp = datetime.strptime(split_log[0].split(' ')[1], '%H:%M:%S.%f')

        cc_aura = split_log[10].strip('"')
        cc_target = split_log[5]

        for i in range(index, len(game_log) - current_index):
            split_log = game_log[i].split(',')
            e_type = split_log[0].split(' ')[3]
            targeted_player = split_log[5]

            if e_type == 'SPELL_AURA_REMOVED' and targeted_player == cc_target: 
                if split_log[10].strip('"') == cc_aura:
                    cc_end_timestamp = datetime.strptime(split_log[0].split(' ')[1], '%H:%M:%S.%f')
                    duration = int((cc_end_timestamp - cc_timestamp).total_seconds())
                    return duration

        return duration



    #First Instantiate a match object for the game
    game = match(file_name, log_path)

    #Log the arena log processing time:
    game.log_process_timestamp = datetime.now().timestamp()

    #Establish match start time by taking the timestamp of the first event and adding 30 seconds
    date, time = game_log[0].split(',')[0].split(' ')[:2]
    game.match_start_time = datetime.strptime(f"{datetime.now().year}-{date.replace('/', '-')}_{time.replace(':','.')}", "%Y-%m-%d_%H.%M.%S.%f") + timedelta(seconds=30)

    #Determine the final death event to occur in the log by going through the log backwards:
    for line in reversed(game_log):
        parts = line.split(',')
        if 'UNIT_DIED' in parts[0]:
            game.last_death_event = line #Save the last death event to determine winning team later
            #Mark match end time and update match object with end time and match length
            date, time = parts[0].split(' ')[:2]
            game.match_end_time = datetime.strptime(f"{datetime.now().year}-{date.replace('/', '-')}_{time.replace(':','.')}", "%Y-%m-%d_%H.%M.%S.%f") 
            game.match_length = int((game.match_end_time - game.match_start_time).total_seconds())
            break

    #Perform the sequential parsing of the arena game, use enumerate to keep track on index.
    for index,line in enumerate(game_log):
        parts = line.split(',')

        event_type = parts[0].split(' ')[3]

        if event_type == 'SPELL_SUMMON':
            active_player = game.players.setdefault(parts[1], player(parts[1], parts[2].strip('"')))
            summoned_pet = game.players.setdefault(parts[5], player(parts[5], parts[6].strip('"')))
            active_player.pets[parts[5]] = summoned_pet
            

        elif event_type != 'ZONE_CHANGE' and event_type != 'SPELL_CAST_SUCCESS':
            #Get our player objects, use setdefault to create 
            active_player = game.players.setdefault(parts[1], player(parts[1], parts[2].strip('"')))
            target_player = game.players.setdefault(parts[5], player(parts[5], parts[6].strip('"')))


            #Add tags to players
            active_player.flags.add(parts[3])
            target_player.flags.add(parts[7])


            #Parse Damage
            if event_type in ['SPELL_DAMAGE', 'SPELL_PERIODIC_DAMAGE', 'DAMAGE_SHIELD']:
                key = parts[10].strip('"')
                damage_amount = int(parts[29])
                active_player.damage[key] = active_player.damage.get(key, 0) + damage_amount
                target_player.damage_taken[key] = target_player.damage_taken.get(key, 0) + damage_amount

            elif event_type == 'SWING_DAMAGE':
                key = 'Melee Attack'
                damage_amount = int(parts[26])
                active_player.damage[key] = active_player.damage.get(key, 0) + damage_amount
                target_player.damage_taken[key] = target_player.damage_taken.get(key, 0) + damage_amount

            elif event_type == 'RANGE_DAMAGE':
                key = 'Ranged Attack'
                damage_amount = int(parts[26])
                active_player.damage[key] = active_player.damage.get(key, 0) + damage_amount
                target_player.damage_taken[key] = target_player.damage_taken.get(key, 0) + damage_amount


            #Parse Misses
            if event_type in ['SPELL_MISSED', 'SPELL_PERIODIC_MISSED']:
                if parts[12] in ['MISS', 'RESIST']:
                    key = parts[10].strip('"')
                    active_player.misses[key] = active_player.misses.get(key, 0) + 1         
                   
            elif event_type == ['SWING_MISSED']:
                if parts[9] in ['MISS', 'RESIST']:
                    key = 'Melee Attack'
                    active_player.misses[key] = active_player.misses.get(key, 0) + 1         

            elif event_type == ['RANGE_MISSED']:
                if parts[9] in ['MISS', 'RESIST']:
                        key = 'Ranged Attack'
                        active_player.misses[key] = active_player.misses.get(key, 0) + 1         


            #Parse Healing
            if event_type in ['SPELL_HEAL', 'SPELL_PERIODIC_HEAL']:
                key = parts[10].strip('"')
                healing_amount = int(parts[29])
                active_player.healing[key] = active_player.healing.get(key,0) + healing_amount


            #Parse Mitigation
            if event_type == 'SPELL_ABSORBED':
                if len(parts) == 21:
                    mitigating_player = game.players.setdefault(parts[12], player(parts[12], parts[13].strip('"')))
                    key = parts[17].strip('"')
                    mitigation_amount = int(parts[19])
                    mitigating_player.mitigation[key] = mitigating_player.mitigation.get(key,0) + mitigation_amount
                else:
                    mitigating_player = game.players.setdefault(parts[9], player(parts[9], parts[10].strip('"')))
                    key = parts[14].strip('"')
                    mitigation_amount = int(parts[16])
                    mitigating_player.mitigation[key] = mitigating_player.mitigation.get(key,0) + mitigation_amount   


            #Parse Dispels and purges
            if event_type in ['SPELL_DISPEL', 'SPELL_STOLEN']:
                key = parts[13].strip('"')
                #Determine whether offensive or defensive dispel, if debuff, count as defensive dispel and accumulate dispels, otherwise accumulate purges
                if parts[15] == 'DEBUFF\n':
                    active_player.dispels[key] = active_player.dispels.get(key, 0) + 1

                else:                
                    active_player.purges[key] = active_player.purges.get(key, 0) + 1

            
            #Parse Interrupts
            if event_type == 'SPELL_INTERRUPT':
                key = parts[13].strip('"')
                if target_player.played_spec == None:
                    get_player_class_and_spec(target_player)
                interrupt_duration = get_interrupt_duration(key, target_player.played_spec, parts[10].strip('"'))
                active_player.interrupts[key] = active_player.interrupts.get(key,0) + 1
                target_player.interrupted += 1
                target_player.interrupted_duration += interrupt_duration

            
            #Parse CCs and Classes
            if event_type == 'SPELL_AURA_APPLIED':
                aura = parts[10].strip('"')

                if aura in crowd_control_spells:
                    duration = get_crowd_control_duration(line,index)
                    active_player.crowd_control[aura] = active_player.crowd_control.get(aura,0) + duration
                    target_player.crowd_controlled_duration += duration

                




            
           
    print(game.players)

    print("Beep")



    




























    #Return the match object now that processing is complete
    return game
























if __name__ == '__main__':
    # Test locally with a local file
    input_file = './output/2024-6-5_21.51_blades_edge_arena_d625c13f3d23d203526de3ef2bb543d26dfd5b6e3e7b097a734d1aac850fbcb4.txt'
    output_dir = './output/processed_games/'
    os.makedirs(output_dir, exist_ok=True)
    
    with open(input_file, 'r') as file:
        content = file.readlines()
    
    processed_match = process_match_from_arena_log(content,'2024-6-5_21.51_blades_edge_arena_d625c13f3d23d203526de3ef2bb543d26dfd5b6e3e7b097a734d1aac850fbcb4.txt','gs://cata-colosseum-arena-matches/2024-6-5_21.51_blades_edge_arena_d625c13f3d23d203526de3ef2bb543d26dfd5b6e3e7b097a734d1aac850fbcb4.txt')
    print(processed_match.to_dict())

    #Lets test cloud
    #event = {}
    #event['bucket'] = 'cata-colosseum-arena-matches'
    #event['name'] = '2024-6-5_21.47_nagrand_arena_5dca061ef2fe30971c8152061aa01e86c4da51923ccc7f86854ee37b32e678a2.txt'
    #process_game_file(event,None)