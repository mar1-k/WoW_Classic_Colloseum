import re
import streamlit as st
import pandas as pd
from PIL import Image
from io import BytesIO
import base64
from google.cloud import storage, bigquery


def validate_file_name(file_name):
    """Validates the file name to match the expected naming convention."""
    pattern = r'WoWCombatLog-\d{6}_\d{6}\.txt'
    return bool(re.match(pattern, file_name))


def upload_to_gcs(file, bucket_name):
    """Uploads the file to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file.name)
    blob.upload_from_file(file)


def fetch_matches():
    # Initialize the BigQuery client
    bq_client = bigquery.Client()

    # Define the SQL query
    query = """
    SELECT 
        arena_map, 
        game_type, 
        match_start_time, 
        match_end_time, 
        match_length, 
        log_process_timestamp, 
        team_1, 
        team_2, 
        winner 
    FROM 
        `cata-colosseum.cata_colosseum_beta.cata-colosseum_arena_matches` 
    """

    # Run the query
    query_job = bq_client.query(query)
    results = query_job.result()  # Waits for job to complete

    # Convert the query result to a list of dictionaries
    matches = []
    for row in results:
        matches.append({
            "Arena Map": row.arena_map,
            "Game Type": row.game_type,
            "Match Start Time": row.match_start_time,
            "Match End Time": row.match_end_time,
            "Match Length": f"{round(row.match_length / 60, 2):.2f} minutes",
            "Log Process Timestamp": row.log_process_timestamp,
            "Team 1": row.team_1,
            "Team 2": row.team_2,
            "Winner": row.winner
        })
    return matches

def show_home_page():
    st.title("Home Page")
    st.write("Welcome to the Home Page") 

def show_upload_page():
    st.title('Upload Your WoW Combat Logs')
    uploaded_files = st.file_uploader("Upload your WoW combat log files, The files are found in your WoW classic install directory under classic and are named <WoWCombatLog-MMDDYY_######.txt> Only arena data will be processed by Cata Colloseum.", type="txt", accept_multiple_files=True)
    st.write("This website will only accept untampered combat logs.")
    if uploaded_files:
        bucket_name = 'cata-colosseum-arena-logs'
        for uploaded_file in uploaded_files:
            if validate_file_name(uploaded_file.name):
                st.success(f"File {uploaded_file.name} is valid. Uploading to Cata Colosseum...")
                upload_to_gcs(uploaded_file, bucket_name)
                st.write(f"Upload successful! Parsed matches will appear at the matches page momentarily!")
            else:
                st.error(f"Invalid file name for {uploaded_file.name}. The file name must match the pattern: WoWCombatLog-060224_040929.txt")

def combine_icons(player_icons, output_size=(100, 50)):
    """Combine multiple icons into a single image."""
    images = [Image.open(icon) for icon in player_icons]
    widths, heights = zip(*(i.size for i in images))

    # Calculate total width and max height for the combined image
    total_width = sum(widths)
    max_height = max(heights)

    # Create a new image with the appropriate size
    combined_image = Image.new('RGBA', (total_width, max_height))

    # Paste each image into the combined image
    x_offset = 0
    for img in images:
        combined_image.paste(img, (x_offset, 0))
        x_offset += img.width

    return combined_image

def get_base64_image(image):
    """Convert an image to a base64 string."""
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode("utf-8")

def show_matches_page():
    spec_icons = {
        "Unknown Unknown": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/unknown_unknown.jpg",
        "Blood Death Knight": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/blood_death_knight.jpg",
        "Frost Death Knight": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/frost_death_knight.jpg",
        "Unholy Death Knight": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/unholy_death_knight.jpg",
        "Balance Druid": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/balance_druid.jpg",
        "Feral Druid": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/feral_druid.jpg",
        "Restoration Druid": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/restoration_druid.jpg",
        "Beast Mastery Hunter": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/beast_mastery_hunter.jpg",
        "Marksmanship Hunter": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/marksmanship_hunter.jpg",
        "Survival Hunter": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/survival_hunter.jpg",
        "Arcane Mage": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/arcane_mage.jpg",
        "Fire Mage": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/fire_mage.jpg",
        "Frost Mage": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/frost_mage.jpg",
        "Holy Paladin": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/holy_paladin.jpg",
        "Protection Paladin": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/protection_paladin.jpg",
        "Retribution Paladin": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/retribution_paladin.jpg",
        "Discipline Priest": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/discipline_priest.jpg",
        "Holy Priest": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/holy_priest.jpg",
        "Shadow Priest": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/shadow_priest.jpg",
        "Assassination Rogue": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/assassination_rogue.jpg",
        "Outlaw Rogue": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/outlaw_rogue.jpg",
        "Subtlety Rogue": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/subtlety_rogue.jpg",
        "Elemental Shaman": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/elemental_shaman.jpg",
        "Enhancement Shaman": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/enhancement_shaman.jpg",
        "Restoration Shaman": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/restoration_shaman.jpg",
        "Affliction Warlock": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/affliction_warlock.jpg",
        "Demonology Warlock": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/demonology_warlock.jpg",
        "Destruction Warlock": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/destruction_warlock.jpg",
        "Arms Warrior": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/arms_warrior.jpg",
        "Fury Warrior": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/fury_warrior.jpg",
        "Protection Warrior": "/Users/mar1/projects/cata_classic_arena_logs/spec_icons/protection_warrior.jpg"
    }

    st.title("Matches Overview")

    # Fetch matches from BigQuery
    matches = fetch_matches()

    if matches:
        st.write("Showing the latest matches:")
        df = pd.DataFrame(matches)

        # Combine icons for Team 1 and Team 2
        df['Team 1 Icons'] = df['Team 1'].apply(lambda x: combine_icons([spec_icons[info.split(" | ")[1]] for info in x]))
        df['Team 2 Icons'] = df['Team 2'].apply(lambda x: combine_icons([spec_icons[info.split(" | ")[1]] for info in x]))

        # Convert images to base64
        df['Team 1 Icons'] = df['Team 1 Icons'].apply(lambda img: "data:image/png;base64," + get_base64_image(img))
        df['Team 2 Icons'] = df['Team 2 Icons'].apply(lambda img: "data:image/png;base64," + get_base64_image(img))

        #df['Team 1'] = df['Team 1'].apply(color_player_names)
        #df['Team 2'] = df['Team 2'].apply(color_player_names)
        
        # Arrange columns
        df = df[['Arena Map', 'Game Type', 'Log Process Timestamp', 'Match Length', 'Team 1 Icons', 'Team 2 Icons', 'Winner', 'Team 1', 'Team 2', 'Match Start Time', 'Match End Time']]

        st.dataframe(df, column_config={
            "Team 1 Icons": st.column_config.ImageColumn(label="Team 1 Composition", help="Icons representing Team 1 Composition"),
            "Team 2 Icons": st.column_config.ImageColumn(label="Team 2 Composition", help="Icons representing Team 2 Composition")
        }, hide_index=True)

    else:
        st.write("No matches found.")


def color_for_class(player_class):
    colors = {
        "Death Knight": "#A93226", # Dark Red
        "Druid": "#F39C12",        # Orange
        "Hunter": "#D4EFDF",       # Light Green
        "Mage": "#AED6F1",         # Light Blue
        "Paladin": "#F4B0EB",      # Pink
        "Priest": "#FDFEFE",       # White
        "Rogue": "#F9E79F",        # Yellow
        "Shaman": "#0F52F5",       # Dark Blue
        "Warlock": "#5B2C6F",      # Purple
        "Warrior": "#935116"       # Brown
    }
    return colors.get(player_class, "black")  # default color if class not found



def color_player_names(players):
    colored_players = []
    for player in players:
        name, spec_class = player.split(" | ")
        player_class = spec_class.split()[-1]  # Extract class name from specialization
        color = color_for_class(player_class)  # Default to gray if class notfound
        colored_players.append(f'<span style="color: {color};">{name}</span>')
    return " | ".join(colored_players)


def show_test_matches_page():
    st.title("Matches Overview")


def main():
    #
    # hide_streamlit_style = """
    # <style>
    # #MainMenu {visibility: hidden;}
    # footer {visibility: hidden;}
    # .stDeployButton {visibility: hidden;}
    # </style>
    # """
    #st.markdown(hide_streamlit_style, unsafe_allow_html=False) 
    st.set_page_config(layout="wide")

    page = st.query_params.get("page", "home")
    st.sidebar.image("streamlit_app/images/cata_colosseum_logo.jpeg", use_column_width=True)
    st.sidebar.page_link("https://www.cata-colosseum.com/?page=home", label="Home Page", icon="🏠", disabled=True)
    st.sidebar.page_link("https://www.cata-colosseum.com/?page=upload", label="Upload Logs",  icon="📜", disabled=True)
    st.sidebar.page_link("https://www.cata-colosseum.com/?page=matches", label="Matches", icon="⚔️", disabled=True)
    st.sidebar.page_link("https://www.cata-colosseum.com/?page=players", label="Players",  icon="🧙", disabled=True)
    
    st.header("Welcome to Cata Colosseum!")
    st.subheader("Analyze your World of Warcraft Cataclysm Classic arena matches from combat logs!")

    st.divider()

    if page == "home":
        show_home_page()
    elif page == "upload":
        show_upload_page()
    elif page == "matches":
        show_matches_page()
    elif page == "test":
        show_test_matches_page()
    else:
        st.warning("URL not found. You have been directed to the home page.")
        show_home_page()


if __name__ == "__main__":
    main()
