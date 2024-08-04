import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from googleapiclient.discovery import build
import pandas as pd
import sqlite3
import re
import time
import logging
import os

# YouTube API key
api_key = 'YT API KEY'
youtube = build('youtube', 'v3', developerKey=api_key, cache_discovery=False)

# Set up logging
logging.basicConfig(level=logging.INFO, filename='youtube_scraper.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Get the directory of the script
script_dir = os.path.dirname(os.path.abspath(__file__))

def sanitize_column_names(df):
    """Sanitize DataFrame column names to ensure they are valid SQL column names."""
    df.columns = [re.sub(r'\W|^(?=\d)', '_', col) for col in df.columns]
    return df

def execute_request_with_retries(request, retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = request.execute()
            logging.info(f"Request successful: {response}")
            return response
        except Exception as e:
            logging.error(f"Attempt {attempt+1} failed: {e}")
            print(f"Attempt {attempt+1} failed: {e}")
            time.sleep(delay)
    raise Exception("All retry attempts failed")

def get_channels(progress_callback):
    channels = []
    niches = ['pranks', 'entertainment']
    progress_increment = 100 / (len(niches) * 50)
    current_progress = 0

    # Load existing channel IDs from the database
    database_path = os.path.join(script_dir, 'youtube_channels.db')
    conn = sqlite3.connect(database_path)
    conn.execute('DROP TABLE IF EXISTS channels')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            niche TEXT,
            channel_id TEXT PRIMARY KEY,
            channel_name TEXT,
            channel_link TEXT,
            subscribers INTEGER
        )
    ''')
    conn.commit()
    existing_channel_ids = pd.read_sql_query("SELECT channel_id FROM channels", conn)['channel_id'].tolist()
    conn.close()

    for niche in niches:
        page_token = None
        while len(channels) < 100:
            request = youtube.search().list(
                part='snippet',
                maxResults=50,
                q=niche,
                type='video',
                videoDuration='long',
                pageToken=page_token
            )
            try:
                response = execute_request_with_retries(request)
            except Exception as e:
                logging.error(f"An error occurred: {e}")
                print(f"An error occurred: {e}")
                break

            if 'items' not in response:
                logging.warning("No items in response: %s", response)
                print("No items in response:", response)
                break

            for item in response['items']:
                channel_id = item['snippet']['channelId']
                if channel_id in existing_channel_ids:
                    continue

                channel_title = item['snippet']['channelTitle']
                channel_link = f"https://www.youtube.com/channel/{channel_id}"

                # Get channel details
                channel_request = youtube.channels().list(
                    part='statistics',
                    id=channel_id
                )
                try:
                    channel_response = execute_request_with_retries(channel_request)
                except Exception as e:
                    logging.error(f"An error occurred while getting channel details for {channel_id}: {e}")
                    print(f"An error occurred while getting channel details for {channel_id}: {e}")
                    continue

                if 'items' not in channel_response or len(channel_response['items']) == 0:
                    logging.warning(f"No items in channel response for channel ID {channel_id}: %s", channel_response)
                    print(f"No items in channel response for channel ID {channel_id}: ", channel_response)
                    continue

                for channel in channel_response['items']:
                    subscribers = int(channel['statistics']['subscriberCount'])
                    if subscribers >= 10000:
                        channels.append({
                            'niche': niche,
                            'channel_id': channel_id,
                            'channel_name': channel_title,
                            'channel_link': channel_link,
                            'subscribers': subscribers
                        })

                        # Check if 100 leads have been collected
                        if len(channels) >= 100:
                            break

                # Update progress
                current_progress += progress_increment
                progress_callback(current_progress)

            if len(channels) >= 100:
                break

            page_token = response.get('nextPageToken')
            if not page_token:
                break

    return pd.DataFrame(channels).drop_duplicates(subset=['channel_link'])

def store_data(channels_df):
    if channels_df.empty:
        logging.info("No data to store.")
        print("No data to store.")
        return None

    # Select and reorder columns
    channels_df = channels_df[['niche', 'channel_name', 'subscribers', 'channel_link']]

    # Sanitize column names
    channels_df = sanitize_column_names(channels_df)
    
    # Print DataFrame schema for debugging
    print("DataFrame Schema:")
    print(channels_df.dtypes)
    
    # Define paths for the database and Excel file
    database_path = os.path.join(script_dir, 'youtube_channels.db')
    excel_path = os.path.join(script_dir, 'youtube_channels.xlsx')

    # Create table with correct schema if it doesn't exist
    conn = sqlite3.connect(database_path)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            niche TEXT,
            channel_id TEXT PRIMARY KEY,
            channel_name TEXT,
            channel_link TEXT,
            subscribers INTEGER
        )
    ''')
    conn.commit()
    
    # Save to SQLite
    channels_df.to_sql('channels', conn, if_exists='append', index=False)
    conn.close()

    # Load existing data from Excel if it exists
    if os.path.exists(excel_path):
        existing_df = pd.read_excel(excel_path)
        combined_df = pd.concat([existing_df, channels_df]).drop_duplicates(subset=['channel_id']).reset_index(drop=True)
    else:
        combined_df = channels_df

    # Sort combined data by subscribers count
    combined_df = combined_df.sort_values(by='subscribers', ascending=False)

    # Save to Excel
    combined_df.to_excel(excel_path, index=False)
    logging.info(f"Data stored in {excel_path}")
    print(f"Data stored in {excel_path}")

    return combined_df


class YouTubeScraperApp:
    def __init__(self, root):
        self.root = root
        self.root.title("YouTube Lead Scraper")
        self.root.geometry("500x400")

        self.progress = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(root, variable=self.progress, maximum=100)
        self.progress_bar.pack(pady=20, padx=20, fill=tk.X)

        self.scrape_button = tk.Button(root, text="Scrape Leads", command=self.scrape_leads)
        self.scrape_button.pack(pady=10)

        self.export_excel_button = tk.Button(root, text="Export to Excel", command=self.export_to_excel, state=tk.DISABLED)
        self.export_excel_button.pack(pady=5)

        self.export_csv_button = tk.Button(root, text="Export to CSV", command=self.export_to_csv, state=tk.DISABLED)
        self.export_csv_button.pack(pady=5)

        self.channels_df = pd.DataFrame()

    def update_progress(self, value):
        self.progress.set(value)
        self.root.update_idletasks()

    def scrape_leads(self):
        self.progress.set(0)
        self.scrape_button.config(state=tk.DISABLED)
        self.channels_df = get_channels(self.update_progress)
        self.channels_df = store_data(self.channels_df)
        if self.channels_df is not None and not self.channels_df.empty:
            self.export_excel_button.config(state=tk.NORMAL)
            self.export_csv_button.config(state=tk.NORMAL)
            messagebox.showinfo("Scrape Done", "Lead scraping is complete!")
        self.scrape_button.config(state=tk.NORMAL)

    def export_to_excel(self):
        if not self.channels_df.empty:
            file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
            if file_path:
                self.channels_df.to_excel(file_path, index=False)
                messagebox.showinfo("Export Complete", f"Data exported to {file_path}")

    def export_to_csv(self):
        if not self.channels_df.empty:
            file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
            if file_path:
                self.channels_df.to_csv(file_path, index=False)
                messagebox.showinfo("Export Complete", f"Data exported to {file_path}")

if __name__ == '__main__':
    logging.info("Starting YouTube scraper script.")
    root = tk.Tk()
    app = YouTubeScraperApp(root)
    root.mainloop()
    logging.info("YouTube scraper script finished.")
    print("YouTube scraper script finished.")
