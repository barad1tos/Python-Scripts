#!/usr/bin/env python3
import csv
import subprocess
import os
import tempfile
from tqdm import tqdm

input_file = '/Users/romanborodavkin/Downloads/lastfmstats-barad1toz.csv'
applescript_code = '''
on run argv
    set artistName to item 1 of argv
    set albumName to item 2 of argv
    set trackName to item 3 of argv
    set playCount to item 4 of argv as integer

    tell application "Music"
        set theTrack to (every track of library playlist 1 whose artist is artistName and album is albumName and name is trackName)
        if (count of theTrack) is 1 then
            set played count of item 1 of theTrack to playCount
        end if
    end tell
end run
'''

play_counts = {}
with open(input_file, 'r', encoding='utf-8') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=';')
    next(csvreader)  # Skip header row

    for row in csvreader:
        artist, album, track, _ = row
        play_counts[(artist, album, track)] = play_counts.get((artist, album, track), 0) + 1

with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as temp_file:
    temp_file.write(applescript_code)
    temp_file_path = temp_file.name

for (artist, album, track), count in tqdm(play_counts.items(), total=len(play_counts)):
    subprocess.run(['osascript', temp_file_path, artist, album, track, str(count)], encoding='utf-8')

os.remove(temp_file_path)
