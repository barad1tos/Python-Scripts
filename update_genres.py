import os
import sys
import argparse
from collections import Counter
from mutagen import File


def find_files(extensions, directory):
    result = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if any(file.lower().endswith(ext) for ext in extensions):
                result.append(os.path.join(root, file))
    return result


def find_dominant_genre(files):
    genre_counts = Counter()
    first_genre = None
    for file in files:
        try:
            audio = File(file)

            if audio is not None:
                genre_key = "©gen" if file.lower().endswith(".m4p") else "TCON"
                if genre_key in audio:
                    genre = audio[genre_key][0]
                    genre_counts[genre] += 1
                    # Debug message
                    print(f"Processed genre for {file}: {audio[genre_key][0]}")

                    if first_genre is None:
                        first_genre = genre
            else:
                continue

        except Exception as e:
            log_error(f"Error processing {file}: {e}")

    print(f"Genre counts: {genre_counts}")  # Debug message

    if not genre_counts:
        return None

    total_tracks = sum(genre_counts.values())
    dominant_genre, dominant_genre_count = genre_counts.most_common(1)[0]

    if dominant_genre_count / total_tracks >= 0.6:
        return dominant_genre
    elif first_genre is not None:
        return first_genre
    else:
        return None


def update_genre(files, dominant_genre):
    updated_files = []
    for file in files:
        try:
            audio = File(file)

            if audio is not None:
                genre_key = "©gen" if file.lower().endswith(".m4p") else "TCON"
                if genre_key not in audio or audio[genre_key][0] != dominant_genre:
                    original_genre = audio[genre_key][0]
                    audio[genre_key] = [dominant_genre]
                    print(f"Updating genre for {file}")  # Debug message
                    audio.save()
                    print(f"Updated genre for {file} to {dominant_genre}")
                    updated_files.append(
                        (file, original_genre, dominant_genre))

        except Exception as e:
            log_error(f"Error updating {file}: {e}")

    return updated_files


def log_error(message):
    with open(os.path.expanduser("~/Desktop/error_log.txt"), "a") as log_file:
        log_file.write(f"{message}\n")


def main(args):
    path = args.path

    if os.path.isfile(path):
        start_dir = os.path.dirname(path)
    elif os.path.isdir(path):
        start_dir = path
    else:
        print("Error: Invalid path argument")
        sys.exit(1)

    m4p_files = find_files([".m4p"], start_dir)
    mp3_files = find_files([".mp3"], start_dir)

    all_files = m4p_files + mp3_files
    print(f"Processing {len(all_files)} files...")
    dominant_genre = find_dominant_genre(all_files)

    if dominant_genre is not None:
        print(f"Dominant genre found: {dominant_genre}")
        updated_files = update_genre(all_files, dominant_genre)

        if args.changelog:
            for file_path, original_genre, new_genre in updated_files:
                print(
                    f"Changed genre for {file_path}: {original_genre} -> {new_genre}")
    else:
        print("No dominant genre found")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update genres for audio files.")
    parser.add_argument(
        "path", type=str, help="Path to the folder containing audio files.")
    parser.add_argument("--changelog", action="store_true",
                        help="Output a changelog of the updated files and their genre changes.")

    args = parser.parse_args()
    main(args)
