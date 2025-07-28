from glob import glob
import laion_clap
from pathlib import Path
from math import ceil
from tqdm import tqdm
import signal
import sys
import os

# General configurations
batch_size = 10
write_header = True

# Interruption handling
interrupted = False


def signal_handler(sig, frame):
    global interrupted
    print(
        "\n\n🛑 Interruption signal received. Finishing current batch and saving progress..."
    )
    interrupted = True


# Set up signal handler for graceful interruption
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Files configuration
mtg_jamendo = {
    "name": "mtg_jamendo",
    "type": "human",
    "folder": "/media/giovanni/Giow Files/humaness_percept/datasets/mtg-jamendo-dataset/mtg-jamendo-raw_30s/**",
    "format": "mp3",
}

# change this line to config the dataset
dataset = mtg_jamendo

current_directory = (
    "/media/giovanni/Giow Files/humaness_percept/dataset_processor/generate_embeddings"
)
dataset_name = dataset["name"]
dataset_folder = dataset["folder"]
dataset_format = dataset["format"]
dataset_type = dataset["type"]

output_file = f"{current_directory}/data/output/{dataset_type}/embeds_{dataset_name}_clapaudio.csv"

songs = []
songs_folder = f"{dataset_folder}/*.{dataset_format}"
print(f"{songs_folder=}\n")

for filename in glob(songs_folder, recursive=True):
    temp = filename.split("/")
    songs.append(
        {
            "id": "track_" + temp[-1][:-4].zfill(7),  # get name of file
            # "origin": temp[-2], # get internet original font
            "path": filename,
        }
    )

num_audio_files = len(songs)
print(f"Existem {num_audio_files} músicas .{dataset_format}")
input("Continuar? Pressione qualquer tecla...")


model = laion_clap.CLAP_Module(enable_fusion=False, amodel="HTSAT-base")
model.load_ckpt(
    "./music_audioset_epoch_15_esc_90.14.pt", verbose=False
)  # download the default pretrained checkpoint.

output_file = Path(output_file)
output_file.parent.mkdir(exist_ok=True, parents=True)

# Check if output file exists and has content to resume from
processed_files = set()
start_header = write_header

if output_file.exists() and output_file.stat().st_size > 0:
    print(f"📂 Found existing output file: {output_file}")
    with open(output_file, "r") as file:
        lines = file.readlines()
        if lines:
            # Skip header if it exists
            start_line = 1 if lines[0].startswith("file,") else 0
            for line in lines[start_line:]:
                if line.strip():
                    file_id = line.split(",")[0]
                    processed_files.add(file_id)

    print(f"📊 Found {len(processed_files)} already processed files")
    start_header = False  # Don't write header if file exists

    # Filter out already processed songs
    songs = [song for song in songs if song["id"] not in processed_files]
    print(f"🔄 Remaining files to process: {len(songs)}")
else:
    print(f"📝 Creating new output file: {output_file}")

if start_header:
    with open(output_file, "a+") as file:
        file.write("file,")
        file.write(",".join(["e" + str(i) for i in range(512)]))
        file.write("\n")


### GERAR EMBEDS
remaining_files = len(songs)
num_iterations = ceil(remaining_files / batch_size)
print(f"Total number of iterations = {num_iterations}")
print(f"Total number of files to process = {remaining_files}")
print(f"💡 Press Ctrl+C to safely interrupt the process")
files_corrupted = []

# songs becomes the batches (for memory sake)
songs = [songs[i : i + batch_size] for i in range(0, len(songs), batch_size)]

try:
    for i, batch in tqdm(enumerate(songs), total=len(songs)):
        # Check for interruption
        if interrupted:
            print("🔄 Process interrupted. Progress has been saved.")
            break

        # print(f"========== Iteration #{i+1} ===========")
        # print([song['id'] for song in batch]) # print songs in this batch to capture corrupted files
        audio_files = [song["path"] for song in batch]

        try:
            audio_embed = model.get_audio_embedding_from_filelist(
                x=audio_files, use_tensor=False
            )
        except Exception as e:
            if len(batch) == 1:
                corrupted_file = batch[0]["id"]
                files_corrupted.append(corrupted_file)
                print(f"######### WARNING (corrupted file): {corrupted_file}")
            else:
                for song in batch:
                    songs.append([song])
                print(f"######### WARNING (corrupted list): some file is corrupted!")
            continue

        for i, song in enumerate(batch):
            # song["embed"] = audio_embed[i] this fills all the memory (saving embeds in memory is not a good idea)

            with open(output_file, "a+") as file:
                file.write(song["id"] + ",")
                for i, val in enumerate(audio_embed[i]):
                    if i < 511:
                        file.write(str(val) + ",")
                    else:
                        file.write(str(val))

                file.write("\n")

        # Save progress periodically (every 10 batches)
        if (i + 1) % 10 == 0:
            print(f"💾 Progress saved - Batch {i + 1}/{len(songs)} completed")

except KeyboardInterrupt:
    print("\n🔄 Keyboard interrupt received. Progress has been saved.")
except Exception as e:
    print(f"\n❌ Unexpected error: {e}")
    print("💾 Progress has been saved up to the last completed batch.")

print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
print("Corrupted list of files:")
print(files_corrupted)
print(f"✅ Process completed. Output saved to: {output_file}")
