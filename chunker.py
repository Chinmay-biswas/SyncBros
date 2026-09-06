from pathlib import Path

CHUNK_SIZE=1024*1024

def split_file(file_path,chunk_size=CHUNK_SIZE):
    with Path(file_path).open("rb") as source:
        ind=0
        while data := source.read(chunk_size):
            yield ind,data
            ind+=1