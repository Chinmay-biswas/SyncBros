import hashlib
import json
from pathlib import Path
from chunker import CHUNK_SIZE,split_file
from chunk_store import store_chunk

def hash_file(file_path):
    dig=hashlib.sha256()
    with Path(file_path).open("rb") as source:
        while data := source.read(CHUNK_SIZE):
            dig.update(data)
    return dig.hexdigest()

def create_manifest(file_path,store_dir):
    path=Path(file_path)
    chunks=[{"index":i,"hash":store_chunk(data,store_dir),"size":len(data)} for i,data in split_file(path)]
    return {"filename":path.name,"size":path.stat().st_size,"file_hash":hash_file(path),"chunk_size":CHUNK_SIZE,"chunks":chunks}

def save_manifest(manifest,output_path):
    Path(output_path).write_text(json.dumps(manifest,indent=2),encoding="utf-8")

def load_manifest(manifest_path):
    return json.loads(Path(manifest_path).read_text(encoding="utf-8"))

