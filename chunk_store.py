from pathlib import Path
from tempfile import NamedTemporaryFile
from hasher import hash_chunk

def _path(store_dir,chunk_hash):
    if len(chunk_hash)!=64 or any(c not in "0123456789abcdef" for c in chunk_hash):
        raise ValueError("SHA-256 chunk hash galat bhai")
    return Path(store_dir)/chunk_hash

def has_chunk(chunk_hash,store_dir):
    return _path(store_dir,chunk_hash).is_file()

def store_chunk(data,store_dir):
    chunk_hash,store=hash_chunk(data),Path(store_dir)
    store.mkdir(parents=True,exist_ok=True)
    destination=_path(store,chunk_hash)
    if not destination.exists():
        with NamedTemporaryFile(dir=store,delete=False) as temp:
            temp.write(data)
            temporary=Path(temp.name)
        temporary.replace(destination)
    return chunk_hash

def get_chunk(chunk_hash,store_dir):
    data=_path(store_dir,chunk_hash).read_bytes()
    if hash_chunk(data)!=chunk_hash:
        raise ValueError(f"Stored chunk verify na ho paa rha:{chunk_hash}")
    return data

def reconstruct_file(chunk_hashes,store_dir,output_path):
    output=Path(output_path);
    output.parent.mkdir(parents=True,exist_ok=True)
    temp=output.with_suffix(output.suffix+".part")
    with temp.open("wb") as destination:
        for chunk_hash in chunk_hashes:
            destination.write(get_chunk(chunk_hash,store_dir))
    temp.replace(output)


    