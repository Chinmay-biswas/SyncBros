import argparse
from pathlib import Path
from chunk_store import get_chunk,reconstruct_file,has_chunk,store_chunk
from manifest import create_manifest,hash_file,save_manifest

def find_missing_chunks(manifest,target):
    return [chunk["hash"]for chunk in manifest["chunks"]if not has_chunk(chunk["hash"],target)]

def transfer_chunk(chunk_hash,source,target):
    data=get_chunk(chunk_hash,source)
    if store_chunk(data,target)!=chunk_hash:
        raise ValueError("Chunk Verification galat")

def reconstruct_from_manifest(manifest,store,output):
    reconstruct_file([c["hash"] for c in  manifest["chunks"]],store,output)
    if hash_file(output)!=manifest["file_hash"]:
        Path(output).unlink(missing_ok=True)
        raise ValueError("Reconstructed file failed verification")

def local_sync(source_file,source_store,target_store,target_directory):
    manifest=create_manifest(source_file,source_store)
    missing=find_missing_chunks(manifest,target_store)
    for chunk_hash in missing:
        transfer_chunk(chunk_hash,source_store,target_store)
    output=Path(target_directory)/manifest["filename"]
    reconstruct_from_manifest(manifest,target_store,output)
    return manifest,len(missing),output

if __name__=="__main__":
    par=argparse.ArgumentParser(description="Run verified incremental local sync.")
    par.add_argument("source_file")
    par.add_argument("--source-store",default="node_a/chunk_store")
    par.add_argument("--target-store",default="node_b/chunk_store")
    par.add_argument("--target-directory",default="node_b/files")
    args=par.parse_args()
    manifest,count,output=local_sync(args.source_file,args.source_store,args.target_store,args.target_directory)
    save_manifest(manifest,output.with_suffix(output.suffix+".manifest.json"))
    print(f"Synchronized {output.name}:transferred {count}/{len(manifest['chunks'])}chunks;verified SHA-256")
