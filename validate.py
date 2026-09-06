import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from chunker import CHUNK_SIZE,split_file
from chunk_store import has_chunk
from manifest import create_manifest,hash_file
from sender import send_file
from sync import local_sync
ROOT=Path(__file__).resolve().parent
def check(label,condition,detail):
    outcome="PASS" if condition else "FAIL"
    print(f"[{outcome}] {label}: {detail}")
    return condition

def unused_port():
    with socket.socket()as probe:
        probe.bind(("127.0.0.1",0))
        return probe.getsockname()[1]

def make_sample(path):
    path.write_bytes(b"A"*CHUNK_SIZE+b"B"*CHUNK_SIZE+b"C"*CHUNK_SIZE+b"D"*(128*1024))

def main():
    checks=[]
    with tempfile.TemporaryDirectory(prefix="file-sync-validation-")as temp:
        temp=Path(temp)
        src=temp/"source.bin"
        src_store,dst_store=temp/"source-store",temp/"target-store"
        dst_dir=temp/"target-files"
        make_sample(src)
        started=time.perf_counter()
        chunks=list(split_file(src))
        checks.append(check("chunking",len(chunks)==4,
            f"{src.stat().st_size/1024/1024:.3f} MiB split into {len(chunks)} chunks (expected 4)"))
        meta=create_manifest(src,src_store)
        checks.append(check("manifest + SHA-256",len(meta["chunks"])==4 and len(meta["file_hash"])==64,
            f"manifest has {len(meta['chunks'])} chunk entries and a 64-character full-file hash"))
        checks.append(check("content-addressed storage",all(has_chunk(c["hash"],src_store)for c in meta["chunks"]),
            f"all {len(meta['chunks'])} chunks are present in the source store"))
        first_meta,first_n,first_out=local_sync(src,src_store,dst_store,dst_dir)
        checks.append(check("initial local sync",first_n==4 and hash_file(src)==hash_file(first_out),
            f"transferred {first_n}/4 chunks; reconstructed SHA-256 matches source"))
        with src.open("r+b")as file:
            file.write(b"Z"*100)
        _,changed_n,changed_out=local_sync(src,src_store,dst_store,dst_dir)
        checks.append(check("incremental sync",changed_n==1 and hash_file(src)==hash_file(changed_out),
            f"after a 100-byte change, transferred {changed_n}/4 chunks; reconstructed SHA-256 matches"))
        port=unused_port()
        server=subprocess.Popen([sys.executable,"receiver.py","--host","127.0.0.1","--port",str(port),
            "--store",str(temp/"network-store"),"--output",str(temp/"network-files")],cwd=ROOT,
            stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        try:
            time.sleep(0.4)
            sent_n,sent_meta=send_file("127.0.0.1",port,src,src_store)
            received=temp/"network-files"/src.name
            checks.append(check("localhost network sync",sent_n==4 and received.is_file()and hash_file(src)==hash_file(received),
                f"sent {sent_n}/4 chunks over TCP; received SHA-256 matches source"))
        except Exception as err:
            checks.append(check("localhost network sync",False,str(err)))
        finally:
            server.terminate()
            try:
                server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server.kill()
        elapsed=time.perf_counter()-started
        passed=sum(checks)
        print(f"\nResult: {passed}/{len(checks)} checks passed in {elapsed:.2f} seconds.")
        print("Not covered: physical-LAN two-way polling, deletion sync, conflicts, retries, security, and multi-node scaling.")
        return 0 if passed==len(checks)else 1

if __name__=="__main__":
    raise SystemExit(main())
