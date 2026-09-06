import argparse
import threading
import time
from pathlib import Path
from manifest import hash_file
from receiver import serve
from sender import TransferRejected,send_file
def get_files(folder):
    files={}
    folder=Path(folder)
    for path in folder.rglob("*"):
        if path.is_file()and not path.name.endswith(".part"):
            files[path.relative_to(folder).as_posix()]=hash_file(path)
    return files

def watch_folder(folder,peers,peer_port,store_dir,interval):
    folder=Path(folder)
    folder.mkdir(parents=True,exist_ok=True)
    known=get_files(folder)
    pending={}
    print(f"Watching {folder.resolve()}")
    while True:
        time.sleep(interval)
        current_files=get_files(folder)
        for name,digest in current_files.items():
            if known.get(name)!=digest:
                pending[name]=set(peers)
                action="CREATE" if name not in known else "UPDATE"
                print(f"[LOCAL {action}] {(folder/name).resolve()}")
        for name in list(pending):
            if name not in current_files:
                del pending[name]
        for name,waiting in list(pending.items()):
            if "/" in name:
                print(f"Skipping nested file: {name}")
                del pending[name]
                continue
            for peer in list(waiting):
                try:
                    count,meta=send_file(peer,peer_port,folder/name,store_dir)
                    waiting.remove(peer)
                    print(f"Synced {name} to {peer}: {count}/{len(meta['chunks'])} chunks")
                except TransferRejected as err:
                    waiting.remove(peer)
                    print(f"[ADMIN DENIED] {name} by {peer}: {err}")
                except Exception as err:
                    print(f"Retrying {name} to {peer}: {err}")
            if not waiting:
                del pending[name]
        known=current_files

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("peers",nargs="*")
    parser.add_argument("--peer",action="append",default=[])
    parser.add_argument("--role",choices=("admin","member"),default="member")
    parser.add_argument("--port",type=int,default=5000)
    parser.add_argument("--peer-port",type=int)
    parser.add_argument("--sync-dir",default="sync_folder")
    parser.add_argument("--store",default="node_store/chunk_store")
    parser.add_argument("--interval",type=float,default=2)
    args=parser.parse_args()
    peers=list(dict.fromkeys(args.peers+args.peer))
    if not peers:
        parser.error("provide a peer")
    if args.role=="admin" and len(peers)<2:
        parser.error("admin needs at least two peers")
    peer_port=args.peer_port or args.port
    receiver=threading.Thread(
        target=serve,
        args=("0.0.0.0",args.port,args.store,args.sync_dir,args.role=="admin"),
        daemon=True,
    )
    receiver.start()
    watch_folder(args.sync_dir,peers,peer_port,args.store,args.interval)
