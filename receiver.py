import argparse
import json
import socket
from pathlib import Path
from chunk_store import has_chunk,store_chunk
from manifest import hash_file
from sync import reconstruct_from_manifest
def send_message(connection,message):
    connection.sendall(json.dumps(message).encode()+b"\n")

def read_message(reader):
    line=reader.readline()
    if not line:
        raise ConnectionError("Connection closed")
    return json.loads(line)

def handle_connection(connection,address,store_dir,output_dir,require_approval):
    reader=connection.makefile("rb")
    req=read_message(reader)
    if req.get("type")!="manifest":
        raise ValueError("Expected manifest")
    meta=req["manifest"]
    dst=Path(output_dir)/meta["filename"]
    exists=dst.is_file()
    unchanged=exists and hash_file(dst)==meta["file_hash"]
    action="CREATE" if not exists else "UPDATE"
    if require_approval and not unchanged:
        print(f"\n[ADMIN APPROVAL] {action}: {dst.resolve()} from {address[0]}")
        if input("Approve? [y/N]: ").strip().lower()not in ("y","yes"):
            send_message(connection,{"type":"rejected","reason":f"Admin denied {action.lower()}"})
            print(f"[ADMIN DENIED] {dst.resolve()}")
            return
        print(f"[ADMIN APPROVED] {dst.resolve()}")
    missing=[chunk["hash"]for chunk in meta["chunks"]if not has_chunk(chunk["hash"],store_dir)]
    send_message(connection,{"type":"missing","hashes":missing})
    while True:
        req=read_message(reader)
        if req["type"]=="complete":
            if not unchanged:
                reconstruct_from_manifest(meta,store_dir,dst)
            send_message(connection,{"type":"complete"})
            print(f"[{action if not unchanged else 'UNCHANGED'}] {dst.resolve()} from {address[0]}")
            return
        if req["type"]!="chunk" or not 0<=req["size"]<=4*1024*1024:
            raise ValueError("Invalid chunk")
        data=reader.read(req["size"])
        if len(data)!=req["size"]or store_chunk(data,store_dir)!=req["hash"]:
            raise ValueError("Chunk hash mismatch")
        send_message(connection,{"type":"stored"})

def serve(host,port,store_dir,output_dir,require_approval=False):
    with socket.socket(socket.AF_INET,socket.SOCK_STREAM)as server:
        server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        server.bind((host,port))
        server.listen()
        print(f"Listening on {host}:{port}")
        while True:
            conn,address=server.accept()
            with conn:
                try:
                    handle_connection(conn,address,store_dir,output_dir,require_approval)
                except Exception as err:
                    send_message(conn,{"type":"error","error":str(err)})

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--host",default="0.0.0.0")
    parser.add_argument("--port",type=int,default=5000)
    parser.add_argument("--store",default="node_store/chunk_store")
    parser.add_argument("--output",default="sync_folder")
    parser.add_argument("--require-approval",action="store_true")
    args=parser.parse_args()
    serve(args.host,args.port,args.store,args.output,args.require_approval)
