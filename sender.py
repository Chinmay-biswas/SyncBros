import argparse
import json
import socket

from chunk_store import get_chunk
from manifest import create_manifest

class TransferRejected(Exception):
    pass

def send_message(connection,message):
    connection.sendall(json.dumps(message).encode()+b"\n")

def read_message(reader):
    line=reader.readline()
    if not line:
        raise ConnectionError("Connection closed")
    return json.loads(line)

def send_file(host,port,file,store):
    manifest=create_manifest(file,store)
    with socket.create_connection((host,port),timeout=15) as connection:
        connection.settimeout(None)
        reader=connection.makefile("rb")
        send_message(connection,{"type":"manifest","manifest":manifest})
        reply=read_message(reader)

        if reply.get("type")=="rejected":
            raise TransferRejected(reply.get("reason","Rejected by admin"))
        if reply.get("type")!="missing":
            raise ValueError(reply.get("error","Invalid receiver response"))
        missing_hashes=reply["hashes"]
        for chunk_hash in missing_hashes:
            data=get_chunk(chunk_hash,store)
            send_message(connection,{"type":"chunk","hash":chunk_hash,"size":len(data)})
            connection.sendall(data)
            if read_message(reader).get("type")!="stored":
                raise ValueError("Receiver rejected chunk")

        send_message(connection,{"type":"complete"})
        reply=read_message(reader)

    if reply.get("type")!="complete":
        raise ValueError(reply.get("error","Receiver could not save file"))
    return len(missing_hashes),manifest

if __name__=="__main__":
    par=argparse.ArgumentParser()
    par.add_argument("file")
    par.add_argument("host")
    par.add_argument("--port",type=int,default=5000)
    par.add_argument("--store",default="node_store/chunk_store")
    args=par.parse_args()
    count,manifest=send_file(args.host,args.port,args.file,args.store)
    print(f"Sen{count}/{len(manifest['chunks'])} chunks for {manifest['filename']}.")
