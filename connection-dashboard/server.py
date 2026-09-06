from __future__ import annotations
import argparse
import copy
import json
import os
import shutil
import socket
import sys
import threading
import time
import uuid
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from tempfile import mkdtemp
from typing import Any
from urllib.parse import urlparse
DASHBOARD_DIR=Path(__file__).resolve().parent
PROJECT_DIR=DASHBOARD_DIR.parent
if str(PROJECT_DIR)not in sys.path:
    sys.path.insert(0,str(PROJECT_DIR))
from chunk_store import get_chunk,has_chunk,store_chunk
from manifest import create_manifest,hash_file
from sender import TransferRejected
from sync import reconstruct_from_manifest
from dashboard_protocol import ApiError,now_label,read_message,send_message
from collaboration import CollaborationMixin,MAX_MEMBERS
from tree_transfer import TreeTransferMixin
MAX_HTTP_BODY=24*1024*1024
MAX_CHUNK_BYTES=4*1024*1024
MAX_SNAPSHOT_FILES=2_000
APPROVAL_TIMEOUT_SECONDS=15*60
MODE_APPROVAL="approval"
MODE_FULL_SYNC="full_sync"
DIR_HASH="directory"
class PendingDecision:
    def __init__(self)->None:
        self.event=threading.Event()
        self.approved:bool|None=None

def is_digest(value:Any)->bool:
    return isinstance(value,str)and len(value)==64 and all(char in "0123456789abcdef" for char in value)

def safe_filename(value:Any)->str:
    if not isinstance(value,str)or not value.strip():
        raise ApiError("A file name is required.")
    parts=value.split("/")
    reserved={"CON","PRN","AUX","NUL","CONIN$","CONOUT$"}|{f"{p}{i}" for p in ("COM","LPT")for i in "123456789¹²³"}
    for part in parts:
        if (not part or part in {".",".."}or part.endswith((" ","."))
            or any(ord(c)<32 or c in '\\:<>"|?*' for c in part)
            or part.split(".")[0].upper()in reserved
            or part.endswith(".part")or ".syncing-" in part):
            raise ApiError("Use a relative path inside sync_folder with valid file and folder names.")
    return "/".join(parts)

def dir_manifest(name):
    return {"filename":name,"kind":"directory","file_hash":DIR_HASH,"size":0,"chunks":[]}

def parse_peer(value:str,default_port:int)->tuple[str,str,int]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("Peer must use Name=LAN_IP[:port].")
    name,addr=(part.strip()for part in value.split("=",1))
    if not name or not addr:
        raise argparse.ArgumentTypeError("Peer must include both a name and LAN address.")
    host,port=addr,default_port
    if addr.count(":")==1:
        host_part,port_part=addr.rsplit(":",1)
        if port_part.isdigit():
            host,port=host_part,int(port_part)
    if not host or not 1<=port<=65535:
        raise argparse.ArgumentTypeError("Peer port must be between 1 and 65535.")
    return name,host,port

def request_event_type(kind:str)->str:
    return "connection" if kind=="connection" else "change"

class DashboardSyncService(CollaborationMixin,TreeTransferMixin):
    def __init__(
        self,
        *,
        role:str,
        device_name:str,
        sync_dir:Path,
        store_dir:Path,
        state_file:Path,
        peers:list[tuple[str,str,int]],
        approval_timeout:int=APPROVAL_TIMEOUT_SECONDS,
        relay:bool=False,
        auto_sync:bool=True,
        sync_interval:float=2.0,
    )->None:
        self.role=role
        self.device_name=device_name
        self.sync_dir=sync_dir.resolve()
        self.store_dir=store_dir.resolve()
        self.state_file=state_file.resolve()
        self.approval_timeout=approval_timeout
        self.relay=relay
        self.auto_sync=auto_sync
        self.sync_interval=sync_interval
        self.lock=threading.RLock()
        self.file_lock=threading.RLock()
        self.pending:dict[str,PendingDecision]={}
        self.stop_event=threading.Event()
        self.listener:socket.socket|None=None
        self.sync_host=""
        self.sync_port=0
        self.listener_thread:threading.Thread|None=None
        self.watcher_thread:threading.Thread|None=None
        self.inflight_syncs:set[tuple[str,str,str]]=set()
        self.inflight_requests:set[tuple[str,str]]=set()
        self.remote_hashes:dict[str,str]={}
        self.remote_deletions:set[str]=set()
        self.full_sync_initializing=False
        self.tree_queues={}
        self.state=self._load_state()
        self.state["role"]=self.role
        self.state["device_name"]=self.device_name
        self._init_collaboration(peers)
        self.known_files=self._scan_sync_files()
        if self.role=="admin":
            label="Initial admin folder state" if not self.state["folder_states"]else "Changes detected while dashboard was stopped"
            self._record_admin_state(label,force=not self.state["folder_states"])
        with self.lock:
            self._save_locked()

    def _load_state(self)->dict[str,Any]:
        default:dict[str,Any]={
            "role":self.role,
            "device_name":self.device_name,
            "mode":MODE_APPROVAL,
            "users":[],
            "requests":[],
            "history":[],
            "sent_hashes":{},
            "folder_states":[],
        }
        if not self.state_file.is_file():
            return default
        try:
            saved=json.loads(self.state_file.read_text(encoding="utf-8"))
        except (OSError,json.JSONDecodeError):
            return default
        if not isinstance(saved,dict):
            return default
        for key in ("users","requests","history","folder_states"):
            if isinstance(saved.get(key),list):
                default[key]=saved[key]
        if isinstance(saved.get("sent_hashes"),dict):
            default["sent_hashes"]=saved["sent_hashes"]
        if saved.get("mode")in {MODE_APPROVAL,MODE_FULL_SYNC}:
            default["mode"]=saved["mode"]
        for key in ("device_id","outgoing_requests","request_tombstones","roster","admin_endpoint",
                    "member_id","directory_initialized","session_id","advertised_host"):
            if key in saved:
                default[key]=saved[key]
        return default

    def _save_locked(self)->None:
        self.state_file.parent.mkdir(parents=True,exist_ok=True)
        tmp=self.state_file.with_suffix(self.state_file.suffix+".tmp")
        tmp.write_text(json.dumps(self.state,indent=2),encoding="utf-8")
        tmp.replace(self.state_file)

    def _history_locked(self,event_type:str,title:str,detail:str)->None:
        self.state["history"].insert(
            0,
            {
                "id":uuid.uuid4().hex,
                "type":event_type,
                "title":title,
                "detail":detail,
                "time":now_label(),
            },
        )
        del self.state["history"][200:]

    def _record_request_outcome_locked(self,record:dict[str,Any],outcome:str,detail:str)->None:
        record["status"]=outcome
        self._history_locked(request_event_type(str(record.get("kind",""))),f"{record['title']} {outcome}",detail)
        self._save_locked()

    def _mode(self)->str:
        with self.lock:
            return str(self.state.get("mode",MODE_APPROVAL))

    def _upsert_peer(
        self,
        name:str,
        host:str,
        port:int,
        *,
        state:str="available",
        save:bool=True,
    )->dict[str,Any]:
        with self.lock:
            existing=next(
                (item for item in self.state["users"]if item.get("address")==host and item.get("port")==port),
                None,
            )
            if not existing:
                existing=next(
                    (item for item in self.state["users"]if str(item.get("name","")).casefold()==name.casefold()),
                    None,
                )
            if existing:
                endpoint_changed=existing.get("address")!=host or existing.get("port")!=port
                existing.update({"name":name,"address":host,"port":port,"state":state})
                if endpoint_changed:
                    self.state.setdefault("sent_hashes",{}).pop(str(existing["id"]),None)
                user=existing
            else:
                user={"id":uuid.uuid4().hex,"name":name,"address":host,"port":port,"state":state}
                self.state["users"].append(user)
            if save:
                self._save_locked()
            return copy.deepcopy(user)

    def _find_user_locked(self,user_id:str)->dict[str,Any]:
        user=next((item for item in self.state["users"]if item.get("id")==user_id),None)
        if not user:
            raise ApiError("That user is no longer available.")
        return user

    def add_peer(self,name:Any,host:Any,port:Any)->dict[str,Any]:
        self._require_admin()
        if not isinstance(name,str)or not name.strip():
            raise ApiError("A user name is required.")
        if not isinstance(host,str)or not host.strip():
            raise ApiError("A LAN IP address or host name is required.")
        try:
            parsed_port=int(port or self.sync_port or 5000)
        except (TypeError,ValueError)as err:
            raise ApiError("Port must be a number.")from err
        if not 1<=parsed_port<=65535:
            raise ApiError("Port must be between 1 and 65535.")
        with self.lock:
            if len(self.state["users"])>=MAX_MEMBERS:
                raise ApiError("The team has two members already. Delete obsolete members first.")
            if any(u["name"].casefold()==name.strip().casefold()or (u["address"],u["port"])==(host.strip(),parsed_port)
                   for u in self.state["users"]):
                raise ApiError("This name or LAN endpoint already exists. Edit that member instead.")
            user=self._upsert_peer(name.strip(),host.strip(),parsed_port)
            self._history_locked(
                "connection",
                f"Added {user['name']} as an available user",
                f"LAN endpoint {user['address']}:{user['port']}",
            )
            self._save_locked()
        return user

    def update_peer(self,user_id:Any,name:Any,host:Any,port:Any)->dict[str,Any]:
        self._require_admin()
        if not isinstance(user_id,str)or not user_id:
            raise ApiError("A user must be selected.")
        if not isinstance(name,str)or not name.strip():
            raise ApiError("A user name is required.")
        if not isinstance(host,str)or not host.strip():
            raise ApiError("A LAN IP address or host name is required.")
        try:
            parsed_port=int(port or self.sync_port or 5000)
        except (TypeError,ValueError)as err:
            raise ApiError("Port must be a number.")from err
        if not 1<=parsed_port<=65535:
            raise ApiError("Port must be between 1 and 65535.")
        with self.lock:
            user=self._find_user_locked(user_id)
            duplicate=next(
                (
                    item
                    for item in self.state["users"]
                    if item.get("id")!=user_id
                    and ((item.get("address")==host.strip()and item.get("port")==parsed_port)
                         or str(item.get("name","")).casefold()==name.strip().casefold())
                ),
                None,
            )
            if duplicate:
                raise ApiError("Another saved user already uses that name or LAN endpoint.")
            old_endpoint=f"{user.get('address')}:{user.get('port')}"
            endpoint_changed=user.get("address")!=host.strip()or user.get("port")!=parsed_port
            user.update({"name":name.strip(),"address":host.strip(),"port":parsed_port})
            if endpoint_changed:
                user["state"]="available"
                self.state.setdefault("sent_hashes",{}).pop(user_id,None)
            self._history_locked(
                "connection",
                f"Updated {user['name']} LAN address",
                f"{old_endpoint} → {user['address']}:{user['port']}",
            )
            self._save_locked()
            return copy.deepcopy(user)

    def state_snapshot(self)->dict[str,Any]:
        with self.lock:
            snap=copy.deepcopy(self.state)
            snap["users"]=self._roster_locked()
            snap["self_id"]=self.device_id if self.role=="admin" else self.state["member_id"]
            snap["protocol"]=3
            records=self.state["requests"]if self.role=="admin" else self.state["outgoing_requests"]
            snap["requests"]=[self._public_request(r)for r in records if r.get("status")!="deleted"]
            snap["board"]=dict(self.board_status,admin_endpoint=self.state["admin_endpoint"])
            own=next((u for u in snap["users"]if u["id"]==snap["self_id"]),None)
            snap["membership_state"]=own.get("state","available")if own else "available"
            for key in ("outgoing_requests","request_tombstones","roster","sent_hashes"):
                snap.pop(key,None)
            visible_states:list[dict[str,Any]]=[]
            for entry in reversed(snap.get("folder_states",[])):
                if not isinstance(entry,dict):
                    continue
                visible_states.append(
                    {
                        "id":entry.get("id"),
                        "parent_id":entry.get("parent_id"),
                        "time":entry.get("time"),
                        "label":entry.get("label"),
                        "file_count":entry.get("file_count",0),
                        "folder_count":entry.get("folder_count",0),
                        "change_count":entry.get("change_count",0),
                    }
                )
            snap["folder_states"]=visible_states
            snap["sync"]={
                "listening":self.listener is not None,
                "host":self.sync_host,
                "port":self.sync_port,
                "folder":str(self.sync_dir),
                "chunk_store":str(self.store_dir),
                "auto_watch":self.auto_sync,
                "relay":self.relay,
                "initializing":self.full_sync_initializing,
            }
            return snap

    def _safe_destination(self,filename:Any)->Path:
        name=safe_filename(filename)
        dst=self.sync_dir
        for part in name.split("/"):
            dst=dst/part
            if dst.is_symlink()or (dst.exists()and getattr(dst.lstat(),"st_file_attributes",0)&0x400):
                raise ApiError("Sync paths must not contain symbolic links or junctions.")
        dst=dst.resolve()
        if not dst.is_relative_to(self.sync_dir)or dst==self.sync_dir:
            raise ApiError("The destination must stay inside sync_folder.")
        return dst

    def _scan_sync_files(self)->dict[str,tuple[Path,str]]:
        self.sync_dir.mkdir(parents=True,exist_ok=True)
        files:dict[str,tuple[Path,str]]={}
        def fail(err):
            raise err

        for root,dirs,names in os.walk(self.sync_dir,followlinks=False,onerror=fail):
            for name in list(dirs)+names:
                path=Path(root)/name
                rel=path.relative_to(self.sync_dir).as_posix()
                try:
                    self._safe_destination(rel)
                except ApiError:
                    if name in dirs:
                        dirs.remove(name)
                    continue
                if path.is_dir():
                    files[rel]=(path,DIR_HASH)
                elif path.is_file():
                    files[rel]=(path,hash_file(path))
        return files

    def _capture_folder_manifests(self)->dict[str,dict[str,Any]]:
        manifests:dict[str,dict[str,Any]]={}
        with self.file_lock:
            files=self._scan_sync_files()
            for name,(path,expected_hash)in files.items():
                if expected_hash==DIR_HASH:
                    manifests[name]=dir_manifest(name)
                    continue
                try:
                    meta=create_manifest(path,self.store_dir)
                    if meta["file_hash"]!=expected_hash:
                        meta=create_manifest(path,self.store_dir)
                    if not path.is_file()or hash_file(path)!=meta["file_hash"]:
                        raise ApiError("A file changed while capturing the folder. Try again after saving it.")
                    meta["filename"]=name
                    manifests[name]=meta
                except OSError:
                    raise ApiError("A file could not be read while capturing the folder. Close it and try again.")
        return self._validate_snapshot_manifests(list(manifests.values()))

    def _validate_manifest(self,candidate:Any)->dict[str,Any]:
        if not isinstance(candidate,dict):
            raise ApiError("Invalid file manifest.")
        filename=safe_filename(candidate.get("filename"))
        if candidate.get("kind")=="directory":
            if candidate.get("chunks")not in (None,[])or candidate.get("size",0)!=0:
                raise ApiError("A directory entry cannot contain file data.")
            return dir_manifest(filename)
        if candidate.get("kind","file")!="file":
            raise ApiError("Invalid entry kind.")
        if not is_digest(candidate.get("file_hash")):
            raise ApiError("Manifest file hash is invalid.")
        size=candidate.get("size")
        if not isinstance(size,int)or size<0:
            raise ApiError("Manifest file size is invalid.")
        chunks=candidate.get("chunks")
        if not isinstance(chunks,list):
            raise ApiError("Manifest chunk list is invalid.")
        if len(chunks)>100_000:
            raise ApiError("Manifest contains too many chunks.")
        total=0
        normalized_chunks:list[dict[str,Any]]=[]
        for idx,item in enumerate(chunks):
            if not isinstance(item,dict):
                raise ApiError("Manifest chunk list is invalid.")
            chunk_hash=item.get("hash")
            chunk_size=item.get("size")
            if not is_digest(chunk_hash)or not isinstance(chunk_size,int)or not 0<=chunk_size<=MAX_CHUNK_BYTES:
                raise ApiError("Manifest chunk is invalid.")
            if item.get("index",idx)!=idx:
                raise ApiError("Manifest chunk ordering is invalid.")
            total+=chunk_size
            if total>size:
                raise ApiError("Manifest chunk sizes exceed the file size.")
            normalized_chunks.append({"index":idx,"hash":chunk_hash,"size":chunk_size})
        if total!=size:
            raise ApiError("Manifest chunk sizes do not match the file size.")
        return {
            "filename":filename,
            "size":size,
            "file_hash":candidate["file_hash"],
            "chunk_size":candidate.get("chunk_size"),
            "chunks":normalized_chunks,
        }

    def _validate_snapshot_manifests(self,candidates:Any)->dict[str,dict[str,Any]]:
        if not isinstance(candidates,list)or len(candidates)>MAX_SNAPSHOT_FILES:
            raise ApiError(f"A folder snapshot may contain at most {MAX_SNAPSHOT_FILES:,} files and folders.")
        manifests:dict[str,dict[str,Any]]={}
        seen=set()
        for candidate in candidates:
            meta=self._validate_manifest(candidate)
            name=meta["filename"]
            if name.casefold()in seen:
                raise ApiError("A folder snapshot cannot contain duplicate file names.")
            seen.add(name.casefold())
            manifests[name]=meta
        for name in list(manifests):
            parts=name.split("/")
            for i in range(1,len(parts)):
                parent="/".join(parts[:i])
                if parent in manifests:
                    if manifests[parent].get("kind")!="directory":
                        raise ApiError("A file cannot also be the parent of another entry.")
                elif parent.casefold()in seen:
                    raise ApiError("Folder path casing must be consistent.")
                else:
                    manifests[parent]=dir_manifest(parent)
                    seen.add(parent.casefold())
        if len(manifests)>MAX_SNAPSHOT_FILES:
            raise ApiError("The folder contains too many entries.")
        return manifests

    def _staging_root(self)->Path:
        root=(self.state_file.parent/".sync-staging").resolve()
        root.mkdir(parents=True,exist_ok=True)
        return root

    def _new_staging_dir(self,prefix:str)->Path:
        root=self._staging_root()
        directory=Path(mkdtemp(prefix=prefix,dir=root)).resolve()
        if directory.parent!=root:
            raise RuntimeError("Could not create a safe snapshot staging directory.")
        return directory

    def _cleanup_staging_dir(self,directory:Path)->None:
        root=self._staging_root()
        if directory.parent==root and directory.name.startswith(("incoming-","revert-","snapshot-","approved-","delta-")):
            shutil.rmtree(directory,ignore_errors=True)

    def _stage_snapshot(self,manifests:dict[str,dict[str,Any]],*,prefix:str)->Path:
        staging=self._new_staging_dir(prefix)
        try:
            for name,meta in sorted(manifests.items(),key=lambda item:(item[0].count("/"),item[0])):
                if meta.get("kind")=="directory":
                    (staging/name).mkdir(parents=True,exist_ok=True)
                    continue
                if any(not has_chunk(chunk["hash"],self.store_dir)for chunk in meta["chunks"]):
                    raise ApiError(f"Chunk data for {name} is not available locally.")
                reconstruct_from_manifest(meta,self.store_dir,staging/name)
            return staging
        except Exception:
            self._cleanup_staging_dir(staging)
            raise

    def _apply_staged_snapshot(self,staging:Path,manifests:dict[str,dict[str,Any]],*,remote:bool)->tuple[int,int]:
        with self.file_lock:
            for name in manifests:
                self._safe_destination(name)
            before=self._scan_sync_files()
            target_names=set(manifests)
            removed={name for name in before if name not in manifests
                     or (before[name][1]==DIR_HASH)!=(manifests[name].get("kind")=="directory")}
            for name in sorted(removed,key=lambda n:(n.count("/"),n),reverse=True):
                dst=self._safe_destination(name)
                if dst.is_dir():
                    dst.rmdir()
                else:
                    dst.unlink(missing_ok=True)
                if remote:
                    self.remote_deletions.add(name)
            for name,meta in sorted(manifests.items(),key=lambda item:(item[0].count("/"),item[0])):
                dst=self._safe_destination(name)
                if meta.get("kind")=="directory":
                    dst.mkdir(parents=True,exist_ok=True)
                    continue
                if before.get(name,(None,None))[1]==meta["file_hash"]:
                    continue
                src=(staging/name).resolve()
                if not src.is_relative_to(staging)or not src.is_file():
                    raise ApiError("Snapshot staging is invalid.")
                dst.parent.mkdir(parents=True,exist_ok=True)
                tmp=dst.with_name(f"{dst.name}.syncing-{uuid.uuid4().hex}")
                try:
                    shutil.copyfile(src,tmp)
                    if hash_file(tmp)!=meta["file_hash"]:
                        raise ValueError(f"Staged file verification failed for {name}.")
                    tmp.replace(dst)
                finally:
                    tmp.unlink(missing_ok=True)
                if remote:
                    self.remote_hashes[name]=meta["file_hash"]
            self.known_files=self._scan_sync_files()
            changed=sum(
                1
                for name,meta in manifests.items()
                if before.get(name,(None,None))[1]!=meta["file_hash"]
            )
            return changed,len(set(before)-target_names)

    def _apply_snapshot(self,manifests:dict[str,dict[str,Any]],*,remote:bool,prefix:str)->tuple[int,int]:
        manifests=self._validate_snapshot_manifests(list(manifests.values()))
        staging=self._stage_snapshot(manifests,prefix=prefix)
        try:
            return self._apply_staged_snapshot(staging,manifests,remote=remote)
        finally:
            self._cleanup_staging_dir(staging)

    def _resolve_state_locked(self,state_id:str|None=None)->dict[str,dict[str,Any]]:
        states=self.state.get("folder_states",[])
        if not isinstance(states,list)or not states:
            return {}
        limit=len(states)
        if state_id is not None:
            idx=next((i for i,item in enumerate(states)if item.get("id")==state_id),None)
            if idx is None:
                raise ApiError("That saved folder state is no longer available.")
            limit=idx+1
        resolved:dict[str,dict[str,Any]]={}
        for entry in states[:limit]:
            changes=entry.get("changes",{})
            if not isinstance(changes,dict):
                continue
            for name,meta in changes.items():
                if meta is None:
                    resolved.pop(name,None)
                elif isinstance(meta,dict):
                    resolved[name]=meta
        return copy.deepcopy(resolved)

    def _record_admin_state(
        self,
        label:str,
        *,
        force:bool=False,
        snapshot:dict[str,dict[str,Any]]|None=None,
    )->dict[str,Any]|None:
        if self.role!="admin":
            return None
        curr=snapshot if snapshot is not None else self._capture_folder_manifests()
        with self.lock:
            states=self.state.setdefault("folder_states",[])
            prev=self._resolve_state_locked()if states else {}
            changes:dict[str,dict[str,Any]|None]={}
            for name in sorted(set(prev)|set(curr)):
                old_hash=prev.get(name,{}).get("file_hash")
                new_manifest=curr.get(name)
                new_hash=new_manifest.get("file_hash")if new_manifest else None
                if old_hash!=new_hash:
                    changes[name]=copy.deepcopy(new_manifest)if new_manifest else None
            if not changes and states and not force:
                return None
            entry={
                "id":uuid.uuid4().hex,
                "parent_id":states[-1].get("id")if states else None,
                "time":now_label(),
                "label":label,
                "file_count":sum(m.get("kind")!="directory" for m in curr.values()),
                "folder_count":sum(m.get("kind")=="directory" for m in curr.values()),
                "change_count":len(changes),
                "changes":changes if states else copy.deepcopy(curr),
            }
            states.append(entry)
            self._save_locked()
            return copy.deepcopy(entry)

    def revert_admin_state(self,state_id:Any,*,send_to_all:bool=False)->None:
        if self.role!="admin":
            raise ApiError("Only the administrator can restore a saved folder state.")
        if not isinstance(state_id,str)or not state_id:
            raise ApiError("Choose a saved folder state.")
        with self.lock:
            desired=self._resolve_state_locked(state_id)
            target=next((item for item in self.state["folder_states"]if item.get("id")==state_id),None)
        changed,removed=self._apply_snapshot(desired,remote=False,prefix="revert-")
        self._record_admin_state(f"Reverted to {target.get('time')if target else 'saved state'}",force=True,snapshot=desired)
        with self.lock:
            self._history_locked(
                "change",
                "Admin folder restored",
                f"Restored {len(desired)} file/folder entries; {changed} updated and {removed} removed.",
            )
            self._save_locked()
        if send_to_all:
            self.send_admin_folder_to_all()

    def start_sync_listener(self,host:str,port:int)->None:
        listener=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        listener.bind((host,port))
        listener.listen()
        listener.settimeout(0.5)
        self.listener=listener
        self.sync_host=host
        self.sync_port=listener.getsockname()[1]
        if host!="0.0.0.0":
            self.state["advertised_host"]=host
        else:
            try:
                self.state["advertised_host"]=socket.gethostbyname(socket.gethostname())
            except OSError:
                pass
        self.stop_event.clear()
        self.listener_thread=threading.Thread(target=self._accept_loop,name="dashboard-sync-listener",daemon=True)
        self.listener_thread.start()
        if self.role=="member":
            self.board_thread=threading.Thread(target=self._board_loop,name="dashboard-shared-board",daemon=True)
            self.board_thread.start()
        if self.auto_sync:
            self.watcher_thread=threading.Thread(target=self._watch_sync_folder,name="dashboard-sync-watcher",daemon=True)
            self.watcher_thread.start()
        with self.lock:
            watcher_detail="Full-sync watcher is ready" if self.auto_sync else "Full-sync watcher was disabled with --no-auto-sync"
            self._history_locked("system","Sync listener started",f"Listening on {self.sync_host}:{self.sync_port}. {watcher_detail}.")
            self._save_locked()

    def stop(self)->None:
        self.stop_event.set()
        self.board_wake.set()
        if self.listener:
            try:
                self.listener.close()
            except OSError:
                pass
            self.listener=None
        if self.listener_thread:
            self.listener_thread.join(timeout=2)
        if self.watcher_thread:
            self.watcher_thread.join(timeout=2)
        if self.board_thread:
            self.board_thread.join(timeout=6)
        with self.lock:
            workers=list(self.workers)
        for worker in workers:
            worker.join(timeout=2)

    def _accept_loop(self)->None:
        while not self.stop_event.is_set():
            try:
                assert self.listener is not None
                conn,address=self.listener.accept()
            except socket.timeout:
                continue
            except (OSError,AssertionError):
                break
            self._spawn(self._handle_socket,conn,address)

    def _handle_socket(self,connection:socket.socket,address:tuple[str,int])->None:
        with connection:
            try:
                connection.settimeout(30)
                reader=connection.makefile("rb")
                req=read_message(reader)
                request_type=req.get("type")
                if request_type=="board_state":
                    send_message(connection,self._board_reply(req,address,connection.getsockname()[0]))
                elif request_type=="submit_request":
                    self._receive_submission(connection,reader,address,req)
                elif request_type=="tree_update":
                    self._validate_transfer_sender(req,address)
                    self._receive_tree_update(connection,reader,address,req)
                elif request_type=="delete_request":
                    self._require_admin()
                    self._member_for_packet(req,address)
                    self._delete_shared_request(req.get("request_id"),req["device_id"])
                    send_message(connection,{"type":"request_deleted"})
                elif request_type=="manifest":
                    self._validate_transfer_sender(req,address)
                    self._handle_manifest(connection,reader,address,req)
                elif request_type=="batch_manifest":
                    raise ApiError("A batch manifest must follow a folder change request.")
                elif request_type in {"folder_change_request","full_sync_request","connection_request"}:
                    raise ApiError("Update and restart connection-dashboard on this member laptop. This admin uses saved approval requests (version 3).")
                elif request_type=="mode_start":
                    self._validate_transfer_sender(req,address)
                    self._handle_mode_start(connection,address,req)
                elif request_type=="mode_stop":
                    self._validate_transfer_sender(req,address)
                    self._handle_mode_stop(connection,address,req)
                elif request_type=="snapshot_finalize":
                    self._validate_transfer_sender(req,address)
                    self._handle_snapshot_finalize(connection,address,req)
                elif request_type=="delete_file":
                    self._validate_transfer_sender(req,address)
                    self._handle_delete_file(connection,address,req)
                else:
                    raise ApiError("Unknown dashboard sync message.")
            except Exception as err:
                try:
                    send_message(connection,{"type":"error","error":str(err)})
                except OSError:
                    pass

    def _queue_request(self,record:dict[str,Any])->tuple[str,PendingDecision]:
        req_id=uuid.uuid4().hex
        record.update({"id":req_id,"status":"pending","submitted_at":now_label()})
        decision=PendingDecision()
        with self.lock:
            self.state["requests"].insert(0,record)
            self.pending[req_id]=decision
            self._history_locked(request_event_type(str(record["kind"])),record["title"],"Awaiting administrator approval")
            self._save_locked()
        return req_id,decision

    def _wait_for_approval(self,request_id:str,decision:PendingDecision)->str:
        if not decision.event.wait(self.approval_timeout):
            with self.lock:
                rec=next((item for item in self.state["requests"]if item.get("id")==request_id),None)
                if rec and rec.get("status")=="pending":
                    self._record_request_outcome_locked(rec,"expired","No administrator decision was made in time.")
                self.pending.pop(request_id,None)
            return "expired"
        with self.lock:
            self.pending.pop(request_id,None)
        return "approved" if decision.approved is True else "rejected"

    def decide_request(self,request_id:str,approved:bool)->None:
        self._require_admin()
        with self.lock:
            curr=next((r for r in self.state["requests"]if r["id"]==request_id),None)
            if curr and curr.get("protocol")==3:
                self._decide_durable_request(request_id,approved)
                return
        with self.lock:
            decision=self.pending.get(request_id)
            rec=next((item for item in self.state["requests"]if item.get("id")==request_id),None)
            if not decision or not rec or rec.get("status")!="pending":
                raise ApiError("This request is no longer waiting for a decision.")
            rec["status"]="approved" if approved else "rejected"
            decision.approved=approved
            self._save_locked()
            decision.event.set()

    def _receive_manifest_chunks(
        self,
        connection:socket.socket,
        reader:Any,
        manifest:dict[str,Any],
        destination:Path|None,
    )->None:
        missing=[chunk["hash"]for chunk in manifest["chunks"]if not has_chunk(chunk["hash"],self.store_dir)]
        send_message(connection,{"type":"missing","hashes":missing})
        while True:
            packet=read_message(reader)
            if packet.get("type")=="complete":
                if destination is not None:
                    reconstruct_from_manifest(manifest,self.store_dir,destination)
                send_message(connection,{"type":"complete"})
                return
            size=packet.get("size")
            if packet.get("type")!="chunk" or not isinstance(size,int)or not 0<=size<=MAX_CHUNK_BYTES:
                raise ApiError("Invalid chunk packet.")
            data=reader.read(size)
            if len(data)!=size or store_chunk(data,self.store_dir)!=packet.get("hash"):
                raise ApiError("Chunk hash verification failed.")
            send_message(connection,{"type":"stored"})

    def _stream_manifest(
        self,
        connection:socket.socket,
        reader:Any,
        manifest:dict[str,Any],
        *,
        packet_type:str="manifest",
    )->int:
        send_message(
            connection,
            {
                "type":packet_type,
                "manifest":manifest,
                "sender_name":self.device_name,
                "device_id":self.device_id,
                "peer_port":self.sync_port,
            },
        )
        reply=read_message(reader)
        if reply.get("type")in {"rejected","folder_change_rejected"}:
            raise TransferRejected(reply.get("reason","The administrator rejected the change."))
        if reply.get("type")!="missing":
            raise ValueError(reply.get("error","Receiver rejected the file manifest."))
        missing_hashes=reply.get("hashes")
        if not isinstance(missing_hashes,list):
            raise ValueError("Receiver did not return a valid chunk list.")
        for chunk_hash in missing_hashes:
            if not is_digest(chunk_hash):
                raise ValueError("Receiver returned an invalid chunk hash.")
            data=get_chunk(chunk_hash,self.store_dir)
            send_message(connection,{"type":"chunk","hash":chunk_hash,"size":len(data)})
            connection.sendall(data)
            if read_message(reader).get("type")!="stored":
                raise ValueError("Receiver rejected a chunk.")
        send_message(connection,{"type":"complete"})
        reply=read_message(reader)
        if reply.get("type")!="complete":
            raise ValueError(reply.get("error","Receiver could not save the verified file."))
        return len(missing_hashes)

    def _send_control(self,user:dict[str,Any],message:dict[str,Any])->dict[str,Any]:
        with socket.create_connection((user["address"],int(user["port"])),timeout=15)as conn:
            conn.settimeout(None)
            reader=conn.makefile("rb")
            packet=dict(message)
            packet.setdefault("sender_name",self.device_name)
            packet.setdefault("peer_port",self.sync_port)
            packet.setdefault("device_id",self.device_id)
            send_message(conn,packet)
            reply=read_message(reader)
        if reply.get("type")=="error":
            raise ValueError(reply.get("error","Peer rejected the dashboard control message."))
        return reply

    def _send_manifest_to_user(self,user:dict[str,Any],manifest:dict[str,Any])->int:
        with socket.create_connection((user["address"],int(user["port"])),timeout=15)as conn:
            conn.settimeout(None)
            reader=conn.makefile("rb")
            return self._stream_manifest(conn,reader,manifest)

    def _handle_manifest(self,connection:socket.socket,reader:Any,address:tuple[str,int],request:dict[str,Any])->None:
        meta=self._validate_manifest(request.get("manifest"))
        dst=self._safe_destination(meta["filename"])
        exists=dst.is_file()
        unchanged=exists and hash_file(dst)==meta["file_hash"]
        sender_name=str(request.get("sender_name")or address[0]).strip()[:80]or address[0]
        requires_approval=self.role=="admin" and self._mode()!=MODE_FULL_SYNC and not unchanged
        req_id:str|None=None
        if requires_approval:
            action="Create" if not exists else "Update"
            request_record={
                "kind":"change",
                "title":f"{action} {meta['filename']}",
                "user":sender_name,
                "file_name":meta["filename"],
                "detail":f"Legacy one-file request from {sender_name}",
            }
            req_id,decision=self._queue_request(request_record)
            outcome=self._wait_for_approval(req_id,decision)
            if outcome!="approved":
                reason="Administrator rejected the file change." if outcome=="rejected" else "Administrator did not review the file change in time."
                send_message(connection,{"type":"rejected","reason":reason})
                if outcome=="rejected":
                    with self.lock:
                        rec=next((item for item in self.state["requests"]if item.get("id")==req_id),None)
                        if rec:
                            self._record_request_outcome_locked(rec,"rejected","The admin left the sync folder unchanged.")
                return
        self._receive_manifest_chunks(connection,reader,meta,None if unchanged else dst)
        if not unchanged:
            with self.file_lock:
                self.remote_hashes[meta["filename"]]=meta["file_hash"]
                self.known_files=self._scan_sync_files()
            if self.role=="admin":
                label=f"Accepted file from {sender_name}" if requires_approval else f"Full-sync update from {sender_name}"
                self._record_admin_state(label)
        with self.lock:
            if req_id:
                rec=next((item for item in self.state["requests"]if item.get("id")==req_id),None)
                if rec:
                    self._record_request_outcome_locked(rec,"completed","SHA-256 verified and applied to the admin folder.")
            else:
                title=f"Applied full-sync update from {sender_name}" if self.role=="admin" and self._mode()==MODE_FULL_SYNC else f"Received {meta['filename']}"
                detail=f"{meta['size']:,} bytes, SHA-256 verified."
                self._history_locked("change",title,detail)
                self._save_locked()
        if not unchanged and self.role=="admin" and self._mode()==MODE_FULL_SYNC:
            self._relay_manifest_to_members(meta,exclude_name=sender_name)

    def _queue_manifest_to_user(self,user:dict[str,Any],manifest:dict[str,Any],*,source:str,force:bool=False)->bool:
        if user.get("state")!="connected":
            return False
        file_hash=str(manifest["file_hash"])
        key=(str(user["id"]),str(manifest["filename"]),file_hash)
        with self.lock:
            sent=self.state.setdefault("sent_hashes",{}).setdefault(str(user["id"]),{})
            if not force and (sent.get(manifest["filename"])==file_hash or key in self.inflight_syncs):
                return False
            self.inflight_syncs.add(key)
        threading.Thread(
            target=self._send_manifest_worker,
            args=(copy.deepcopy(user),copy.deepcopy(manifest),key,source),
            daemon=True,
        ).start()
        return True

    def _queue_file_to_user(self,user:dict[str,Any],file_path:Path,*,source:str)->bool:
        if not file_path.is_file():
            return False
        try:
            meta=create_manifest(file_path,self.store_dir)
        except OSError:
            return False
        return self._queue_manifest_to_user(user,meta,source=source)

    def _send_manifest_worker(
        self,
        user:dict[str,Any],
        manifest:dict[str,Any],
        key:tuple[str,str,str],
        source:str,
    )->None:
        try:
            transferred=self._send_manifest_to_user(user,manifest)
            delivered=True
            detail=f"{transferred}/{len(manifest['chunks'])} changed chunk(s) sent."
        except TransferRejected as err:
            delivered=False
            detail=str(err)
        except Exception as err:
            delivered=False
            detail=str(err)
        with self.lock:
            self.inflight_syncs.discard(key)
            if delivered:
                self.state.setdefault("sent_hashes",{}).setdefault(str(user["id"]),{})[manifest["filename"]]=manifest["file_hash"]
                if source=="manual":
                    self._history_locked("change",f"Sent {manifest['filename']} to {user['name']}",detail)
            else:
                self._history_locked("change",f"Could not send {manifest['filename']} to {user['name']}",detail)
            self._save_locked()

    def request_folder_change(self,user_id:Any)->None:
        self.submit_member_request("folder_change")

    def sync_folder_changes(self,user_id:Any)->None:
        self.request_folder_change(user_id)

    def start_full_sync(self,*,initiated_by:str|None=None)->int:
        if self.role!="admin":
            raise ApiError("Only the administrator can start full sync.")
        with self.lock:
            if self.state.get("mode")==MODE_FULL_SYNC:
                raise ApiError("Full sync is already active.")
            if any(r.get("kind")=="folder_change" and r.get("status")=="applying" for r in self.state["requests"]):
                raise ApiError("Wait for the approved folder request to finish before starting full sync.")
            self.state["mode"]=MODE_FULL_SYNC
            self.state["session_id"]=uuid.uuid4().hex
            self.full_sync_initializing=False
            actor=f" after approving {initiated_by}" if initiated_by else ""
            self._history_locked(
                "system",
                "Full sync started",
                f"Admin folder is the shared baseline{actor}. Connected devices are being notified.",
            )
            targets=[copy.deepcopy(user)for user in self.state["users"]if user.get("state")=="connected"]
            self._save_locked()
        self._record_admin_state("Full sync started",force=True)
        for user in targets:
            self._schedule_baseline(user)
        return len(targets)

    def stop_full_sync(self)->int:
        if self.role!="admin":
            raise ApiError("Only the administrator can stop full sync.")
        with self.lock:
            if self.state.get("mode")!=MODE_FULL_SYNC:
                raise ApiError("Full sync is not active.")
            self.state["mode"]=MODE_APPROVAL
            self.full_sync_initializing=False
            targets=[copy.deepcopy(user)for user in self.state["users"]if user.get("state")=="connected"]
            self._history_locked("system","Full sync stopped","Member folder changes now need one admin approval request.")
            self._save_locked()
        for user in targets:
            threading.Thread(target=self._send_mode_stop_worker,args=(user,),daemon=True).start()
        return len(targets)

    def request_full_sync(self,user_id:Any)->None:
        self.submit_member_request("full_sync")

    def send_admin_folder(self,user_id:Any)->None:
        if self.role!="admin":
            raise ApiError("Only the administrator can replace a member folder with the admin folder.")
        if not isinstance(user_id,str):
            raise ApiError("Choose a connected member.")
        with self.lock:
            user=copy.deepcopy(self._find_user_locked(user_id))
        if user.get("state")!="connected":
            raise ApiError("Connect this member before sending the admin folder.")
        threading.Thread(
            target=self._push_admin_snapshot_worker,
            args=(user,"admin folder send",False),
            daemon=True,
        ).start()

    def send_admin_folder_to_all(self)->int:
        if self.role!="admin":
            raise ApiError("Only the administrator can send the shared folder to all devices.")
        with self.lock:
            targets=[copy.deepcopy(user)for user in self.state["users"]if user.get("state")=="connected"]
        for user in targets:
            threading.Thread(
                target=self._push_admin_snapshot_worker,
                args=(user,"admin folder send",False),
                daemon=True,
            ).start()
        return len(targets)

    def _push_admin_snapshot_worker(self,user:dict[str,Any],reason:str,announce_mode:bool)->None:
        try:
            with self.file_lock:
                snap=self._capture_folder_manifests()
                task=self._queue_tree_update(user,snap,[],replace=True,announce=announce_mode,label=reason)
            while not task["done"].wait(0.2):
                if self.stop_event.is_set():
                    return
            if task["error"]:
                raise task["error"]
            transferred=task["chunks"]
            with self.lock:
                self.state.setdefault("sent_hashes",{})[str(user["id"])]={
                    name:meta["file_hash"]for name,meta in snap.items()
                }
                self._history_locked(
                    "change",
                    f"Sent admin folder to {user['name']}",
                    f"{len(snap)} file/folder entries, {transferred} changed chunk(s); {reason}.",
                )
                self._save_locked()
        except Exception as err:
            with self.lock:
                self._history_locked("change",f"Could not send admin folder to {user['name']}",str(err))
                self._save_locked()

    def _send_mode_stop_worker(self,user:dict[str,Any])->None:
        try:
            self._send_control(user,{"type":"mode_stop","message":"Full sync stopped by the administrator."})
        except Exception as err:
            with self.lock:
                self._history_locked("system",f"Could not notify {user['name']} that full sync stopped",str(err))
                self._save_locked()

    def _handle_mode_start(self,connection:socket.socket,address:tuple[str,int],request:dict[str,Any])->None:
        if self.role=="admin":
            raise ApiError("Only members accept a full-sync start message.")
        with self.lock:
            self.state["mode"]=MODE_FULL_SYNC
            self.state["session_id"]=request.get("session_id",self.state["session_id"])
            self.full_sync_initializing=True
            sender=str(request.get("sender_name")or address[0])
            self._history_locked("system","Full sync started",f"Administrator {sender} is sending the shared folder baseline.")
            self._save_locked()
        send_message(connection,{"type":"mode_ack"})

    def _handle_mode_stop(self,connection:socket.socket,address:tuple[str,int],request:dict[str,Any])->None:
        if self.role=="admin":
            raise ApiError("Only members accept a full-sync stop message.")
        with self.lock:
            self.state["mode"]=MODE_APPROVAL
            self.full_sync_initializing=False
            sender=str(request.get("sender_name")or address[0])
            self._history_locked("system","Full sync stopped",f"Administrator {sender} switched back to approval mode.")
            self._save_locked()
        send_message(connection,{"type":"mode_ack"})

    def _handle_snapshot_finalize(self,connection:socket.socket,address:tuple[str,int],request:dict[str,Any])->None:
        if self.role=="admin":
            raise ApiError("Only a member accepts an administrator folder snapshot.")
        candidates=request.get("files")
        if not isinstance(candidates,list)or len(candidates)>MAX_SNAPSHOT_FILES:
            raise ApiError("Invalid administrator folder snapshot.")
        expected:dict[str,str]={}
        for item in candidates:
            if not isinstance(item,dict):
                raise ApiError("Invalid administrator folder snapshot.")
            name=safe_filename(item.get("filename"))
            digest=item.get("file_hash")
            if not is_digest(digest)or name in expected:
                raise ApiError("Invalid administrator folder snapshot.")
            expected[name]=digest
        with self.file_lock:
            curr=self._scan_sync_files()
            mismatched=[name for name,digest in expected.items()if curr.get(name,(None,None))[1]!=digest]
            if mismatched:
                raise ApiError(f"Snapshot verification failed for {', '.join(mismatched[:3])}.")
            for name in set(curr)-set(expected):
                self._safe_destination(name).unlink(missing_ok=True)
                self.remote_deletions.add(name)
            self.remote_hashes.update(expected)
            self.known_files=self._scan_sync_files()
        with self.lock:
            self.full_sync_initializing=False
            self.state["session_id"]=request.get("session_id",self.state["session_id"])
            sender=str(request.get("sender_name")or address[0])
            self._history_locked(
                "change",
                "Admin folder snapshot applied",
                f"{len(expected)} file(s) now match administrator {sender}.",
            )
            self._save_locked()
        send_message(connection,{"type":"snapshot_applied"})

    def _relay_manifest_to_members(self,manifest:dict[str,Any],*,exclude_name:str)->None:
        with self.lock:
            targets=[
                copy.deepcopy(user)
                for user in self.state["users"]
                if user.get("state")=="connected" and str(user.get("name","")).casefold()!=exclude_name.casefold()
            ]
        for user in targets:
            self._queue_manifest_to_user(user,manifest,source="full-sync relay")

    def _queue_delete_to_user(self,user:dict[str,Any],filename:str,*,source:str)->None:
        threading.Thread(target=self._delete_worker,args=(user,filename,source),daemon=True).start()

    def _delete_worker(self,user:dict[str,Any],filename:str,source:str)->None:
        try:
            reply=self._send_control(user,{"type":"delete_file","filename":filename,"source":source})
            if reply.get("type")!="deleted":
                raise ValueError(reply.get("error","Peer did not apply the deletion."))
        except Exception as err:
            with self.lock:
                self._history_locked("change",f"Could not relay deletion of {filename} to {user['name']}",str(err))
                self._save_locked()

    def _relay_delete_to_members(self,filename:str,*,exclude_name:str)->None:
        with self.lock:
            targets=[
                copy.deepcopy(user)
                for user in self.state["users"]
                if user.get("state")=="connected" and str(user.get("name","")).casefold()!=exclude_name.casefold()
            ]
        for user in targets:
            self._queue_delete_to_user(user,filename,source="full-sync relay")

    def _handle_delete_file(self,connection:socket.socket,address:tuple[str,int],request:dict[str,Any])->None:
        if self._mode()!=MODE_FULL_SYNC:
            raise ApiError("Deletion propagation is available only while full sync is active.")
        filename=safe_filename(request.get("filename"))
        sender_name=str(request.get("sender_name")or address[0]).strip()[:80]or address[0]
        dst=self._safe_destination(filename)
        with self.file_lock:
            existed=dst.is_file()
            dst.unlink(missing_ok=True)
            self.remote_deletions.add(filename)
            self.known_files=self._scan_sync_files()
        if self.role=="admin":
            self._record_admin_state(f"Full-sync deletion from {sender_name}")
            self._relay_delete_to_members(filename,exclude_name=sender_name)
        with self.lock:
            self._history_locked(
                "change",
                f"Applied full-sync deletion of {filename}",
                f"Requested by {sender_name}; file {'removed' if existed else 'was already absent'}.",
            )
            self._save_locked()
        send_message(connection,{"type":"deleted"})

    def _full_sync_targets(self)->list[dict[str,Any]]:
        with self.lock:
            connected=[copy.deepcopy(user)for user in self.state["users"]if user.get("state")=="connected"]
        if self.role=="admin":
            return connected
        try:
            return [self._admin_target(require_connected=True)]
        except ApiError:
            return []

    def _watch_sync_folder(self)->None:
        while not self.stop_event.wait(self.sync_interval):
            try:
                with self.file_lock:
                    curr=self._scan_sync_files()
                    prev=self.known_files
                    if self._mode()!=MODE_FULL_SYNC or self.full_sync_initializing:
                        self.known_files=curr
                        continue
                    changed={name for name,(_,digest)in curr.items()if prev.get(name,(None,None))[1]!=digest}
                    removed=sorted(set(prev)-set(curr))
                    if not changed and not removed:
                        continue
                    snap=self._capture_folder_manifests()
                    files={name:snap[name]for name in changed if name in snap}
                    self.known_files={name:(self.sync_dir/name,m["file_hash"])for name,m in snap.items()}
                    if self.role=="admin":
                        self._record_admin_state("Admin full-sync folder update",snapshot=snap)
                    for target in self._full_sync_targets():
                        self._queue_tree_update(target,files,removed)
            except (OSError,ApiError)as err:
                with self.lock:
                    self._history_locked("change","Could not scan sync folder",str(err))
                    self._save_locked()

    def request_connection(self,user_id:Any)->None:
        self.submit_member_request("connection")

class DashboardHandler(SimpleHTTPRequestHandler):
    server_version="SyncDashboard/3.0"
    def __init__(self,*args:Any,directory:str|None=None,**kwargs:Any)->None:
        super().__init__(*args,directory=directory or str(DASHBOARD_DIR),**kwargs)

    @property
    def service(self)->DashboardSyncService:
        return self.server.service

    def log_message(self,format:str,*args:Any)->None:
        return

    def _json_response(self,status:int,payload:dict[str,Any])->None:
        data=json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length",str(len(data)))
        self.send_header("Cache-Control","no-store")
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self)->dict[str,Any]:
        try:
            length=int(self.headers.get("Content-Length","0"))
        except ValueError as err:
            raise ApiError("Invalid request length.")from err
        if length<=0 or length>MAX_HTTP_BODY:
            raise ApiError("Request body must be between 1 byte and 24 MiB.")
        try:
            data=json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError,json.JSONDecodeError)as err:
            raise ApiError("Request body must be valid JSON.")from err
        if not isinstance(data,dict):
            raise ApiError("Request body must be a JSON object.")
        return data

    def do_GET(self)->None:
        path=urlparse(self.path).path
        if path=="/api/state":
            self._json_response(HTTPStatus.OK,self.service.state_snapshot())
            return
        if path=="/":
            self.path="/index.html"
        super().do_GET()

    def do_POST(self)->None:
        path=urlparse(self.path).path
        try:
            body=self._read_json()
            if path=="/api/peers":
                self.service.add_peer(body.get("name"),body.get("host"),body.get("port"))
            elif path=="/api/admin-connection":
                self.service.configure_admin(body.get("host"),body.get("port"))
            elif path=="/api/board/refresh":
                self.service.refresh_board()
            elif path.startswith("/api/peers/")and path.endswith("/delete"):
                self.service.delete_peer(path.split("/")[3])
            elif path.startswith("/api/peers/")and path.endswith("/update"):
                parts=path.split("/")
                if len(parts)!=5 or not parts[3]:
                    raise ApiError("Invalid peer update route.")
                self.service.update_peer(parts[3],body.get("name"),body.get("host"),body.get("port"))
            elif path=="/api/connections":
                self.service.request_connection(body.get("user_id"))
            elif path in {"/api/folder-change","/api/sync-folder"}:
                self.service.request_folder_change(body.get("user_id"))
            elif path=="/api/full-sync/request":
                self.service.request_full_sync(body.get("user_id"))
            elif path=="/api/full-sync/start":
                self.service.start_full_sync()
            elif path=="/api/full-sync/stop":
                self.service.stop_full_sync()
            elif path=="/api/admin-sync":
                self.service.send_admin_folder(body.get("user_id"))
            elif path=="/api/admin-sync/all":
                self.service.send_admin_folder_to_all()
            elif path.startswith("/api/states/")and path.endswith("/revert"):
                self.service.revert_admin_state(path.split("/")[3],send_to_all=False)
            elif path.startswith("/api/states/")and path.endswith("/revert-all"):
                self.service.revert_admin_state(path.split("/")[3],send_to_all=True)
            elif path.startswith("/api/requests/")and path.endswith("/approve"):
                self.service.decide_request(path.split("/")[3],True)
            elif path.startswith("/api/requests/")and path.endswith("/reject"):
                self.service.decide_request(path.split("/")[3],False)
            elif path.startswith("/api/requests/")and path.endswith("/delete"):
                self.service.delete_request(path.split("/")[3])
            elif path.startswith("/api/requests/")and path.endswith("/retry"):
                self.service.retry_request(path.split("/")[3])
            else:
                self._json_response(HTTPStatus.NOT_FOUND,{"error":"Unknown local API route."})
                return
        except ApiError as err:
            self._json_response(HTTPStatus.BAD_REQUEST,{"error":str(err)})
            return
        except Exception as err:
            self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR,{"error":str(err)})
            return
        self._json_response(HTTPStatus.OK,self.service.state_snapshot())

def make_http_server(host:str,port:int,service:DashboardSyncService)->ThreadingHTTPServer:
    server=ThreadingHTTPServer((host,port),DashboardHandler)
    server.daemon_threads=True
    server.service=service
    return server

def main()->None:
    parser=argparse.ArgumentParser(description="Run the offline browser dashboard for the verified chunked LAN sync project.")
    parser.add_argument("--role",choices=("admin","member"),default="admin")
    parser.add_argument("--name",default="This device",help="Name shown to other dashboard users.")
    parser.add_argument("--peer",action="append",default=[],help="Known peer as Name=LAN_IP[:port]. Repeat for more users.")
    parser.add_argument("--web-host",default="127.0.0.1",help="Browser listener; 127.0.0.1 keeps the webpage on this device.")
    parser.add_argument("--web-port",type=int,default=8080)
    parser.add_argument("--sync-host",default="0.0.0.0",help="TCP listener; use 127.0.0.1 for an all-local demo.")
    parser.add_argument("--sync-port",type=int,default=5000)
    parser.add_argument("--sync-dir",default=str(PROJECT_DIR/"sync_folder"))
    parser.add_argument("--store",default=str(PROJECT_DIR/"node_store"/"chunk_store"))
    parser.add_argument("--state-file",default=str(DASHBOARD_DIR/"dashboard_state.json"))
    parser.add_argument(
        "--relay",
        action="store_true",
        help="Accepted for older commands. Full-sync mode always relays through the admin.",
    )
    parser.add_argument(
        "--no-auto-sync",
        action="store_false",
        dest="auto_sync",
        help="Disable automatic watcher propagation during full sync (diagnostic use only).",
    )
    parser.add_argument("--sync-interval",type=float,default=2.0,help="Folder polling interval in seconds (default: 2).")
    args=parser.parse_args()
    if not 1<=args.web_port<=65535 or not 0<=args.sync_port<=65535:
        parser.error("Ports must be between 1 and 65535 (or 0 for an automatic sync port).")
    if args.sync_interval<=0:
        parser.error("--sync-interval must be greater than zero.")
    peers=[parse_peer(value,args.sync_port or 5000)for value in args.peer]
    service=DashboardSyncService(
        role=args.role,
        device_name=args.name,
        sync_dir=Path(args.sync_dir),
        store_dir=Path(args.store),
        state_file=Path(args.state_file),
        peers=peers,
        relay=args.relay,
        auto_sync=args.auto_sync,
        sync_interval=args.sync_interval,
    )
    service.start_sync_listener(args.sync_host,args.sync_port)
    http_server=make_http_server(args.web_host,args.web_port,service)
    actual_web_port=http_server.server_address[1]
    print(f"Dashboard: http://{args.web_host}:{actual_web_port}")
    print(f"Sync listener: {args.sync_host}:{service.sync_port}")
    print("Press Ctrl+C to stop.")
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping dashboard.")
    finally:
        http_server.server_close()
        service.stop()

if __name__=="__main__":
    main()
