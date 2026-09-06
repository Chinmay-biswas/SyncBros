import copy
import queue
import socket
import threading
from chunk_store import get_chunk,has_chunk,store_chunk
from dashboard_protocol import ApiError,read_message,send_message
class TreeTransferMixin:
    def _queue_tree_update(self,user,files,removed,*,replace=False,announce=False,label="full-sync update"):
        task={"user":copy.deepcopy(user),"files":copy.deepcopy(files),"removed":list(removed),
              "replace":replace,"announce":announce,"label":label,"session":self.state["session_id"],
              "done":threading.Event(),"error":None,"chunks":0}
        with self.lock:
            jobs=self.tree_queues.get(user["id"])
            if jobs is None:
                jobs=queue.Queue()
                self.tree_queues[user["id"]]=jobs
                self._spawn(self._tree_worker,jobs)
            jobs.put(task)
        return task

    def _tree_worker(self,jobs):
        while not self.stop_event.is_set():
            try:
                task=jobs.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                if task["session"]!=self.state["session_id"]:
                    raise ApiError("The sync session changed before this transfer started.")
                if not task["replace"]and self._mode()!="full_sync":
                    raise ApiError("Full sync stopped before this transfer started.")
                if task["announce"]:
                    reply=self._send_control(task["user"],{"type":"mode_start","session_id":task["session"],
                                                          "message":"Full sync started by the administrator."})
                    if reply.get("type")!="mode_ack":
                        raise ApiError("The member did not acknowledge full sync.")
                task["chunks"]=self._send_tree_update(task)
            except Exception as err:
                task["error"]=err
                with self.lock:
                    self._history_locked("change",f"Could not sync folders to {task['user']['name']}",str(err))
                    self._save_locked()
            finally:
                task["done"].set()
                jobs.task_done()
        while True:
            try:
                task=jobs.get_nowait()
            except queue.Empty:
                break
            task["error"]=ApiError("The server stopped before this transfer started.")
            task["done"].set()
            jobs.task_done()

    def _send_tree_update(self,task):
        user=task["user"]
        files=task["files"]
        allowed={c["hash"]for m in files.values()for c in m["chunks"]}
        with socket.create_connection((user["address"],int(user["port"])),timeout=5)as conn:
            conn.settimeout(30)
            with conn.makefile("rb")as reader:
                send_message(conn,{"type":"tree_update","files":list(files.values()),"removed":task["removed"],
                    "replace":task["replace"],"session_id":task["session"],"label":task["label"],
                    "device_id":self.device_id,"sender_name":self.device_name,"peer_port":self.sync_port})
                reply=read_message(reader)
                if reply.get("type")!="tree_missing":
                    raise ApiError(reply.get("error","Update and restart SyncBros on every laptop for folder support."))
                missing=reply.get("hashes")
                if not isinstance(missing,list)or any(h not in allowed for h in missing):
                    raise ApiError("Peer requested chunks outside this folder update.")
                for digest in missing:
                    data=get_chunk(digest,self.store_dir)
                    send_message(conn,{"type":"chunk","hash":digest,"size":len(data)})
                    conn.sendall(data)
                    reply=read_message(reader)
                    if reply.get("type")!="stored":
                        raise ApiError(reply.get("error","Peer could not store a chunk."))
                send_message(conn,{"type":"tree_complete"})
                reply=read_message(reader)
                if reply.get("type")!="tree_applied":
                    raise ApiError(reply.get("error","Peer could not apply the folder update."))
        return len(missing)

    def _merge_tree_update(self,current,files,removed,replace):
        desired={}if replace else dict(current)
        for name in removed:
            desired={n:m for n,m in desired.items()if n!=name and not n.startswith(name+"/")}
        for name,meta in files.items():
            if meta.get("kind")!="directory":
                desired={n:m for n,m in desired.items()if not n.startswith(name+"/")}
            desired[name]=meta
        return self._validate_snapshot_manifests(list(desired.values()))

    def _check_tree_mode(self,req):
        replace=req.get("replace",False)
        if not isinstance(replace,bool):
            raise ApiError("Invalid folder replacement flag.")
        if replace:
            if self.role=="admin":
                raise ApiError("Members must request approval before replacing the admin folder.")
            if self._mode()=="full_sync" and req.get("session_id")!=self.state["session_id"]:
                raise ApiError("This baseline belongs to an old sync session.")
        elif (self._mode()!="full_sync" or self.full_sync_initializing
              or req.get("session_id")!=self.state["session_id"]):
            raise ApiError("Wait for the current full-sync baseline before sending folder updates.")
        return replace

    def _receive_tree_update(self,conn,reader,address,req):
        replace=self._check_tree_mode(req)
        files=self._validate_snapshot_manifests(req.get("files"))
        removed=req.get("removed",[])
        if not isinstance(removed,list)or len(removed)>2000:
            raise ApiError("Invalid removed-path list.")
        for name in list(files)+removed:
            self._safe_destination(name)
        missing={c["hash"]for m in files.values()for c in m["chunks"]if not has_chunk(c["hash"],self.store_dir)}
        send_message(conn,{"type":"tree_missing","hashes":sorted(missing)})
        while True:
            chunk=read_message(reader)
            if chunk.get("type")=="tree_complete":
                if missing:
                    raise ApiError("The folder update is missing chunks.")
                break
            size,digest=chunk.get("size"),chunk.get("hash")
            if chunk.get("type")!="chunk" or digest not in missing or not isinstance(size,int)or not 0<=size<=4*1024*1024:
                raise ApiError("Invalid folder chunk.")
            data=reader.read(size)
            if len(data)!=size or store_chunk(data,self.store_dir)!=digest:
                raise ApiError("Folder chunk verification failed.")
            missing.remove(digest)
            send_message(conn,{"type":"stored"})
        with self.file_lock:
            self._check_tree_mode(req)
            current=self._capture_folder_manifests()
            desired=self._merge_tree_update(current,files,removed,replace)
            self._apply_snapshot(desired,remote=True,prefix="delta-")
            if self.role=="admin":
                self._record_admin_state(f"Full-sync folder update from {req.get('sender_name',address[0])}",snapshot=desired)
                for peer in self._full_sync_targets():
                    if peer.get("device_id")!=req.get("device_id"):
                        self._queue_tree_update(peer,files,removed)
            elif replace:
                self.full_sync_initializing=False
                self.state["session_id"]=req.get("session_id",self.state["session_id"])
            with self.lock:
                self._history_locked("change","Admin folder snapshot applied" if replace else "Folder update applied",
                    f"{len(files)} file/folder entries received; {len(removed)} paths removed.")
                self._save_locked()
        send_message(conn,{"type":"tree_applied"})
