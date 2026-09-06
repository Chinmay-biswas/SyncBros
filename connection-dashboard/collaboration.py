from __future__ import annotations
import copy
import socket
import threading
import time
import uuid
from chunk_store import get_chunk,has_chunk,store_chunk
from dashboard_protocol import ApiError,now_label,read_message,send_message
ACTIVE_REQUESTS={"sending","uploading","pending","applying","cancel_pending"}
TERMINAL_REQUESTS={"completed","rejected","failed","expired","cancelled"}
MAX_MEMBERS=2
class CollaborationMixin:
    def _init_collaboration(self,peers):
        self.board_lock=threading.Lock()
        self.board_wake=threading.Event()
        self.board_thread=None
        self.board_status={"online":self.role=="admin","error":"","last_checked":None}
        self.last_seen={}
        self.baseline_transfers=set()
        self.workers=set()
        self.device_id=self.state.setdefault("device_id",uuid.uuid4().hex)
        self.state.setdefault("outgoing_requests",[])
        self.state.setdefault("request_tombstones",{})
        self.state.setdefault("roster",[])
        self.state.setdefault("admin_endpoint",None)
        self.state.setdefault("member_id",None)
        self.state.setdefault("session_id","")
        self.state.setdefault("advertised_host","")
        unique=[]
        for user in self.state["users"]:
            if any((u["address"],u["port"])==(user["address"],user["port"])
                   or u["name"].casefold()==user["name"].casefold()for u in unique):
                continue
            user.setdefault("role","member" if self.role=="admin" else "admin")
            unique.append(user)
        self.state["users"]=unique
        if self.role=="admin":
            if not self.state.get("directory_initialized"):
                for name,host,port in peers:
                    if not any(u["name"].casefold()==name.casefold()
                               or (u["address"],u["port"])==(host,port)for u in unique):
                        self._upsert_peer(name,host,port,state="available",save=False)
                self.state["directory_initialized"]=True
        else:
            if not self.state["admin_endpoint"]:
                target=peers[0]if peers else None
                if target:
                    name,host,port=target
                    self.state["admin_endpoint"]={"name":name,"address":host,"port":port}
                elif unique:
                    self.state["admin_endpoint"]={key:unique[0][key]for key in ("name","address","port")}
            self.state["users"]=[]
            if self.state["mode"]=="full_sync":
                self.full_sync_initializing=True
        for rec in self.state["requests"]:
            if rec.get("protocol")!=3 and rec.get("status")in {"pending","approved"}:
                rec.update(status="expired",detail="Old connection ended. The member can send a new request.")
            elif rec.get("status")in {"uploading","applying"}:
                rec.update(status="failed",detail="Server restarted during this operation; retry the request.")
        for rec in self.state["outgoing_requests"]:
            if rec.get("status")=="sending":
                rec.update(status="failed",detail="Dashboard restarted before receipt was confirmed. Retry to check delivery.")

    def _spawn(self,target,*args):
        def run():
            try:
                target(*args)
            finally:
                with self.lock:
                    self.workers.discard(threading.current_thread())

        worker=threading.Thread(target=run,daemon=True)
        with self.lock:
            self.workers.add(worker)
        worker.start()

    def _require_admin(self):
        if self.role!="admin":
            raise ApiError("Only the administrator can change the shared member list.")

    def _roster_locked(self):
        if self.role=="admin":
            return [{"id":self.device_id,"device_id":self.device_id,
                     "name":self.device_name,"role":"admin","state":"connected",
                     "address":self.state["advertised_host"]or self.sync_host,
                     "port":self.sync_port}]+[dict(u,role="member")for u in self.state["users"]]
        if self.state["roster"]:
            return copy.deepcopy(self.state["roster"])
        addr=self.state.get("admin_endpoint")
        return [dict(addr,id="admin-setup",role="admin",state="available")]if addr else []

    def _admin_target(self,*,require_connected=False):
        if self.role!="member":
            raise ApiError("This action sends a member request to the administrator.")
        with self.lock:
            addr=self.state.get("admin_endpoint")
            if not addr:
                raise ApiError("Set the administrator's LAN IP using Admin connection first.")
            roster=self._roster_locked()
            admin=next((u for u in roster if u.get("role")=="admin"),None)
            member=next((u for u in roster if u.get("device_id")==self.device_id),None)
            if require_connected and (not member or member.get("state")!="connected"):
                raise ApiError("Ask the administrator to approve your connection before sending changes.")
            return dict(addr,id=admin["id"]if admin else "admin-setup",role="admin",
                        state="connected" if member and member.get("state")=="connected" else "available")

    def configure_admin(self,host,port):
        if self.role!="member":
            raise ApiError("The admin manages members in the shared list.")
        if not isinstance(host,str)or not host.strip():
            raise ApiError("Enter the administrator's LAN IP.")
        try:
            port=int(port or 5000)
        except (ValueError,TypeError)as err:
            raise ApiError("Port must be a number.")from err
        if not 1<=port<=65535:
            raise ApiError("Port must be between 1 and 65535.")
        with self.board_lock,self.lock:
            old=self.state.get("admin_endpoint")or {}
            self.state["admin_endpoint"]={"name":"Admin Hub","address":host.strip(),"port":port}
            if (old.get("address"),old.get("port"))!=(host.strip(),port):
                self.state["roster"]=[]
                self.state["users"]=[]
                self.state["member_id"]=None
            self.board_status.update(online=False,error="Connecting to the administrator…")
            self._save_locked()
        self.board_wake.set()

    def delete_peer(self,user_id):
        self._require_admin()
        with self.lock:
            peer=self._find_user_locked(user_id)
            if any(r.get("owner_id")==peer.get("device_id")and r.get("status")=="applying"
                   for r in self.state["requests"]):
                raise ApiError("Wait for this member's approved change to finish before removing them.")
            for rec in list(self.state["requests"]):
                if rec.get("owner_id")and rec.get("owner_id")==peer.get("device_id")and rec.get("status")in ACTIVE_REQUESTS:
                    self._delete_shared_request(rec["id"])
            self.state["users"].remove(peer)
            self.state["sent_hashes"].pop(user_id,None)
            self._history_locked("connection",f"Removed {peer['name']}","Removed from the shared team. Files and saved history are retained.")
            self._save_locked()

    def _member_for_packet(self,request,address,*,bind=False):
        owner=request.get("device_id")
        port=request.get("peer_port")
        if not isinstance(owner,str)or len(owner)!=32 or any(c not in "0123456789abcdef" for c in owner):
            raise ApiError("Update the dashboard on this member laptop; its device identity is missing.")
        if not isinstance(port,int)or not 1<=port<=65535:
            raise ApiError("Invalid member listener port.")
        member=next((u for u in self.state["users"]if u.get("device_id")==owner),None)
        if not member:
            member=next((u for u in self.state["users"]if not u.get("device_id")
                           and (u["address"],u["port"])==(address[0],port)),None)
            if member and bind:
                member["device_id"]=owner
        if member and bind:
            if (member["address"],member["port"])!=(address[0],port):
                member.update(address=address[0],port=port)
                self.state["sent_hashes"].pop(member["id"],None)
            self.last_seen[member["id"]]=time.monotonic()
        return member

    def _public_request(self,record):
        res={k:copy.deepcopy(v)for k,v in record.items()if k not in {"snapshot","protocol","target"}}
        member=next((u for u in self._roster_locked()if u.get("device_id")==record.get("owner_id")),None)
        if member:
            res["user"]=member["name"]
            labels={"connection":"Connection request","folder_change":"Folder change request","full_sync":"Full sync request"}
            if record.get("kind")in labels:
                res["title"]=f"{labels[record['kind']]} from {member['name']}"
        return res

    def _board_reply(self,request,address,local_host):
        if self.role!="admin":
            raise ApiError("This IP belongs to a member dashboard. Enter the administrator laptop's LAN IP in Admin connection.")
        with self.lock:
            member=self._member_for_packet(request,address,bind=True)
            self.state["advertised_host"]=local_host
            self._save_locked()
            reply={"type":"board_state","protocol":3,"roster":self._roster_locked(),
                     "mode":self.state["mode"],"session_id":self.state["session_id"],
                     "member_id":member["id"]if member else None,
                     "requests":[self._public_request(r)for r in self.state["requests"]if r.get("owner_id")==request["device_id"]],
                     "deleted_requests":[key for key,owner in self.state["request_tombstones"].items()if owner==request["device_id"]]}
            if member and member.get("state")=="connected" and self._mode()=="full_sync":
                if request.get("session_id")!=self.state["session_id"]or not request.get("baseline_ready"):
                    self._schedule_baseline(copy.deepcopy(member))
            return reply

    def _schedule_baseline(self,user):
        with self.lock:
            if user["id"]in self.baseline_transfers:
                return
            self.baseline_transfers.add(user["id"])
        def push():
            try:
                self._push_admin_snapshot_worker(user,"full-sync baseline",True)
            finally:
                with self.lock:
                    self.baseline_transfers.discard(user["id"])

        self._spawn(push)

    def _exchange(self,target,packet,timeout=5):
        with socket.create_connection((target["address"],target["port"]),timeout=timeout)as conn:
            conn.settimeout(timeout)
            with conn.makefile("rb")as reader:
                send_message(conn,dict(packet,device_id=self.device_id,sender_name=self.device_name,peer_port=self.sync_port))
                reply=read_message(reader)
        if reply.get("type")=="error":
            raise ApiError(reply.get("error","The administrator could not accept this request."))
        return reply

    def _validate_transfer_sender(self,packet,address):
        if self.role=="admin":
            if not packet.get("device_id"):
                if self._mode()=="full_sync":
                    raise ApiError("Full sync requires a connected dashboard member.")
                return
            with self.lock:
                member=self._member_for_packet(packet,address)
                if not member or member.get("state")!="connected":
                    raise ApiError("This device is not a connected member of the admin's team.")
        else:
            target=self._admin_target(require_connected=True)
            admin=next((u for u in self.state["roster"]if u.get("role")=="admin"),{})
            if packet.get("device_id")!=admin.get("device_id")or address[0]!=socket.gethostbyname(target["address"]):
                raise ApiError("Only the configured administrator can update this member folder.")

    def refresh_board(self):
        if self.role=="admin":
            return
        with self.board_lock:
            target=self._admin_target()
            try:
                reply=self._exchange(target,{"type":"board_state","session_id":self.state["session_id"],
                                               "baseline_ready":not self.full_sync_initializing})
                if reply.get("type")!="board_state" or reply.get("protocol")!=3:
                    raise ApiError("Update and restart connection-dashboard on the admin laptop too.")
                with self.lock:
                    self.state["roster"]=reply["roster"]
                    self.state["member_id"]=reply.get("member_id")
                    member=next((u for u in reply["roster"]if u["id"]==reply.get("member_id")),None)
                    admin=next(u for u in reply["roster"]if u["role"]=="admin")
                    self.state["admin_endpoint"]["name"]=admin["name"]
                    if member:
                        self.device_name=member["name"]
                        self.state["device_name"]=self.device_name
                    self.state["users"]=[dict(admin,state=member["state"]if member else "available")]
                    active=bool(member and member.get("state")=="connected")
                    mode=reply["mode"]if active else "approval"
                    if mode!=self.state["mode"]:
                        self._history_locked("system","Full sync started" if mode=="full_sync" else "Full sync stopped",
                                             "Mode refreshed from the administrator.")
                    if mode=="approval":
                        self.full_sync_initializing=False
                    elif self.state["mode"]!=mode or self.state["session_id"]!=reply["session_id"]:
                        self.full_sync_initializing=True
                    self.state["mode"]=mode
                    self.state["session_id"]=reply["session_id"]
                    by_id={r["id"]:r for r in reply["requests"]}
                    deleted=set(reply["deleted_requests"])
                    for rec in self.state["outgoing_requests"]:
                        remote=by_id.get(rec["id"])
                        if rec["id"]in deleted:
                            rec["status"]="deleted"
                        elif remote and rec.get("status")!="cancel_pending":
                            self._set_outgoing_result(rec,remote)
                    self.board_status={"online":True,"error":"","last_checked":now_label()}
                    self._save_locked()
            except Exception as err:
                with self.lock:
                    self.board_status.update(online=False,last_checked=now_label(),
                        error=f"Cannot refresh admin at {target['address']}:{target['port']}: {err}")
                raise

    def _board_loop(self):
        while not self.stop_event.is_set():
            try:
                self.refresh_board()
                with self.lock:
                    cancellations=[r["id"]for r in self.state["outgoing_requests"]if r.get("status")=="cancel_pending"]
                for req_id in cancellations:
                    self._cancel_outgoing(req_id)
            except Exception:
                pass
            self.board_wake.wait(2)
            self.board_wake.clear()

    def _set_outgoing_result(self,record,remote):
        prev=record.get("status")
        for key in ("status","title","user","detail","submitted_at"):
            if key in remote:
                record[key]=remote[key]
        status=record.get("status")
        if status in TERMINAL_REQUESTS and status!=prev and record.get("notified_status")!=status:
            label={"folder_change":"Folder change","full_sync":"Full sync","connection":"Connection"}.get(record["kind"],"Request")
            outcome="approved" if status=="completed" else status
            self._history_locked("connection" if record["kind"]=="connection" else "change",
                                 f"{label} {outcome}",record.get("detail",""))
            record["notified_status"]=status

    def submit_member_request(self,kind):
        try:
            self.refresh_board()
        except Exception as err:
            raise ApiError(str(err))from err
        target=self._admin_target(require_connected=kind!="connection")
        if kind!="connection" and self._mode()=="full_sync":
            raise ApiError("Full sync is active. Folder changes propagate automatically.")
        with self.lock:
            if any(r.get("kind")==kind and r.get("status")in ACTIVE_REQUESTS for r in self.state["outgoing_requests"]):
                raise ApiError("This request is already active. Check Change requests or cancel it there.")
            rec={"id":uuid.uuid4().hex,"kind":kind,"owner_id":self.device_id,
                      "user":self.device_name,"title":f"{kind.replace('_',' ').capitalize()} request from {self.device_name}",
                      "status":"sending","submitted_at":now_label(),"detail":"Sending to the administrator…","target":target}
            self.state["outgoing_requests"].insert(0,rec)
            self._save_locked()
        self._spawn(self._submit_worker,rec["id"])

    def _submit_worker(self,request_id):
        request_key=("request",request_id)
        with self.lock:
            rec=next(r for r in self.state["outgoing_requests"]if r["id"]==request_id)
            self.inflight_requests.add(request_key)
        try:
            target=self._admin_target(require_connected=rec["kind"]!="connection")
            if rec["kind"]=="folder_change" and "snapshot" not in rec:
                snap=self._capture_folder_manifests()
                with self.lock:
                    rec["snapshot"]=snap
                    self._save_locked()
            snap=rec.get("snapshot",{})
            with socket.create_connection((target["address"],target["port"]),timeout=5)as conn:
                conn.settimeout(30)
                with conn.makefile("rb")as reader:
                    with self.lock:
                        if rec["status"]in {"cancel_pending","deleted"}:
                            return
                    send_message(conn,{"type":"submit_request","request_id":request_id,"kind":rec["kind"],
                        "device_id":self.device_id,"sender_name":self.device_name,"peer_port":self.sync_port,
                        "files":list(snap.values())})
                    reply=read_message(reader)
                    if reply.get("type")=="request_upload":
                        allowed={c["hash"]for m in snap.values()for c in m["chunks"]}
                        for digest in reply["hashes"]:
                            if digest not in allowed:
                                raise ApiError("Administrator requested a chunk outside this folder state.")
                            with self.lock:
                                if rec["status"]in {"cancel_pending","deleted"}:
                                    return
                            data=get_chunk(digest,self.store_dir)
                            send_message(conn,{"type":"chunk","hash":digest,"size":len(data)})
                            conn.sendall(data)
                            if read_message(reader).get("type")!="stored":
                                raise ApiError("Administrator could not store the requested chunk.")
                        send_message(conn,{"type":"request_upload_complete"})
                        reply=read_message(reader)
                    if reply.get("type")!="request_received":
                        raise ApiError(reply.get("error","No receipt from admin. Update the dashboard on both laptops."))
            with self.lock:
                if rec["status"]not in {"cancel_pending","deleted"}:
                    self._set_outgoing_result(rec,reply["request"])
                    self._save_locked()
            self.board_wake.set()
        except Exception as err:
            with self.lock:
                if rec["status"]not in {"cancel_pending","deleted"}:
                    addr=self.state.get("admin_endpoint")or {}
                    rec.update(status="failed",detail=f"No confirmed receipt from {addr.get('address')}:{addr.get('port')}: {err}. Retry checks the same request ID.")
                    self._save_locked()
        finally:
            with self.lock:
                self.inflight_requests.discard(request_key)

    def _receive_submission(self,connection,reader,address,packet):
        self._require_admin()
        kind,req_id=packet.get("kind"),packet.get("request_id")
        if kind not in {"connection","folder_change","full_sync"}or not isinstance(req_id,str)or len(req_id)!=32:
            raise ApiError("Invalid approval request.")
        with self.lock:
            member=self._member_for_packet(packet,address,bind=True)
            owner=packet["device_id"]
            if req_id in self.state["request_tombstones"]:
                raise ApiError("This request has been cancelled or deleted.")
            old=next((r for r in self.state["requests"]if r["id"]==req_id),None)
            if old and old.get("owner_id")!=owner:
                raise ApiError("This request belongs to another member.")
            if old and old["status"]not in {"failed"}:
                if old["status"]=="uploading":
                    raise ApiError("This request is still uploading. Retry after the earlier upload ends.")
                send_message(connection,{"type":"request_received","request":self._public_request(old)})
                return
            if kind!="connection" and (not member or member.get("state")!="connected"):
                raise ApiError("The administrator must approve this member's connection first.")
            if kind!="connection" and self._mode()=="full_sync":
                raise ApiError("Full sync is active on the administrator. Refresh your dashboard.")
            if any(r.get("owner_id")==owner and r.get("kind")==kind and r.get("status")in ACTIVE_REQUESTS for r in self.state["requests"]):
                raise ApiError("An earlier request is still active. Cancel it or wait for a decision.")
            snap=self._validate_snapshot_manifests(packet.get("files",[]))if kind=="folder_change" else {}
            name=member["name"]if member else str(packet.get("sender_name")or address[0]).strip()[:80]
            labels={"connection":"Connection request","folder_change":"Folder change request","full_sync":"Full sync request"}
            rec={"id":req_id,"protocol":3,"kind":kind,"owner_id":owner,
                "title":f"{labels[kind]} from {name}","user":name,"peer_host":address[0],"peer_port":packet["peer_port"],
                "status":"uploading" if kind=="folder_change" else "pending","submitted_at":now_label(),
                "detail":"Receiving changed chunks…" if kind=="folder_change" else "Awaiting administrator approval.",
                "snapshot":snap,"file_count":sum(m.get("kind")!="directory" for m in snap.values()),
                "folder_count":sum(m.get("kind")=="directory" for m in snap.values())}
            if old:
                self.state["requests"].remove(old)
            self.state["requests"].insert(0,rec)
            self._save_locked()
        try:
            if kind=="folder_change":
                missing={c["hash"]for m in snap.values()for c in m["chunks"]if not has_chunk(c["hash"],self.store_dir)}
                send_message(connection,{"type":"request_upload","hashes":sorted(missing)})
                while True:
                    chunk=read_message(reader)
                    with self.lock:
                        if req_id in self.state["request_tombstones"]:
                            raise ApiError("Request was cancelled during upload.")
                    if chunk.get("type")=="request_upload_complete":
                        if missing:
                            raise ApiError("The folder request has missing chunks.")
                        break
                    size,digest=chunk.get("size"),chunk.get("hash")
                    if chunk.get("type")!="chunk" or digest not in missing or not isinstance(size,int)or not 0<=size<=4*1024*1024:
                        raise ApiError("Invalid request chunk.")
                    data=reader.read(size)
                    if len(data)!=size or store_chunk(data,self.store_dir)!=digest:
                        raise ApiError("Request chunk verification failed.")
                    missing.remove(digest)
                    send_message(connection,{"type":"stored"})
                with self.lock:
                    if req_id in self.state["request_tombstones"]:
                        raise ApiError("Request was cancelled during upload.")
                    rec.update(status="pending",detail=f"One complete folder state: {rec['file_count']} file(s), {rec['folder_count']} folder(s). Awaiting administrator approval.")
                    self._save_locked()
            send_message(connection,{"type":"request_received","request":self._public_request(rec)})
        except Exception as err:
            with self.lock:
                if rec["status"]=="uploading":
                    rec.update(status="failed",detail=f"Upload interrupted: {err}. The member can retry.")
                    self._save_locked()
            raise

    def _decide_durable_request(self,request_id,approved):
        self._require_admin()
        with self.lock:
            rec=next(r for r in self.state["requests"]if r["id"]==request_id)
            if rec["status"]!="pending":
                raise ApiError("This request is no longer awaiting approval.")
            if not approved:
                rec["detail"]="Administrator rejected this request. The admin folder is unchanged."
                self._record_request_outcome_locked(rec,"rejected",rec["detail"])
                return
            if rec["kind"]!="connection" and self._mode()=="full_sync":
                raise ApiError("Stop full sync before approving an approval-mode request.")
            rec["status"]="applying"
            self._save_locked()
        self._spawn(self._apply_durable_request,request_id)

    def _apply_durable_request(self,request_id):
        with self.lock:
            rec=next(r for r in self.state["requests"]if r["id"]==request_id)
        try:
            if rec["kind"]=="connection":
                with self.lock:
                    peer=next((u for u in self.state["users"]if u.get("device_id")==rec["owner_id"]
                                 or (not u.get("device_id")and (u["address"],u["port"])==(rec["peer_host"],rec["peer_port"]))),None)
                    if peer:
                        peer.update(device_id=rec["owner_id"],state="connected",address=rec["peer_host"],port=rec["peer_port"])
                    else:
                        if len(self.state["users"])>=MAX_MEMBERS:
                            raise ApiError("The team has two members already. Delete an obsolete member and ask this member to retry.")
                        if any(u["name"].casefold()==rec["user"].casefold()for u in self.state["users"]):
                            raise ApiError("Another device has this name. Edit its LAN IP or rename that member first.")
                        peer={"id":uuid.uuid4().hex,"device_id":rec["owner_id"],"role":"member","name":rec["user"],
                                "address":rec["peer_host"],"port":rec["peer_port"],"state":"connected"}
                        self.state["users"].append(peer)
                detail=f"Connected {peer['name']}. The shared team is now available on this member dashboard."
            elif rec["kind"]=="folder_change":
                with self.file_lock:
                    self._record_admin_state("Admin folder before approved member request")
                    changed,removed=self._apply_snapshot(rec["snapshot"],remote=True,prefix="approved-")
                    self._record_admin_state(f"Approved folder state from {rec['user']}",snapshot=rec["snapshot"])
                detail=f"Applied one folder state: {changed} updated, {removed} removed."
            else:
                count=self.start_full_sync(initiated_by=rec["user"])
                detail=f"Full sync started for {count} member(s)."
            with self.lock:
                rec["detail"]=detail
                self._record_request_outcome_locked(rec,"completed",detail)
        except Exception as err:
            with self.lock:
                rec["detail"]=str(err)
                self._record_request_outcome_locked(rec,"failed",str(err))

    def _delete_shared_request(self,request_id,owner_id=None):
        with self.lock:
            rec=next((r for r in self.state["requests"]if r["id"]==request_id),None)
            if rec is None:
                if owner_id:
                    prior=self.state["request_tombstones"].get(request_id)
                    if prior and prior!=owner_id:
                        raise ApiError("This request belongs to another member.")
                    self.state["request_tombstones"][request_id]=owner_id
                    self._save_locked()
                return
            if owner_id and rec.get("owner_id")!=owner_id:
                raise ApiError("Members may cancel or delete only their own requests.")
            if rec["status"]in {"applying","approved"}:
                raise ApiError("The approved operation is in progress. Wait for it to finish before deleting its entry.")
            decision=self.pending.pop(request_id,None)
            if decision:
                decision.approved=False
                decision.event.set()
            cancelled=rec["status"]in ACTIVE_REQUESTS
            self.state["request_tombstones"][request_id]=rec.get("owner_id","")
            self.state["requests"].remove(rec)
            self._history_locked("change",f"{rec['title']} {'cancelled' if cancelled else 'deleted'}",
                                 "Request removed. Saved history and folder files are retained.")
            self._save_locked()

    def delete_request(self,request_id):
        if self.role=="admin":
            self._delete_shared_request(request_id)
            return
        with self.lock:
            rec=next((r for r in self.state["outgoing_requests"]if r["id"]==request_id),None)
            if not rec:
                raise ApiError("This request is no longer available.")
            if rec["status"]=="applying":
                raise ApiError("Wait for the approved operation to finish.")
            rec["status"]="cancel_pending"
            rec["detail"]="Cancelling at the administrator. Confirmation will appear when connected."
            self._save_locked()
        self.board_wake.set()

    def _cancel_outgoing(self,request_id):
        target=self._admin_target()
        reply=self._exchange(target,{"type":"delete_request","request_id":request_id})
        if reply.get("type")!="request_deleted":
            raise ApiError("Administrator has not confirmed cancellation.")
        with self.lock:
            rec=next((r for r in self.state["outgoing_requests"]if r["id"]==request_id),None)
            if rec:
                rec["status"]="deleted"
                self._save_locked()

    def retry_request(self,request_id):
        if self.role!="member":
            raise ApiError("Only the sender retries a request.")
        with self.lock:
            rec=next((r for r in self.state["outgoing_requests"]if r["id"]==request_id),None)
            if not rec or rec["status"]!="failed":
                raise ApiError("Only a failed request can be retried.")
            rec.update(status="sending",detail="Retrying receipt with the administrator…")
            self._save_locked()
        self._spawn(self._submit_worker,request_id)
