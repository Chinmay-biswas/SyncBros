from __future__ import annotations
import copy
import socket
import threading
import time
import uuid

from chunk_store import get_chunk, has_chunk, store_chunk
from dashboard_protocol import ApiError, now_label, read_message, send_message


ACTIVE_REQUESTS = {"sending","uploading","pending","applying","cancle_pending"}
TERMINAL_REQUESTS = {"completed","rejected","failed","canceled","expired"}


MAX_MEMBERS=2


class CollaborationMinxin:
    def _int_collaboration(self,perrs):
        self.board_lock=threading.Lock()
        self.board_wake=threading.Event()
        self.board_thread=None
        self.board_status={"online": self.role=="admin", "error": "", "last_checked":None}
        self.last_seen= {}
        self.baseline_transfers=set()
        self.workers=set()
        self.device_id=self.state.setdefault("device_id", uuid.uuid4().hex)
        self.state.setdefault("outgoing_requests",[])
        self.state.setdefault("request_tombstones",[])
        self.state.setdefault("roster",[])
        self.state.setdefault("admin_endpoints",None)
        self.state.setdefault("member_ids",[])
        self.state.setdefault("session_id","")
        self.state.setdefault("advertised_host","")

        unique=[]


        for user in self.state["users"]:
            if any((u["address"],u["port"])== (user["address"],user["port"])
                   or u["name"].casefold()==user["name"].casefold() for u in unique):
                continue
            user.setdefault("role","member" if self.role=="admin" else "admin")
            unique.append(user)
        self.state["users"]=unique
        if self.role=="admin":
            if not self.state.get("directory_initialized"):
                for name,host,port in perrs:
                    if not any(u["name"].casefold()==name.casefold()
                               or (u["address"],u["port"])==(host,port) for u in unique):
                        self._upsert_peer(name,host,port, state="available", save=False)
                    self.state["directory_initialilzed"]=True

