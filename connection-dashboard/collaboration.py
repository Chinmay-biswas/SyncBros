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
        
