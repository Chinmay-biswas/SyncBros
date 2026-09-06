from __future__ import annotations
import json
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
for path in (str(HERE),str(ROOT)):
    if path not in sys.path:
        sys.path.insert(0,path)
from sender import send_file
from server import DashboardSyncService,make_http_server
def wait_for(predicate,timeout:float=12.0):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        value=predicate()
        if value:
            return value
        time.sleep(0.03)
    raise AssertionError("Timed out waiting for dashboard state")

def post_json(url:str,payload:dict):
    req=urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type":"application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req,timeout=4)as resp:
        return json.loads(resp.read())

def file_map(directory:Path)->dict[str,bytes]:
    if not directory.exists():
        return {}
    return {
        path.name:path.read_bytes()
        for path in directory.iterdir()
        if path.is_file()and not path.name.endswith(".part")and ".syncing-" not in path.name
    }

def pending_request(service:DashboardSyncService,kind:str):
    return next(
        (
            req
            for req in service.state_snapshot()["requests"]
            if req.get("kind")==kind and req.get("status")=="pending"
        ),
        None,
    )

def user_by_name(service:DashboardSyncService,name:str)->dict:
    return next(user for user in service.state_snapshot()["users"]if user["name"]==name)

def main()->None:
    with tempfile.TemporaryDirectory()as tmp:
        root=Path(tmp)
        admin_dir=root/"admin-files"
        b_dir=root/"member-b-files"
        c_dir=root/"member-c-files"
        admin_dir.mkdir(parents=True)
        b_dir.mkdir(parents=True)
        c_dir.mkdir(parents=True)
        (admin_dir/"admin-keep.txt").write_text("admin initial state",encoding="utf-8")
        (b_dir/"member-state.txt").write_text("member B requested state",encoding="utf-8")
        (c_dir/"stale-member-c.txt").write_text("replace me later",encoding="utf-8")
        admin=DashboardSyncService(
            role="admin",
            device_name="Admin Hub",
            sync_dir=admin_dir,
            store_dir=root/"admin-store",
            state_file=root/"admin-state.json",
            peers=[],
            approval_timeout=5,
            auto_sync=True,
            sync_interval=0.05,
        )
        admin.start_sync_listener("127.0.0.1",0)
        b=DashboardSyncService(
            role="member",
            device_name="Member B",
            sync_dir=b_dir,
            store_dir=root/"member-b-store",
            state_file=root/"member-b-state.json",
            peers=[("Admin Hub","127.0.0.1",admin.sync_port)],
            approval_timeout=5,
            auto_sync=True,
            sync_interval=0.05,
        )
        b.start_sync_listener("127.0.0.1",0)
        c=DashboardSyncService(
            role="member",
            device_name="Member C",
            sync_dir=c_dir,
            store_dir=root/"member-c-store",
            state_file=root/"member-c-state.json",
            peers=[("Admin Hub","127.0.0.1",admin.sync_port)],
            approval_timeout=5,
            auto_sync=True,
            sync_interval=0.05,
        )
        c.start_sync_listener("127.0.0.1",0)
        web=make_http_server("127.0.0.1",0,admin)
        web_worker=threading.Thread(target=web.serve_forever,daemon=True)
        web_worker.start()
        try:
            web_url=f"http://127.0.0.1:{web.server_address[1]}"
            with urllib.request.urlopen(web_url,timeout=3)as resp:
                assert b"Work together, safely." in resp.read()
            with urllib.request.urlopen(f"{web_url}/api/state",timeout=3)as resp:
                public_state=json.loads(resp.read())
                assert public_state["role"]=="admin"
                assert public_state["mode"]=="approval"
                assert public_state["folder_states"]
                assert "changes" not in public_state["folder_states"][0]
            added=post_json(f"{web_url}/api/peers",{"name":"Temporary","host":"127.0.0.1","port":5099})
            temporary_peer=next(user for user in added["users"]if user["name"]=="Temporary")
            updated=post_json(
                f"{web_url}/api/peers/{temporary_peer['id']}/update",
                {"name":"Temporary","host":"127.0.0.2","port":5098},
            )
            edited_peer=next(user for user in updated["users"]if user["id"]==temporary_peer["id"])
            assert edited_peer["address"]=="127.0.0.2"
            assert edited_peer["port"]==5098
            assert edited_peer["state"]=="available"
            post_json(f"{web_url}/api/peers/{temporary_peer['id']}/delete",{})
            for member in (b,c):
                member.request_connection(None)
                req=wait_for(lambda:pending_request(admin,"connection"))
                admin.decide_request(req["id"],True)
                wait_for(lambda:member.state_snapshot()["membership_state"]=="connected")
            assert user_by_name(admin,"Member B")["state"]=="connected"
            assert user_by_name(admin,"Member C")["state"]=="connected"
            legacy_file=b_dir/"legacy-sender.txt"
            legacy_file.write_text("legacy sender compatibility",encoding="utf-8")
            sender_result:dict[str,object]={}
            def send_legacy()->None:
                try:
                    sender_result["result"]=send_file("127.0.0.1",admin.sync_port,legacy_file,root/"member-b-store")
                except Exception as err:
                    sender_result["error"]=err

            legacy_thread=threading.Thread(target=send_legacy)
            legacy_thread.start()
            req=wait_for(lambda:pending_request(admin,"change"))
            admin.decide_request(req["id"],True)
            legacy_thread.join(timeout=5)
            assert not legacy_thread.is_alive()
            assert "error" not in sender_result,sender_result.get("error")
            assert (admin_dir/"legacy-sender.txt").read_text(encoding="utf-8")=="legacy sender compatibility"
            b.request_folder_change(user_by_name(b,"Admin Hub")["id"])
            req=wait_for(lambda:pending_request(admin,"folder_change"))
            assert req["file_count"]==len(file_map(b_dir))
            assert len([item for item in admin.state_snapshot()["requests"]if item.get("kind")=="folder_change" and item.get("status")=="pending"])==1
            admin.decide_request(req["id"],True)
            wait_for(lambda:file_map(admin_dir)==file_map(b_dir))
            wait_for(lambda:not b.inflight_requests)
            assert "admin-keep.txt" not in file_map(admin_dir)
            (b_dir/"rejected-by-admin.txt").write_text("do not copy",encoding="utf-8")
            before_rejection=file_map(admin_dir)
            b.request_folder_change(user_by_name(b,"Admin Hub")["id"])
            req=wait_for(lambda:pending_request(admin,"folder_change"))
            admin.decide_request(req["id"],False)
            wait_for(lambda:any(event["title"]=="Folder change rejected" for event in b.state_snapshot()["history"]))
            assert file_map(admin_dir)==before_rejection
            assert (b_dir/"rejected-by-admin.txt").is_file()
            admin.send_admin_folder(user_by_name(admin,"Member C")["id"])
            wait_for(lambda:file_map(c_dir)==file_map(admin_dir))
            b.request_full_sync(user_by_name(b,"Admin Hub")["id"])
            req=wait_for(lambda:pending_request(admin,"full_sync"))
            admin.decide_request(req["id"],True)
            wait_for(lambda:admin.state_snapshot()["mode"]=="full_sync")
            wait_for(lambda:b.state_snapshot()["mode"]=="full_sync" and c.state_snapshot()["mode"]=="full_sync")
            wait_for(lambda:not b.state_snapshot()["sync"]["initializing"]and not c.state_snapshot()["sync"]["initializing"])
            wait_for(lambda:file_map(b_dir)==file_map(admin_dir)==file_map(c_dir))
            assert not (b_dir/"rejected-by-admin.txt").exists()
            admin.stop_full_sync()
            wait_for(lambda:b.state_snapshot()["mode"]=="approval" and c.state_snapshot()["mode"]=="approval")
            post_json(f"{web_url}/api/full-sync/start",{})
            wait_for(lambda:b.state_snapshot()["mode"]=="full_sync" and c.state_snapshot()["mode"]=="full_sync")
            wait_for(lambda:not b.state_snapshot()["sync"]["initializing"]and not c.state_snapshot()["sync"]["initializing"])
            wait_for(lambda:file_map(b_dir)==file_map(admin_dir)==file_map(c_dir))
            (b_dir/"from-member-b.txt").write_text("automatic member update",encoding="utf-8")
            wait_for(lambda:(admin_dir/"from-member-b.txt").is_file()and (c_dir/"from-member-b.txt").is_file())
            assert not pending_request(admin,"folder_change")
            (admin_dir/"from-admin.txt").write_text("automatic admin update",encoding="utf-8")
            wait_for(lambda:(b_dir/"from-admin.txt").is_file()and (c_dir/"from-admin.txt").is_file())
            (admin_dir/"from-admin.txt").unlink()
            wait_for(lambda:not (b_dir/"from-admin.txt").exists()and not (c_dir/"from-admin.txt").exists())
            restore_id=admin.state_snapshot()["folder_states"][0]["id"]
            (admin_dir/"revert-me.txt").write_text("remove through state restore",encoding="utf-8")
            wait_for(lambda:(b_dir/"revert-me.txt").is_file()and (c_dir/"revert-me.txt").is_file())
            admin.revert_admin_state(restore_id,send_to_all=True)
            wait_for(lambda:not (admin_dir/"revert-me.txt").exists())
            wait_for(lambda:not (b_dir/"revert-me.txt").exists()and not (c_dir/"revert-me.txt").exists())
            assert file_map(admin_dir)==file_map(b_dir)==file_map(c_dir)
        finally:
            web.shutdown()
            web.server_close()
            web_worker.join(timeout=2)
            c.stop()
            b.stop()
            admin.stop()
    print("dashboard full-sync and approval-mode test passed")

if __name__=="__main__":
    main()
