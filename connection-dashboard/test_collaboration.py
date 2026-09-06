import copy
import json
import tempfile
import threading
import unittest
import urllib.error
from pathlib import Path
from dashboard_protocol import ApiError
from server import DashboardSyncService,make_http_server
from test_dashboard import file_map,pending_request,post_json,wait_for
class CollaborationTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.root=Path(self.temporary.name)
        self.services=[]
        self.admin=self.create("admin","Admin Hub","admin")
        self.b=self.create("member","Samir local name","b",[("Wrong admin label","127.0.0.1",self.admin.sync_port)])
        self.c=self.create("member","Member C","c",[("Admin Hub","127.0.0.1",self.admin.sync_port)])
        for member in (self.b,self.c):
            member.request_connection(None)
            req=wait_for(lambda:pending_request(self.admin,"connection"))
            self.admin.decide_request(req["id"],True)
            wait_for(lambda:member.state_snapshot()["membership_state"]=="connected")
        self.refresh_members()

    def create(self,role,name,directory,peers=(),port=0):
        service=DashboardSyncService(role=role,device_name=name,sync_dir=self.root/directory/"files",
            store_dir=self.root/directory/"chunks",state_file=self.root/directory/"state.json",
            peers=list(peers),auto_sync=False,approval_timeout=2)
        service.start_sync_listener("127.0.0.1",port)
        self.services.append(service)
        return service

    def tearDown(self):
        for service in reversed(self.services):
            service.stop()
        self.temporary.cleanup()

    def refresh_members(self):
        self.b.refresh_board()
        self.c.refresh_board()

    def own(self,member):
        return next(u for u in self.admin.state_snapshot()["users"]if u.get("device_id")==member.device_id)

    def request(self,member):
        (member.sync_dir/"one.txt").write_text("first member file",encoding="utf-8")
        (member.sync_dir/"two.txt").write_text("second member file",encoding="utf-8")
        (self.admin.sync_dir/"admin-only.txt").write_text("remove only on approval",encoding="utf-8")
        member.request_folder_change(None)
        req=wait_for(lambda:pending_request(self.admin,"folder_change"))
        wait_for(lambda:not member.inflight_requests)
        member.refresh_board()
        return req

    def test_shared_names_ids_deletion_permissions_and_restart(self):
        peer=self.own(self.b)
        self.admin.update_peer(peer["id"],"Samir",peer["address"],peer["port"])
        self.refresh_members()
        expected=self.admin.state_snapshot()["users"]
        self.assertEqual(len(expected),3)
        self.assertEqual(expected,self.b.state_snapshot()["users"])
        self.assertEqual(expected,self.c.state_snapshot()["users"])
        self.assertEqual(self.b.device_name,"Samir")
        for mutate in (lambda:self.b.add_peer("Intruder","127.0.0.1",5999),
                       lambda:self.b.update_peer(peer["id"],"Different","127.0.0.1",5999),
                       lambda:self.b.delete_peer(peer["id"])):
            with self.assertRaises(ApiError):
                mutate()
        with self.assertRaises(ApiError):
            self.admin.add_peer("Fourth device","127.0.0.1",5999)
        removed=self.own(self.c)
        self.admin.delete_peer(removed["id"])
        self.refresh_members()
        self.assertEqual(self.admin.state_snapshot()["users"],self.b.state_snapshot()["users"])
        self.assertEqual(self.admin.state_snapshot()["users"],self.c.state_snapshot()["users"])
        with self.assertRaises(ApiError):
            self.c.request_folder_change(None)
        port=self.admin.sync_port
        self.admin.stop()
        self.admin=self.create("admin","Admin Hub","admin",[(removed["name"],removed["address"],removed["port"])],port)
        self.assertEqual(len(self.admin.state_snapshot()["users"]),2,"Old CLI seeds must not resurrect deleted members")

    def test_request_receipt_survives_restart_and_member_offline(self):
        req=self.request(self.b)
        self.assertEqual(req["file_count"],2)
        before=file_map(self.admin.sync_dir)
        self.assertEqual(self.b.state_snapshot()["requests"][0]["status"],"pending")
        self.assertNotIn("snapshot",self.admin.state_snapshot()["requests"][0])
        self.assertEqual(file_map(self.admin.sync_dir),before)
        self.b.stop()
        port=self.admin.sync_port
        self.admin.stop()
        self.admin=self.create("admin","Admin Hub","admin",port=port)
        self.admin.decide_request(req["id"],True)
        wait_for(lambda:file_map(self.admin.sync_dir)==file_map(self.b.sync_dir))
        self.assertFalse((self.admin.sync_dir/"admin-only.txt").exists())

    def test_admin_cancels_and_member_cancels_without_applying(self):
        req=self.request(self.b)
        before=file_map(self.admin.sync_dir)
        self.admin.delete_request(req["id"])
        self.b.refresh_board()
        self.assertFalse(any(r["id"]==req["id"]for r in self.b.state_snapshot()["requests"]))
        self.assertEqual(file_map(self.admin.sync_dir),before)
        with self.assertRaises(ApiError):
            self.admin.decide_request(req["id"],True)
        req=self.request(self.b)
        with self.assertRaises(ApiError):
            self.c._exchange(self.c._admin_target(),{"type":"delete_request","request_id":req["id"]})
        self.b.delete_request(req["id"])
        wait_for(lambda:not any(r["id"]==req["id"]for r in self.admin.state_snapshot()["requests"]))
        self.assertEqual(file_map(self.admin.sync_dir),before)

    def test_retry_is_idempotent_and_decision_notified_once(self):
        req=self.request(self.b)
        before=file_map(self.admin.sync_dir)
        with self.b.lock:
            outgoing=next(r for r in self.b.state["outgoing_requests"]if r["id"]==req["id"])
            outgoing["status"]="failed"
        self.b.retry_request(req["id"])
        wait_for(lambda:not self.b.inflight_requests and outgoing["status"]=="pending")
        self.assertEqual(sum(r["id"]==req["id"]for r in self.admin.state_snapshot()["requests"]),1)
        self.admin.decide_request(req["id"],False)
        self.b.refresh_board()
        self.b.refresh_board()
        self.assertEqual(file_map(self.admin.sync_dir),before)
        self.assertEqual(sum(h["title"]=="Folder change rejected" for h in self.b.state_snapshot()["history"]),1)
        self.admin.delete_request(req["id"])
        self.b.refresh_board()
        self.assertFalse(any(r["id"]==req["id"]for r in self.b.state_snapshot()["requests"]))

    def test_stale_mode_and_wrong_local_peer_list_repaired(self):
        with self.b.lock:
            self.b.state["mode"]="full_sync"
            self.b.state["users"].insert(0,{"id":"wrong","name":"Another member","role":"member",
                "address":"127.0.0.1","port":self.c.sync_port,"state":"connected"})
        self.b.request_folder_change("wrong")
        req=wait_for(lambda:pending_request(self.admin,"folder_change"))
        self.assertEqual(self.b.state_snapshot()["mode"],"approval")
        self.assertFalse(pending_request(self.c,"folder_change"))
        self.admin.delete_request(req["id"])

    def test_member_http_cannot_edit_team_or_approve(self):
        web=make_http_server("127.0.0.1",0,self.b)
        thread=threading.Thread(target=web.serve_forever,daemon=True)
        thread.start()
        url=f"http://127.0.0.1:{web.server_address[1]}"
        try:
            for path,body in (("/api/peers",{"name":"Wrong","host":"127.0.0.1","port":5050}),
                               (f"/api/peers/{self.own(self.c)['id']}/delete",{}),
                               ("/api/requests/fake/approve",{})):
                with self.assertRaises(urllib.error.HTTPError)as err:
                    post_json(url+path,body)
                self.assertEqual(err.exception.code,400)
        finally:
            web.shutdown()
            web.server_close()
            thread.join()

if __name__=="__main__":
    unittest.main(verbosity=2)
