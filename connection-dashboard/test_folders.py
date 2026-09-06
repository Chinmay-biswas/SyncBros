import shutil
import tempfile
import unittest
from pathlib import Path
import test_collaboration as collab
from chunker import CHUNK_SIZE
from dashboard_protocol import ApiError
from server import DashboardSyncService,dir_manifest,safe_filename
from test_dashboard import pending_request,wait_for
class FolderTests(unittest.TestCase):
    setUp=collab.CollaborationTests.setUp
    tearDown=collab.CollaborationTests.tearDown
    create=collab.CollaborationTests.create
    refresh_members=collab.CollaborationTests.refresh_members

    def tree(self,service):
        return {p.relative_to(service.sync_dir).as_posix():None if p.is_dir()else p.read_bytes()
                for p in service.sync_dir.rglob("*")if not p.name.endswith(".part")and ".syncing-" not in p.name}

    def write(self,service,name,data="text"):
        path=service.sync_dir/name
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(data if isinstance(data,bytes)else data.encode("utf-8"))

    def remove(self,service,name):
        path=(service.sync_dir/name).resolve()
        self.assertTrue(path.is_relative_to(self.root.resolve()))
        self.assertNotEqual(path,self.root.resolve())
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

    def propose(self,member):
        member.request_folder_change(None)
        req=wait_for(lambda:pending_request(self.admin,"folder_change"))
        wait_for(lambda:not member.inflight_requests)
        return req

    def approve(self,req):
        self.admin.decide_request(req["id"],True)
        wait_for(lambda:next(r for r in self.admin.state_snapshot()["requests"]if r["id"]==req["id"])["status"]=="completed")

    def same(self):
        return self.tree(self.admin)==self.tree(self.b)==self.tree(self.c)

    def full_sync(self):
        self.admin.start_full_sync()
        wait_for(lambda:self.b._mode()==self.c._mode()=="full_sync"
                 and not self.b.full_sync_initializing and not self.c.full_sync_initializing and self.same())

    def test_nested_approval_rejection_restore_and_restart(self):
        self.write(self.admin,"old/only.txt","admin")
        (self.admin.sync_dir/"old/empty").mkdir()
        before=self.tree(self.admin)
        old=self.admin._record_admin_state("Nested baseline",force=True)["id"]
        self.write(self.b,"docs/report.txt","first")
        self.write(self.b,"other/report.txt","second")
        (self.b.sync_dir/"empty/deep").mkdir(parents=True)
        req=self.propose(self.b)
        self.assertEqual(req["file_count"],2)
        self.assertEqual(req["folder_count"],4)
        self.assertEqual(self.tree(self.admin),before)
        self.admin.decide_request(req["id"],False)
        self.assertEqual(self.tree(self.admin),before)
        self.b.refresh_board()
        req=self.propose(self.b)
        self.approve(req)
        self.assertEqual(self.tree(self.admin),self.tree(self.b))
        self.admin.stop()
        self.admin=self.create("admin","Admin Hub","admin")
        self.admin.revert_admin_state(old)
        self.assertEqual(self.tree(self.admin),before)

    def test_empty_folder_only_request_survives_restart(self):
        (self.b.sync_dir/"empty/child").mkdir(parents=True)
        req=self.propose(self.b)
        self.assertEqual(req["file_count"],0)
        self.assertEqual(req["folder_count"],2)
        self.b.stop()
        self.admin.stop()
        self.admin=self.create("admin","Admin Hub","admin")
        self.approve(req)
        self.assertTrue((self.admin.sync_dir/"empty/child").is_dir())

    def test_admin_send_replaces_nested_tree_and_file_directory_types(self):
        self.write(self.b,"swap/old.txt")
        self.write(self.c,"swap/old.txt")
        self.write(self.b,"docs","old file")
        self.write(self.c,"docs","old file")
        self.write(self.admin,"swap","new file")
        self.write(self.admin,"docs/new.txt","new nested file")
        (self.admin.sync_dir/"empty").mkdir()
        self.admin.send_admin_folder_to_all()
        wait_for(self.same)
        self.remove(self.admin,"swap")
        (self.admin.sync_dir/"swap/deep").mkdir(parents=True)
        self.admin.send_admin_folder_to_all()
        wait_for(self.same)

    def test_full_sync_nested_create_rename_delete_and_restore(self):
        self.write(self.admin,"baseline/report.txt","baseline")
        (self.admin.sync_dir/"baseline/empty").mkdir()
        self.write(self.c,"stale/only.txt")
        baseline=self.tree(self.admin)
        old=self.admin._record_admin_state("Before automatic sync",force=True)["id"]
        self.full_sync()
        for service in (self.admin,self.b,self.c):
            service.sync_interval=0.05
            service._spawn(service._watch_sync_folder)
        (self.b.sync_dir/"new/empty").mkdir(parents=True)
        wait_for(lambda:(self.c.sync_dir/"new/empty").is_dir()and self.same())
        self.write(self.b,"new/deep/report.txt","member edit")
        self.write(self.b,"different/report.txt","different file")
        wait_for(lambda:(self.c.sync_dir/"different/report.txt").is_file()and self.same())
        (self.b.sync_dir/"new").rename(self.b.sync_dir/"renamed")
        wait_for(lambda:(self.c.sync_dir/"renamed/deep/report.txt").is_file()and self.same())
        self.remove(self.b,"renamed")
        wait_for(lambda:not (self.c.sync_dir/"renamed").exists()and self.same())
        self.remove(self.admin,"different")
        self.write(self.admin,"different","folder became file")
        wait_for(lambda:(self.c.sync_dir/"different").is_file()and self.same())
        self.remove(self.admin,"different")
        (self.admin.sync_dir/"different/empty").mkdir(parents=True)
        wait_for(lambda:(self.c.sync_dir/"different/empty").is_dir()and self.same())
        self.admin.revert_admin_state(old,send_to_all=True)
        wait_for(lambda:self.tree(self.admin)==baseline and self.same())

    def test_nested_transfers_reuse_unchanged_chunks(self):
        self.full_sync()
        self.write(self.b,"big/data.bin",b"A"*CHUNK_SIZE+b"B"*CHUNK_SIZE)
        target=self.b._admin_target(require_connected=True)
        def send():
            task=self.b._queue_tree_update(target,self.b._capture_folder_manifests(),[])
            self.assertTrue(task["done"].wait(10))
            self.assertIsNone(task["error"])
            wait_for(self.same)
            return task["chunks"]

        self.assertEqual(send(),2)
        self.assertEqual(send(),0)
        with (self.b.sync_dir/"big/data.bin").open("r+b")as file:
            file.write(b"Z")
        self.assertEqual(send(),1)

    def test_unsafe_paths_and_conflicting_trees_rejected(self):
        for name in ("../outside","/absolute","C:/outside","a/../../outside","a\\b","a//b",
                     "a/./b","a/CON.txt","a/file:stream","a/trailing.","a/temp.part"):
            with self.subTest(name=name),self.assertRaises(ApiError):
                safe_filename(name)
        self.assertEqual(safe_filename("docs/sub folder/notes.txt"),"docs/sub folder/notes.txt")
        self.write(self.b,"file","data")
        file=self.b._capture_folder_manifests()["file"]
        for entries in ([file,dir_manifest("file/child")],[dir_manifest("A"),dir_manifest("a")],
                        [dir_manifest("A"),dir_manifest("a/child")]):
            with self.assertRaises(ApiError):
                self.admin._validate_snapshot_manifests(entries)
        with self.assertRaises(ApiError):
            self.admin._check_tree_mode({"replace":True})
        with self.assertRaises(ApiError):
            self.admin._check_tree_mode({"replace":False,"session_id":self.admin.state["session_id"]})

    def test_symlink_destination_cannot_escape_sync_folder(self):
        with tempfile.TemporaryDirectory()as tmp:
            link=self.admin.sync_dir/"linked"
            try:
                link.symlink_to(tmp,target_is_directory=True)
            except OSError:
                self.skipTest("Creating symlinks requires Windows Developer Mode or administrator rights")
            self.assertNotIn("linked",self.admin._scan_sync_files())
            with self.assertRaises(ApiError):
                self.admin._safe_destination("linked/outside.txt")

if __name__=="__main__":
    unittest.main(verbosity=2)
