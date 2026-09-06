import json
import socket
import unittest
from dashboard_protocol import read_message,send_message
class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.tx,self.rx=socket.socketpair()
        self.tx.settimeout(1)
        self.rx.settimeout(1)
        self.reader=self.rx.makefile("rb")

    def tearDown(self):
        self.reader.close()
        self.rx.close()
        self.tx.close()

    def test_send_ends_with_newline(self):
        msg={"type":"error","error":"peer disconnected"}
        send_message(self.tx,msg)
        raw=self.rx.recv(4096)
        self.assertEqual(raw,json.dumps(msg,separators=(",",":")).encode("utf-8")+b"\n")

    def test_separate_messages_on_open_connection(self):
        msgs=[{"type":"request_received","name":"समीर","detail":"one\ntwo"},{"type":"mode_ack"}]
        for msg in msgs:
            send_message(self.tx,msg)
        for msg in msgs:
            self.assertEqual(read_message(self.reader),msg)

    def test_chunk_header_keeps_binary_data_separate(self):
        data=b"\x00\xff\nchunk"
        msg={"type":"chunk","size":len(data)}
        send_message(self.tx,msg)
        self.tx.sendall(data)
        send_message(self.tx,{"type":"complete"})
        self.assertEqual(read_message(self.reader),msg)
        self.assertEqual(self.reader.read(len(data)),data)
        self.assertEqual(read_message(self.reader),{"type":"complete"})

    def test_disconnected_peer(self):
        self.tx.close()
        with self.assertRaises(ConnectionError):
            read_message(self.reader)

if __name__=="__main__":
    unittest.main(verbosity=2)
