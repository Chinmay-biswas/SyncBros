# Verified File Sync Prototype

Files are divided into 1 MiB SHA-256-addressed chunks. Only chunks absent from the destination are transferred; every chunk and the reconstructed file are verified.

## Acceptance checks

Run the automated local checks without modifying your demo folders:

```powershell
python validate.py
```

Expected approximate output: `6/6 checks passed` in a few seconds. The test uses a 3.125 MiB file (four chunks), checks a full transfer, modifies 100 bytes, confirms that only one of four chunks transfers, then verifies a localhost TCP transfer.

## Two-machine LAN test

On Node B:

```powershell
python receiver.py --host 0.0.0.0 --port 5000 --store node_store\chunk_store --output sync_folder
```

On Node A (replace with Node B's private LAN IP):

```powershell
python sender.py sync_folder\large_test.bin 192.168.1.50 --port 5000 --store node_store\chunk_store
```

Use a trusted LAN only. If Windows Firewall prompts, allow Python on private networks; do not expose port 5000 publicly. The verified result appears in `sync_folder`.

This is a two-node MVP. Authentication, encryption, deletion propagation, conflicts, retry logic, and multi-peer scheduling are intentionally not yet implemented.

## Two-way automatic sync

Run this on **both** machines, replacing the peer IP with the other machine's private LAN IP:

```powershell
python node.py 192.168.1.51 --sync-dir sync_folder
```

Files created or modified directly in either machine's `sync_folder` are detected within roughly two seconds and synchronized to the other machine. Keep the default `sync_folder` flat for now: nested folders and deletion propagation are intentionally not implemented. Start both nodes before testing, and use the same port on both sides.

## Three-device admin/hub topology

Choose one device as the **admin hub**. It is a relay node: files received from one member are detected in its own sync folder and sent to the other member. It is not an authenticated administrator yet.

Assume these private LAN IPs:

```text
Admin:  192.168.1.50
Member B: 192.168.1.51
Member C: 192.168.1.52
```

Run these commands from the project folder:

```powershell
# Admin device
python node.py --role admin --peer 192.168.1.51 --peer 192.168.1.52

# Member B
python node.py 192.168.1.50

# Member C
python node.py 192.168.1.50
```

Start all three before testing. A file added on B follows `B -> Admin -> C`; a file added on C follows `C -> Admin -> B`. The sender retries a changed file for any configured peer that is temporarily unreachable. Each device must allow its Python listener on port 5000 through Windows Firewall for private networks.

### Admin approval workflow

Only the terminal running the admin command decides whether a member's proposed change is accepted. When B or C creates or updates a file, the admin PowerShell shows the exact destination path and waits:

```text
[ADMIN APPROVAL] CREATE requested by 192.168.1.51: C:\...\sync_folder\hello.txt
Approve this change? [y/N]:
```

Type `y` then Enter to accept and relay it; type `n` (or just Enter) to deny it. A denial is reported to the requesting member and is not retried. Changes made directly in the admin's own `sync_folder` are sent to both members without an approval prompt.

Every receiving terminal prints a clear result, for example:

```text
[CREATE] C:\...\sync_folder\hello.txt received from 192.168.1.50 and SHA-256 verified
[UPDATE] C:\...\sync_folder\test.txt received from 192.168.1.50 and SHA-256 verified
```
