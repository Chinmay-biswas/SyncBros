# SyncBros local dashboard

This folder is the separate local web dashboard for the chunked LAN file-sync project one level above it. It has no package install, cloud service, external API, web font, or internet requirement.
Run the dashboard server on every device that participates in the two dashboard modes. It uses the parent project's SHA-256 manifests, chunk store, and verified reconstruction functions. The browser stays on its own laptop at http://127.0.0.1:8080; only the configured private-LAN TCP listener talks to the other laptops.

## Three-device setup

Replace the examples with the current private LAN IP address of each laptop:
    Admin Hub: 192.168.1.50
    Member B: 192.168.1.51
    Member C: 192.168.1.52

From the project root, run one command on each device.
Admin Hub:

    python connection-dashboard\server.py --role admin --name "Admin Hub" --peer "Member B=192.168.1.51" --peer "Member C=192.168.1.52" --relay

Member B:

    python connection-dashboard\server.py --role member --name "Member B" --peer "Admin Hub=192.168.1.50"

Member C:

    python connection-dashboard\server.py --role member --name "Member C" --peer "Admin Hub=192.168.1.50"
Then open http://127.0.0.1:8080 in the browser on each device. Do not run node.py or receiver.py on the same laptop and TCP sync port while this dashboard service is running.

The --relay flag is kept so the earlier command still works. In the new full-sync mode the admin always relays updates to every connected member, whether or not that flag is present.

## Change a laptop IP in the webpage

The addresses in the commands only seed the dashboard. Go to **Connections**, find a device, select **Edit LAN IP**, change its private LAN IP address or sync port, and save.

The changed address is saved locally in connection-dashboard/dashboard_state.json and is used by later connections and transfers. The device is marked Available after an address change, so click **Connect** again before requesting or sending a folder.

## Mode 1: Approval mode

Approval mode is the default while full sync is off.

1. A member edits files locally in its sync_folder.
2. The member clicks **Request folder change** for the connected administrator.
3. The dashboard sends one request describing the member's entire directory tree, including nested files and empty folders, not one request per file.
4. The admin sees one queue item for that member and chooses **Approve** or **Reject**.

The member uploads missing chunks into the admin chunk cache before the request is ready for approval. On approval, files are reconstructed in a temporary staging folder, SHA-256 verified, and then the admin sync_folder becomes equal to the requested member tree. Files and folders absent from that approved state are removed. No live admin files are changed during upload or on rejection.

On rejection, the admin sync_folder is not touched. The member still has its local edits and can revise them or send a later request. The decision produces one outcome notification for that folder request.

While approval mode is active, the admin can select **Send admin folder** beside any connected member. This replaces that member's entire sync_folder with the admin's current folder. The admin also has a one-click **Sync admin folder to all** control while full sync is active.

## Mode 2: Full sync

The administrator can click **Start full sync** immediately. A member can instead click **Request full sync**; that creates one admin approval request. If approved:

1. The admin sends a full-sync-start message to every connected member.
2. The admin folder is sent as the shared baseline, so every member folder becomes identical to the admin folder.
3. Once each member has applied that baseline, edits and deletions on any connected device propagate automatically through the admin hub.

Transfers always announce file manifests first and send only chunks missing from the recipient's local content-addressed chunk store. A changed file therefore transfers only its changed chunks where possible. Folder snapshots send file/hash metadata to establish the complete state and to propagate deletions.

The member dashboard displays **Receiving admin baseline** until its initial folder replacement finishes. Do not edit that member sync_folder during that short baseline step: the admin baseline intentionally wins at the start of a full-sync session.

The admin can click **Stop full sync** to return every reachable member dashboard to approval mode. Full-sync conflict handling is deterministic but simple: the latest update received by the admin becomes the shared version. It does not merge two simultaneous edits to the same file.

## Admin history and restore

Only the admin has **Saved folder states** in the History page.

- The first state stores the initial folder manifest map.
- Later states store only changed manifests and deletion markers, not full duplicate copies of unchanged files.
- File data remains deduplicated in the project's chunk store.
- **Restore** rebuilds the admin sync_folder from a selected prior state.
- **Restore + send all** also starts a full admin-folder send to every connected member.

## Scope and LAN safety

- The dashboard supports nested files, empty folders, renames, and directory deletion. Use relative paths within sync_folder; symbolic links and junctions are excluded.
- Folder transfers use ordered batches per peer and request only missing file chunks. A snapshot can contain up to 2,000 combined file/folder entries.
- Update the dashboard source on every laptop, including tree_transfer.py, and restart all servers before using directory sync. Keep each laptop's existing state file and chunk store. Then start a fresh full-sync session or send the admin folder to all.
- Full folder-state requests and full sync require the dashboard service on the participating laptops. The original sender.py still works as a compatible one-file approval sender.
- This is a trusted-LAN prototype, not encrypted or authenticated production file sharing. Use it only with devices and private addresses you trust.
- If a laptop cannot connect, first test the actual listener port from the other laptop:

      Test-NetConnection 192.168.1.51 -Port 5000

  Campus or guest Wi-Fi can isolate peers even when the devices appear to have similar IP addresses.

## Local verification

From the project root:

    python connection-dashboard\test_dashboard.py
    python connection-dashboard\test_collaboration.py
    python connection-dashboard\test_dashboard_protocol.py
    python connection-dashboard\test_folders.py
    python validate.py
The dashboard test starts an admin and two members on loopback addresses. It verifies one whole-folder approval/rejection, direct admin folder sending, member-requested and admin-started full sync, automatic relays, deletion propagation, history restore, HTTP controls, and sender.py compatibility.
