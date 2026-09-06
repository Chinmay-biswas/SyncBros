let state={
  role:"admin",
  device_name:"This device",
  mode:"approval",
  users:[],
  requests:[],
  history:[],
  folder_states:[],
  sync:{}
};
let editingUserId=null;
let editingAdminConnection=false;
let seenRequestOutcomes=null;
const avatarColors=["avatar-pink","avatar-green","avatar-orange","avatar-blue","avatar-yellow"];
const $=sel=>document.querySelector(sel);
function escapeHtml(value=""){
  return String(value).replace(/[&<>'"]/g,ch=>({
    "&":"&amp;",
    "<":"&lt;",
    ">":"&gt;",
    "'":"&#39;",
    '"':"&quot;"
  })[ch]);
}

function initials(name){
  return String(name).trim().split(/\s+/).slice(0,2).map(word=>word[0]||"").join("").toUpperCase()||"U";
}

function isAdmin(){
  return state.role==="admin";
}

function fullSyncActive(){
  return state.mode==="full_sync";
}

async function api(path,opts={}){
  const resp=await fetch(path,{
    method:opts.method||"GET",
    headers:opts.body ?{
      "Content-Type":"application/json"
    }:undefined,
    body:opts.body ?JSON.stringify(opts.body):undefined
  });
  const data=await resp.json().catch(()=>({}));
  if (!resp.ok)throw new Error(data.error||"The local dashboard could not complete that action.");
  return data;
}

function renderHeader(){
  $("#deviceName").textContent=state.device_name;
  $("#profileInitials").textContent=initials(state.device_name);
  const role=isAdmin()?"Administrator" :"Member";
  $("#roleLabel").textContent=role+(fullSyncActive()?" · Full sync active" :" · Approval mode");
  const running=Boolean(state.sync&&state.sync.listening);
  $("#syncStatus").textContent=running ?"Sync listening on port "+state.sync.port :"Sync listener is unavailable";
  $("#syncDot").classList.toggle("offline-dot",!running);
  $("#addPeerButton").classList.toggle("hidden",!isAdmin());
  $("#adminConnectionButton").classList.toggle("hidden",isAdmin());
  const board=state.board||{};
  const addr=board.admin_endpoint;
  $("#boardStatus").textContent=isAdmin()?"You manage the shared team. Names and member changes appear on every connected dashboard." :board.online ?"Shared team refreshed from "+(addr ?addr.name+" · "+addr.address+":"+addr.port :"the administrator")+"." :board.error||"Set the administrator’s LAN IP to load the shared team.";
  $("#boardStatus").classList.toggle("board-error",!isAdmin()&&!board.online);
}

function renderModePanel(){
  const connected=state.users.filter(user=>user.state==="connected");
  const active=fullSyncActive();
  const starting=Boolean(state.sync&&state.sync.initializing);
  const modeName=active ?starting ?"Preparing full sync baseline" :"Full sync is active" :"Approval mode is active";
  let desc;
  let actions="";
  if (isAdmin()){
    desc=active ?"Every file change and deletion is relayed through this admin. New devices receive the complete admin folder baseline." :"Members can work locally, then submit one complete folder state for your single approval.";
    if (active){
      actions='<button class="outline-button mode-stop-button">Stop full sync</button><button class="primary-button mode-sync-all-button">Sync admin folder to all</button>';
    }else {
      actions='<button class="primary-button mode-start-button">Start full sync</button><button class="outline-button mode-sync-all-button">Sync admin folder to all</button>';
    }
  }else {
    desc=active ?starting ?"The administrator is replacing this folder with the shared baseline. Wait for the baseline to finish before editing." :"Your local changes and deletions now propagate automatically through the administrator." :"Your edits stay local until you send one whole-folder request to the administrator.";
    const canAsk=state.membership_state==="connected";
    actions=active ?'<button class="outline-button" disabled>'+(starting ?"Receiving admin baseline" :"Automatic sync active")+"</button>" :'<button class="primary-button mode-request-full-button" '+(canAsk ?"" :"disabled")+">Request full sync</button>";
  }
  $("#modePanel").innerHTML='<article class="mode-panel '+(active ?"mode-full" :"mode-approval")+'">'+'<div><p class="eyebrow">SYNC MODE</p><h2>'+modeName+'</h2><p>'+desc+"</p></div>"+'<div class="mode-actions">'+actions+"</div>"+"</article>";
  const start=$(".mode-start-button");
  const stop=$(".mode-stop-button");
  const syncAll=$(".mode-sync-all-button");
  const req=$(".mode-request-full-button");
  if (start)start.addEventListener("click",startFullSync);
  if (stop)stop.addEventListener("click",stopFullSync);
  if (syncAll)syncAll.addEventListener("click",syncAdminFolderToAll);
  if (req)req.addEventListener("click",requestFullSync);
}

function renderUsers(){
  const query=$("#userSearch").value.trim().toLowerCase();
  const users=state.users.filter(user=>(user.name+" "+user.address).toLowerCase().includes(query));
  $("#connectionBadge").textContent=state.users.length;
  $("#connectionHint").textContent=isAdmin()?"One admin and two members. Edit names here or delete obsolete members; every dashboard uses this list." :"The administrator manages this team. Send connection and change requests using the administrator’s card.";
  if (!users.length){
    $("#userGrid").innerHTML='<article class="empty-card"><h3>No matching users</h3><p>'+(isAdmin()?"Add a member or change your search." :"Set Admin connection to load the shared team, or change your search.")+'</p></article>';
    return;
  }
  $("#userGrid").innerHTML=users.map((user,i)=>{
    const connected=user.state==="connected";
    const self=user.id===state.self_id;
    const administrator=user.role==="admin";
    let actions="";
    if (isAdmin()&&!administrator){
      actions='<button class="primary-button admin-sync-button" data-id="'+escapeHtml(user.id)+'" '+(connected ?"" :"disabled")+'>Send admin folder</button>';
    }else if (!isAdmin()&&administrator){
      const joined=state.membership_state==="connected";
      const pending=state.requests.some(r=>r.kind==="connection"&&["sending","pending"].includes(r.status));
      actions=!joined ?'<button class="outline-button connect-button" data-id="'+escapeHtml(user.id)+'" '+(pending ?"disabled" :"")+'>'+(pending ?"Connection requested" :"Request connection")+'</button>' :fullSyncActive()?'<button class="primary-button" disabled>Full sync active</button>' :'<button class="primary-button folder-change-button" data-id="'+escapeHtml(user.id)+'">Request folder change</button>';
    }
    const status=administrator ?"Administrator" :connected ?"Approved member" :"Awaiting connection";
    const manage=isAdmin()&&!administrator ?'<div class="member-management"><button class="edit-peer-button" data-id="'+escapeHtml(user.id)+'">Edit member</button><button class="delete-peer-button" data-id="'+escapeHtml(user.id)+'">Delete member</button></div>' :"";
    return '<article class="user-card">'+'<div class="user-top"><div class="avatar '+avatarColors[i%avatarColors.length]+'">'+escapeHtml(initials(user.name))+'</div><div><h3>'+escapeHtml(user.name)+(self ?' <span class="self-label">You</span>' :'')+"</h3><small>"+escapeHtml(user.address)+":"+escapeHtml(user.port)+"</small></div></div>"+'<div class="status">'+status+"</div>"+'<div class="user-actions">'+actions+"</div>"+manage+"</article>";
  }).join("");
  document.querySelectorAll(".connect-button").forEach(btn=>btn.addEventListener("click",()=>requestConnection(btn.dataset.id)));
  document.querySelectorAll(".folder-change-button").forEach(btn=>btn.addEventListener("click",()=>requestFolderChange(btn.dataset.id)));
  document.querySelectorAll(".admin-sync-button").forEach(btn=>btn.addEventListener("click",()=>syncAdminFolder(btn.dataset.id)));
  document.querySelectorAll(".edit-peer-button").forEach(btn=>btn.addEventListener("click",()=>openPeerModal(btn.dataset.id)));
  document.querySelectorAll(".delete-peer-button").forEach(btn=>btn.addEventListener("click",()=>deletePeer(btn.dataset.id)));
}

function requestKind(kind){
  if (kind==="connection")return {
    label:"Connection",
    icon:"◎"
  };
  if (kind==="full_sync")return {
    label:"Full sync",
    icon:"⇄"
  };
  if (kind==="folder_change")return {
    label:"Folder state",
    icon:"▣"
  };
  return {
    label:"File change",
    icon:"↗"
  };
}

function renderRequests(){
  const pending=state.requests.filter(req=>req.status==="pending");
  const admin=isAdmin();
  $("#requestBadge").textContent=pending.length;
  $("#queueStatus").textContent=pending.length+" awaiting review";
  $("#requestDescription").textContent=admin ?"One request per folder state. Approve, reject, or cancel it before any folder changes are applied." :"Track delivery and the administrator’s decision. Awaiting approval means the admin has received your request.";
  if (!state.requests.length){
    $("#requestList").innerHTML='<article class="empty-card"><h3>'+(admin ?"All caught up" :"No requests yet")+"</h3><p>"+(admin ?"Incoming connection and folder-state requests will appear here." :"Your outgoing requests and decisions appear here.")+"</p></article>";
    return;
  }
  $("#requestList").innerHTML=state.requests.map((req,i)=>{
    const pendingRequest=req.status==="pending";
    const kind=requestKind(req.kind);
    const labels={
      sending:"Sending…",
      uploading:"Receiving chunks…",
      pending:"Awaiting approval",
      applying:"Applying approved change…",
      completed:"Approved",
      rejected:"Rejected",
      failed:"Failed",
      expired:"Expired",
      cancel_pending:"Cancellation awaiting connection"
    };
    let controls=admin&&pendingRequest ?'<button class="approve-button" data-request="'+escapeHtml(req.id)+'" data-action="approve">✓ Approve</button><button class="reject-button" data-request="'+escapeHtml(req.id)+'" data-action="reject">Reject</button>' :"";
    if (!admin&&req.status==="failed")controls+='<button class="outline-button" data-request="'+escapeHtml(req.id)+'" data-action="retry">Retry</button>';
    if (!["applying","approved","cancel_pending"].includes(req.status)){
      const cancel=["pending","sending","uploading"].includes(req.status);
      controls+='<button class="reject-button" data-request="'+escapeHtml(req.id)+'" data-action="delete">'+(cancel ?"Cancel request" :"Delete request")+'</button>';
    }
    const buttons='<div class="request-controls"><span class="request-meta request-'+escapeHtml(req.status)+'">'+escapeHtml(labels[req.status]||req.status)+'</span><div class="request-actions">'+controls+'</div></div>';
    return '<article class="request-card">'+'<div class="avatar '+avatarColors[i%avatarColors.length]+'">'+kind.icon+"</div>"+'<div class="request-body"><h3>'+escapeHtml(req.title)+'</h3><p><span class="request-kind">'+kind.label+"</span> "+escapeHtml(req.detail||req.user||"")+"</p></div>"+buttons+"</article>";
  }).join("");
  document.querySelectorAll("[data-request]").forEach(btn=>btn.addEventListener("click",()=>decideRequest(btn.dataset.request,btn.dataset.action)));
}

function renderHistory(){
  const filter=$("#historyFilter").value;
  const events=state.history.filter(event=>filter==="all"||event.type===filter);
  $("#historyList").innerHTML=events.length ?events.map(event=>'<div class="history-row"><div><span class="event-icon">'+(event.type==="connection" ?"◎" :event.type==="system" ?"●" :"↗")+"</span><span>"+escapeHtml(event.title)+"<small>"+escapeHtml(event.detail)+"</small></span></div><time>"+escapeHtml(event.time)+"</time></div>").join(""):'<div class="history-row"><span>No activity matches this filter.</span><time>—</time></div>';
  if (!isAdmin()){
    $("#stateHistory").innerHTML='<article class="empty-card"><h3>Administrator-only states</h3><p>Saved folder states and restore actions are available on the admin dashboard.</p></article>';
    return;
  }
  const states=state.folder_states||[];
  $("#stateHistory").innerHTML=states.length ?states.map((entry,i)=>'<article class="state-row">'+'<div><strong>'+escapeHtml(entry.label||"Saved folder state")+"</strong><small>"+escapeHtml(entry.time)+" · "+escapeHtml(entry.file_count)+" file(s) · "+escapeHtml(entry.change_count)+" delta(s)"+"</small></div>"+'<div class="state-actions"><button class="outline-button restore-button" data-state="'+escapeHtml(entry.id)+'">Restore</button><button class="primary-button restore-all-button" data-state="'+escapeHtml(entry.id)+'">Restore + send all</button></div>'+"</article>").join(""):'<article class="empty-card"><h3>No saved states yet</h3><p>The first admin state is saved when the dashboard starts.</p></article>';
  document.querySelectorAll(".restore-button").forEach(btn=>btn.addEventListener("click",()=>restoreState(btn.dataset.state,false)));
  document.querySelectorAll(".restore-all-button").forEach(btn=>btn.addEventListener("click",()=>restoreState(btn.dataset.state,true)));
}

function render(){
  renderHeader();
  renderModePanel();
  renderUsers();
  renderRequests();
  renderHistory();
  if (state.protocol!==3){
    $("#boardStatus").textContent="Dashboard update ready. Restart server.py on this laptop and install the same update on every member laptop.";
    $("#boardStatus").classList.add("board-error");
    document.querySelectorAll(".user-actions button, .member-management button, .mode-actions button, .request-actions button, #addPeerButton, #adminConnectionButton").forEach(btn=>{
      btn.disabled=true;
    });
  }
}

async function refresh({
  quiet=false
}={}){
  try {
    state=await api("/api/state");
    render();
    const results=new Map(state.requests.filter(r=>["completed","rejected","failed"].includes(r.status)).map(r=>[r.id,r.status]));
    if (!isAdmin()&&seenRequestOutcomes){
      for (const req of state.requests){
        if (results.has(req.id)&&seenRequestOutcomes.get(req.id)!==req.status){
          toast(req.title+": "+(req.status==="completed" ?"approved" :req.status),req.status==="failed");
        }
      }
    }
    seenRequestOutcomes=results;
  }catch (err){
    $("#syncStatus").textContent="Start server.py to use the local dashboard";
    $("#connectionHint").textContent="Open this page through http://127.0.0.1:8080 after starting server.py. It does not use the internet.";
    if (!quiet)toast(err.message,true);
  }
}

async function requestConnection(uid){
  try {
    state=await api("/api/connections",{
      method:"POST",
      body:{
        user_id:uid
      }
    });
    render();
    toast("Sending connection request. Track its receipt in Change requests.");
  }catch (err){
    toast(err.message,true);
  }
}

async function requestFolderChange(uid){
  try {
    state=await api("/api/folder-change",{
      method:"POST",
      body:{
        user_id:uid
      }
    });
    render();
    toast("Sending one folder request. Track its receipt in Change requests.");
  }catch (err){
    toast(err.message,true);
  }
}

async function requestFullSync(){
  const admin=state.users.find(user=>user.role==="admin");
  if (!admin){
    toast("Connect the administrator before requesting full sync.",true);
    return;
  }
  try {
    state=await api("/api/full-sync/request",{
      method:"POST",
      body:{
        user_id:admin.id
      }
    });
    render();
    toast("Sending full-sync request. Track its receipt in Change requests.");
  }catch (err){
    toast(err.message,true);
  }
}

async function startFullSync(){
  try {
    state=await api("/api/full-sync/start",{
      method:"POST",
      body:{}
    });
    render();
    toast("Full sync started. Connected devices are receiving the admin baseline.");
  }catch (err){
    toast(err.message,true);
  }
}

async function stopFullSync(){
  try {
    state=await api("/api/full-sync/stop",{
      method:"POST",
      body:{}
    });
    render();
    toast("Full sync stopped. Member changes now need one approval request.");
  }catch (err){
    toast(err.message,true);
  }
}

async function syncAdminFolder(uid){
  try {
    state=await api("/api/admin-sync",{
      method:"POST",
      body:{
        user_id:uid
      }
    });
    render();
    toast("Admin folder transfer started for the selected member.");
  }catch (err){
    toast(err.message,true);
  }
}

async function syncAdminFolderToAll(){
  try {
    state=await api("/api/admin-sync/all",{
      method:"POST",
      body:{}
    });
    render();
    toast("Admin folder transfer started for all connected members.");
  }catch (err){
    toast(err.message,true);
  }
}

async function restoreState(sid,sendToAll){
  try {
    state=await api("/api/states/"+encodeURIComponent(sid)+(sendToAll ?"/revert-all" :"/revert"),{
      method:"POST",
      body:{}
    });
    render();
    toast(sendToAll ?"Saved admin state restored and sent to all connected members." :"Saved admin state restored.");
  }catch (err){
    toast(err.message,true);
  }
}

function openPeerModal(uid=null){
  const user=uid ?state.users.find(item=>item.id===uid):null;
  editingUserId=user ?user.id :null;
  editingAdminConnection=false;
  $("#peerName").disabled=false;
  $("#peerModalEyebrow").textContent=user ?"EDIT LAN USER" :"ADD LAN USER";
  $("#peerModalTitle").textContent=user ?"Edit "+user.name :"Add a user";
  $("#peerModalDescription").textContent=user ?"This name is shared by all dashboards. Changing the LAN address requires a fresh connection approval." :"Add a member to the shared team using their LAN IP and sync port. This team supports two members.";
  $("#submitPeer").innerHTML=user ?"Save LAN address <span>✓</span>" :"Add available user <span>＋</span>";
  $("#peerName").value=user ?user.name :"";
  $("#peerHost").value=user ?user.address :"";
  $("#peerPort").value=user ?user.port :state.sync.port||5000;
  $("#peerModal").classList.remove("hidden");
  $("#peerModal").setAttribute("aria-hidden","false");
  $("#peerName").focus();
}

function openAdminConnection(){
  openPeerModal();
  editingAdminConnection=true;
  const addr=(state.board||{}).admin_endpoint||{};
  $("#peerModalEyebrow").textContent="ADMIN CONNECTION";
  $("#peerModalTitle").textContent="Connect to the administrator";
  $("#peerModalDescription").textContent="Enter the admin laptop’s LAN IP, not this member’s IP. Names and members are loaded from that administrator.";
  $("#peerName").value=addr.name||"Administrator";
  $("#peerName").disabled=true;
  $("#peerHost").value=addr.address||"";
  $("#peerPort").value=addr.port||5000;
  $("#submitPeer").textContent="Save admin connection";
  $("#peerHost").focus();
}

async function deletePeer(uid){
  const user=state.users.find(u=>u.id===uid);
  if (!user||!window.confirm("Remove "+user.name+" from the shared team on every dashboard? Their files will be kept."))return;
  try {
    state=await api("/api/peers/"+encodeURIComponent(uid)+"/delete",{
      method:"POST",
      body:{}
    });
    render();
    toast("Member removed from the shared team.");
  }catch (err){
    toast(err.message,true);
  }
}

function closeModal(id){
  const modal=$("#"+id);
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden","true");
  if (id==="peerModal"){
    editingUserId=null;
    $("#peerName").value="";
    $("#peerHost").value="";
    $("#peerPort").value=state.sync.port||5000;
  }
}

async function submitPeer(){
  try {
    $("#submitPeer").disabled=true;
    const body={
      name:$("#peerName").value,
      host:$("#peerHost").value,
      port:$("#peerPort").value
    };
    const setup=editingAdminConnection;
    state=await api(setup ?"/api/admin-connection" :editingUserId ?"/api/peers/"+encodeURIComponent(editingUserId)+"/update" :"/api/peers",{
      method:"POST",
      body
    });
    const edited=Boolean(editingUserId);
    closeModal("peerModal");
    render();
    toast(setup ?"Admin connection saved. Refreshing shared team…" :edited ?"Shared member updated." :"Member added to the shared team.");
  }catch (err){
    toast(err.message,true);
  }finally {
    $("#submitPeer").disabled=false;
  }
}

async function decideRequest(reqId,action){
  if (action==="delete"&&!window.confirm("Cancel or delete this request? This does not revert an already applied change."))return;
  try {
    state=await api("/api/requests/"+encodeURIComponent(reqId)+"/"+action,{
      method:"POST",
      body:{
        action
      }
    });
    render();
    const msgs={
      approve:"Approval saved. Applying the request…",
      reject:"Request rejected.",
      delete:isAdmin()?"Request removed." :"Cancellation requested. Waiting for admin confirmation…",
      retry:"Retry started. Checking receipt with the admin…"
    };
    toast(msgs[action]);
  }catch (err){
    toast(err.message,true);
  }
}

function toast(message,isError=false){
  const el=$("#toast");
  el.textContent=message;
  el.classList.toggle("toast-error",isError);
  el.classList.add("show");
  window.clearTimeout(toast.timer);
  toast.timer=window.setTimeout(()=>el.classList.remove("show"),3600);
}

document.querySelectorAll(".nav-link").forEach(btn=>btn.addEventListener("click",()=>{
  document.querySelectorAll(".nav-link").forEach(item=>item.classList.remove("active"));
  btn.classList.add("active");
  document.querySelectorAll(".view").forEach(view=>view.classList.add("hidden"));
  $("#"+btn.dataset.view+"View").classList.remove("hidden");
  $("#pageTitle").textContent=btn.dataset.view==="requests" ?"Change requests" :btn.dataset.view[0].toUpperCase()+btn.dataset.view.slice(1);
}));
$("#userSearch").addEventListener("input",renderUsers);
$("#historyFilter").addEventListener("change",renderHistory);
$("#addPeerButton").addEventListener("click",()=>openPeerModal());
$("#adminConnectionButton").addEventListener("click",openAdminConnection);
$("#submitPeer").addEventListener("click",submitPeer);
document.querySelectorAll("[data-close]").forEach(btn=>btn.addEventListener("click",()=>closeModal(btn.dataset.close)));
document.querySelectorAll(".modal").forEach(modal=>modal.addEventListener("click",event=>{
  if (event.target===modal)closeModal(modal.id);
}));
refresh();
window.setInterval(()=>{
  if (document.visibilityState==="visible")refresh({
    quiet:true
  });
},2000);
