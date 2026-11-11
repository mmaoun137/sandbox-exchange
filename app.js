(function(){
"use strict";
/* ================= Markets =================
   src "bn" = Binance public WebSocket (no key, updates every second)
   src "fh" = Finnhub WebSocket (free key, real-time US trades) */
const CLS_COL={Crypto:"#e9a23b",Stocks:"#7aa7ff",ETFs:"#b58cff",Forex:"#4fd1c5",Commodities:"#e0c36a"};
const RAW=[
 ["BTC","Bitcoin","Crypto","bn","BTCUSDT",2,20],["ETH","Ethereum","Crypto","bn","ETHUSDT",2,20],["SOL","Solana","Crypto","bn","SOLUSDT",2,20],
 ["XRP","XRP","Crypto","bn","XRPUSDT",4,20],["BNB","BNB","Crypto","bn","BNBUSDT",2,20],["DOGE","Dogecoin","Crypto","bn","DOGEUSDT",5,20],
 ["ADA","Cardano","Crypto","bn","ADAUSDT",4,20],["AVAX","Avalanche","Crypto","bn","AVAXUSDT",2,20],["LINK","Chainlink","Crypto","bn","LINKUSDT",3,20],
 ["LTC","Litecoin","Crypto","bn","LTCUSDT",2,20],["SUI","Sui","Crypto","bn","SUIUSDT",4,20],["TRX","TRON","Crypto","bn","TRXUSDT",4,20],
 ["PAXG","Gold token (1 oz, 24/7)","Commodities","bn","PAXGUSDT",2,10],["EUR","Euro vs USDT (≈ EUR/USD)","Forex","bn","EURUSDT",4,20],
 ["NVDA","NVIDIA","Stocks","fh","NVDA",2,4],["AAPL","Apple","Stocks","fh","AAPL",2,4],["MSFT","Microsoft","Stocks","fh","MSFT",2,4],
 ["TSLA","Tesla","Stocks","fh","TSLA",2,4],["AMZN","Amazon","Stocks","fh","AMZN",2,4],["GOOGL","Alphabet","Stocks","fh","GOOGL",2,4],
 ["META","Meta Platforms","Stocks","fh","META",2,4],["AMD","AMD","Stocks","fh","AMD",2,4],["PLTR","Palantir","Stocks","fh","PLTR",2,4],
 ["COIN","Coinbase","Stocks","fh","COIN",2,4],["SPY","S&P 500 ETF","ETFs","fh","SPY",2,5],["QQQ","Nasdaq 100 ETF","ETFs","fh","QQQ",2,5],
 ["GLD","Gold ETF","Commodities","fh","GLD",2,5],["SLV","Silver ETF","Commodities","fh","SLV",2,5],["USO","Crude Oil ETF","Commodities","fh","USO",2,5]
];
const FEE=0.001, LIQ=0.9;
const A={},LIST=[],BYID={};
RAW.forEach(r=>{const a={s:r[0],n:r[1],c:r[2],src:r[3],id:r[4],d:r[5],maxLev:r[6]};a.hours=a.src==="bn"?"24/7":"us";A[a.s]=a;LIST.push(a);BYID[a.id]=a});
const CRYPTO=LIST.filter(a=>a.src==="bn"), STOCKS=LIST.filter(a=>a.src==="fh");
const BN_WS=["wss://data-stream.binance.vision","wss://stream.binance.com:9443"];
const BN_REST=["https://data-api.binance.vision","https://api.binance.com"];
const TF_BN=["1s","1m","5m","15m","1h","4h","1d"], TF_FH=["1m","5m","15m"];

/* ================= helpers ================= */
const $=id=>document.getElementById(id);
const fx=(v,d)=>Number(v).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const usd=(v,d=2)=>(v<0?"-$":"$")+fx(Math.abs(v),d);
const sgnUsd=v=>(v>0?"+":"")+usd(v);
const pct=v=>(v>0?"+":"")+v.toFixed(2)+"%";
const cls=v=>v>0?"up":v<0?"down":"muted";
const qd=a=>a.c==="Forex"?0:a.src==="bn"?(a.d>=4?1:a.s==="BTC"||a.s==="PAXG"?5:3):4;
const fq=(q,a)=>fx(q,Math.min(qd(a),6));
const compact=v=>v>=1e9?(v/1e9).toFixed(2)+"B":v>=1e6?(v/1e6).toFixed(2)+"M":v>=1e3?(v/1e3).toFixed(1)+"K":v.toFixed(0);
const p2=n=>String(n).padStart(2,"0");
const hhmm=t=>{const d=new Date(t);return p2(d.getHours())+":"+p2(d.getMinutes())};
const hhmmss=t=>{const d=new Date(t);return hhmm(t)+":"+p2(d.getSeconds())};
const dmy=t=>new Date(t).toLocaleDateString("en-US",{month:"short",day:"numeric"});
const parse=s=>{const v=parseFloat(String(s).replace(/[, ]/g,""));return isFinite(v)?v:NaN};
const esc=s=>String(s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const ls={get(k,d){try{const v=localStorage.getItem(k);return v?JSON.parse(v):d}catch(e){return d}},set(k,v){try{localStorage.setItem(k,JSON.stringify(v))}catch(e){}}};

/* ================= market hours ================= */
function nyNow(){const p=new Intl.DateTimeFormat("en-US",{timeZone:"America/New_York",weekday:"short",hour:"numeric",minute:"numeric",hour12:false}).formatToParts(new Date());const g=t=>p.find(x=>x.type===t).value;return{wd:g("weekday"),mins:(+g("hour")%24)*60+ +g("minute")}}
function usOpen(){const {wd,mins}=nyNow();return wd!=="Sat"&&wd!=="Sun"&&mins>=570&&mins<960}
const isOpen=a=>a.hours==="24/7"||usOpen();

/* ================= state ================= */
const KEY="sx-account-v2";
const fresh=()=>({cash:5e6,start:5e6,positions:{},orders:[],fills:[],eq:[],realized:0,fees:0,nid:1,wins:0,losses:0});
let S=ls.get(KEY,null)||fresh();
const save=()=>ls.set(KEY,S);
let FHKEY=ls.get("sx-fh-key","");
const M={};
const TICKS=ls.get("sx-stock-ticks",{});
LIST.forEach(a=>{M[a.s]={p:null,bid:null,ask:null,o:null,h:null,l:null,vol:null,chg:null,pc:null,last:0,tape:[],tapeT:0,ticks:(TICKS[a.s]||[]).filter(k=>Date.now()-k.t<4*864e5)}});
const QC=ls.get("sx-last-quotes",{});LIST.forEach(a=>{const q=QC[a.s];if(q){Object.assign(M[a.s],{p:q.p,chg:q.chg,pc:q.pc,h:q.h,l:q.l,vol:q.vol,stale:true})}});
function persistQuotes(){const q={};LIST.forEach(a=>{const m=M[a.s];if(m.p)q[a.s]={p:m.p,chg:m.chg,pc:m.pc,h:m.h,l:m.l,vol:m.vol}});ls.set("sx-last-quotes",q);const t={};STOCKS.forEach(a=>t[a.s]=M[a.s].ticks.slice(-1500));ls.set("sx-stock-ticks",t)}

/* ================= price feeds ================= */
const feeds={c:{state:"connecting",last:0},s:{state:FHKEY?"connecting":"nokey",last:0}};
function setPill(k,st){feeds[k].state=st;renderPills()}

// ---- Binance (crypto, gold token, euro) ----
let bn=null,bnIdx=0,bnRetry=0,bnKline=null,bnReq=1;
function bnConnect(){
  const streams=CRYPTO.flatMap(a=>{const l=a.id.toLowerCase();return[l+"@miniTicker",l+"@bookTicker"]}).join("/");
  let ws;try{ws=new WebSocket(BN_WS[bnIdx]+"/stream?streams="+streams)}catch(e){return bnRetryLater(false)}
  bn=ws;if(feeds.c.state!=="live")setPill("c","connecting");
  let opened=false;const t=setTimeout(()=>{if(!opened)try{ws.close()}catch(e){}},8000);
  ws.onopen=()=>{opened=true;clearTimeout(t);bnRetry=0;if(bnKline)bnSend("SUBSCRIBE",[bnKline])};
  ws.onmessage=e=>{let m;try{m=JSON.parse(e.data)}catch(x){return}if(m&&m.data)onBn(m.data)};
  ws.onclose=()=>{clearTimeout(t);if(bn!==ws)return;bn=null;bnRetryLater(opened)};
  ws.onerror=()=>{};
}
function bnRetryLater(opened){if(!opened)bnIdx=(bnIdx+1)%BN_WS.length;bnRetry++;setPill("c",bnRetry>3?"down":"connecting");setTimeout(bnConnect,Math.min(15000,800*bnRetry))}
function bnSend(method,params){if(bn&&bn.readyState===1)bn.send(JSON.stringify({method,params,id:bnReq++}))}
function setKlineSub(stream){if(bnKline===stream)return;if(bnKline)bnSend("UNSUBSCRIBE",[bnKline]);bnKline=stream;if(stream)bnSend("SUBSCRIBE",[stream])}
function onBn(d){
  if(d.e==="24hrMiniTicker"){const a=BYID[d.s];if(!a)return;const m=M[a.s];m.o=+d.o;m.h=+d.h;m.l=+d.l;m.vol=+d.q;setPrice(a,+d.c);m.chg=(m.p/m.o-1)*100;feeds.c.last=Date.now();if(feeds.c.state!=="live")setPill("c","live");return}
  if(d.e==="kline"){const a=BYID[d.s],k=d.k;if(!a||a.s!==chart.sym||k.i!==chart.tf)return;const c={t:k.t,o:+k.o,h:+k.h,l:+k.l,c:+k.c,v:+k.v},cs=chart.c,last=cs[cs.length-1];
    if(last&&last.t===c.t)cs[cs.length-1]=c;else if(!last||c.t>last.t){cs.push(c);if(cs.length>1500)cs.shift()}chartDirty=true;return}
  if(d.b&&d.a&&d.s){const a=BYID[d.s];if(!a)return;const m=M[a.s];m.bid=+d.b;m.ask=+d.a;if(!m.p)m.p=(m.bid+m.ask)/2;runTriggers(a)}
}
async function bnKlines(sym,tf){
  for(let i=0;i<BN_REST.length;i++){const h=BN_REST[(bnIdx+i)%BN_REST.length];
    try{const r=await fetch(`${h}/api/v3/klines?symbol=${sym}&interval=${tf}&limit=500`);if(!r.ok)continue;const j=await r.json();
      return j.map(k=>({t:k[0],o:+k[1],h:+k[2],l:+k[3],c:+k[4],v:+k[5]}))}catch(e){}}
  return null;
}

// ---- Finnhub (US stocks & ETFs) ----
let fh=null,fhRetry=0,fhQuoteTimer=null;
function fhConnect(){
  if(fh){const o=fh;fh=null;try{o.close()}catch(e){}}
  if(!FHKEY){setPill("s","nokey");return}
  setPill("s","connecting");
  let ws;try{ws=new WebSocket("wss://ws.finnhub.io?token="+encodeURIComponent(FHKEY))}catch(e){return}
  fh=ws;
  ws.onopen=()=>{fhRetry=0;STOCKS.forEach(a=>ws.send(JSON.stringify({type:"subscribe",symbol:a.id})));setPill("s",usOpen()?"live":"closed")};
  ws.onmessage=e=>{let m;try{m=JSON.parse(e.data)}catch(x){return}
    if(m.type==="trade"&&m.data){const last={};m.data.forEach(t=>{if(!last[t.s]||t.t>=last[t.s].t)last[t.s]=t});for(const k in last){const a=BYID[k];if(a)stockTick(a,last[k].p,last[k].t)}feeds.s.last=Date.now();if(feeds.s.state!=="live"&&usOpen())setPill("s","live")}
    else if(m.type==="error"){setPill("s","badkey")}};
  ws.onclose=()=>{if(fh!==ws)return;fh=null;fhRetry++;if(feeds.s.state!=="badkey")setPill("s","connecting");setTimeout(fhConnect,Math.min(30000,1500*fhRetry))};
  ws.onerror=()=>{};
  fhQuotes();clearInterval(fhQuoteTimer);fhQuoteTimer=setInterval(fhQuotes,60000);
}
async function fhQuotes(){
  if(!FHKEY)return;
  for(const a of STOCKS){
    try{const r=await fetch(`https://finnhub.io/api/v1/quote?symbol=${a.id}&token=${encodeURIComponent(FHKEY)}`);
      if(r.status===401||r.status===403){setPill("s","badkey");return}
      if(!r.ok)continue;const q=await r.json();if(!(q.c>0))continue;const m=M[a.s];
      m.pc=q.pc;m.h=q.h;m.l=q.l;m.o=q.o;
      if(!usOpen()||!m.p||m.stale||Date.now()-m.last>120000){m.stale=false;setPrice(a,q.c)}
      m.chg=m.pc?(m.p/m.pc-1)*100:q.dp;
    }catch(e){}
    await new Promise(r=>setTimeout(r,120));
  }
  if(feeds.s.state==="connecting"&&!usOpen())setPill("s","closed");
}
function stockTick(a,p,t){const m=M[a.s];m.stale=false;setPrice(a,p);if(m.pc)m.chg=(p/m.pc-1)*100;
  const mt=Math.floor((t||Date.now())/60000)*60000,last=m.ticks[m.ticks.length-1];
  if(last&&last.t===mt){last.h=Math.max(last.h,p);last.l=Math.min(last.l,p);last.c=p}else if(!last||mt>last.t){const o=last?last.c:p;m.ticks.push({t:mt,o,h:Math.max(o,p),l:Math.min(o,p),c:p})}
  if(a.s===chart.sym)chartDirty=true}

function setPrice(a,p){if(!(p>0))return;const m=M[a.s],prev=m.p;m.p=p;m.last=Date.now();
  if(prev&&prev!==p&&Date.now()-m.tapeT>900){m.tape.unshift({t:Date.now(),p,dir:p>prev?1:-1});m.tapeT=Date.now();if(m.tape.length>14)m.tape.length=14}
  if(a.src==="fh"){m.bid=p*(1-0.0001);m.ask=p*(1+0.0001)}
  runTriggers(a)}

/* ================= trading engine ================= */
const live=a=>M[a.s].p>0&&!M[a.s].stale&&Date.now()-M[a.s].last<(a.src==="bn"?30000:a.hours==="us"&&usOpen()?300000:Infinity);
const bid=a=>M[a.s].bid||M[a.s].p, ask=a=>M[a.s].ask||M[a.s].p;
const markOf=sym=>M[sym].p||(S.positions[sym]&&S.positions[sym].entry)||0;
const upnl=p=>p.qty*(markOf(p.sym)-p.entry);
const holds=()=>S.orders.reduce((s,o)=>s+(o.hold||0),0);
function equity(){let e=S.cash+holds();for(const k in S.positions){const p=S.positions[k];e+=p.margin+upnl(p)}return e}
function marginUsed(){let m=0;for(const k in S.positions)m+=S.positions[k].margin;return m+holds()}
function needFor(sym,side,qty,px,lev){const pos=S.positions[sym],sg=side==="buy"?1:-1;let open=qty;if(pos&&Math.sign(pos.qty)!==sg)open=Math.max(0,qty-Math.abs(pos.qty));return open*px/lev+qty*px*FEE}
let dirtyAcct=false;
function fill(sym,side,qty,px,lev,sl,tp,tag){
  const a=A[sym],sg=side==="buy"?1:-1,fee=qty*px*FEE;let pos=S.positions[sym],rem=qty,pnl=0;
  if(pos&&Math.sign(pos.qty)!==sg){const cq=Math.min(rem,Math.abs(pos.qty)),fr=cq/Math.abs(pos.qty),g=cq*(px-pos.entry)*Math.sign(pos.qty),mr=pos.margin*fr;
    S.cash+=mr+g;pos.margin-=mr;pos.qty-=Math.sign(pos.qty)*cq;pnl+=g;rem-=cq;if(Math.abs(pos.qty)<1e-12){delete S.positions[sym];pos=null}}
  if(rem>1e-12){const m=rem*px/lev;
    if(pos){const q=Math.abs(pos.qty)+rem;pos.entry=(pos.entry*Math.abs(pos.qty)+px*rem)/q;pos.qty=sg*q;pos.margin+=m;pos.lev=Math.max(1,Math.round(q*pos.entry/pos.margin))}
    else S.positions[sym]=pos={sym,qty:sg*rem,entry:px,margin:m,lev,sl:null,tp:null,t:Date.now()};
    S.cash-=m;if(sl)pos.sl=sl;if(tp)pos.tp=tp}
  S.cash-=fee;S.fees+=fee;S.realized+=pnl;if(pnl!==0){if(pnl-fee>0)S.wins++;else S.losses++}
  S.fills.unshift({id:S.nid++,t:Date.now(),sym,side,qty,px,fee,pnl,tag});if(S.fills.length>500)S.fills.length=500;
  toast(`${tag?tag+" · ":""}${side==="buy"?"Bought":"Sold"} ${fq(qty,a)} ${sym} @ ${fx(px,a.d)}${pnl?` · P&L ${sgnUsd(pnl)}`:""}`,side==="buy"?"b":"s");
  if(navigator.vibrate)try{navigator.vibrate(15)}catch(e){}
  dirtyAcct=true;save();
}
function validSlTp(o,px){const L=o.side==="buy";if(o.sl&&(L?o.sl>=px:o.sl<=px))return false;if(o.tp&&(L?o.tp<=px:o.tp>=px))return false;return true}
const slTpMsg=o=>o.side==="buy"?"For a long, stop-loss must be below the entry price and take-profit above it.":"For a short, stop-loss must be above the entry price and take-profit below it.";
function placeOrder(o){
  const a=A[o.sym];
  if(!(o.qty>0))return "Enter an amount above zero.";
  if(o.type==="market"){
    if(!isOpen(a))return "The US market is closed (it opens 16:30 Beirut time, Mon–Fri). Place a limit order instead; it fills when trading resumes.";
    if(!live(a))return a.src==="fh"&&!FHKEY?"Stocks need your free Finnhub key. Tap ⚙ Settings to add it.":`No live price for ${a.s} right now. Wait for the feed to reconnect.`;
    const px=o.side==="buy"?ask(a):bid(a),need=needFor(o.sym,o.side,o.qty,px,o.lev);
    if(need>S.cash+1e-6)return `Not enough buying power. This needs ${usd(need)} and you have ${usd(S.cash)}.`;
    if(!validSlTp(o,px))return slTpMsg(o);
    fill(o.sym,o.side,o.qty,px,o.lev,o.sl,o.tp,"Market");return null}
  if(!(o.price>0))return "Enter a price for this order.";
  const need=needFor(o.sym,o.side,o.qty,o.price,o.lev);
  if(need>S.cash+1e-6)return `Not enough buying power. This needs ${usd(need)} and you have ${usd(S.cash)}.`;
  if(!validSlTp(o,o.price))return slTpMsg(o);
  o.id=S.nid++;o.t=Date.now();o.hold=need;S.cash-=need;S.orders.push(o);dirtyAcct=true;save();
  toast(`${o.type==="limit"?"Limit":"Stop"} ${o.side} ${fq(o.qty,a)} ${o.sym} @ ${fx(o.price,a.d)} placed`);return null;
}
function cancel(id){const i=S.orders.findIndex(o=>o.id===id);if(i<0)return;S.cash+=S.orders[i].hold||0;S.orders.splice(i,1);dirtyAcct=true}
function closePos(sym,frac,tag){const p=S.positions[sym];if(!p)return "No position.";const a=A[sym];
  if(!tag&&!isOpen(a))return `${a.s} trades only while the US market is open. Set a stop-loss / take-profit instead.`;
  if(!live(a))return "No live price right now, so the position can't be closed yet.";
  const q=Math.abs(p.qty)*frac,side=p.qty>0?"sell":"buy";fill(sym,side,q,side==="sell"?bid(a):ask(a),p.lev,null,null,tag||"Close");return null}
function runTriggers(a){
  if(!live(a)||!isOpen(a))return;
  const sym=a.s,b=bid(a),k=ask(a);
  if(S.orders.length)for(const o of S.orders.slice()){if(o.sym!==sym)continue;let px=null;
    if(o.type==="limit"){if(o.side==="buy"&&k<=o.price)px=Math.min(o.price,k);if(o.side==="sell"&&b>=o.price)px=Math.max(o.price,b)}
    else{if(o.side==="buy"&&k>=o.price)px=k;if(o.side==="sell"&&b<=o.price)px=b}
    if(px==null)continue;cancel(o.id);
    if(needFor(o.sym,o.side,o.qty,px,o.lev)>S.cash+1e-6){toast(`${o.sym} ${o.type} order rejected: not enough margin at fill`,"s");continue}
    fill(o.sym,o.side,o.qty,px,o.lev,o.sl,o.tp,o.type==="limit"?"Limit":"Stop")}
  const p=S.positions[sym];if(!p)return;const L=p.qty>0;
  if(p.margin+upnl(p)<=p.margin*(1-LIQ))return void closePos(sym,1,"Liquidated");
  if(p.sl&&(L?b<=p.sl:k>=p.sl))return void closePos(sym,1,"Stop-loss");
  if(p.tp&&(L?b>=p.tp:k<=p.tp))return void closePos(sym,1,"Take-profit");
}
const liqPrice=p=>p.entry-Math.sign(p.qty)*(p.margin*LIQ)/Math.abs(p.qty);

/* ================= UI state ================= */
let sel=ls.get("sx-sel","BTC");if(!A[sel])sel="BTC";
let fCls="All",side="buy",otype="market",unit="usd",lev=1,zoom=8,offset=0,btab="pos",ptab="pos",editing=null,hover=null;
let tf="1m",chart={sym:null,tf:null,c:[],loading:false},chartDirty=true;
const shown={};

/* watchlist */
const classes=["All","Crypto","Stocks","ETFs","Forex","Commodities"];
$("cls").innerHTML=classes.map(c=>`<button data-v="${c}" class="${c==="All"?"on":""}">${c}</button>`).join("");
$("cls").onclick=e=>{const b=e.target.closest("button");if(!b)return;fCls=b.dataset.v;[...$("cls").children].forEach(x=>x.classList.toggle("on",x===b));buildWL()};
$("q").oninput=buildWL;
function buildWL(){const q=$("q").value.trim().toLowerCase();
  const rows=LIST.filter(a=>(fCls==="All"||a.c===fCls)&&(!q||a.s.toLowerCase().includes(q)||a.n.toLowerCase().includes(q)));
  $("wl").innerHTML=rows.length?rows.map(a=>`<button class="wrow ${a.s===sel?"sel":""}" data-s="${a.s}"><span class="ico" style="background:${CLS_COL[a.c]}">${a.s.slice(0,3)}</span><span style="min-width:0"><div class="wsym">${a.s}<span class="closed" id="wx_${a.s}" ${isOpen(a)?"hidden":""}>closed</span></div><div class="wname">${esc(a.n)}</div></span><span class="wpx"><span id="w_${a.s}">–</span><small id="wc_${a.s}"></small></span></button>`).join(""):`<div class="empty">No market matches “${esc(q)}”.</div>`;
  for(const k in shown)delete shown[k];updWL()}
$("wl").onclick=e=>{const b=e.target.closest(".wrow");if(!b)return;select(b.dataset.s);if(isPhone())setView("trade")};
function updWL(){for(const a of LIST){const el=document.getElementById("w_"+a.s);if(!el)continue;const m=M[a.s];if(!(m.p>0))continue;
  el.textContent=fx(m.p,a.d);const pp=shown[a.s];if(pp!=null&&pp!==m.p){const row=el.parentNode.parentNode;row.classList.remove("flash-up","flash-down");void row.offsetWidth;row.classList.add(m.p>pp?"flash-up":"flash-down")}shown[a.s]=m.p;
  const ce=document.getElementById("wc_"+a.s);if(isFinite(m.chg)&&m.chg!=null){ce.textContent=pct(m.chg);ce.className=cls(m.chg)}
  const x=document.getElementById("wx_"+a.s);if(x)x.hidden=isOpen(a)}}
function select(s){sel=s;ls.set("sx-sel",s);offset=0;hover=null;buildWL();setupTf();setupTicket();loadChart();renderSym();renderQuote()}

/* symbol header + quote */
function renderSym(){const a=A[sel],m=M[sel];
  $("sName").textContent=a.s;$("sFull").textContent=a.n;
  $("sPx").textContent=m.p>0?fx(m.p,a.d):"–";$("sPx").className="big "+cls(m.chg||0);
  const st=a.src==="fh"&&!FHKEY?"Add Finnhub key in ⚙":isOpen(a)?(a.hours==="24/7"?"Open 24/7":"US market open"):"US market closed";
  $("sStat").innerHTML=`<i class="dot ${isOpen(a)&&live(a)?"on":""}"></i>${st}`;
  $("sChgL").textContent=a.src==="bn"?"24h change":"Today";
  $("sChg").textContent=isFinite(m.chg)&&m.chg!=null?pct(m.chg):"–";$("sChg").className=cls(m.chg||0);
  $("sHi").textContent=m.h>0?fx(m.h,a.d):"–";$("sLo").textContent=m.l>0?fx(m.l,a.d):"–";
  $("sHiL").textContent=a.src==="bn"?"24h high":"Day high";$("sLoL").textContent=a.src==="bn"?"24h low":"Day low";
  const pos=S.positions[sel];
  if(pos){const u=upnl(pos);$("sPos").innerHTML=`<span class="${pos.qty>0?"up":"down"}">${pos.qty>0?"Long":"Short"} ${fq(Math.abs(pos.qty),a)}</span> <span class="${cls(u)}">${sgnUsd(u)}</span>`}else $("sPos").textContent="None"}
function renderQuote(){const a=A[sel],m=M[sel];$("bkSym").textContent=a.s;
  $("qBid").textContent=m.bid>0?fx(bid(a),a.d):"–";$("qAsk").textContent=m.ask>0?fx(ask(a),a.d):"–";
  $("qSpr").textContent=m.bid>0&&m.ask>0?fx(ask(a)-bid(a),a.d)+(a.src==="fh"?" (est.)":""):"–";
  $("qVol").textContent=m.vol>0?"$"+compact(m.vol):"–";
  $("qSrc").textContent=a.src==="bn"?"Binance":"Finnhub";
  $("qUpd").textContent=m.last?Math.max(0,Math.round((Date.now()-m.last)/1000))+"s ago":"–";
  $("tape").innerHTML=`<div class="tapeh">Recent price changes</div>`+(m.tape.length?m.tape.map(t=>`<div class="trow"><span class="muted">${hhmmss(t.t)}</span><span class="${t.dir>0?"up":"down"}">${t.dir>0?"▲":"▼"} ${fx(t.p,a.d)}</span></div>`).join(""):`<div class="muted" style="padding:6px 12px;font-size:12px">${isOpen(a)?"Changes appear here as trades come in.":"Market closed. Last price shown."}</div>`)}

/* pills */
function renderPills(){
  const c=feeds.c,s=feeds.s;
  const ct={live:"Crypto live",connecting:"Crypto connecting…",down:"Crypto offline · retrying"}[c.state];
  const st={live:"Stocks live",connecting:"Stocks connecting…",closed:"Stocks · US market closed",nokey:"Stocks · add key",badkey:"Stocks · key rejected"}[s.state];
  $("pillC").className="pill "+c.state;$("pillC").lastChild.textContent=ct;
  $("pillS").className="pill "+s.state;$("pillS").lastChild.textContent=st;
}
$("pillS").onclick=()=>{if(feeds.s.state==="nokey"||feeds.s.state==="badkey")openSettings()};

/* ================= chart ================= */
function setupTf(){const a=A[sel],list=a.src==="bn"?TF_BN:TF_FH;if(!list.includes(tf))tf="1m";
  $("tf").innerHTML=list.map(v=>`<button data-v="${v}" class="${v===tf?"on":""}">${v.toUpperCase()==="1D"?"1D":v}</button>`).join("")}
$("tf").onclick=e=>{const b=e.target.closest("button");if(!b)return;tf=b.dataset.v;[...$("tf").children].forEach(x=>x.classList.toggle("on",x===b));offset=0;loadChart()};
const TFMIN={"1m":1,"5m":5,"15m":15};
function agg(c,m){if(m===1)return c.slice();const out=[];for(const k of c){const t=Math.floor(k.t/(m*60000))*m*60000,l=out[out.length-1];if(l&&l.t===t){l.h=Math.max(l.h,k.h);l.l=Math.min(l.l,k.l);l.c=k.c}else out.push({t,o:k.o,h:k.h,l:k.l,c:k.c,v:0})}return out}
async function loadChart(){const a=A[sel],want={sym:sel,tf};chart={sym:sel,tf,c:[],loading:true};chartDirty=true;
  if(a.src==="bn"){setKlineSub(a.id.toLowerCase()+"@kline_"+tf);const rows=await bnKlines(a.id,tf);if(chart.sym!==want.sym||chart.tf!==want.tf)return;
    if(rows){const live=chart.c;chart.c=rows;if(live.length){const l=live[live.length-1],r=rows[rows.length-1];if(l.t===r.t)rows[rows.length-1]=l;else if(l.t>r.t)rows.push(l)}}
    chart.loading=false;chart.err=!rows;chartDirty=true}
  else{setKlineSub(null);chart.loading=false;chart.c=agg(M[sel].ticks,TFMIN[tf]);chartDirty=true}}
function niceStep(r){const e=Math.pow(10,Math.floor(Math.log10(r))),f=r/e;return(f<1.5?1:f<3?2:f<7?5:10)*e}
const css=n=>getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const COL={};["--up","--down","--accent","--muted","--faint","--line","--line2","--fg","--bg"].forEach(k=>COL[k]=css(k));
const MONO=css("--mono"),SANS=css("--sans");
const cv=$("chart"),ctx=cv.getContext("2d");let CW=0,CH=0,geo=null;
function sizeChart(){const r=cv.getBoundingClientRect(),d=window.devicePixelRatio||1;CW=r.width;CH=r.height;cv.width=Math.max(1,Math.round(CW*d));cv.height=Math.max(1,Math.round(CH*d));ctx.setTransform(d,0,0,d,0,0);chartDirty=true}
new ResizeObserver(sizeChart).observe(cv);
function tlabel(t){return tf==="1d"?dmy(t):tf==="4h"||tf==="1h"?dmy(t)+" "+hhmm(t):tf==="1s"?hhmmss(t):hhmm(t)}
function drawChart(){
  if(!CW||!CH)return;const a=A[sel],m=M[sel];
  if(A[sel].src==="fh")chart.c=agg(m.ticks,TFMIN[tf]||1);
  let cs=chart.c;
  // keep last candle in sync with the live price between kline pushes
  if(a.src==="bn"&&cs.length&&m.p>0){const l=cs[cs.length-1];l.c=m.p;l.h=Math.max(l.h,m.p);l.l=Math.min(l.l,m.p)}
  const axisW=68,axisH=20,W=CW-axisW,H=CH-axisH;ctx.clearRect(0,0,CW,CH);
  const ce=$("cempty");
  if(cs.length<2){ce.hidden=false;
    ce.innerHTML=chart.loading?"Loading chart…":a.src==="fh"?(FHKEY?`Stock candles build from live trades while the app is open${isOpen(a)?"":" and the US market is open"}.<br>Come back during market hours (16:30–23:00 Beirut).`:`Add your free Finnhub key in <b>⚙ Settings</b> to stream stock prices.`):chart.err?"Couldn't load chart history. Check your connection.":"Waiting for data…";
    geo=null;$("legend").innerHTML="";return}
  ce.hidden=true;
  const n=Math.max(10,Math.floor(W/zoom));offset=Math.min(Math.max(0,offset),Math.max(0,cs.length-5));
  const end=cs.length-Math.round(offset),start=Math.max(0,end-n),vis=cs.slice(start,end);if(!vis.length)return;
  let hi=-Infinity,lo=Infinity,vmax=0;vis.forEach(k=>{hi=Math.max(hi,k.h);lo=Math.min(lo,k.l);vmax=Math.max(vmax,k.v||0)});
  const pad=(hi-lo)*.08||hi*.002;hi+=pad;lo-=pad;
  const top=24,mainH=H-top-4,y=p=>top+(hi-p)/(hi-lo)*mainH,x=i=>(i+.5)*zoom+(W-vis.length*zoom);
  geo={hi,lo,top,mainH,W,H};
  ctx.font="11px "+MONO;ctx.textBaseline="middle";ctx.lineWidth=1;
  const st=niceStep((hi-lo)/6);ctx.strokeStyle=COL["--line"];ctx.fillStyle=COL["--muted"];
  for(let p=Math.ceil(lo/st)*st;p<=hi;p+=st){const yy=Math.round(y(p))+.5;ctx.beginPath();ctx.moveTo(0,yy);ctx.lineTo(W,yy);ctx.stroke();ctx.textAlign="left";ctx.fillText(fx(p,a.d),W+6,yy)}
  ctx.textAlign="center";ctx.textBaseline="top";const every=Math.max(1,Math.ceil(100/zoom));
  vis.forEach((k,i)=>{if((start+i)%every)return;const xx=Math.round(x(i))+.5;ctx.strokeStyle=COL["--line"];ctx.beginPath();ctx.moveTo(xx,top);ctx.lineTo(xx,H);ctx.stroke();ctx.fillStyle=COL["--faint"];ctx.fillText(tlabel(k.t),xx,H+4)});
  if(vmax>0){const vh=mainH*.16;vis.forEach((k,i)=>{const h=(k.v||0)/vmax*vh;ctx.fillStyle=(k.c>=k.o?COL["--up"]:COL["--down"])+"40";ctx.fillRect(x(i)-zoom*.35,H-h,Math.max(1,zoom*.7),h)})}
  vis.forEach((k,i)=>{const c=k.c>=k.o?COL["--up"]:COL["--down"],xx=Math.round(x(i));ctx.strokeStyle=c;ctx.fillStyle=c;ctx.beginPath();ctx.moveTo(xx+.5,y(k.h));ctx.lineTo(xx+.5,y(k.l));ctx.stroke();
    const bw=Math.max(1,Math.floor(zoom*.7)),y1=y(Math.max(k.o,k.c)),y2=y(Math.min(k.o,k.c));ctx.fillRect(xx-Math.floor(bw/2),y1,bw,Math.max(1,y2-y1))});
  const ma=(len,col)=>{ctx.strokeStyle=col;ctx.lineWidth=1.4;ctx.beginPath();let first=true;for(let i=0;i<vis.length;i++){const gi=start+i;if(gi<len-1)continue;let s=0;for(let j=gi-len+1;j<=gi;j++)s+=cs[j].c;const yy=y(s/len);if(first){ctx.moveTo(x(i),yy);first=false}else ctx.lineTo(x(i),yy)}ctx.stroke();ctx.lineWidth=1};
  if($("ma20").checked)ma(20,COL["--accent"]);if($("ma50").checked)ma(50,"#7aa7ff");
  const line=(p,col,label,dash)=>{if(!(p>lo&&p<hi))return;const yy=Math.round(y(p))+.5;ctx.setLineDash(dash||[4,4]);ctx.strokeStyle=col;ctx.beginPath();ctx.moveTo(0,yy);ctx.lineTo(W,yy);ctx.stroke();ctx.setLineDash([]);
    ctx.font="10px "+SANS;const tw=ctx.measureText(label).width+10;ctx.fillStyle=col;ctx.fillRect(4,yy-8,tw,16);ctx.fillStyle=COL["--bg"];ctx.textAlign="left";ctx.textBaseline="middle";ctx.fillText(label,9,yy);ctx.font="11px "+MONO};
  const pos=S.positions[sel];
  if(pos){line(pos.entry,pos.qty>0?COL["--up"]:COL["--down"],`${pos.qty>0?"LONG":"SHORT"} ${fq(Math.abs(pos.qty),a)}  ${sgnUsd(upnl(pos))}`,[6,3]);
    if(pos.sl)line(pos.sl,COL["--down"],"SL "+fx(pos.sl,a.d));if(pos.tp)line(pos.tp,COL["--up"],"TP "+fx(pos.tp,a.d));if(pos.lev>1)line(liqPrice(pos),COL["--muted"],"LIQ "+fx(liqPrice(pos),a.d),[2,4])}
  S.orders.filter(o=>o.sym===sel).forEach(o=>line(o.price,COL["--accent"],`${o.type.toUpperCase()} ${o.side.toUpperCase()} ${fq(o.qty,a)}`));
  const lp=m.p>0?m.p:vis[vis.length-1].c,lv=vis[vis.length-1],lc=lv.c>=lv.o?COL["--up"]:COL["--down"],ly=Math.round(y(Math.min(hi,Math.max(lo,lp))))+.5;
  ctx.setLineDash([1,3]);ctx.strokeStyle=lc;ctx.beginPath();ctx.moveTo(0,ly);ctx.lineTo(W,ly);ctx.stroke();ctx.setLineDash([]);
  ctx.fillStyle=lc;ctx.fillRect(W,ly-9,axisW,18);ctx.fillStyle="#08100c";ctx.textAlign="left";ctx.textBaseline="middle";ctx.font="600 11px "+MONO;ctx.fillText(fx(lp,a.d),W+5,ly);ctx.font="11px "+MONO;
  let k=vis[vis.length-1];
  if(hover&&hover.x<W&&hover.y<H){const i=Math.min(vis.length-1,Math.max(0,Math.floor((hover.x-(W-vis.length*zoom))/zoom)));k=vis[i]||k;
    ctx.strokeStyle=COL["--muted"];ctx.setLineDash([3,3]);ctx.beginPath();ctx.moveTo(Math.round(x(i))+.5,top);ctx.lineTo(Math.round(x(i))+.5,H);ctx.moveTo(0,hover.y+.5);ctx.lineTo(W,hover.y+.5);ctx.stroke();ctx.setLineDash([]);
    const hp=hi-(hover.y-top)/mainH*(hi-lo);ctx.fillStyle=COL["--line2"];ctx.fillRect(W,hover.y-9,axisW,18);ctx.fillStyle=COL["--fg"];ctx.textAlign="left";ctx.textBaseline="middle";ctx.fillText(fx(hp,a.d),W+5,hover.y)}
  const ch=(k.c/k.o-1)*100;
  $("legend").innerHTML=`<span>${tlabel(k.t)}</span><span>O <b>${fx(k.o,a.d)}</b></span><span>H <b>${fx(k.h,a.d)}</b></span><span>L <b>${fx(k.l,a.d)}</b></span><span>C <b class="${cls(ch)}">${fx(k.c,a.d)}</b></span><span class="${cls(ch)}">${pct(ch)}</span>`;
  $("hint").textContent=otype!=="market"?"Tap chart to set "+otype+" price":"";
}
let drag=null;
cv.addEventListener("pointerdown",e=>{drag={x:e.clientX,o:offset,moved:false};try{cv.setPointerCapture(e.pointerId)}catch(x){}});
cv.addEventListener("pointermove",e=>{const r=cv.getBoundingClientRect();hover={x:e.clientX-r.left,y:e.clientY-r.top};if(drag){const dx=e.clientX-drag.x;if(Math.abs(dx)>4)drag.moved=true;if(drag.moved)offset=drag.o+dx/zoom}chartDirty=true});
cv.addEventListener("pointerup",e=>{if(drag&&!drag.moved&&geo&&otype!=="market"){const r=cv.getBoundingClientRect(),yy=e.clientY-r.top;if(yy>geo.top&&yy<geo.H){const p=geo.hi-(yy-geo.top)/geo.mainH*(geo.hi-geo.lo);$("oPx").value=p.toFixed(A[sel].d);updTicket()}}drag=null;if(e.pointerType!=="mouse"){hover=null;chartDirty=true}});
cv.addEventListener("pointerleave",()=>{hover=null;chartDirty=true});
cv.addEventListener("wheel",e=>{e.preventDefault();zoom=Math.min(30,Math.max(2,zoom*(e.deltaY<0?1.12:.89)));chartDirty=true},{passive:false});
$("zIn").onclick=()=>{zoom=Math.min(30,zoom*1.25);chartDirty=true};$("zOut").onclick=()=>{zoom=Math.max(2,zoom*.8);chartDirty=true};$("zLive").onclick=()=>{offset=0;chartDirty=true};
$("ma20").onchange=$("ma50").onchange=()=>{chartDirty=true};

/* ================= ticket ================= */
function setupTicket(){const a=A[sel],levs=[1,2,3,5,10,20].filter(v=>v<=a.maxLev);if(a.maxLev===4)levs.push(4);levs.sort((x,y)=>x-y);if(lev>a.maxLev)lev=1;
  $("lev").innerHTML=levs.map(v=>`<button data-v="${v}" class="${v===lev?"on":""}">${v}×</button>`).join("");$("levMax").textContent="max "+a.maxLev+"×";
  if(otype!=="market"&&M[sel].p>0)$("oPx").value=M[sel].p.toFixed(a.d);$("oSl").value="";$("oTp").value="";updTicket()}
$("lev").onclick=e=>{const b=e.target.closest("button");if(!b)return;lev=+b.dataset.v;[...$("lev").children].forEach(x=>x.classList.toggle("on",x===b));updTicket()};
[$("sideB"),$("sideS")].forEach(b=>b.onclick=()=>{side=b.dataset.side;$("sideB").classList.toggle("on",side==="buy");$("sideS").classList.toggle("on",side==="sell");updTicket()});
$("otype").onclick=e=>{const b=e.target.closest("button");if(!b)return;otype=b.dataset.v;[...$("otype").children].forEach(x=>x.classList.toggle("on",x===b));$("pxField").hidden=otype==="market";$("pxLbl").textContent=otype==="limit"?"Limit price":"Stop trigger price";if(otype!=="market"&&!$("oPx").value&&M[sel].p>0)$("oPx").value=M[sel].p.toFixed(A[sel].d);updTicket();chartDirty=true};
$("unitBtn").onclick=()=>{const a=A[sel],px=refPx(),v=parse($("oAmt").value);if(!(px>0))return;unit=unit==="usd"?"qty":"usd";if(isFinite(v))$("oAmt").value=unit==="qty"?(v/px).toFixed(Math.min(qd(a),6)):(v*px).toFixed(2);updTicket()};
$("pct").onclick=e=>{const b=e.target.closest("button");if(!b)return;const f=+b.dataset.v/100,a=A[sel],px=refPx();if(!(px>0))return;const u=S.cash*f*lev/(1+FEE*lev)*.999;$("oAmt").value=unit==="usd"?u.toFixed(2):(u/px).toFixed(Math.min(qd(a),6));updTicket()};
["oAmt","oPx","oSl","oTp"].forEach(id=>$(id).addEventListener("input",updTicket));
$("slTpOn").onchange=()=>{$("slTp").hidden=!$("slTpOn").checked;if($("slTpOn").checked&&!$("oSl").value){const a=A[sel],p=refPx(),L=side==="buy";if(p>0){$("oSl").value=(p*(L?.97:1.03)).toFixed(a.d);$("oTp").value=(p*(L?1.06:.94)).toFixed(a.d)}}updTicket()};
function refPx(){const a=A[sel];if(otype!=="market"){const v=parse($("oPx").value);if(v>0)return v}return M[sel].p>0?(side==="buy"?ask(a):bid(a)):NaN}
function readOrder(){const px=refPx(),v=parse($("oAmt").value),qty=unit==="usd"?v/px:v,on=$("slTpOn").checked;
  return{sym:sel,side,type:otype,qty:Math.round(qty*1e8)/1e8,price:otype==="market"?null:px,lev,sl:on?parse($("oSl").value)||null:null,tp:on?parse($("oTp").value)||null:null,px}}
function updTicket(){const a=A[sel],o=readOrder(),pos=S.positions[sel];
  $("unitBtn").textContent=unit==="usd"?"USD":a.s;$("avail").textContent=usd(S.cash);
  const notional=o.qty*o.px,ok=isFinite(notional),need=ok?needFor(sel,side,o.qty,o.px,lev):0,closing=pos&&Math.sign(pos.qty)!==(side==="buy"?1:-1);
  const liq=lev>1&&o.qty>0?o.px*(1-(side==="buy"?1:-1)*LIQ/lev):null;
  let rr="";if(o.sl&&o.tp&&o.qty>0)rr=`<dt>Risk / reward</dt><dd><span class="down">-${usd(Math.abs(o.px-o.sl)*o.qty,0)}</span> / <span class="up">+${usd(Math.abs(o.tp-o.px)*o.qty,0)}</span></dd>`;
  $("sum").innerHTML=`<dt>${otype==="market"?"Est. fill":"Order price"}</dt><dd>${ok?fx(o.px,a.d):"–"}</dd><dt>Quantity</dt><dd>${ok?fq(o.qty,a):"–"} ${a.s}</dd><dt>Position value</dt><dd>${ok?usd(notional):"–"}</dd><dt>Margin + fee</dt><dd>${usd(need)}</dd>${liq&&!closing?`<dt>Liquidation ≈</dt><dd>${fx(liq,a.d)}</dd>`:""}${rr}${closing?`<dt>Effect</dt><dd class="muted">reduces your ${pos.qty>0?"long":"short"}</dd>`:""}${otype==="market"&&!isOpen(a)?`<dt class="down">Market closed</dt><dd class="muted">use a limit order</dd>`:""}`;
  const verb=side==="buy"?(closing?"Buy to cover":"Buy / Long"):(closing?"Sell to close":"Sell / Short");
  $("submit").textContent=`${verb} ${a.s}`;$("submit").className="submit "+(side==="buy"?"b":"s");$("submit").disabled=!(o.qty>0)}
$("submit").onclick=()=>{const o=readOrder();delete o.px;const err=placeOrder(o);$("msg").textContent=err||"";$("msg").className="msg "+(err?"down":"");if(!err)renderAll()};

/* ================= blotter ================= */
function blotHTML(tab){
  if(tab==="pos"){const ps=Object.values(S.positions);if(!ps.length)return`<div class="empty">No open positions. Pick a market and place your first trade.</div>`;
    return`<div class="grow h"><div>Market</div><div>Size</div><div>Entry</div><div>Mark</div><div>P&amp;L</div><div>SL / TP</div><div></div></div>`+ps.map(p=>{const a=A[p.sym],u=upnl(p),r=u/p.margin*100;
      return`<div class="grow"><div class="s"><span><button data-sel="${p.sym}" style="font-weight:600">${p.sym}</button><span class="pill2 ${p.qty>0?"long":"short"}">${p.qty>0?"LONG":"SHORT"} ${p.lev}×</span></span></div>
      <div data-l="Size">${fq(Math.abs(p.qty),a)}<div class="muted" style="font-size:11px">${usd(Math.abs(p.qty)*markOf(p.sym),0)}</div></div><div data-l="Entry">${fx(p.entry,a.d)}</div><div data-l="Mark">${fx(markOf(p.sym),a.d)}</div>
      <div data-l="P&L" class="${cls(u)}">${sgnUsd(u)}<div style="font-size:11px">${pct(r)}</div></div><div data-l="SL / TP" class="muted">${p.sl?fx(p.sl,a.d):"–"} / ${p.tp?fx(p.tp,a.d):"–"}</div>
      <div class="acts"><button class="sm" data-edit="${p.sym}">SL/TP</button><button class="sm" data-half="${p.sym}">Close ½</button><button class="sm x" data-close="${p.sym}">Close</button></div>
      ${editing===p.sym?`<div class="edit"><span class="muted">Stop-loss</span><div class="inp"><input id="eSl" inputmode="decimal" value="${p.sl?p.sl.toFixed(a.d):""}"></div><span class="muted">Take-profit</span><div class="inp"><input id="eTp" inputmode="decimal" value="${p.tp?p.tp.toFixed(a.d):""}"></div><button class="sm" data-save="${p.sym}">Save</button><span class="down" id="eMsg" style="font-size:12px"></span></div>`:""}</div>`}).join("")}
  if(tab==="ord"){if(!S.orders.length)return`<div class="empty">No pending orders. Limit and stop orders wait here until price reaches them.</div>`;
    return`<div class="grow h"><div>Market</div><div>Type</div><div>Price</div><div>Size</div><div>Distance</div><div>Reserved</div><div></div></div>`+S.orders.map(o=>{const a=A[o.sym],d=M[o.sym].p>0?(o.price/M[o.sym].p-1)*100:0;
      return`<div class="grow"><div class="s"><span>${o.sym}<span class="pill2 ${o.side==="buy"?"long":"short"}">${o.side.toUpperCase()} ${o.lev}×</span></span></div><div data-l="Type">${o.type}</div><div data-l="Price">${fx(o.price,a.d)}</div><div data-l="Size">${fq(o.qty,a)}</div><div data-l="Distance" class="muted">${pct(d)}</div><div data-l="Reserved">${usd(o.hold,0)}</div><div class="acts"><button class="sm x" data-cancel="${o.id}">Cancel</button></div></div>`}).join("")}
  if(!S.fills.length)return`<div class="empty">Your filled trades will be listed here.</div>`;
  return`<div class="grow h"><div>Market</div><div>Time</div><div>Price</div><div>Size</div><div>Fee</div><div>Realized</div><div></div></div>`+S.fills.slice(0,150).map(f=>{const a=A[f.sym]||{d:2,c:"Stocks",src:"fh"};
    return`<div class="grow"><div class="s"><span>${f.sym}<span class="pill2 ${f.side==="buy"?"long":"short"}">${f.side.toUpperCase()}</span><span class="pill2 n">${esc(f.tag||"")}</span></span></div><div data-l="Time" class="muted">${dmy(f.t)} ${hhmm(f.t)}</div><div data-l="Price">${fx(f.px,a.d)}</div><div data-l="Size">${fq(f.qty,a)}</div><div data-l="Fee" class="muted">${usd(f.fee)}</div><div data-l="Realized" class="${cls(f.pnl)}">${f.pnl?sgnUsd(f.pnl):"–"}</div><div></div></div>`}).join("")}
function blotClick(e){const t=e.target.closest("button");if(!t)return;const d=t.dataset;
  if(d.close||d.half){const err=closePos(d.close||d.half,d.close?1:.5);if(err)toast(err,"s");renderAll()}
  else if(d.cancel){cancel(+d.cancel);save();toast("Order cancelled");renderAll()}
  else if(d.edit){editing=editing===d.edit?null:d.edit;renderBlot(true)}
  else if(d.save){const p=S.positions[d.save],sl=parse($("eSl").value),tp=parse($("eTp").value),mk=markOf(d.save),L=p.qty>0;
    if(isFinite(sl)&&(L?sl>=mk:sl<=mk)){$("eMsg").textContent=L?"Stop-loss must be below the current price.":"Stop-loss must be above the current price.";return}
    if(isFinite(tp)&&(L?tp<=mk:tp>=mk)){$("eMsg").textContent=L?"Take-profit must be above the current price.":"Take-profit must be below the current price.";return}
    p.sl=isFinite(sl)?sl:null;p.tp=isFinite(tp)?tp:null;editing=null;save();toast(`${d.save} stop-loss / take-profit updated`);renderAll()}
  else if(d.sel){select(d.sel);if(isPhone())setView("trade")}}
$("blot").onclick=blotClick;$("pblot").onclick=blotClick;
$("btabs").onclick=e=>{const b=e.target.closest("button");if(!b)return;btab=b.dataset.v;[...$("btabs").children].forEach(x=>x.classList.toggle("on",x===b));renderBlot(true)};
$("ptabs").onclick=e=>{const b=e.target.closest("button");if(!b)return;ptab=b.dataset.v;[...$("ptabs").children].forEach(x=>x.classList.toggle("on",x===b));renderBlot(true)};
function renderBlot(force){if(editing&&!force)return;$("blot").innerHTML=blotHTML(btab);$("pblot").innerHTML=blotHTML(ptab);
  $("cPos").textContent=Object.keys(S.positions).length;$("cOrd").textContent=S.orders.length;$("cHist").textContent=S.fills.length}

/* ================= portfolio ================= */
function renderPF(){const e=equity(),tot=e-S.start,un=Object.values(S.positions).reduce((s,p)=>s+upnl(p),0),n=S.wins+S.losses;
  $("kpis").innerHTML=[["Equity",usd(e),""],["Total return",pct(tot/S.start*100),cls(tot)],["Unrealized P&L",sgnUsd(un),cls(un)],["Realized P&L",sgnUsd(S.realized),cls(S.realized)],["Fees paid",usd(S.fees),""],["Available cash",usd(S.cash),""],["Margin in use",usd(marginUsed()),""],["Win rate",n?Math.round(S.wins/n*100)+"% of "+n:"–",""]].map(k=>`<div class="kpi"><label>${k[0]}</label><b class="${k[2]}">${k[1]}</b></div>`).join("");
  const ex={};let tx=0;Object.values(S.positions).forEach(p=>{const v=Math.abs(p.qty)*markOf(p.sym);ex[A[p.sym].c]=(ex[A[p.sym].c]||0)+v;tx+=v});
  $("alloc").innerHTML=tx?Object.entries(ex).map(([c,v])=>`<i style="width:${v/tx*100}%;background:${CLS_COL[c]}"></i>`).join(""):"";
  $("allocLeg").innerHTML=tx?Object.entries(ex).map(([c,v])=>`<span style="--c:${CLS_COL[c]}">${c} ${usd(v,0)}</span>`).join("")+`<span style="--c:transparent">Gross ${(tx/e).toFixed(2)}× equity</span>`:"No exposure yet.";drawEq()}
function drawEq(){const c=$("eqc");if(!c.offsetWidth)return;const d=window.devicePixelRatio||1,W=c.offsetWidth,H=140;c.width=W*d;c.height=H*d;const g=c.getContext("2d");g.setTransform(d,0,0,d,0,0);
  const pts=S.eq.length>1?S.eq:[{t:Date.now()-1,e:S.start},{t:Date.now(),e:equity()}];let hi=Math.max(...pts.map(p=>p.e),S.start),lo=Math.min(...pts.map(p=>p.e),S.start);const pad=(hi-lo)*.1||S.start*.001;hi+=pad;lo-=pad;
  const L=70,x=i=>L+i/(pts.length-1)*(W-L-4),y=v=>6+(hi-v)/(hi-lo)*(H-14);
  g.font="10px "+MONO;g.fillStyle=COL["--faint"];g.textBaseline="middle";[hi,(hi+lo)/2,lo].forEach(v=>{g.strokeStyle=COL["--line"];g.beginPath();g.moveTo(L,y(v));g.lineTo(W,y(v));g.stroke();g.fillText(usd(v,0),0,y(v))});
  g.setLineDash([3,3]);g.strokeStyle=COL["--muted"];g.beginPath();g.moveTo(L,y(S.start));g.lineTo(W,y(S.start));g.stroke();g.setLineDash([]);
  const last=pts[pts.length-1].e,col=last>=S.start?COL["--up"]:COL["--down"];
  g.beginPath();pts.forEach((p,i)=>i?g.lineTo(x(i),y(p.e)):g.moveTo(x(i),y(p.e)));g.strokeStyle=col;g.lineWidth=1.6;g.stroke();g.lineTo(x(pts.length-1),H);g.lineTo(L,H);g.closePath();g.fillStyle=col+"22";g.fill();g.beginPath();g.arc(x(pts.length-1),y(last),3,0,7);g.fillStyle=col;g.fill()}

/* ================= header, views, settings ================= */
function renderHead(){const e=equity(),t=e-S.start;$("hEq").textContent=usd(e);$("hPnl").textContent=sgnUsd(t)+" ("+pct(t/S.start*100)+")";$("hPnl").className="num "+cls(t);$("hCash").textContent=usd(S.cash);
  $("clock").textContent=new Date().toLocaleTimeString("en-US",{hour12:false})}
const isPhone=()=>matchMedia("(max-width:980px)").matches;
let view="trade";
function setView(v){view=v;document.body.dataset.view=v;[...$("mnav").children].forEach(b=>b.classList.toggle("on",b.dataset.v===v));$("pfBtn").textContent=v==="portfolio"?"Back to chart":"Portfolio";requestAnimationFrame(()=>{sizeChart();renderPF()});window.scrollTo(0,0)}
$("mnav").onclick=e=>{const b=e.target.closest("button");if(b)setView(b.dataset.v)};
$("pfBtn").onclick=()=>setView(view==="portfolio"?"trade":"portfolio");
function toast(t,k){const el=document.createElement("div");el.className="toast "+(k||"");el.textContent=t;$("toasts").prepend(el);setTimeout(()=>el.remove(),4000);while($("toasts").children.length>4)$("toasts").lastChild.remove()}
function openSettings(){$("fhKey").value=FHKEY;$("setMsg").textContent="";$("settings").hidden=false}
$("setBtn").onclick=openSettings;
$("setClose").onclick=()=>$("settings").hidden=true;
$("setSave").onclick=()=>{const v=$("fhKey").value.trim();FHKEY=v;ls.set("sx-fh-key",v);$("setMsg").textContent=v?"Saved. Connecting stocks…":"Key removed.";fhConnect();setTimeout(()=>{$("settings").hidden=true;renderSym()},900)};
$("setReset").onclick=()=>{$("settings").hidden=true;$("modal").hidden=false};
$("mNo").onclick=()=>$("modal").hidden=true;
$("mYes").onclick=()=>{S=fresh();editing=null;save();$("modal").hidden=true;toast("Account reset to $5,000,000");renderAll()};
document.addEventListener("keydown",e=>{if(e.key==="Escape"){$("modal").hidden=true;$("settings").hidden=true}});

/* install hint (iPhone Safari, not yet on home screen) */
const standalone=window.navigator.standalone===true||matchMedia("(display-mode: standalone)").matches;
const ios=/iphone|ipad|ipod/i.test(navigator.userAgent)||(navigator.platform==="MacIntel"&&navigator.maxTouchPoints>1);
if(ios&&!standalone&&!ls.get("sx-install-dismissed",false))$("install").hidden=false;
$("installX").onclick=()=>{$("install").hidden=true;ls.set("sx-install-dismissed",true)};

function renderAll(){renderHead();renderSym();updTicket();renderBlot();chartDirty=true;if(view==="portfolio")renderPF()}

/* ================= loops ================= */
let lastP=null;
function frame(){const m=M[sel];if(m.p!==lastP){lastP=m.p;renderSym();chartDirty=true}if(chartDirty){chartDirty=false;drawChart()}requestAnimationFrame(frame)}
setInterval(()=>{updWL();renderHead();renderQuote();updTicket();renderBlot();if(view==="portfolio")renderPF();renderPills();
  if(feeds.c.state==="live"&&Date.now()-feeds.c.last>20000)setPill("c","connecting");
  if(feeds.s.state==="live"&&!usOpen())setPill("s","closed");
  if(dirtyAcct){dirtyAcct=false;renderSym()}},1000);
setInterval(()=>{S.eq.push({t:Date.now(),e:equity()});if(S.eq.length>4000)S.eq.splice(0,S.eq.length-4000);save();persistQuotes()},10000);
setInterval(()=>{buildWL()},60000);
document.addEventListener("visibilitychange",()=>{if(document.hidden){save();persistQuotes()}else{if(!bn)bnConnect();if(FHKEY&&!fh)fhConnect();loadChart()}});

setView("trade");buildWL();setupTf();setupTicket();renderAll();renderQuote();renderPills();requestAnimationFrame(frame);
bnConnect();fhConnect();loadChart();
if("serviceWorker" in navigator&&location.protocol==="https:")navigator.serviceWorker.register("sw.js").catch(()=>{});
})();
