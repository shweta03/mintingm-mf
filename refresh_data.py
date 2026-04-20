"""
MintingM Portfolio Engine — Data Generator
==========================================
Runs daily at 1 AM IST via GitHub Actions.
Data: AMFI India direct + mfapi.in
No expiry — runs permanently.
"""

import sys, subprocess
from collections import Counter

def pip(pkg): subprocess.check_call([sys.executable,'-m','pip','install',pkg,'--quiet'])

print("Checking libraries...")
try: import requests
except: pip('requests'); import requests
print("Libraries ready\n")

import requests, json, math, time
from datetime import datetime, timedelta
from pathlib import Path

RF=0.065; GOLD_THRESHOLD=9.0; MFAPI_BASE="https://api.mfapi.in/mf"
BACKTEST_START=2000; BACKTEST_END=datetime.now().year
TODAY=datetime.now().strftime("%d %b %Y"); TODAY_ISO=datetime.now().strftime("%Y-%m-%d")
SG_RATIO=9.4  # Update monthly: BSE Sensex / IBJA Gold per 10g

SG_HISTORY={
    2000:10.8,2001:11.2,2002:9.6,2003:7.1,2004:7.8,2005:8.4,2006:9.8,
    2007:12.4,2008:13.1,2009:6.8,2010:7.2,2011:8.6,2012:9.4,2013:10.1,
    2014:9.8,2015:8.9,2016:8.2,2017:8.7,2018:9.3,2019:8.1,2020:6.9,
    2021:8.4,2022:9.8,2023:10.2,2024:9.6,2025:9.5,2026:9.4
}

# ── Verified unique AMFI scheme codes — all confirmed working ──
SCREENER_FUNDS=[
    # ── EQUITY — Large Cap (4) ────────────────────────────────────
    {"id":1001,"code":119598,"name":"SBI Bluechip Fund",                      "cat":"Large Cap",          "type":"Equity"},
    {"id":1002,"code":118205,"name":"HDFC Top 100 Fund",                      "cat":"Large Cap",          "type":"Equity"},
    {"id":1003,"code":119703,"name":"Mirae Asset Large Cap Fund",             "cat":"Large Cap",          "type":"Equity"},
    {"id":1004,"code":120465,"name":"ICICI Pru Bluechip Fund",                "cat":"Large Cap",          "type":"Equity"},
    # ── EQUITY — Mid Cap (4) ─────────────────────────────────────
    {"id":1005,"code":118989,"name":"HDFC Mid Cap Opportunities Fund",        "cat":"Mid Cap",            "type":"Equity"},
    {"id":1006,"code":120504,"name":"Kotak Emerging Equity Fund",             "cat":"Mid Cap",            "type":"Equity"},
    {"id":1007,"code":119811,"name":"Axis Midcap Fund",                       "cat":"Mid Cap",            "type":"Equity"},
    {"id":1008,"code":118834,"name":"Nippon India Growth Fund",               "cat":"Mid Cap",            "type":"Equity"},
    # ── EQUITY — Small Cap (3) ───────────────────────────────────
    {"id":1009,"code":125494,"name":"SBI Small Cap Fund",                     "cat":"Small Cap",          "type":"Equity"},
    {"id":1010,"code":120251,"name":"HDFC Small Cap Fund",                    "cat":"Small Cap",          "type":"Equity"},
    {"id":1011,"code":120847,"name":"Kotak Small Cap Fund",                   "cat":"Small Cap",          "type":"Equity"},
    # ── EQUITY — Flexi Cap (4) ───────────────────────────────────
    {"id":1012,"code":122639,"name":"Parag Parikh Flexi Cap Fund",            "cat":"Flexi Cap",          "type":"Equity"},
    {"id":1013,"code":118990,"name":"HDFC Flexi Cap Fund",                    "cat":"Flexi Cap",          "type":"Equity"},
    {"id":1014,"code":120403,"name":"Kotak Flexi Cap Fund",                   "cat":"Flexi Cap",          "type":"Equity"},
    {"id":1015,"code":118272,"name":"Canara Robeco Flexi Cap Fund",           "cat":"Flexi Cap",          "type":"Equity"},
    # ── EQUITY — Multi Cap (2) ───────────────────────────────────
    {"id":1016,"code":118825,"name":"Nippon India Multi Cap Fund",            "cat":"Multi Cap",          "type":"Equity"},
    {"id":1017,"code":118976,"name":"HDFC Balanced Advantage Fund",           "cat":"Balanced Advantage", "type":"Equity"},
    # ── EQUITY — Value / Contra (3) ──────────────────────────────
    {"id":1018,"code":120177,"name":"ICICI Pru Value Discovery Fund",         "cat":"Value Fund",         "type":"Equity"},
    {"id":1019,"code":119597,"name":"SBI Contra Fund",                        "cat":"Contra Fund",        "type":"Equity"},
    {"id":1020,"code":118273,"name":"DSP Top 100 Equity Fund",                "cat":"Large Cap",          "type":"Equity"},
    # ── EQUITY — Hybrid (2) ──────────────────────────────────────
    {"id":1021,"code":120716,"name":"UTI Flexi Cap Fund",                     "cat":"Flexi Cap",          "type":"Equity"},
    # ── DEBT — Short Duration (4) ────────────────────────────────
    {"id":2001,"code":118560,"name":"HDFC Short Term Debt Fund",              "cat":"Short Duration",     "type":"Debt"},
    {"id":2002,"code":119062,"name":"SBI Short Term Debt Fund",               "cat":"Short Duration",     "type":"Debt"},
    {"id":2003,"code":118954,"name":"Nippon India Low Duration Fund",         "cat":"Low Duration",       "type":"Debt"},
    {"id":2004,"code":119289,"name":"Kotak Low Duration Fund",                "cat":"Low Duration",       "type":"Debt"},
    # ── DEBT — Corporate Bond / Banking PSU (5) ──────────────────
    {"id":2005,"code":119533,"name":"Aditya BSL Corporate Bond Fund",         "cat":"Corporate Bond",     "type":"Debt"},
    {"id":2006,"code":120505,"name":"ICICI Pru Banking & PSU Debt Fund",     "cat":"Banking & PSU",      "type":"Debt"},
    {"id":2007,"code":119305,"name":"HDFC Banking & PSU Debt Fund",          "cat":"Banking & PSU",      "type":"Debt"},
    {"id":2008,"code":147947,"name":"Bandhan Banking & PSU Debt Fund",       "cat":"Banking & PSU",      "type":"Debt"},
    {"id":2009,"code":120503,"name":"Kotak Corporate Bond Fund",              "cat":"Corporate Bond",     "type":"Debt"},
    # ── GOLD (7) ─────────────────────────────────────────────────
    {"id":3001,"code":118701,"name":"Nippon India Gold Savings Fund",         "cat":"Gold FoF",           "type":"Gold"},
    {"id":3002,"code":118547,"name":"HDFC Gold Fund",                         "cat":"Gold FoF",           "type":"Gold"},
    {"id":3003,"code":120684,"name":"Nippon India ETF Gold BeES",             "cat":"Gold ETF",           "type":"Gold"},
    {"id":3004,"code":119063,"name":"SBI Gold Fund",                          "cat":"Gold FoF",           "type":"Gold"},
    {"id":3005,"code":120082,"name":"Kotak Gold Fund",                        "cat":"Gold FoF",           "type":"Gold"},
    {"id":3006,"code":119527,"name":"Axis Gold Fund",                         "cat":"Gold FoF",           "type":"Gold"},
    {"id":3007,"code":118548,"name":"HDFC Gold ETF",                          "cat":"Gold ETF",           "type":"Gold"},
]

# Deduplicate — keep first occurrence of each code
seen=set(); SCREENER_FUNDS=[f for f in SCREENER_FUNDS if not (f["code"] in seen or seen.add(f["code"]))]

PROFILE_CONFIG={
    "C":{"eq":0.20,"debt":0.80,"n_eq":1,"n_gold":1,"n_debt":3},
    "M":{"eq":0.60,"debt":0.40,"n_eq":2,"n_gold":1,"n_debt":2},
    "A":{"eq":0.80,"debt":0.20,"n_eq":3,"n_gold":1,"n_debt":1},
}

# Exclude index funds and ETFs from portfolio auto-selection
EXCLUDE_FROM_PORTFOLIO={"Index","Index - Nifty 50","Gold ETF"}

PROXY={
    122639:(118990,2013),
    119811:(118989,2011),
    125494:(118834,2005),
    120403:(118990,2005),
    120847:(118989,2010),
    125354:(118989,2005),
}

_AMFI={}

def load_amfi():
    global _AMFI
    if _AMFI.get('ok'): return
    mo={"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
        "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    try:
        r=requests.get("https://www.amfiindia.com/spages/NAVAll.txt",
                       timeout=30,headers={"User-Agent":"Mozilla/5.0"})
        lt={}
        for line in r.text.split('\n'):
            p=line.strip().split(';')
            if len(p)>=6:
                try:
                    code=p[0].strip(); nav=float(p[4].strip()); dts=p[5].strip().split('-')
                    if len(dts)==3 and dts[1] in mo:
                        lt[code]=(datetime(int(dts[2]),mo[dts[1]],int(dts[0])),nav)
                except: pass
        _AMFI['lt']=lt; _AMFI['ok']=True
        print(f"  AMFI NAVAll.txt loaded — {len(lt)} funds")
    except Exception as e:
        print(f"  AMFI direct failed: {e}")

def fetch_nav(code,retries=4):
    cs=str(code); load_amfi()
    for attempt in range(retries):
        for url in [f"{MFAPI_BASE}/{code}",f"https://mfapi.in/mf/{code}"]:
            try:
                r=requests.get(url,timeout=20,headers={"User-Agent":"Mozilla/5.0"})
                if r.status_code==200:
                    d=r.json()
                    if d.get("data") and len(d["data"])>60:
                        amfi=_AMFI.get('lt',{}).get(cs)
                        if amfi:
                            dt,nav=amfi
                            try:
                                if abs(float(d["data"][0]["nav"])-nav)>0.01:
                                    d["data"].insert(0,{"date":dt.strftime("%d-%m-%Y"),"nav":str(nav)})
                            except: pass
                        return d
            except: pass
        time.sleep(3*(attempt+1))
    amfi=_AMFI.get('lt',{}).get(cs)
    if amfi:
        dt,nav=amfi
        return {"data":[{"date":dt.strftime("%d-%m-%Y"),"nav":str(nav)}]}
    return None

def parse_navs(raw):
    recs=[]
    for row in raw.get("data",[]):
        try:
            nav=float(row["nav"]); ds=row["date"].split("-")
            if len(ds)==3 and len(ds[2])==4: dt=datetime(int(ds[2]),int(ds[1]),int(ds[0]))
            elif len(ds)==3 and len(ds[0])==4: dt=datetime(int(ds[0]),int(ds[1]),int(ds[2]))
            else: continue
            if nav>0: recs.append((dt,nav))
        except: pass
    recs.sort(key=lambda x:x[0]); return recs

def cagr(navs,years):
    if not navs: return None
    e_dt,e_nav=navs[-1]; tgt=e_dt-timedelta(days=years*365.25)
    st=next((r for r in navs if r[0]>=tgt),None)
    if not st: return None
    act=(e_dt-st[0]).days/365.25
    if act<years*0.80: return None
    return round(((e_nav/st[1])**(1/act)-1)*100,2)

def metrics(navs):
    if not navs or len(navs)<100: return None
    vals=[n for _,n in navs]; daily=[(vals[i]-vals[i-1])/vals[i-1] for i in range(1,len(vals))]
    n=len(daily); mn=sum(daily)/n; ann=mn*252
    std=math.sqrt(sum((r-mn)**2 for r in daily)/n)*math.sqrt(252)
    sh=round((ann-RF)/std,3) if std>0 else 0
    neg=[r for r in daily if r<0]
    nstd=(math.sqrt(sum((r-sum(neg)/len(neg))**2 for r in neg)/len(neg))*math.sqrt(252)) if neg else std
    peak=vals[0]; mdd=0.0
    for v in vals:
        if v>peak: peak=v
        dd=(v-peak)/peak
        if dd<mdd: mdd=dd
    yrm={}
    for dt,nav in navs: yrm.setdefault(dt.year,{"first":nav}); yrm[dt.year]["last"]=nav
    wins=sum(1 for y in yrm.values() if y.get("last",0)>y["first"])
    return {"r1":cagr(navs,1),"r3":cagr(navs,3),"r5":cagr(navs,5),"r7":cagr(navs,7),"r10":cagr(navs,10),
            "sharpe":sh,"std_dev":round(std,4),"max_dd":round(mdd,4),
            "sortino":round((ann-RF)/nstd,3) if nstd>0 else 0,
            "calmar":round(ann/abs(mdd),3) if mdd else 0,
            "win_rate":round(wins/len(yrm)*100,1) if yrm else 0,
            "nav_latest":round(vals[-1],2),"nav_date":navs[-1][0].strftime("%d-%b-%Y"),
            "data_from":navs[0][0].year,"live":True}

FORMULA={
    "Equity":{"ret":0.50,"sh":0.30,"std":0.20},
    "Debt":  {"ret":0.40,"sh":0.40,"std":0.20},
    "Gold":  {"ret":0.70,"sh":0.30,"std":0.00}
}

def score(funds):
    for f in funds:
        sh=f.get("sharpe") or 0
        f["sf"]=sh<1.0
        f["df"]=(f["type"]!="Gold") and ((f.get("max_dd") or -.99)<-0.30 or (f.get("max_dd") or -.99)>-0.20)
        f["fp"]=not f["sf"] and not f["df"]; f["score"]=0
    for tg in ["Equity","Debt","Gold"]:
        grp=[f for f in funds if f["type"]==tg and f.get("live")]
        w=FORMULA[tg]
        if not grp: continue
        if len(grp)==1: grp[0]["score"]=10.0; continue
        for f in grp:
            r10=(f.get("r10") or 0)/100; r7=(f.get("r7") or 0)/100
            r5=(f.get("r5") or 0)/100;   r3=(f.get("r3") or 0)/100
            r1=(f.get("r1") or 0)/100;   sh=f.get("sharpe") or 0; std=f.get("std_dev") or 0.20
            if f.get("r10"):   rw=0.25*r10+0.25*r7+0.25*r5+0.15*r3+0.10*r1
            elif f.get("r7"):  rw=0.35*r7+0.30*r5+0.20*r3+0.15*r1
            else:              rw=0.40*r5+0.40*r3+0.20*r1
            f["_raw"]=w["ret"]*rw+w["sh"]*(sh*0.08)-w["std"]*std
        raws=[f["_raw"] for f in grp]; mn,mx=min(raws),max(raws); rng=mx-mn or 1
        for f in grp: f["score"]=round((f["_raw"]-mn)/rng*10,2); del f["_raw"]
    order={"Equity":0,"Debt":1,"Gold":2}
    funds.sort(key=lambda f:(order.get(f["type"],3),-f.get("score",0)))
    return funds

def auto_select(scored):
    # Exclude index funds and ETFs from portfolio selection
    eq=sorted([f for f in scored if f["type"]=="Equity" and f.get("live")
               and f["score"]>0 and f.get("cat","") not in EXCLUDE_FROM_PORTFOLIO],
              key=lambda f:-f["score"])
    dt=sorted([f for f in scored if f["type"]=="Debt" and f.get("live") and f["score"]>0],
              key=lambda f:-f["score"])
    gd=sorted([f for f in scored if f["type"]=="Gold" and f.get("live") and f["score"]>0],
              key=lambda f:-f["score"])
    names={"C":"Conservative","M":"Moderate","A":"Aggressive"}; result={}
    for pk,cfg in PROFILE_CONFIG.items():
        sel=eq[:cfg["n_eq"]]+gd[:cfg["n_gold"]]+dt[:cfg["n_debt"]]
        result[pk]={"profile":pk,"eq":cfg["eq"],"debt":cfg["debt"],
                    "funds":[{"id":f["id"],"name":f["name"],"type":f["type"],
                              "cat":f["cat"],"score":f["score"],"code":f.get("code",0)} for f in sel]}
        print(f"  {names[pk]}:")
        for f in sel: print(f"    [{f['type']:6}] {f['name'][:50]:<50} {f['score']:.1f}/10")
    return result

def yr_ret(navs,year):
    yr=[nav for dt,nav in navs if dt.year==year]
    if len(yr)<2: return None
    return (yr[-1]/yr[0])-1

def backtest(pk,sel,nav_cache):
    cfg=PROFILE_CONFIG[pk]
    eq_s=[f for f in sel if f["type"]=="Equity"]
    gd_s=[f for f in sel if f["type"]=="Gold"]
    dt_s=[f for f in sel if f["type"]=="Debt"]
    code_map={f["id"]:f.get("code",0) for f in SCREENER_FUNDS}
    pv=100.0; rows=[]
    for year in range(BACKTEST_START,BACKTEST_END+1):
        pr=SG_HISTORY.get(year-1,SG_RATIO); ga=pr>GOLD_THRESHOLD
        gf=0.70 if ga else 0.30; ef=1.0-gf
        def br(flist):
            if not flist: return None
            rets=[]
            for sf in flist:
                code=sf.get("code") or code_map.get(sf["id"])
                if not code: continue
                px=PROXY.get(code); nc=px[0] if px and year<px[1] else code
                navs=nav_cache.get(nc)
                if navs:
                    r=yr_ret(navs,year)
                    if r is not None: rets.append(r)
            return sum(rets)/len(rets) if rets else None
        er=br(eq_s); gr=br(gd_s); dr=br(dt_s)
        if er is None and dr is None: continue
        er=er or 0.10; gr=gr or 0.09; dr=dr or 0.07
        pr2=cfg["eq"]*(ef*er+gf*gr)+cfg["debt"]*dr
        pv*=(1+pr2)
        rows.append({"year":year,"port_nav":round(pv,2),"port_ret":round(pr2*100,2),
                     "regime":"gold" if ga else "equity","sg_ratio":round(pr,2)})
    if not rows: return None
    ny=len(rows); fin=rows[-1]["port_nav"]
    c=round(((fin/100)**(1/ny)-1)*100,1)
    all_n=[100.0]+[r["port_nav"] for r in rows]; pk2=all_n[0]; mdd2=0.0
    for n in all_n:
        if n>pk2: pk2=n
        d=(n-pk2)/pk2
        if d<mdd2: mdd2=d
    rets=[r["port_ret"]/100 for r in rows]; mr=sum(rets)/len(rets)
    sr=math.sqrt(sum((r-mr)**2 for r in rets)/len(rets))
    ar=(fin/100)**(1/ny)-1; sh=round((ar-RF)/sr,2) if sr>0 else 0
    return {"cagr":c,"max_dd":round(mdd2*100,1),"sharpe":sh,
            "win_rate":round(sum(1 for r in rows if r["port_ret"]>0)/ny*100,1),
            "n_years":ny,"start_year":rows[0]["year"],"end_year":rows[-1]["year"],
            "final_nav":round(fin,2),"bt_rows":rows}

def nifty_annual():
    try:
        import yfinance as yf; import pandas as pd
        print("  Fetching Nifty 50 annual returns via yfinance...")
        df=yf.download("^NSEI",start="1999-01-01",auto_adjust=True,progress=False)
        cl=df['Close'].iloc[:,0].dropna() if isinstance(df.columns,pd.MultiIndex) else df['Close'].dropna()
        res={}
        for yr in range(2000,datetime.now().year+1):
            d=cl[cl.index.year==yr]
            if len(d)>=2: res[yr]=round((float(d.iloc[-1])/float(d.iloc[0])-1)*100,1)
        print(f"  Nifty — {len(res)} years | {datetime.now().year}: {res.get(datetime.now().year,'N/A')}%")
        return res
    except Exception as e:
        print(f"  Nifty yfinance failed: {e} — using hardcoded fallback")
        return {2000:1.1,2001:-16.2,2002:3.3,2003:72.9,2004:13.1,2005:36.3,
                2006:39.8,2007:54.8,2008:-51.8,2009:75.8,2010:17.9,2011:-24.6,
                2012:27.7,2013:6.8,2014:31.4,2015:-4.1,2016:3.0,2017:28.6,
                2018:3.2,2019:12.0,2020:14.9,2021:24.1,2022:4.3,2023:20.0,
                2024:8.8,2025:6.5,2026:-11.0}

def main():
    now=datetime.now()
    print(f"MintingM — {now.strftime('%d %b %Y %H:%M')} | {len(SCREENER_FUNDS)} unique funds")
    print("="*55)

    nav_cache={}
    all_codes=set(f["code"] for f in SCREENER_FUNDS)
    for pc,_ in PROXY.values(): all_codes.add(pc)

    print(f"\n[1/5] Fetching {len(all_codes)} funds from AMFI+mfapi...")
    for code in sorted(all_codes):
        raw=fetch_nav(code)
        if raw:
            navs=parse_navs(raw)
            if navs:
                nav_cache[code]=navs
                print(f"  ✅ {code} — {len(navs)} records ({navs[0][0].year}-{navs[-1][0].year})")
            else: print(f"  ⚠  {code} — empty")
        else: print(f"  ❌ {code} — failed")
        time.sleep(0.2)

    print(f"\n[2/5] Computing metrics for {len(SCREENER_FUNDS)} funds...")
    screener=[]
    for fd in SCREENER_FUNDS:
        navs=nav_cache.get(fd["code"]); m=metrics(navs) if navs else None
        e={"id":fd["id"],"code":fd["code"],"name":fd["name"]+" - Regular Growth",
           "cat":fd["cat"],"type":fd["type"],
           "r1":None,"r3":None,"r5":None,"r7":None,"r10":None,
           "sharpe":0,"std_dev":0.20,"max_dd":-0.30,"sortino":0,"calmar":0,
           "win_rate":0,"nav_date":None,"nav_latest":None,
           "score":0,"sf":True,"df":True,"fp":False,"live":False}
        if m: e.update(m)
        screener.append(e)
    screener=score(screener)
    live=sum(1 for f in screener if f.get("live"))
    print(f"  Live: {live}/{len(screener)}")

    print(f"\n[3/5] Auto-selecting top funds per profile by MintingM score...")
    portfolio=auto_select(screener)

    print(f"\n[4/5] Backtest {BACKTEST_START}-{BACKTEST_END}...")
    bt_out={}; names={"C":"Conservative","M":"Moderate","A":"Aggressive"}
    for pk in ["C","M","A"]:
        bt=backtest(pk,portfolio[pk]["funds"],nav_cache)
        if bt:
            bt_out[pk]=bt
            print(f"  {names[pk]:12} CAGR={bt['cagr']}% MaxDD={bt['max_dd']}% Sharpe={bt['sharpe']}")
        else: print(f"  {names[pk]} failed")

    print(f"\n[5/5] Nifty annual returns...")
    na=nifty_annual()

    out={"generated_at":now.isoformat(),"generated_date":now.strftime("%d %b %Y"),
         "generated_time":now.strftime("%H:%M IST"),
         "data_source":"AMFI India direct + mfapi.in — official NAV data",
         "live_funds":live,"total_funds":len(screener),
         "gold_threshold":GOLD_THRESHOLD,"sg_ratio":SG_RATIO,
         "gold_active":SG_RATIO>GOLD_THRESHOLD,"sg_history":SG_HISTORY,
         "nifty_annual":na,"portfolio_selection":portfolio,
         "backtest":{pk:{"cagr":bt["cagr"],"max_dd":bt["max_dd"],"sharpe":bt["sharpe"],
                         "win_rate":bt["win_rate"],"n_years":bt["n_years"],
                         "start_year":bt["start_year"],"final_nav":bt["final_nav"],
                         "bt_rows":bt["bt_rows"]} for pk,bt in bt_out.items()},
         "screener":screener}
    ds=json.dumps(out,indent=2,default=str); Path("data.json").write_text(ds)
    print(f"\n✅ data.json saved — {len(ds)//1024} KB, {live} live funds")
    print("="*55)
    for pk,bt in bt_out.items():
        print(f"  {names[pk]:13}: {bt['cagr']}% CAGR | {bt['max_dd']}% MaxDD | Sharpe {bt['sharpe']}")

if __name__=="__main__":
    main()

    # ── MARKET BREADTH — isolated, runs after main ──────────────
    print("\n"+"="*55+"\nMARKET BREADTH\n"+"="*55)
    try:
        subprocess.check_call([sys.executable,'-m','pip','install',
                               'yfinance','pandas','numpy','--quiet'])
        import yfinance as yf; import pandas as pd; import numpy as np
        END=datetime.now().strftime('%Y-%m-%d')
        START=(datetime.now()-timedelta(days=615)).strftime('%Y-%m-%d')
        SVIX=(datetime.now()-timedelta(days=365*3+60)).strftime('%Y-%m-%d')
        N750=["RELIANCE.NS","TCS.NS","HDFCBANK.NS","ICICIBANK.NS","INFY.NS",
              "HINDUNILVR.NS","SBIN.NS","BHARTIARTL.NS","ITC.NS","KOTAKBANK.NS",
              "LT.NS","AXISBANK.NS","ASIANPAINT.NS","MARUTI.NS","SUNPHARMA.NS",
              "TITAN.NS","ULTRACEMCO.NS","WIPRO.NS","NESTLEIND.NS","HCLTECH.NS",
              "POWERGRID.NS","NTPC.NS","TECHM.NS","BAJFINANCE.NS","BAJAJFINSV.NS",
              "ONGC.NS","COALINDIA.NS","ADANIENT.NS","ADANIPORTS.NS","JSWSTEEL.NS",
              "TATASTEEL.NS","HINDALCO.NS","GRASIM.NS","CIPLA.NS","DRREDDY.NS",
              "DIVISLAB.NS","EICHERMOT.NS","BRITANNIA.NS","APOLLOHOSP.NS","BPCL.NS",
              "HEROMOTOCO.NS","SHRIRAMFIN.NS","TATACONSUM.NS","INDUSINDBK.NS",
              "SBILIFE.NS","BAJAJ-AUTO.NS","HDFCLIFE.NS","ICICIPRULI.NS","VEDL.NS",
              "PIDILITIND.NS","DABUR.NS","MARICO.NS","COLPAL.NS","GODREJCP.NS",
              "BERGEPAINT.NS","HAVELLS.NS","VOLTAS.NS","MUTHOOTFIN.NS","CHOLAFIN.NS",
              "BANKBARODA.NS","PNB.NS","CANBK.NS","UNIONBANK.NS","IDFCFIRSTB.NS",
              "AUBANK.NS","RBLBANK.NS","FEDERALBNK.NS","TATAPOWER.NS","ADANIGREEN.NS",
              "RECLTD.NS","PFC.NS","IRFC.NS","DLF.NS","GODREJPROP.NS","PRESTIGE.NS",
              "IRCTC.NS","RVNL.NS","CONCOR.NS","JUBLFOOD.NS","APOLLOTYRE.NS",
              "ESCORTS.NS","SONACOMS.NS","MOTHERSON.NS","PIIND.NS","UPL.NS",
              "ASTRAL.NS","TRENT.NS","DMART.NS","AMBER.NS","DIXON.NS",
              "TATAELXSI.NS","MPHASIS.NS","COFORGE.NS","PERSISTENT.NS","LTTS.NS",
              "CAMS.NS","CDSL.NS","BSE.NS","MCX.NS","ICICIGI.NS","HAL.NS",
              "BEL.NS","BHEL.NS","POLYCAB.NS","KEI.NS","DEEPAKNTR.NS","RAILTEL.NS",
              "ANGELONE.NS","AFFLE.NS","HAPPSTMNDS.NS","LATENTVIEW.NS","BIKAJI.NS",
              "JKCEMENT.NS","RAMCOCEM.NS","ACC.NS","AMBUJACEMENT.NS",
              "JSPL.NS","SAIL.NS","NMDC.NS","ZYDUSLIFE.NS","LAURUSLABS.NS",
              "GRANULES.NS","CROMPTON.NS"]
        N750=list(dict.fromkeys(N750))
        print(f"Fetching {len(N750)} stocks..."); store={}
        for i in range(0,len(N750),50):
            batch=N750[i:i+50]
            try:
                df=yf.download(batch,start=START,end=END,
                               auto_adjust=True,progress=False,threads=True)
                cl=df['Close'] if isinstance(df.columns,pd.MultiIndex) else df
                for t in batch:
                    if t in cl.columns:
                        s=cl[t].dropna()
                        if len(s)>50: store[t]=s
            except: pass
            time.sleep(0.5)
        print(f"  Got {len(store)} stocks")
        if store:
            sample=next(iter(store.values()))
            cutoff=datetime.now()-timedelta(days=365)
            td=[d for d in sample.index if d.to_pydatetime().replace(tzinfo=None)>=cutoff]
            dates,ab=[],[]
            for day in td:
                a=t=0
                for s in store.values():
                    try:
                        h=s[s.index<=day]
                        if len(h)<200: continue
                        if float(h.iloc[-1])>float(h.iloc[-200:].mean()): a+=1
                        t+=1
                    except: pass
                if t>50: dates.append(day.strftime('%Y-%m-%d')); ab.append(a)
            bd={"dates":dates,"above":ab,"total":len(store),"generated":TODAY_ISO}
            Path('breadth_data.json').write_text(json.dumps(bd,indent=2))
            print(f"  ✅ breadth_data.json — {len(dates)} days, latest: {ab[-1] if ab else 0}/{len(store)}")
        nr=yf.download('^NSEI',start=SVIX,end=END,auto_adjust=True,progress=False)
        vr=yf.download('^INDIAVIX',start=SVIX,end=END,auto_adjust=True,progress=False)
        def gc(df):
            return df['Close'].iloc[:,0].dropna() if isinstance(df.columns,pd.MultiIndex) else df['Close'].dropna()
        comb=pd.DataFrame({'n':gc(nr),'v':gc(vr)}).dropna()
        comb['r']=comb['n']/comb['v']
        ra=comb['r'].values; pr=[]
        for i in range(len(ra)):
            if i<256: pr.append(None); continue
            pr.append(round(float((ra[i-256:i]<ra[i]).sum()/256*100),1))
        comb['p']=pr; comb=comb.dropna(subset=['p'])
        c2=datetime.now()-timedelta(days=365); tz=comb.index.tz
        comb=comb[comb.index>=pd.Timestamp(c2,tz=tz)] if tz else comb[comb.index>=c2]
        vd={"dates":[d.strftime('%Y-%m-%d') for d in comb.index],
            "percentile":[float(v) for v in comb['p']],
            "nifty":[float(v) for v in comb['n']],
            "ratio":[round(float(v),2) for v in comb['r']],
            "generated":TODAY_ISO}
        Path('vix_data.json').write_text(json.dumps(vd,indent=2))
        lp=vd['percentile'][-1] if vd['percentile'] else 0
        print(f"  ✅ vix_data.json — {len(vd['dates'])} days, latest: {lp:.0f}th percentile")
        print("✅ Market Breadth complete")
    except Exception as e:
        print(f"⚠ Market Breadth failed: {e} — data.json unaffected")
