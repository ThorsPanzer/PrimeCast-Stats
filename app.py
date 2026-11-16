"""PrimeCast Stats v1.38 - local Riot API / OBS overlay, Python 3.10+ stdlib only."""
import base64, hashlib, io, json, os, re, sqlite3, threading, time, traceback, urllib.error, urllib.parse, urllib.request, sys, webbrowser, zipfile, subprocess, tempfile, shutil, secrets
from html.parser import HTMLParser
from collections import Counter, defaultdict
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB = ROOT / 'primecast.sqlite3'
CONFIG = ROOT / 'config.json'
HOST, PORT = '127.0.0.1', 8767
DEFAULT = {'api_key':'','platform':'euw1','region':'europe','season_start':'2026-01-01T00:00:00Z','teams':[{'name':'BLUE TEAM','players':['']*5,'logo':''},{'name':'RED TEAM','players':['']*5,'logo':''}]}
ROLES = ['TOP','JUNGLE','MID','ADC','SUPPORT']
# Broadcast visual preferences are separate from team/scan configuration so users
# can change them safely while the Riot download worker is running.
PREFERENCES_DEFAULT = {
    'blue_color':'#4DBBEB', 'red_color':'#F56C91', 'font':'Impact',
    'show_first_blood':True, 'show_dragons':True, 'show_baron':True,
    'show_swings':True, 'show_damage_composition':True, 'number_locale':'de-DE'
}
FONT_CHOICES = {'Impact','Anton','Oswald','Rajdhani','Archivo Black'}
NUMBER_LOCALES = {'de-DE','en-US'}
def preferences():
    c=config().get('preferences', {})
    if not isinstance(c,dict): c={}
    prefs={**PREFERENCES_DEFAULT,**{k:v for k,v in c.items() if k in PREFERENCES_DEFAULT}}
    if prefs.get('font') not in FONT_CHOICES:prefs['font']='Impact'
    if prefs.get('number_locale') not in NUMBER_LOCALES:prefs['number_locale']='de-DE'
    return prefs

def validate_preferences(data):
    if not isinstance(data,dict):raise ValueError('Invalid settings payload')
    p=preferences()
    for name in ('blue_color','red_color'):
        if name in data:
            value=str(data[name]).upper()
            if not re.fullmatch(r'#[0-9A-F]{6}',value):raise ValueError('Choose a valid hex color.')
            p[name]=value
    if 'number_locale' in data:
        if data['number_locale'] not in NUMBER_LOCALES:raise ValueError('Unsupported number format')
        p['number_locale']=data['number_locale']
    if 'font' in data:
        if data['font'] not in FONT_CHOICES:raise ValueError('Unsupported font')
        p['font']=data['font']
    for name in ('show_first_blood','show_dragons','show_baron','show_swings','show_damage_composition'):
        if name in data:
            if not isinstance(data[name],bool):raise ValueError('Toggle must be true or false')
            p[name]=data[name]
    return p


overlay_control = {'role':0,'revision':0,'auto':True,'interval':12,'last_switch':time.monotonic()}
state = {'running':False,'cancel':False,'progress':'Ready','done':0,'total':0,'error':None,'updated':None}
lock = threading.RLock()
# Counts team-side swaps. A SoloQ scan always retains its original logical slots;
# data is published into the CURRENT side by applying the swap revision.
side_revision = 0
last_calls = {}
from collections import deque
request_times = deque()

def setup():
    if not CONFIG.exists():
        save_config(DEFAULT)
    with db() as con:
        if con.execute('PRAGMA user_version').fetchone()[0] < 2:
            con.execute('CREATE TABLE IF NOT EXISTS matches (id TEXT PRIMARY KEY, body TEXT NOT NULL)')
        con.execute('CREATE TABLE IF NOT EXISTS results (slot INTEGER PRIMARY KEY, body TEXT NOT NULL)')
        con.execute('CREATE TABLE IF NOT EXISTS postgame (match_id TEXT PRIMARY KEY, body TEXT NOT NULL, updated REAL NOT NULL)')
        con.execute('CREATE TABLE IF NOT EXISTS matchlists (puuid TEXT NOT NULL, season_start INTEGER NOT NULL, ids TEXT NOT NULL, complete INTEGER NOT NULL, updated REAL NOT NULL, PRIMARY KEY(puuid,season_start))')
    migrate_player_match_cache()

def migrate_player_match_cache():
    """v1.21: migrate ten-participant JSON into *only known players'*
    compact per-player matches; retain completed Match ID lists and postgame.

    A single SQLite transaction prevents a partially migrated database from
    losing the legacy cache if the application closes during conversion.
    """
    with db() as con:
        version = con.execute('PRAGMA user_version').fetchone()[0]
        con.execute('''CREATE TABLE IF NOT EXISTS player_matches (
            puuid TEXT NOT NULL,
            match_id TEXT NOT NULL,
            champion_id INTEGER NOT NULL,
            champion_name TEXT NOT NULL,
            won INTEGER NOT NULL,
            season_year INTEGER NOT NULL,
            PRIMARY KEY (puuid, match_id)
        ) WITHOUT ROWID''')
        if version >= 2:
            return
        legacy_count = con.execute('SELECT COUNT(*) FROM matches').fetchone()[0]
        print(f'Migrating {legacy_count} legacy match cache records to per-player format...', flush=True)
        # Only players previously scanned by PrimeCast are relevant; do NOT
        # automatically preserve all ten participants of a cached match.
        refs = {}
        known = {}
        for puuid, start, ids in con.execute('SELECT puuid,season_start,ids FROM matchlists'):
            yr = datetime.fromtimestamp(int(start), timezone.utc).year
            known[puuid] = max(yr, known.get(puuid, 0))
            try:
                for mid in json.loads(ids):
                    refs.setdefault(mid, {})[puuid] = yr
            except (ValueError, TypeError):
                continue
        for (body,) in con.execute('SELECT body FROM results'):
            try:
                value = json.loads(body)
                puuid = value.get('puuid')
                if puuid:
                    yr = datetime.fromtimestamp(int(value.get('season_start') or 0),timezone.utc).year
                    known[puuid] = max(yr or 2026, known.get(puuid, 0))
            except (ValueError, TypeError, OverflowError, OSError):
                pass
        converted = 0
        for mid, body in con.execute('SELECT id,body FROM matches'):
            try: info=json.loads(body).get('info', {})
            except (ValueError, TypeError): continue
            if info.get('queueId') != 420: continue
            exact_refs = refs.get(mid)
            for participant in info.get('participants', []):
                puuid = participant.get('puuid')
                if not puuid: continue
                if exact_refs is not None:
                    year=exact_refs.get(puuid)
                    if year is None: continue
                else:
                    year=known.get(puuid)
                    if not year: continue
                con.execute('''INSERT OR IGNORE INTO player_matches
                    (puuid,match_id,champion_id,champion_name,won,season_year)
                    VALUES (?,?,?,?,?,?)''',
                    (puuid, mid, int(participant.get('championId') or 0),
                     str(participant.get('championName') or participant.get('championId') or '?'),
                     int(bool(participant.get('win'))), year))
                converted += 1
        # Do not delete legacy rows before all insertions have succeeded.
        con.execute('DROP TABLE matches')
        con.execute('PRAGMA user_version=2')
        con.commit()
    try:
        with db() as con: con.execute('VACUUM')
    except sqlite3.Error as exc:
        print(f'Database converted; VACUUM can be retried later: {exc}',flush=True)
    print(f'Per-player records: {converted}; database size: {DB.stat().st_size/1048576:.1f} MB',flush=True)


def cache_summary():
    """The team match cache is intentionally independent of saved postgame."""
    with db() as con:
        counts=[{'year':int(year),'matches':int(count)} for year,count in con.execute(
            'SELECT season_year, COUNT(*) FROM player_matches GROUP BY season_year ORDER BY season_year DESC')]
    return {'selected_year':season_info()['year'], 'years':counts,
            'size_mb':round(DB.stat().st_size / 1048576,2)}


def purge_prior_seasons():
    """Explicitly purge only SoloQ data older than the currently selected year.

    The selected year's matchlists, player matches, profiles and all Postgame
    statistics remain untouched. No silent purge at new-year rollover.
    """
    selected=season_info()['year']
    cutoff=int(datetime(selected,1,1,tzinfo=timezone.utc).timestamp())
    with lock:
        if state['running']:
            raise ValueError('Stop the current SoloQ update before cleaning the cache.')
        with db() as con:
            count=con.execute('SELECT COUNT(*) FROM player_matches WHERE season_year<?',(selected,)).fetchone()[0]
            pages=con.execute('SELECT COUNT(*) FROM matchlists WHERE season_start<?',(cutoff,)).fetchone()[0]
            con.execute('DELETE FROM player_matches WHERE season_year<?',(selected,))
            con.execute('DELETE FROM matchlists WHERE season_start<?',(cutoff,))
        if count or pages:
            with db() as con:con.execute('VACUUM')
    return {'removed_matches':count,'removed_matchlists':pages, **cache_summary()}


def get_player_match(match_id, puuid, region, year):
    """Return champion/win for precisely one PUUID; never persist all players."""
    with db() as con:
        row=con.execute('SELECT champion_id,champion_name,won FROM player_matches '
                        'WHERE puuid=? AND match_id=?',(puuid,match_id)).fetchone()
    if row:
        return {'championId':row[0], 'championName':row[1], 'win':bool(row[2])}
    full=request(region,'/lol/match/v5/matches/'+pathpart(match_id))
    info=full.get('info', {})
    if info.get('queueId') != 420: return None
    player=next((x for x in info.get('participants',[]) if x.get('puuid')==puuid),None)
    if player is None: return None
    champion_id = int(player.get('championId') or 0)
    champion_name = str(player.get('championName') or champion_id or '?')
    won = int(bool(player.get('win')))
    with db() as con:
        con.execute('INSERT OR REPLACE INTO player_matches '
                    '(puuid,match_id,champion_id,champion_name,won,season_year) VALUES (?,?,?,?,?,?)',
                    (puuid,match_id,champion_id,champion_name,won,int(year)))
    return {'championId':champion_id, 'championName':champion_name, 'win':bool(won)}


def db():
    return sqlite3.connect(DB, timeout=30)

def save_config(v):
    tmp = CONFIG.with_suffix('.tmp')
    tmp.write_text(json.dumps(v,ensure_ascii=False,indent=2),encoding='utf-8')
    tmp.replace(CONFIG)

def config():
    c = dict(DEFAULT)
    try: c.update(json.loads(CONFIG.read_text(encoding='utf-8')))
    except (OSError,ValueError): pass
    # Preserve explicitly selected ranked year; legacy 2026 split dates migrate to full-year start.
    if c.get('season_start') in ('2026-07-29T10:00:00Z','2026-01-08T11:00:00Z'):
        c['season_start']='2026-01-01T00:00:00Z'
    return c

# no auto-save of date in config(): persisted when saving settings

def season_info(c=None):
    c=c or config()
    now=datetime.now(timezone.utc)
    start=datetime.fromisoformat(c['season_start'].replace('Z','+00:00'))
    return {'year':start.year,'current_year':now.year,'needs_confirmation':False,
            'message':'Stats are calculated from January 1 of the selected ranked year.'}

def sanitized_config():
    c=config(); c['api_key']=''; c['has_key']=bool(config().get('api_key')); return c

def status(message=None, done=None, total=None):
    with lock:
        if message is not None: state['progress']=message
        if done is not None: state['done']=done
        if total is not None: state['total']=total

def check_cancel():
    if state['cancel']: raise InterruptedError('Scan cancelled. Downloaded matches remain in the cache.')

def request(host, path, params=None):
    c=config()
    key=c.get('api_key','').strip()
    if not key: raise RuntimeError('Enter your Riot Personal API key first.')
    qs=('?'+urllib.parse.urlencode(params)) if params else ''
    url=f'https://{host}.api.riotgames.com{path}{qs}'
    for attempt in range(7):
        check_cancel()
        # Personal key: 100 requests per rolling 120 seconds. Reserve some headroom.
        # Match API method limits may be lower; honor 429 Retry-After below.
        while True:
            check_cancel()
            with lock:
                now=time.monotonic()
                while request_times and now-request_times[0]>=120:request_times.popleft()
                wait=max(0,120-(now-request_times[0])+.25) if len(request_times)>=95 else 0
                if not wait:request_times.append(now)
            if not wait:break
            status(f'Riot API rate limit: next requests in {wait:.0f} s')
            time.sleep(min(wait,2))
        req=urllib.request.Request(url,headers={'X-Riot-Token':key,'User-Agent':'PrimeCastStats/1.0','Accept':'application/json'})
        try:
            with urllib.request.urlopen(req,timeout=25) as r:
                return json.loads(r.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code==429:
                try: retry=float(e.headers.get('Retry-After','10'))
                except ValueError: retry=10
                status(f'Riot rate limit reached – waiting {retry:.0f} s')
                time.sleep(min(max(retry,2),130)); continue
            if e.code in (500,502,503,504):
                time.sleep(min(3*(attempt+1),20));continue
            explanations={400:'Invalid request',401:'API key invalid or expired',403:'API access denied (key/permissions)',404:'Riot ID not found'}
            raise RuntimeError(f'Riot HTTP {e.code}: {explanations.get(e.code,"Request failed")}') from None
        except urllib.error.URLError as e:
            if attempt==6: raise RuntimeError(f'Network error: {e.reason}') from None
            time.sleep(3)
    raise RuntimeError('Riot API did not respond after several attempts.')

def pathpart(s): return urllib.parse.quote(str(s),safe='')

def get_match_ids(puuid, region, since):
    """Season-wide list, with resumable page checkpoints and incremental refresh.

    A complete cached list is reused for 12 hours. Afterwards only recent
    pages are normally fetched, until an already-known ID is encountered.
    """
    with db() as con:
        row=con.execute('SELECT ids, complete, updated FROM matchlists WHERE puuid=? AND season_start=?',
                        (puuid,since)).fetchone()
    cached=json.loads(row[0]) if row else []
    if row and row[1] and time.time()-row[2]<43200:
        return cached, True

    def checkpoint(ids, complete):
        with db() as con:
            con.execute('INSERT OR REPLACE INTO matchlists VALUES (?,?,?,?,?)',
                        (puuid,since,json.dumps(ids,separators=(',',':')),int(complete),time.time()))

    # An interrupted initial traversal resumes from the last saved page.
    # For complete lists, start at the newest page and stop at overlap.
    incremental=bool(row and row[1])
    ids=[] if incremental else list(cached)
    existing=set(cached) if incremental else set(ids)
    new_ids=[]
    start=0 if incremental else len(ids)
    while True:
        check_cancel()
        page=request(region,'/lol/match/v5/matches/by-puuid/'+pathpart(puuid)+'/ids',
                     {'queue':420,'startTime':since,'start':start,'count':100})
        overlap=incremental and any(mid in existing for mid in page)
        page_fresh=[]
        for mid in page:
            if mid not in existing:
                page_fresh.append(mid)
                existing.add(mid)
        if incremental:
            # Keep the original complete list until the refresh is successful.
            new_ids.extend(page_fresh)
            candidate=new_ids+cached
        else:
            ids.extend(page_fresh)
            candidate=ids
            checkpoint(ids,False)
        status(f'Match list: {len(candidate)} SoloQ games found')
        if overlap or len(page)<100:
            checkpoint(candidate,True)
            return candidate,True
        start+=100


def publish_profile(profile, baseline_revision):
    """Commit a scan result to the CURRENT display slot, not the slot at scan start.

    The I/O-heavy Riot request runs without holding the UI/config lock. Only
    the tiny final SQLite write is serialized with team swaps, so switching
    sides while 10,000 matches are loading never cancels or loses the scan.
    """
    with lock:
        target=(int(profile['slot'])+5*((side_revision-baseline_revision)%2))%10
        out=dict(profile)
        out['slot']=target
        with db() as con:
            con.execute('INSERT OR REPLACE INTO results (slot,body) VALUES (?,?)',
                        (target,json.dumps(out,ensure_ascii=False)))


def resolve_player(slot, riot_id, region, platform, since, baseline_revision=0):
    """Fetch inexpensive profile/rank data and publish it immediately."""
    if '#' not in riot_id: raise ValueError('Use the Riot ID format Name#TAG.')
    name,tag=riot_id.rsplit('#',1)
    if not name.strip() or not tag.strip(): raise ValueError('Use the Riot ID format Name#TAG.')
    acct=request(region,'/riot/account/v1/accounts/by-riot-id/'+pathpart(name.strip())+'/'+pathpart(tag.strip()))
    puuid=acct['puuid']
    entries=request(platform,'/lol/league/v4/entries/by-puuid/'+pathpart(puuid))
    solo=next((x for x in entries if x.get('queueType')=='RANKED_SOLO_5x5'),None)
    wins=int(solo.get('wins',0)) if solo else 0
    losses=int(solo.get('losses',0)) if solo else 0
    partial={'slot':slot,'riot_id':f'{acct.get("gameName",name)}#{acct.get("tagLine",tag)}',
             'puuid':puuid,'season_start':since,
             'tier':solo.get('tier') if solo else None,'rank':solo.get('rank') if solo else None,
             'lp':solo.get('leaguePoints') if solo else None,'wins':wins,'losses':losses,
             'games':wins+losses,'winrate':round(wins/(wins+losses)*100,1) if wins+losses else None,
             'top':[],'season_games':0,'found_ids':0,'missing':0,'complete':False,
             'updated':datetime.now(timezone.utc).isoformat()}
    # Keep already-complete champion stats on refresh while new matches download.
    with lock:
        target=(slot+5*((side_revision-baseline_revision)%2))%10
        with db() as con:
            old=con.execute('SELECT body FROM results WHERE slot=?',(target,)).fetchone()
    if old:
        try:
            existing=json.loads(old[0])
            if existing.get('puuid')==puuid and existing.get('season_start')==since:
                for key in ('top','season_games','found_ids','missing','complete'):
                    partial[key]=existing.get(key,partial[key])
        except (ValueError,TypeError): pass
    publish_profile(partial,baseline_revision)
    return partial


def analyze_champions(profile, since, region):
    """Finish season champion analysis, using locally cached match bodies."""
    puuid=profile['puuid']
    ids,complete=get_match_ids(puuid,region,since)
    champions=defaultdict(lambda:{'games':0,'wins':0})
    valid=0;missing=0
    for i,mid in enumerate(ids):
        check_cancel()
        if i==0 or i%10==0:
            status(f'{profile["riot_id"]}: {i}/{len(ids)} matches processed',i,len(ids))
        try:player=get_player_match(mid,puuid,region,datetime.fromtimestamp(since,timezone.utc).year)
        except RuntimeError as e:
            if 'HTTP 404' in str(e):missing+=1;continue
            raise
        p=player
        if p is None:missing+=1;continue
        champ=p.get('championName') or str(p.get('championId','?'))
        champions[champ]['games']+=1
        champions[champ]['wins']+=int(bool(p.get('win')))
        valid+=1
    top=sorted(({'name':name,'games':v['games'],'wins':v['wins'],
                 'winrate':round(v['wins']/v['games']*100,1)}
                for name,v in champions.items()),key=lambda c:(-c['games'],-c['wins'],c['name']))[:5]
    profile.update(top=top,season_games=valid,found_ids=len(ids),missing=missing,
                   complete=complete and missing==0,updated=datetime.now(timezone.utc).isoformat())
    return profile

def runner():
    """Ranks first for all players; then one match/player/round in fair order.

    No sampling or hidden shortcuts: the complete selected ranked year's
    match list is still evaluated for each player. On interruption, already
    downloaded match bodies and paginated lists are retained in SQLite.
    """
    try:
        with lock:
            c=config()
            baseline_revision=side_revision
        try:since=int(datetime.fromisoformat(c['season_start'].replace('Z','+00:00')).timestamp())
        except Exception:raise ValueError('Invalid season start')
        slots=[(teamindex*5+roleindex,player.strip())
               for teamindex,team in enumerate(c['teams'][:2])
               for roleindex,player in enumerate(team.get('players',[])[:5]) if player.strip()]
        if not slots:raise ValueError('Enter at least one Riot ID.')
        profiles=[]
        for step,(slot,riot_id) in enumerate(slots,1):
            check_cancel()
            status(f'Rank {step}/{len(slots)}: {riot_id}',step-1,len(slots))
            try:profiles.append(resolve_player(slot,riot_id,c['region'],c['platform'],since,baseline_revision))
            except (RuntimeError,ValueError) as e:
                result={'slot':slot,'riot_id':riot_id,'error':str(e)}
                publish_profile(result,baseline_revision)
        status('Ranks ready · Preparing season match lists',0,len(profiles))
        # Collect IDs first, then interleave downloads. No player monopolizes
        # the match API while the other nine are still waiting.
        scans=[]
        for step,profile in enumerate(profiles,1):
            check_cancel()
            status(f'Match list {step}/{len(profiles)}: {profile["riot_id"]}')
            try:
                ids,complete=get_match_ids(profile['puuid'],c['region'],since)
                scans.append({'profile':profile,'ids':ids,'complete':complete,
                              'champions':defaultdict(lambda:{'games':0,'wins':0}),
                              'valid':0,'missing':0})
            except (RuntimeError,ValueError) as exc:
                profile['update_error']=str(exc)
                publish_profile(profile,baseline_revision)
        total=sum(len(scan['ids']) for scan in scans)
        done=0
        max_rounds=max((len(scan['ids']) for scan in scans),default=0)
        def publish_progress(scan, finished=False):
            """Publish *partial* top 5 while the round-robin scan is running.

            Never label a partial result as a complete season. We intentionally
            retain per-match records, with no precomputed champion-stat table.
            """
            # A previous, more complete display must not collapse to a
            # single champion on refresh. Keep that existing summary until
            # this scan has caught up, and retain a fully completed summary
            # until the refreshed season scan finishes.
            if not finished and (scan['previous_complete'] or scan['valid'] < scan['previous_valid']):
                return
            profile=scan['profile']
            champs=scan['champions']
            top=sorted(({'name':name,'games':v['games'],'wins':v['wins'],
                         'winrate':round(v['wins']/v['games']*100,1)}
                        for name,v in champs.items()),
                       key=lambda x:(-x['games'],-x['wins'],x['name']))[:5]
            profile.update(top=top,season_games=scan['valid'],
                           found_ids=len(scan['ids']),missing=scan['missing'],
                           complete=bool(finished and scan['complete'] and scan['missing']==0),
                           updated=datetime.now(timezone.utc).isoformat())
            publish_profile(profile,baseline_revision)
            scan['last_published']=time.monotonic()
            scan['last_published_count']=scan['processed']
        # An empty season list is also a completed state, not a stalled player.
        for scan in scans:
            scan.update(processed=0,last_published=0.0,last_published_count=0,
                        previous_valid=int(scan['profile'].get('season_games') or 0),
                        previous_complete=bool(scan['profile'].get('complete')))
            if not scan['ids']: publish_progress(scan,finished=True)
        for i in range(max_rounds):
            for scan in scans:
                check_cancel()
                if i >= len(scan['ids']): continue
                profile=scan['profile']
                mid=scan['ids'][i]
                status(f'Season scan {done+1}/{total} · {profile["riot_id"]} · match {i+1}/{len(scan["ids"])}',done,total)
                try:
                    p=get_player_match(mid,profile['puuid'],c['region'],datetime.fromtimestamp(since,timezone.utc).year)
                    if p is None: scan['missing']+=1
                    else:
                        champion=p.get('championName') or str(p.get('championId','?'))
                        scan['champions'][champion]['games']+=1
                        scan['champions'][champion]['wins']+=int(bool(p.get('win')))
                        scan['valid']+=1
                except RuntimeError as exc:
                    if 'HTTP 404' in str(exc):scan['missing']+=1
                    else:raise
                done+=1
                scan['processed']+=1
                finished=i==len(scan['ids'])-1
                # First match is visible immediately. Thereafter update at
                # manageable intervals, even when the total takes 30+ min.
                if (finished or scan['processed']==1 or
                    (scan['processed']-scan['last_published_count']>=5 and
                     time.monotonic()-scan['last_published']>=2.0)):
                    publish_progress(scan,finished=finished)
        status('Update complete',done,total)
    except (RuntimeError,ValueError,InterruptedError) as e:
        with lock:state['error']=str(e);state['progress']=str(e)
    except Exception as e:
        traceback.print_exc()
        with lock:state['error']=f'Unexpected error: {e}';state['progress']=str(e)
    finally:
        with lock:state['running']=False;state['cancel']=False;state['updated']=datetime.now(timezone.utc).isoformat()

def results():
    with lock:
        with db() as con:
            rows=con.execute('SELECT slot,body FROM results').fetchall()
    return {str(i):json.loads(b) for i,b in rows}

# Postgame is intentionally independent from the compact SoloQ match cache.
# Keep only computed broadcast statistics and timeline points, not the Riot raw responses.
postgame_state = {'running': False, 'progress': 'Bereit', 'error': None, 'match_id': None}
H2H_LOCK = threading.RLock()
h2h_control = {'role': 0, 'auto': False, 'seconds': 18, 'changed': time.monotonic()}

def current_h2h_control():
    with H2H_LOCK:
        state = dict(h2h_control)
        if state['auto']:
            steps = int(max(0, time.monotonic() - state['changed']) // state['seconds'])
            state['role'] = (state['role'] + steps) % 5
        state.pop('changed', None)
        return state

POSTGAME_LOCK = threading.RLock()


def parse_match_id(value, platform):
    raw = str(value or '').strip().upper()
    if re.fullmatch(r'\d{7,16}', raw):
        return platform.upper() + '_' + raw
    if re.fullmatch(r'[A-Z0-9]{2,8}_\d{7,16}', raw):
        return raw
    raise ValueError('Game ID (z. B. 8041235471) oder Match ID (z. B. EUW1_8041235471) eingeben.')


def normalize_riot_id(s):
    return re.sub(r'\s+', ' ', str(s or '').strip()).casefold()


def match_to_broadcast(match, timeline, conf):
    info = match['info']
    participants = info.get('participants') or []
    if len(participants) != 10:
        raise ValueError('Riot did not return exactly ten players for this match.')
    lineup = {}
    for team_i, team in enumerate(conf.get('teams', [])[:2]):
        for role_i, riotid in enumerate(team.get('players', [])[:5]):
            if riotid:
                lineup[normalize_riot_id(riotid)] = (team_i, role_i)
    # Lanes from Riot data may be blank/unreliable for Custom Games; honor configured names first.
    slots_by_team = {100: {}, 200: {}}
    identified = []
    for index, p in enumerate(participants):
        teamid = int(p.get('teamId', 0))
        if teamid not in slots_by_team:
            raise ValueError('Unknowne Team-ID in Matchdaten.')
        rid = (str(p.get('riotIdGameName') or p.get('summonerName') or 'Unknown') +
               ('#' + str(p.get('riotIdTagline')) if p.get('riotIdTagline') else ''))
        key = normalize_riot_id(rid)
        match_role = lineup.get(key)
        role_i = match_role[1] if match_role and match_role[0] == (0 if teamid == 100 else 1) else None
        riot_role = str(p.get('teamPosition') or p.get('individualPosition') or '').upper()
        role_alias = {'TOP': 0, 'JUNGLE': 1, 'MIDDLE': 2, 'MID': 2, 'BOTTOM': 3, 'ADC': 3, 'UTILITY': 4, 'SUPPORT': 4}
        if role_i is None: role_i = role_alias.get(riot_role)
        if role_i is not None and role_i in slots_by_team[teamid]: role_i = None
        player = {'name': rid, 'champion': p.get('championName') or '',
                  'champion_id': int(p.get('championId') or 0),
                  'skin_id': int(p.get('championSkinId') or p.get('skinId') or 0),
                  'kills': int(p.get('kills') or 0), 'deaths': int(p.get('deaths') or 0),
                  'assists': int(p.get('assists') or 0),
                  'gold': int(p.get('goldEarned') or 0),
                  'xp': int(p.get('champExperience') or 0),
                  'vision': int(p.get('visionScore') or 0),
                  'damage_taken': int(p.get('totalDamageTaken') or 0),
                  'mitigated': int(p.get('damageSelfMitigated') or 0),
                  'cc_time': round(float(p.get('timeCCingOthers') or 0), 1),
                  'wards': int(p.get('wardsPlaced') or 0),
                  'control_wards': int(p.get('detectorWardsPlaced') or 0),
                  'damage_types': {'physical':int(p.get('physicalDamageDealtToChampions') or 0), 'magic':int(p.get('magicDamageDealtToChampions') or 0), 'true':int(p.get('trueDamageDealtToChampions') or 0)},
                  'damage': int(p.get('totalDamageDealtToChampions') or 0),
                  'cs': int(p.get('totalMinionsKilled') or 0) + int(p.get('neutralMinionsKilled') or 0),
                  'team_id': teamid, 'participant_id': int(p.get('participantId') or index + 1), 'win': bool(p.get('win', False)),
                  'role': ROLES[role_i] if role_i is not None else None}
        identified.append((teamid, role_i, player))
        if role_i is not None: slots_by_team[teamid][role_i] = player
    # Stable fallback in case Riot omitted custom-game lane assignments.
    teams_players = {}
    for teamid in (100, 200):
        items = slots_by_team[teamid]
        remaining = [p for tid, ri, p in identified if tid == teamid and (ri is None or items.get(ri) is not p)]
        for role_i in range(5):
            if role_i not in items and remaining:
                pl = remaining.pop(0); pl['role'] = ROLES[role_i]; items[role_i] = pl
        teams_players[teamid] = [items[r] for r in range(5)]
    team_records = {int(t.get('teamId')): t for t in info.get('teams', [])}
    data_teams = []
    for teamid in (100, 200):
        players = teams_players[teamid]
        entry = team_records.get(teamid, {})
        objectives = entry.get('objectives') or {}
        def objective(k):
            return int((objectives.get(k) or {}).get('kills') or 0)
        bans = sorted(entry.get('bans') or [], key=lambda x: x.get('pickTurn', 0))
        obj = {'turrets': objective('tower'), 'dragons': objective('dragon'),
               'barons': objective('baron'), 'heralds': objective('riftHerald'),
               'voidgrubs': objective('horde'), 'elders': None}
        data_teams.append({'team_id': teamid, 'win': bool(entry.get('win', players[0].get('win', False))),
                           'kills': sum(p['kills'] for p in players), 'deaths': sum(p['deaths'] for p in players),
                           'assists': sum(p['assists'] for p in players),
                           'gold': sum(p['gold'] for p in players),
                           'damage': sum(p['damage'] for p in players),
                           'objectives': obj,
                           'bans': [int(b.get('championId') or 0) for b in bans if int(b.get('championId') or 0) > 0],
                           'players': players})
    # Derived per-player values from the final match, not inferred from SoloQ.
    for team in data_teams:
        total_kills = team['kills']
        total_damage = max(1, team['damage'])
        for player in team['players']:
            player['kp'] = round(100 * (player['kills'] + player['assists']) / total_kills, 1) if total_kills else 0
            player['damage_share'] = round(100 * player['damage'] / total_damage, 1)
            player['gold_share'] = round(100 * player['gold'] / max(1, team['gold']), 1)
    # Extract 15-minute laning stats and objective/first blood events from timeline.
    events = []
    early_stats = {100: {}, 200: {}}
    if timeline:
        frames = (timeline.get('info') or {}).get('frames') or []
        anchor = min(frames, key=lambda f: abs(int(f.get('timestamp') or 0) - 900000)) if frames else None
        if anchor and abs(int(anchor.get('timestamp') or 0) - 900000) <= 65000:
            pf = anchor.get('participantFrames') or {}
            for team_id, team_players in teams_players.items():
                for player in team_players:
                    record = pf.get(str(player['participant_id'])) or {}
                    if record:
                        early_stats[team_id][player['participant_id']] = {
                            'gold': record.get('totalGold'),
                            'cs': int(record.get('minionsKilled') or 0) + int(record.get('jungleMinionsKilled') or 0),
                            'xp': record.get('xp')}
        first_blood = None
        first_baron = None
        for frame in frames:
            for event in frame.get('events') or []:
                etype = event.get('type')
                ms = int(event.get('timestamp') or 0)
                tid = int(event.get('killerTeamId') or 0)
                if tid not in (100, 200):
                    killer = int(event.get('killerId') or 0)
                    tid = next((pidteam for pidteam, pls in teams_players.items() if any(pl['participant_id'] == killer for pl in pls)), 0)
                if etype == 'CHAMPION_KILL' and first_blood is None:
                    first_blood = {'minute': round(ms/60000, 3), 'team_id':tid, 'type':'first_blood'}
                if etype == 'ELITE_MONSTER_KILL':
                    monster = str(event.get('monsterType') or '').upper()
                    if monster == 'DRAGON':
                        events.append({'minute':round(ms/60000,3), 'team_id':tid, 'type':'dragon', 'subtype': str(event.get('monsterSubType') or '')})
                    elif monster == 'BARON_NASHOR' and first_baron is None:
                        first_baron = {'minute':round(ms/60000,3), 'team_id':tid, 'type':'baron'}
        if first_blood: events.insert(0, first_blood)
        if first_baron: events.append(first_baron)
        events.sort(key=lambda e:e['minute'])
    for team_id, team_players in teams_players.items():
        for player in team_players:
            player['at15'] = early_stats[team_id].get(player['participant_id'])
    # Side-relative values are calculated at display time, so swapping teams does not alter raw data.
    # Timeline frame participantFrames holds totalGold for each player, normally every 60 s.
    gold_points = []
    if timeline:
        for frame in (timeline.get('info') or {}).get('frames', []):
            pf = frame.get('participantFrames') or {}
            totals = {100: 0, 200: 0}
            counts = {100: 0, 200: 0}
            id_team = {p['participant_id']: tid for tid, players in teams_players.items() for p in players}
            for pid, values in pf.items():
                tid = id_team.get(int(pid))
                if tid in totals and values.get('totalGold') is not None:
                    totals[tid] += int(values['totalGold']); counts[tid] += 1
            if counts[100] == 5 and counts[200] == 5:
                gold_points.append({'minute': round(int(frame.get('timestamp') or 0) / 60000, 3),
                                    'blue': totals[100], 'red': totals[200],
                                    'difference': totals[100] - totals[200]})
        # Count actual Elder kills only where explicit timeline event subtype is present.
        elder_counts = {100: 0, 200: 0}
        id_team = {p['participant_id']: tid for tid, players in teams_players.items() for p in players}
        for frame in (timeline.get('info') or {}).get('frames', []):
            for event in frame.get('events', []):
                if event.get('type') == 'ELITE_MONSTER_KILL' and event.get('monsterType') == 'DRAGON' and event.get('monsterSubType') == 'ELDER_DRAGON':
                    tid = id_team.get(int(event.get('killerId') or 0)) or int(event.get('killerTeamId') or 0)
                    if tid in elder_counts: elder_counts[tid] += 1
        for team in data_teams: team['objectives']['elders'] = elder_counts[team['team_id']]
    # For some custom matches objective fields are entirely missing, so show unavailable rather than fabricated zero.
    for team in data_teams:
        fields = (team_records.get(team['team_id'], {}).get('objectives') or {})
        for metric, source in [('turrets','tower'),('dragons','dragon'),('barons','baron'),('heralds','riftHerald'),('voidgrubs','horde')]:
            if source not in fields: team['objectives'][metric] = None
    swing = None
    # Highlight only substantial reversals of the preceding gold trend,
    # including reversals late in the game.
    SWING_THRESHOLD = 3000
    for i, a in enumerate(gold_points):
        for b in gold_points[i+1:]:
            delta_time = b['minute'] - a['minute']
            if delta_time > 3.05: break
            if delta_time < 2.5: continue
            change = b['difference'] - a['difference']
            # A swing is a significant *reversal*, not an extension of the
            # winning team's existing trend. Check the previous three minutes
            # for a material trend in the opposite direction. The curve does
            # NOT need to cross zero (e.g. -10k to -4k is a blue recovery).
            lookback = [p for p in gold_points[:i] if 1.5 <= a['minute'] - p['minute'] <= 3.5]
            if not lookback:
                continue
            previous = min(lookback, key=lambda p: abs((a['minute'] - p['minute']) - 3))
            previous_change = a['difference'] - previous['difference']
            reversing = (change > 0 and previous_change <= -750) or (change < 0 and previous_change >= 750)
            if reversing and abs(change) >= SWING_THRESHOLD and (swing is None or abs(change) > abs(swing['change'])):
                swing = {'start':a['minute'],'end':b['minute'],'change':change,
                         'team_id':100 if change > 0 else 200}
    return {'events': events, 'biggest_swing':swing, 'match_id': match['metadata']['matchId'], 'duration': int(info.get('gameDuration') or 0),
            'game_mode': info.get('gameMode'), 'game_creation': info.get('gameCreation'),
            'game_version': info.get('gameVersion'), 'teams': data_teams,
            'gold_points': gold_points, 'timeline_available': timeline is not None,
            'updated': datetime.now(timezone.utc).isoformat()}


def logo_versions(teams):
    """Content hash changes after a swap even if the logo filename stays the same."""
    versions=[]
    for team in teams:
        p=str(team.get('logo') or '')
        file=ROOT/'static'/'logos'/p.rsplit('/',1)[-1] if re.fullmatch(r'/logos/team[01]\.(png|jpg|webp)',p) else None
        versions.append(hashlib.sha256(file.read_bytes()).hexdigest()[:12] if file and file.is_file() else '')
    return versions


def postgame_for_display(match, configured_teams):
    """Map the stored Riot match sides to the user-facing team order by roster.

    The underlying Riot team IDs remain fixed. The presented left/right teams
    are swapped only when the configured rosters match that opposite ordering.
    """
    if not match or len(match.get('teams',[]))!=2:
        return match
    match_teams=match['teams']
    def score(conf, mt):
        roster={normalize_riot_id(p) for p in conf.get('players',[]) if p}
        members={normalize_riot_id(p.get('name','')) for p in mt.get('players',[])}
        return len(roster & members)
    same=score(configured_teams[0],match_teams[0])+score(configured_teams[1],match_teams[1])
    swapped=score(configured_teams[0],match_teams[1])+score(configured_teams[1],match_teams[0])
    if swapped <= same or swapped == 0:
        return match
    output=dict(match)
    output['teams']=[match_teams[1],match_teams[0]]
    output['gold_points']=[{**p,'difference':-p['difference']} for p in match.get('gold_points',[])]
    output['events']=[{**ev,'team_id':(300-ev['team_id'] if ev.get('team_id') in (100,200) else ev.get('team_id',0))} for ev in match.get('events',[])]
    if match.get('biggest_swing'):
        output['biggest_swing']={**match['biggest_swing'],'change':-match['biggest_swing']['change']}
    return output


def compute_biggest_reversal(points, duration):
    """Three-minute material gold trend reversal, including late-game reversals.

    Works on previously cached points, so a UI upgrade needs no Riot re-download.
    """
    if not points:
        return None
    best=None
    for i,a in enumerate(points):
        earlier=[p for p in points[:i] if 1.5 <= a['minute']-p['minute'] <= 3.5]
        if not earlier:
            continue
        previous=min(earlier,key=lambda p:abs(a['minute']-p['minute']-3))
        direction_before=a['difference']-previous['difference']
        for b in points[i+1:]:
            if b['minute']-a['minute']>3.05:
                break
            if b['minute']-a['minute']<2.5:
                continue
            change=b['difference']-a['difference']
            reversing=(change>=3000 and direction_before<=-750) or (change<=-3000 and direction_before>=750)
            if reversing and (best is None or abs(change)>abs(best['change'])):
                best={'start':a['minute'],'end':b['minute'],'change':change,'team_id':100 if change>0 else 200}
    return best

def load_postgame(match_id=None):
    with db() as con:
        if not match_id:
            row = con.execute('SELECT body FROM postgame ORDER BY updated DESC LIMIT 1').fetchone()
        else:
            row = con.execute('SELECT body FROM postgame WHERE match_id=?', (match_id,)).fetchone()
    if not row:return None
    record=json.loads(row[0])
    # Upgrade previously cached graph analysis without contacting Riot.
    record['biggest_swing']=compute_biggest_reversal(record.get('gold_points') or [],record.get('duration'))
    return record


def postgame_runner(match_id):
    try:
        with POSTGAME_LOCK: postgame_state.update(progress='Loading match data…', error=None)
        c = config()
        match = request(c['region'], '/lol/match/v5/matches/' + pathpart(match_id))
        with POSTGAME_LOCK: postgame_state['progress'] = 'Loading match timeline…'
        timeline = None
        try:
            timeline = request(c['region'], '/lol/match/v5/matches/' + pathpart(match_id) + '/timeline')
        except RuntimeError as exc:
            if 'HTTP 404' not in str(exc): raise
            print('Postgame: Timeline unavailable; graph will be omitted.')
        result = match_to_broadcast(match, timeline, c)
        with db() as con:
            con.execute('INSERT OR REPLACE INTO postgame (match_id,body,updated) VALUES (?,?,?)',
                        (match_id, json.dumps(result, ensure_ascii=False, separators=(',',':')), time.time()))
        with POSTGAME_LOCK: postgame_state.update(progress='Postgame loaded', error=None)
        print('Postgame loaded:', match_id, flush=True)
    except Exception as exc:
        traceback.print_exc()
        with POSTGAME_LOCK: postgame_state.update(progress='Postgame error', error=str(exc))
    finally:
        with POSTGAME_LOCK: postgame_state['running'] = False


# All assets are served by this local backend. OBS never depends on an external
# SVG request. CommunityDragon is preferred; a public Data Dragon mirror is
# used only when CommunityDragon is unavailable. Downloads are persisted.
ROLE_ICON_DIR = ROOT/'static'/'lanes'
ROLE_ICON_ALIASES = {
    'top':['top','lane=top','position=top'],
    'jungle':['jungle','jungler','jgl','lane=jungle','position=jungle'],
    'mid':['mid','middle','lane=mid','lane=middle','position=mid'],
    'adc':['adc','bot','bottom','lane=bot','lane=bottom','position=bot'],
    'support':['support','supp','utility','lane=support','position=support'],
}
ROLE_CDRAGON = {'top':'top', 'jungle':'jungle', 'mid':'middle', 'adc':'bottom', 'support':'utility'}
ROLE_GITHUB = {'top':'top', 'jungle':'jungle', 'mid':'middle', 'adc':'bottom', 'support':'support'}
role_icon_lock = threading.RLock()
# Counts team-side swaps. A SoloQ scan always retains its original logical slots;
# data is published into the CURRENT side by applying the swap revision.
side_revision = 0
role_retry_after = {}

def read_role_png(role):
    """Return local official/source PNG bytes, installing them when reachable."""
    if role not in ROLE_ICON_ALIASES: return None
    ROLE_ICON_DIR.mkdir(parents=True,exist_ok=True)
    # Prefer an explicitly supplied icon / cached downloaded file; accept the
    # original source's names without requiring renaming.
    for stem in ROLE_ICON_ALIASES[role]:
        for candidate in ROLE_ICON_DIR.glob('*.png'):
            if candidate.stem.casefold()==stem.casefold():
                payload=candidate.read_bytes()
                if payload.startswith(b'\x89PNG\r\n\x1a\n'):return payload
    with role_icon_lock:
        # Avoid repeated timeouts if the CDN is down during a cast.
        if time.monotonic() < role_retry_after.get(role,0):return None
        base='https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-clash/global/default/assets/images/position-selector/positions/'
        github='https://raw.githubusercontent.com/cjc2040/LoL_DDragon/main/extras/lanes/'
        sources=[base+'icon-position-'+ROLE_CDRAGON[role]+'.png',github+ROLE_GITHUB[role]+'.png']
        for url in sources:
            try:
                req=urllib.request.Request(url,headers={'User-Agent':'PrimeCastStats/1.20','Accept':'image/png'})
                with urllib.request.urlopen(req,timeout=4) as resp:
                    payload=resp.read(350_000)
                if not payload.startswith(b'\x89PNG\r\n\x1a\n'):continue
                tmp=ROLE_ICON_DIR/(role+'.tmp')
                tmp.write_bytes(payload)
                tmp.replace(ROLE_ICON_DIR/(role+'.png'))
                print('Cached role PNG locally:',role)
                return payload
            except (OSError,ValueError) as exc:
                print('Role asset source unreachable:',role,type(exc).__name__)
        role_retry_after[role]=time.monotonic()+90
        return None

def warm_role_icons():
    for role in ROLE_ICON_ALIASES:
        try:read_role_png(role)
        except Exception: pass

# Prime League public match pages: extract ONLY broadcast metadata from the
# main team cards and their own lineup links. The listing order does not
# establish Blue/Red side. The web server may reject automated requests (403);
# a Firefox "Save page, complete" ZIP is also supported for offline import.
class PrimeLeagueHTML(HTMLParser):
    VOID={'img','br','hr','input','meta','link','source','wbr','area','embed','col','base'}
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack=[];self.head=None;self.lineup=None;self.capture=None
        self.teams=[{'name':'','short_name':'','opgg_url':'','players':[], 'logo_source':''} for _ in range(2)]
    def handle_starttag(self,tag,attr):
        a=dict(attr);cls=set((a.get('class') or '').split());depth=len(self.stack)+1
        if tag=='div' and 'content-match-head-team' in cls:
            self.head=(0 if 'side-1' in cls else 1 if 'side-2' in cls else None,depth)
        if tag=='ul' and 'content-match-lineup' in cls:
            self.lineup=(0 if 'side-1' in cls else 1 if 'side-2' in cls else None,depth)
        hi=self.head[0] if self.head else None
        li=self.lineup[0] if self.lineup else None
        if hi is not None:
            if tag=='img' and a.get('alt') and not self.teams[hi]['name']:
                self.teams[hi]['name']=a['alt'].strip()[:50]
                self.teams[hi]['logo_source']=a.get('data-src') or a.get('src') or ''
            if tag=='h2':self.capture=('short_name',hi,depth)
        if li is not None:
            if tag=='a' and 'op.gg' in (a.get('href') or '').lower() and 'multisearch' in (a.get('href') or '').lower():
                self.teams[li]['opgg_url']=a['href']
            if tag=='span' and 'gameaccount-name' in cls:self.capture=('riot',li,depth)
        if tag not in self.VOID:self.stack.append(tag)
    def handle_data(self,data):
        if not self.capture:return
        which,i,_=self.capture
        if which=='short_name':self.teams[i]['short_name']+=data.strip()
        if which=='riot':
            player=data.strip()
            if player and '#' in player and player not in self.teams[i]['players']:
                self.teams[i]['players'].append(player)
    def handle_endtag(self,tag):
        if not self.stack:return
        # HTML from a user-saved page can contain unbalanced browser-injected tags.
        idx=next((j for j in range(len(self.stack)-1,-1,-1) if self.stack[j]==tag),None)
        if idx is None:return
        depth=idx+1
        if self.capture and depth<=self.capture[2]:self.capture=None
        if self.head and depth<=self.head[1]:self.head=None
        if self.lineup and depth<=self.lineup[1]:self.lineup=None
        del self.stack[idx:]


def parse_prime_league_page(html, archive=None, document_url=None):
    parser=PrimeLeagueHTML();parser.feed(html)
    output=[]
    for team in parser.teams:
        if not team['name'] or not team['short_name']:
            raise ValueError('Could not find two Prime League team cards in the supplied match page.')
        source=team.pop('logo_source')
        logo=None
        # Firefox Complete Save rewrites img src into a local *_files folder.
        if archive is not None and source and not urllib.parse.urlsplit(source).netloc:
            basename=urllib.parse.unquote(urllib.parse.urlsplit(source).path).replace('\\','/').split('/')[-1]
            match=next((n for n in archive.namelist() if not n.endswith('/') and n.split('/')[-1]==basename),None)
            if match and archive.getinfo(match).file_size<=750_000:
                data=archive.read(match)
                logo=encode_logo(data)
        elif source and document_url:
            url=urllib.parse.urljoin(document_url,source)
            parts=urllib.parse.urlsplit(url)
            host=(parts.hostname or '').lower()
            if parts.scheme=='https' and (host.endswith('.heartbase.gg') or host=='primeleague.gg' or host.endswith('.primeleague.gg')):
                try:
                    req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'image/avif,image/webp,image/png,image/jpeg,*/*'})
                    with urllib.request.urlopen(req,timeout=8) as resp: logo=encode_logo(resp.read(760000))
                except (OSError, ValueError, urllib.error.URLError):pass
        team['logo_data']=logo
        # The OP.GG link gives the source order, NOT a reliable role assignment.
        team['players']=team['players'][:10]
        output.append(team)
    return output


def encode_logo(data):
    if len(data)>750000:return None
    if data.startswith(b'\x89PNG\r\n\x1a\n'):mime='png'
    elif data.startswith(b'\xff\xd8\xff'):mime='jpeg'
    elif len(data)>12 and data[:4]==b'RIFF' and data[8:12]==b'WEBP':mime='webp'
    else:return None
    return 'data:image/'+mime+';base64,'+base64.b64encode(data).decode('ascii')


def browser_public_prime_page(url, report=None):
    """Optional standard Edge/Chrome DOM fetch of a *publicly viewable* page.

    Does not bypass a login, challenge, CAPTCHA, or a blocked website.
    This is a best-effort browser rendering fallback for plain HTTP clients;
    it has no access to the user's logged-in Firefox session.
    """
    candidates=[]
    if os.name=='nt':
        env=os.environ
        for root in (env.get('PROGRAMFILES(X86)'),env.get('PROGRAMFILES'),env.get('LOCALAPPDATA')):
            if root:
                for rel in ('Microsoft/Edge/Application/msedge.exe',
                            'Google/Chrome/Application/chrome.exe'):
                    exe=Path(root)/rel
                    if exe.is_file(): candidates.append(str(exe))
    else:
        for cmd in ('chromium','chromium-browser','google-chrome'):
            exe=shutil.which(cmd)
            if exe: candidates.append(exe)
    if not candidates:
        if report is not None: report.append({'method':'Headless browser','status':'not installed','detail':'Edge / Chrome / Chromium not found'})
        return None
    for browser in candidates[:2]:
        try:
            with tempfile.TemporaryDirectory(prefix='primecast-public-match-') as profile_dir:
                command=[browser,'--headless=new','--disable-gpu','--no-first-run',
                         '--no-default-browser-check','--disable-extensions',
                         '--disable-background-networking',
                         '--user-data-dir='+profile_dir,
                         '--virtual-time-budget=3500','--dump-dom',url]
                if os.name!='nt': command.insert(1,'--no-sandbox')
                p=subprocess.run(command,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL,timeout=20,check=False)
                if p.returncode!=0 or len(p.stdout)>2_500_000:
                    if report is not None: report.append({'method':'Headless '+Path(browser).stem,'status':'failed','return_code':p.returncode})
                    continue
                raw=p.stdout.decode('utf-8','replace')
                matches='content-match-head-team' in raw and 'content-match-lineup' in raw
                if report is not None:
                    import html as htmlmod
                    title=re.search(r'<title[^>]*>(.*?)</title>',raw,re.I|re.S)
                    report.append({'method':'Headless '+Path(browser).stem,
                                   'status':'match parsed' if matches else 'page lacks match data',
                                   'title':htmlmod.unescape(re.sub(r'\s+',' ',title.group(1)).strip())[:100] if title else '(no title)',
                                   'html_bytes':len(p.stdout)})
                if matches:return raw
        except (OSError,subprocess.TimeoutExpired,UnicodeError) as exc:
            if report is not None: report.append({'method':'Headless '+Path(browser).stem,'status':type(exc).__name__})
    return None


def prime_league_diagnostics(url):
    """Read-only public URL checks. No cookies, credentials or page content returned.

    Allows users to distinguish a Python HTTP block from a browser-level block.
    Restricted to validated Prime League HTTPS match pages (not arbitrary hosts).
    """
    u=urllib.parse.urlsplit(str(url or '').strip())
    if (u.scheme!='https' or (u.hostname or '').lower() not in
        ('primeleague.gg','www.primeleague.gg') or
        not re.fullmatch(r'/(?:en|de)/leagues/matches/\d+[-\w]*',u.path)):
        raise ValueError('Enter a valid Prime League match URL to diagnose.')
    result={'match_id':re.search(r'/matches/(\d+)',u.path).group(1),
            'checks':[], 'tip':'Open the match URL in Firefox. If Firefox works and both app checks fail, the site is restricting automated requests.'}
    req=urllib.request.Request(url,headers={
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/132.0 Safari/537.36',
        'Accept':'text/html,application/xhtml+xml', 'Accept-Language':'en-US,en;q=0.9'})
    start=time.monotonic()
    try:
        with urllib.request.urlopen(req,timeout=10) as resp:
            final=urllib.parse.urlsplit(resp.geturl())
            content=resp.read(90_000)
            result['checks'].append({'method':'Python HTTP','status':resp.status,
                'elapsed_seconds':round(time.monotonic()-start,1),
                'content_type':resp.headers.get('Content-Type',''),
                'expected_page':b'content-match-head-team' in content,
                'same_host':(final.hostname or '').lower() in ('www.primeleague.gg','primeleague.gg')})
    except urllib.error.HTTPError as exc:
        hdr=exc.headers
        sample=exc.read(24000).decode('utf-8','replace').lower()
        result['checks'].append({'method':'Python HTTP','status':exc.code,
            'elapsed_seconds':round(time.monotonic()-start,1),
            'server':(hdr.get('Server','') or '')[:80],
            'content_type':(hdr.get('Content-Type','') or '')[:100],
            'challenge_detected':any(t in sample for t in ('captcha','cloudflare','access denied','security check','bot protection','blocked'))})
    except (urllib.error.URLError,TimeoutError,OSError) as exc:
        result['checks'].append({'method':'Python HTTP','error':type(exc).__name__,
            'elapsed_seconds':round(time.monotonic()-start,1)})
    browser_records=[]
    html=browser_public_prime_page(url,report=browser_records)
    result['checks']+=browser_records
    result['browser_has_match']=bool(html)
    result['summary']='Public match page accessible in headless browser' if html else 'Could not retrieve match data automatically'
    return result


# PrimeCast / Firefox userscript bridge. The website can refuse scripted HTTP (403)
# while still displaying public match information to the user in Firefox.
# A locally installed userscript sends ONLY the visible team/lineup metadata.
# No Firefox cookies, session data or login information are read or transferred.
prime_bridge_lock=threading.RLock()
prime_bridge={'status':'idle','match_id':'','url':'','nonce':'','created':0.0,'teams':None,'error':''}

def valid_prime_league_url(url):
    u=urllib.parse.urlsplit(str(url or '').strip())
    if u.scheme!='https' or (u.hostname or '').lower() not in ('primeleague.gg','www.primeleague.gg'):
        raise ValueError('Enter a valid Prime League match URL.')
    m=re.fullmatch(r'/(?:[a-z]{2}(?:-[a-z]{2})?/)?leagues/matches/(\d+)[-\w]*',u.path,re.I)
    if not m:raise ValueError('Enter a Prime League match URL, not a team page.')
    return m.group(1)

def begin_prime_browser_import(url):
    match_id=valid_prime_league_url(url)
    with prime_bridge_lock:
        prime_bridge.update(status='waiting',match_id=match_id,url=str(url).strip(),nonce=secrets.token_urlsafe(32),
                            created=time.monotonic(),teams=None,error='')
    return {'ok':True,'match_id':match_id}

def browser_import_pending():
    with prime_bridge_lock:
        if prime_bridge['status']!='waiting' or time.monotonic()-prime_bridge['created']>120:
            return {'waiting':False}
        return {'waiting':True,'match_id':prime_bridge['match_id'],'nonce':prime_bridge['nonce']}

def browser_import_status():
    with prime_bridge_lock:
        st=prime_bridge['status']
        if st=='waiting' and time.monotonic()-prime_bridge['created']>120:st='timeout'
        out={'status':st,'match_id':prime_bridge['match_id'],'error':prime_bridge['error']}
        if st=='ready':out['teams']=prime_bridge['teams']
        return out

def accept_prime_browser_import(payload):
    with prime_bridge_lock:
        if prime_bridge['status']!='waiting' or time.monotonic()-prime_bridge['created']>120:
            raise ValueError('There is no pending match import.')
        if not secrets.compare_digest(str(payload.get('nonce','')),prime_bridge['nonce']):
            raise ValueError('Import token mismatch.')
        if valid_prime_league_url(payload.get('url',''))!=prime_bridge['match_id']:
            raise ValueError('The Firefox match URL does not match the requested match.')
        incoming=payload.get('teams')
        if not isinstance(incoming,list) or len(incoming)!=2:
            raise ValueError('Firefox did not find two teams on the page.')
        teams=[]
        for t in incoming:
            if not isinstance(t,dict):raise ValueError('Invalid team data from Firefox.')
            name=str(t.get('name','')).strip()[:80]
            short=str(t.get('short_name','')).strip()[:20]
            if not name or not short:raise ValueError('Team name / short name missing.')
            players=t.get('players') or []
            if not isinstance(players,list):raise ValueError('Invalid player list.')
            players=list(dict.fromkeys(str(p).strip()[:100] for p in players if isinstance(p,str) and '#' in p))[:10]
            opgg=str(t.get('opgg_url',''))[:2048]
            if opgg:
                opgg_parts=urllib.parse.urlsplit(opgg)
                if opgg_parts.scheme!='https' or not (opgg_parts.hostname or '').lower().endswith('op.gg') or 'multisearch' not in opgg_parts.path:
                    opgg=''
            logo=t.get('logo_data')
            if logo is not None:
                if not isinstance(logo,str) or len(logo)>1_010_000:logo=None
                else:
                    m=re.fullmatch(r'data:image/(png|jpeg|webp);base64,([A-Za-z0-9+/=]+)',logo)
                    if not m:logo=None
                    else:
                        try:logo=encode_logo(base64.b64decode(m.group(2),validate=True))
                        except (ValueError,Exception):logo=None
            teams.append({'name':name,'short_name':short,'opgg_url':opgg,'players':players,'logo_data':logo})
        prime_bridge.update(status='ready',teams=teams,error='')
    return {'ok':True,'teams':len(teams),'players':sum(len(t['players']) for t in teams)}


def prime_league_from_url(url):
    url=str(url or '').strip()
    parsed=urllib.parse.urlsplit(url)
    if parsed.scheme!='https' or (parsed.hostname or '').lower() not in ('primeleague.gg','www.primeleague.gg') or not re.fullmatch(r'/(?:en|de)/leagues/matches/\d+[-\w]*',parsed.path):
        raise ValueError('Enter a Prime League match URL (https://www.primeleague.gg/en/leagues/matches/...).')
    req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/132.0 Safari/537.36','Accept':'text/html,application/xhtml+xml','Accept-Language':'en-US,en;q=0.9'})
    try:
        with urllib.request.urlopen(req,timeout=13) as resp:
            if (urllib.parse.urlsplit(resp.geturl()).hostname or '').lower() not in ('primeleague.gg','www.primeleague.gg'):
                raise ValueError('The page redirects outside Prime League.')
            raw=resp.read(2_500_001)
    except urllib.error.HTTPError as exc:
        if exc.code==403:
            # The site may permit an ordinary public browser view even when its
            # HTTP client route refuses the request. Try local Edge, if present.
            page=browser_public_prime_page(url)
            if page:
                return parse_prime_league_page(page,document_url=url)
            raise ValueError('Prime League returned HTTP 403. The public URL cannot be imported automatically from this computer (including browser fallback).') from None
        raise ValueError(f'Prime League returned HTTP {exc.code}.') from None
    except (OSError,TimeoutError) as exc:
        raise ValueError(f'Prime League not reachable: {exc}') from None
    if len(raw)>2_500_000:raise ValueError('Match page is too large.')
    return parse_prime_league_page(raw.decode('utf-8','replace'),document_url=url)


def prime_league_from_zip(encoded):
    raw=base64.b64decode(encoded,validate=True)
    if len(raw)>6_000_000:raise ValueError('Saved ZIP is larger than 6 MB.')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        candidates=[i for i in archive.infolist() if i.filename.lower().endswith(('.htm','.html'))
                    and '/' not in i.filename and i.file_size<2_500_000]
        if not candidates:raise ValueError('ZIP must include the saved Prime League .htm/.html file at its root.')
        page=max(candidates,key=lambda i:i.file_size)
        return parse_prime_league_page(archive.read(page).decode('utf-8','replace'),archive=archive)


# Postgame objective icons: download once, then serve exclusively from local storage.
# The old SVG placeholders remain as offline fallbacks. No manual asset setup.
OBJECTIVE_ICONS = {
    'first_blood': 'kills.png', 'baron': 'baron.png', 'dragon': 'dragon.png',
    'cloud': 'dragon_cloud.png', 'infernal': 'dragon_infernal.png',
    'ocean': 'dragon_ocean.png', 'mountain': 'dragon_mountain.png',
    'hextech': 'dragon_hextech.png', 'chemtech': 'dragon_chemtech.png',
    'elder': 'dragon_elder.png',
}
OBJECTIVE_DIR = ROOT / 'static' / 'objectives'
OBJECTIVE_CDN = 'https://raw.communitydragon.org/latest/game/assets/ux/minimap/icons/'
OBJECTIVE_KILL_URL = 'https://raw.communitydragon.org/latest/plugins/rcp-fe-lol-match-history/global/default/kills.png'
OBJECTIVE_RETRY_AFTER = 3600  # avoid hammering an unavailable CDN
_objective_failures = {}
_objective_lock = threading.Lock()

def objective_local_png(kind):
    if kind not in OBJECTIVE_ICONS: return None
    path = OBJECTIVE_DIR / (kind + '.png')
    if path.is_file():
        try:
            if path.read_bytes()[:8] == b'\x89PNG\r\n\x1a\n':
                return path
        except OSError: pass
    return None

def fetch_objective_icon(kind):
    if kind not in OBJECTIVE_ICONS or objective_local_png(kind): return
    with _objective_lock:
        if objective_local_png(kind): return
        if time.monotonic() - _objective_failures.get(kind, -1e10) < OBJECTIVE_RETRY_AFTER: return
        _objective_failures[kind] = time.monotonic()
    try:
        url = OBJECTIVE_KILL_URL if kind == 'first_blood' else OBJECTIVE_CDN + OBJECTIVE_ICONS[kind]
        req = urllib.request.Request(url, headers={
            'User-Agent':'PrimeCastStats/1.29', 'Accept':'image/png'})
        with urllib.request.urlopen(req, timeout=6) as response:
            data = response.read(500_001)
        if len(data) > 500_000 or not data.startswith(b'\x89PNG\r\n\x1a\n'):
            raise ValueError('Invalid PNG response')
        OBJECTIVE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = OBJECTIVE_DIR / (kind + '.download')
        tmp.write_bytes(data)
        tmp.replace(OBJECTIVE_DIR / (kind + '.png'))
        with _objective_lock:
            _objective_failures.pop(kind, None)
    except (OSError, ValueError, urllib.error.URLError) as exc:
        print(f'Objective icon {kind} not available yet; using local fallback: {exc}')

def warm_objective_icons():
    for kind in OBJECTIVE_ICONS:
        fetch_objective_icon(kind)
    # First Blood uses the standalone Match History kills icon.

# Damage-composition legend uses recognizable AD/AP champion-attribute symbols
# intentionally, as requested; the actual damage statistics remain Physical/Magic/True.
# Use the exact wiki-provided AP SVG and an equivalent AD media title. Wiki URLs
# can change; resolve via the MediaWiki API as a secondary path. Never serve HTML
# error pages as images. Both symbols are cached locally after a successful download.
DAMAGE_ICONS = ('physical', 'magic', 'true')
DAMAGE_DIR = ROOT / 'static' / 'damage-icons'
DAMAGE_WIKI_BASE = 'https://wiki.leagueoflegends.com/en-us/'
DAMAGE_WIKI_NAMES = {
    'physical': ('Attack_damage_icon.svg', 'Attack_damage_icon.png', 'Attack_damage_colored_icon.png'),
    'magic': ('Ability_power_icon.svg', 'Ability_power_icon.png'),
}
_damage_failure = {}
_damage_lock = threading.Lock()

def damage_icon_cached(kind):
    if kind not in DAMAGE_WIKI_NAMES:
        return None
    for suffix in ('.svg', '.png'):
        f = DAMAGE_DIR / (kind + '-wiki' + suffix)
        if f.is_file():
            try:
                data=f.read_bytes()
                if suffix == '.png' and data[:8] == b'\x89PNG\r\n\x1a\n': return f
                if suffix == '.svg' and b'<svg' in data[:1500].lower() and b'</svg>' in data[-200:].lower(): return f
            except OSError: pass
    return None

def _media_url(name):
    import urllib.parse
    q=urllib.parse.urlencode({'action':'query','format':'json','prop':'imageinfo','iiprop':'url','titles':'File:'+name})
    return DAMAGE_WIKI_BASE+'api.php?'+q

def _validated_image_download(url):
    request_obj=urllib.request.Request(url,headers={'User-Agent':'PrimeCastStats/1.32 (League esports local overlay)','Accept':'image/svg+xml,image/png,image/*;q=0.8'})
    with urllib.request.urlopen(request_obj,timeout=7) as response:
        payload=response.read(400001)
    if len(payload)>400000: raise ValueError('Image exceeds size limit')
    if payload[:8] == b'\x89PNG\r\n\x1a\n': return payload,'.png'
    if b'<svg' in payload[:1500].lower() and b'</svg>' in payload[-200:].lower(): return payload,'.svg'
    raise ValueError('Not a PNG or SVG image')

def cache_damage_icon(kind):
    if kind not in DAMAGE_WIKI_NAMES or damage_icon_cached(kind): return
    with _damage_lock:
        if damage_icon_cached(kind) or time.monotonic()-_damage_failure.get(kind,-1e9)<3600:return
        _damage_failure[kind]=time.monotonic()
    for name in DAMAGE_WIKI_NAMES[kind]:
        # First try the direct wiki media resource (matching the URL supplied by
        # the user for AP), then resolve the media title using the site's API.
        url=DAMAGE_WIKI_BASE+'images/'+urllib.parse.quote(name)
        options=[url]
        try:
            api_req=urllib.request.Request(_media_url(name),headers={'User-Agent':'PrimeCastStats/1.32'})
            with urllib.request.urlopen(api_req,timeout=6) as response:
                pages=json.loads(response.read(150000)).get('query',{}).get('pages',{})
            for page in pages.values():
                info=page.get('imageinfo',[])
                if info and info[0].get('url'): options.insert(0,info[0]['url'])
        except (OSError,ValueError,urllib.error.URLError,KeyError): pass
        for link in options:
            try:
                payload,ext=_validated_image_download(link)
                DAMAGE_DIR.mkdir(exist_ok=True,parents=True)
                tmp=DAMAGE_DIR/(kind+'.download')
                tmp.write_bytes(payload)
                tmp.replace(DAMAGE_DIR/(kind+'-wiki'+ext))
                with _damage_lock:_damage_failure.pop(kind,None)
                return
            except (OSError,ValueError,urllib.error.URLError): pass
    print('Damage icon not available yet; using bundled fallback:',kind)

def warm_damage_icons():
    for kind in DAMAGE_WIKI_NAMES: cache_damage_icon(kind)

class Handler(SimpleHTTPRequestHandler):
    def __init__(self,*args,**kwargs): super().__init__(*args,directory=str(ROOT/'static'),**kwargs)
    def log_message(self,*a): pass
    def send_json(self,obj,code=200):
        body=json.dumps(obj,ensure_ascii=False).encode('utf-8')
        self.send_response(code);self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def do_GET(self):
        path=urllib.parse.urlsplit(self.path).path
        if path.startswith('/assets/damage/'):
            kind=path.rsplit('/',1)[-1]
            if kind not in DAMAGE_ICONS:
                return self.send_error(404)
            asset=damage_icon_cached(kind) or DAMAGE_DIR/(kind+'.svg')
            if not asset.is_file():
                return self.send_error(404)
            payload=asset.read_bytes()
            self.send_response(200)
            self.send_header('Content-Type','image/png' if asset.suffix=='.png' else 'image/svg+xml')
            self.send_header('Cache-Control','no-store')
            self.send_header('Content-Length',str(len(payload)))
            self.end_headers()
            try:self.wfile.write(payload)
            except (BrokenPipeError,ConnectionAbortedError,ConnectionResetError):pass
            return
        if path.startswith('/assets/objective/'):
            name = path.rsplit('/', 1)[-1]
            # A stable local endpoint, never a browser-side CDN request.
            # SVG fallback is used until a valid original PNG is cached.
            if name not in set(OBJECTIVE_ICONS) | {'first_blood'}:
                return self.send_error(404)
            png = objective_local_png(name)
            asset = png or (OBJECTIVE_DIR / (name + '.svg'))
            if not asset.is_file(): return self.send_error(404)
            try: payload = asset.read_bytes()
            except OSError: return self.send_error(404)
            self.send_response(200)
            self.send_header('Content-Type', 'image/png' if png else 'image/svg+xml')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            try: self.wfile.write(payload)
            except (BrokenPipeError,ConnectionAbortedError,ConnectionResetError): pass
            return
        if path.startswith('/assets/role/'):
            name=path.rsplit('/',1)[-1]
            role=name.removesuffix('.png')
            if name!=role+'.png' or role not in ROLE_ICON_ALIASES:
                return self.send_error(404)
            png=read_role_png(role)
            if png is None:
                return self.send_error(404,'No local role PNG (retry scheduled)')
            self.send_response(200)
            self.send_header('Content-Type','image/png')
            self.send_header('Content-Length',str(len(png)))
            self.send_header('Cache-Control','public,max-age=3600')
            self.end_headers()
            try:self.wfile.write(png)
            except (BrokenPipeError,ConnectionAbortedError,ConnectionResetError):pass
            return
        if path.startswith('/assets/rank/'):
            name=path.rsplit('/',1)[-1]
            tier=name.removesuffix('.png')
            if name!=tier+'.png' or tier not in ('iron','bronze','silver','gold','platinum','emerald','diamond','master','grandmaster','challenger'):
                return self.send_error(404)
            cache_dir=ROOT/'static'/'cached_rank_icons'
            cache_dir.mkdir(parents=True,exist_ok=True)
            cache_file=cache_dir/name
            # Primary: downloaded CommunityDragon emblem. Backup: Rank=Bronze.png etc.
            payload=None
            if cache_file.is_file():
                candidate=cache_file.read_bytes()
                if candidate.startswith(b'\x89PNG\r\n\x1a\n'):payload=candidate
            if payload is None:
                url='https://raw.communitydragon.org/16.2/plugins/rcp-fe-lol-static-assets/global/default/images/ranked-emblem/emblem-'+tier+'.png'
                try:
                    req=urllib.request.Request(url,headers={'User-Agent':'PrimeCastStats/1.19.1','Accept':'image/png'})
                    with urllib.request.urlopen(req,timeout=5) as resp:
                        candidate=resp.read(600_000)
                    if not candidate.startswith(b'\x89PNG\r\n\x1a\n'):
                        raise ValueError('Received data is not PNG')
                    tmp=cache_file.with_suffix('.tmp')
                    tmp.write_bytes(candidate)
                    tmp.replace(cache_file)
                    payload=candidate
                except Exception as exc:
                    print('Rank emblem CDN unavailable, checking local backup:',tier,str(exc))
                    # Windows normally resolves case-insensitively; scan explicitly so
                    # this works on all supported host filesystems.
                    backup_name='rank='+tier+'.png'
                    backup=next((x for x in cache_dir.iterdir() if x.is_file() and x.name.casefold()==backup_name),None)
                    if backup:
                        candidate=backup.read_bytes()
                        if candidate.startswith(b'\x89PNG\r\n\x1a\n'):
                            payload=candidate
                    if payload is None:
                        return self.send_error(502,'Rank emblem unavailable (CDN and local backup)')
            self.send_response(200)
            self.send_header('Content-Type','image/png')
            self.send_header('Content-Length',str(len(payload)))
            self.send_header('Cache-Control','public,max-age=3600')
            self.end_headers()
            try:self.wfile.write(payload)
            except (BrokenPipeError,ConnectionAbortedError,ConnectionResetError):pass
            return
        if path=='/api/prime-league/browser-pending':return self.send_json(browser_import_pending())
        if path=='/api/prime-league/browser-status':return self.send_json(browser_import_status())
        if path=='/api/postgame/status':
            with POSTGAME_LOCK: result=dict(postgame_state)
            return self.send_json(result)
        if path=='/api/h2h-control':return self.send_json(current_h2h_control())
        if path=='/api/postgame':
            c=config();m=postgame_for_display(load_postgame(),c['teams']);return self.send_json({'match':m,'teams':c['teams'],'logo_versions':logo_versions(c['teams'])})
        if path=='/api/preferences':return self.send_json(preferences())
        if path=='/api/config':return self.send_json(sanitized_config())
        if path=='/api/results':
            with lock:
                c=config();teams=[dict(t) for t in c['teams']];versions=logo_versions(teams)
                entries=results()
            for i,t in enumerate(teams):
                if t.get('logo'):t['logo']+='?v='+versions[i]
            return self.send_json({'results':entries,'teams':teams,'season_start':c['season_start'],'season_info':season_info(c)})
        if path=='/api/season':return self.send_json(season_info())
        if path=='/api/cache/summary':return self.send_json(cache_summary())
        if path=='/api/overlay-state':
            with lock:
                elapsed=time.monotonic()-overlay_control['last_switch']
                if overlay_control['auto'] and elapsed>=overlay_control['interval']:
                    steps=int(elapsed//overlay_control['interval'])
                    overlay_control['role']=(overlay_control['role']+steps)%5
                    overlay_control['revision']+=1
                    overlay_control['last_switch']=time.monotonic()
                o=dict(overlay_control)
            return self.send_json(o)
        if path=='/api/status':
            with lock:s=dict(state)
            return self.send_json(s)
        if path=='/':self.path='/index.html'
        if path in ('/pregame','/overlay'):self.path='/overlay.html'
        if path=='/postgame':self.path='/postgame.html'
        if path=='/head2head':self.path='/head2head.html'
        return super().do_GET()
    def do_POST(self):
        try:
            n=int(self.headers.get('Content-Length','0'))
            max_size=9_000_000 if urllib.parse.urlsplit(self.path).path=='/api/prime-league/preview' else 1_100_000
            if n>max_size: return self.send_json({'error':'Payload too large'},413)
            payload=json.loads(self.rfile.read(n) or b'{}')
            path=urllib.parse.urlsplit(self.path).path
            if path=='/api/prime-league/browser-start':
                return self.send_json(begin_prime_browser_import(payload.get('url','')))
            if path=='/api/prime-league/browser-submit':
                return self.send_json(accept_prime_browser_import(payload))
            if path=='/api/prime-league/diagnose':
                return self.send_json(prime_league_diagnostics(payload.get('url','')))
            if path=='/api/prime-league/preview':
                if payload.get('zip_base64'):
                    teams=prime_league_from_zip(payload['zip_base64'])
                else:
                    teams=prime_league_from_url(payload.get('url',''))
                return self.send_json({'teams':teams,'source':'saved_zip' if payload.get('zip_base64') else 'url'})
            if path=='/api/cache/purge-old':
                return self.send_json(purge_prior_seasons())
            if path=='/api/h2h-control':
                with H2H_LOCK:
                    previous = current_h2h_control()
                    h2h_control['role'] = int(payload.get('role',previous['role']))
                    if not 0 <= h2h_control['role'] <= 4: raise ValueError('Invalid role')
                    h2h_control['auto'] = bool(payload.get('auto',previous['auto']))
                    h2h_control['seconds'] = max(5,min(60,int(payload.get('seconds',previous['seconds']))))
                    h2h_control['changed'] = time.monotonic()
                return self.send_json(current_h2h_control())
            if path=='/api/postgame/load':
                mid=parse_match_id(payload.get('game_id'),config()['platform'])
                with POSTGAME_LOCK:
                    if postgame_state['running']:
                        return self.send_json({'error':'Postgame request already running.'},409)
                    postgame_state.update(running=True,match_id=mid,error=None,progress='Starting Postgame request…')
                threading.Thread(target=postgame_runner,args=(mid,),daemon=True,name='PrimeCast-Postgame').start()
                return self.send_json({'ok':True,'match_id':mid})
            if path=='/api/team-logo':
                team=int(payload.get('team',-1))
                if team not in (0,1):raise ValueError('Invalid team selection')
                raw=str(payload.get('image',''))
                match=re.fullmatch(r'data:image/(png|jpeg|webp);base64,([A-Za-z0-9+/=]+)',raw)
                if not match:raise ValueError('Please use PNG, JPG, or WebP.')
                ext={'png':'png','jpeg':'jpg','webp':'webp'}[match.group(1)]
                binary=base64.b64decode(match.group(2),validate=True)
                if len(binary)>750000:raise ValueError('Logo too large (max. 750 KB).')
                if ext=='png' and not binary.startswith(b'\x89PNG\r\n\x1a\n'):raise ValueError('Invalid PNG file')
                if ext=='jpg' and not binary.startswith(b'\xff\xd8\xff'):raise ValueError('Invalid JPG file')
                if ext=='webp' and not (binary[:4]==b'RIFF' and binary[8:12]==b'WEBP'):raise ValueError('Invalid WebP file')
                folder=ROOT/'static'/'logos';folder.mkdir(exist_ok=True)
                for old in folder.glob(f'team{team}.*'):old.unlink()
                (folder/f'team{team}.{ext}').write_bytes(binary)
                return self.send_json({'logo':f'/logos/team{team}.{ext}'})
            if path=='/api/team-logo-remove':
                team=int(payload.get('team',-1))
                if team not in (0,1):raise ValueError('Invalid team selection')
                folder=ROOT/'static'/'logos'
                if folder.exists():
                    for old in folder.glob(f'team{team}.*'):old.unlink()
                c=config();c['teams'][team]['logo']='';save_config(c)
                return self.send_json({'logo':''})
            if path=='/api/preferences':
                with lock:
                    prefs=validate_preferences(payload)
                    c=config()
                    c['preferences']=prefs
                    save_config(c)
                return self.send_json(prefs)
            if path=='/api/config':
                with lock:
                    if state['running']:return self.send_json({'error':'Cannot edit while loading.'},409)
                c=config()
                if 'api_key' in payload and payload['api_key'].strip():
                    if not re.fullmatch(r'RGAPI-[A-Za-z0-9-]+',payload['api_key'].strip()):raise ValueError('Enter a correctly formatted RGAPI key.')
                    c['api_key']=payload['api_key'].strip()
                for k in ('platform','region','season_start'):
                    if k in payload:c[k]=payload[k]
                if c['platform'] not in ('euw1','eun1','na1','kr','br1','tr1','la1','la2','oc1','jp1','ru'):raise ValueError('Unsupported platform')
                if c['region'] not in ('europe','americas','asia','sea'):raise ValueError('Unsupported routing region')
                old_teams=c.get('teams',[])
                if 'teams' in payload:
                    teams=payload['teams']
                    if len(teams)!=2 or any(len(t.get('players',[]))!=5 for t in teams):raise ValueError('Two teams with five slots each are required.')
                    c['teams']=[{'name':str(t.get('name',''))[:50],'players':[str(p)[:100] for p in t['players']],'logo':str(t.get('logo',''))[:100] if re.fullmatch(r'/logos/team[01]\.(png|jpg|webp)',str(t.get('logo',''))) else '', 'short_name':str(t.get('short_name',''))[:15], 'opgg_url':str(t.get('opgg_url',''))[:1600]} for t in teams]
                start_dt=datetime.fromisoformat(c['season_start'].replace('Z','+00:00'))
                if start_dt.tzinfo is None:raise ValueError('Season date requires a timezone (Z).')
                if start_dt > datetime.now(timezone.utc):raise ValueError('Season start cannot be in the future.')
                if not re.fullmatch(r'20\d\d-01-01T00:00:00Z',c['season_start']):raise ValueError('Select a ranked year.')
                # A season change must not display cached results from a different year.
                previous=config().get('season_start')
                if c['season_start']!=previous:
                    with db() as con:con.execute('DELETE FROM results')
                elif 'teams' in payload:
                    # Keep cached profiles for unchanged slots, but never show
                    # the previous lineup's players after applying a new team.
                    with db() as con:
                        for side in range(2):
                            for role in range(5):
                                old_players=(old_teams[side].get('players') or []) if side<len(old_teams) else []
                                before=str(old_players[role] if role<len(old_players) else '').strip().casefold()
                                after=str(c['teams'][side]['players'][role]).strip().casefold()
                                if before!=after:con.execute('DELETE FROM results WHERE slot=?',(side*5+role,))
                save_config(c);return self.send_json({'ok':True})
            if path=='/api/swap-teams':
                # Keep the team's configured names, players, logo and already analyzed slots together.
                # Do not change the match cache; it is indexed by match ID rather than by side.
                with lock:
                    global side_revision
                    c=config()
                    folder=ROOT/'static'/'logos'
                    # Read both logos before changing either. Preserve alpha and the original encoding.
                    logo_data=[]
                    for team in c['teams'][:2]:
                        path_value=str(team.get('logo',''))
                        m=re.fullmatch(r'/logos/team[01]\.(png|jpg|webp)',path_value)
                        f=folder/path_value.split('/')[-1] if m else None
                        logo_data.append((m.group(1),f.read_bytes()) if f and f.is_file() else None)
                    with db() as con:
                        existing=con.execute('SELECT slot,body FROM results').fetchall()
                        # Slot ids 0..4 are Blue; 5..9 are Red.
                        con.execute('DELETE FROM results')
                        for slot,body in existing:
                            new_slot=(int(slot)+5)%10
                            result=json.loads(body)
                            result['slot']=new_slot
                            con.execute('INSERT INTO results (slot,body) VALUES (?,?)',
                                        (new_slot,json.dumps(result,ensure_ascii=False)))
                    if folder.exists():
                        for i in (0,1):
                            for f in folder.glob(f'team{i}.*'):f.unlink()
                    folder.mkdir(exist_ok=True)
                    swapped=[c['teams'][1],c['teams'][0]]
                    for i,team in enumerate(swapped):
                        logo=logo_data[1-i]
                        if logo:
                            ext,binary=logo
                            (folder/f'team{i}.{ext}').write_bytes(binary)
                            team['logo']=f'/logos/team{i}.{ext}'
                        else:team['logo']=''
                    c['teams']=swapped
                    save_config(c)
                    side_revision+=1
                return self.send_json({'ok':True})
            if path=='/api/overlay-control':
                with lock:
                    if 'role' in payload:
                        v=int(payload['role'])
                        if not 0<=v<=4:raise ValueError('Invalid role')
                        overlay_control['role']=v
                    if 'direction' in payload:
                        d=int(payload['direction'])
                        if d not in (-1,1):raise ValueError('Invalid direction')
                        overlay_control['role']=(overlay_control['role']+d)%5
                    if 'auto' in payload:overlay_control['auto']=bool(payload['auto'])
                    if 'interval' in payload:
                        t=int(payload['interval'])
                        if not 1<=t<=60:raise ValueError('Interval must be between 1 and 60 seconds.')
                        overlay_control['interval']=t
                    overlay_control['revision']+=1
                    overlay_control['last_switch']=time.monotonic()
                    return self.send_json(dict(overlay_control))
            if path=='/api/start':
                with lock:
                    if state['running']:return self.send_json({'error':'An update is already running.'},409)
                    state.update(running=True,cancel=False,error=None,done=0,total=0,progress='Starting scan')
                threading.Thread(target=runner,daemon=True).start()
                return self.send_json({'ok':True})
            if path=='/api/stop':
                with lock:state['cancel']=True
                return self.send_json({'ok':True})
            return self.send_json({'error':'Unknown action'},404)
        except (ValueError,KeyError,TypeError) as e:return self.send_json({'error':str(e)},400)

def auto_start_stats():
    """One startup refresh if a saved key and at least one Riot ID exist."""
    c = config()
    if not c.get('api_key') or not any(str(player).strip() for team in c.get('teams', []) for player in team.get('players', [])):
        print('Auto-refresh skipped: an API key or player IDs are missing.')
        return
    with lock:
        if state['running']:
            return
        state.update(running=True, cancel=False, error=None, done=0, total=0, progress='Automatic refresh started')
    threading.Thread(target=runner, daemon=True, name='PrimeCast-AutoRefresh').start()
    print('Players found: automatic Riot API refresh started.')

def open_dashboard():
    try:
        webbrowser.open(f'http://{HOST}:{PORT}/', new=1)
    except Exception as exc:
        print(f'Could not open browser automatically: {exc}')

def main():
    setup()
    print(f'PrimeCast Stats started: http://{HOST}:{PORT}')
    print('OBS Pre-Game Browser Source: http://127.0.0.1:8767/pregame')
    try:
        server = ThreadingHTTPServer((HOST, PORT), Handler)
        if '--open-browser' in sys.argv:
            threading.Timer(0.8, open_dashboard).start()
        threading.Thread(target=warm_role_icons,daemon=True,name='PrimeCast-RoleAssets').start()
        threading.Thread(target=warm_objective_icons,daemon=True,name='PrimeCast-ObjectiveAssets').start()
        threading.Thread(target=warm_damage_icons,daemon=True,name='PrimeCast-DamageAssets').start()
        auto_start_stats()
        server.serve_forever()
    except KeyboardInterrupt:pass

if __name__=='__main__':main()
