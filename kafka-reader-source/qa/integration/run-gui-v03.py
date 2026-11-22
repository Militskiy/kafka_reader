#!/usr/bin/env python3
"""Observe v0.3 GUI checks, native windows, persisted state, and a real process restart."""
import argparse
import base64
from datetime import datetime, timezone
import json
import hashlib
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import subprocess
import time
from PIL import ImageGrab

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--kind',choices=['rust'],default='rust')
parser.add_argument('--exe',type=Path,required=True)
parser.add_argument('--output',type=Path)
parser.add_argument('--xvfb',type=Path)
parser.add_argument('--timeout',type=int,default=180)
args=parser.parse_args()
exe_sha256=hashlib.sha256(args.exe.read_bytes()).hexdigest()
root=Path(__file__).resolve().parent
tag=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+secrets.token_hex(2)
out=(args.output or root/'work-v0.3.0'/('gui-'+args.kind)/tag).resolve()
out.mkdir(parents=True,exist_ok=False)
data=out/'app-data';data.mkdir()
smoke=out/'smoke';smoke.mkdir()
reopen=out/'reopen';reopen.mkdir()
empty=out/'empty';empty.mkdir()
empty_data=out/'empty-app-data';empty_data.mkdir()
env=dict(os.environ,LIBGL_ALWAYS_SOFTWARE='1',DOTNET_CLI_TELEMETRY_OPTOUT='1',DOTNET_NOLOGO='1')
server=None
serverlog=None
observations=[]

def windows():
    result=subprocess.run(['xwininfo','-root','-tree'],env=env,text=True,capture_output=True,check=True,timeout=5)
    return [(int(identifier,16),title) for identifier,title in re.findall(r'^\s*(0x[0-9a-fA-F]+)\s+"(.*?)":',result.stdout,re.M)]

def capture(window,path):
    result=subprocess.run(['xwininfo','-id',str(window)],env=env,text=True,capture_output=True,check=True,timeout=5)
    def field(pattern):return int(re.search(pattern,result.stdout).group(1))
    x=field(r'Absolute upper-left X:\s*(-?\d+)');y=field(r'Absolute upper-left Y:\s*(-?\d+)')
    w=field(r'Width:\s*(\d+)');h=field(r'Height:\s*(\d+)')
    ImageGrab.grab(xdisplay=env['DISPLAY'],bbox=(x,y,x+w,y+h)).save(path)
    return [w,h]

def run(arguments,directory,label,workspace=None):
    seen=set();phase_started={};main_window=None
    (directory/'native').mkdir()
    with (directory/'application.log').open('w') as log:
        process=subprocess.Popen([str(args.exe.resolve()),*arguments,'--data-dir',str(workspace or data)],env=env,cwd=str(args.exe.resolve().parent),stdout=log,stderr=subprocess.STDOUT)
        deadline=time.monotonic()+args.timeout
        try:
            while process.poll() is None:
                if time.monotonic()>deadline:raise TimeoutError(f'{label} exceeded {args.timeout}s')
                try:
                    phase=json.loads((directory/'gui-phase.json').read_text())
                    name=str(phase['phase']);title=str(phase['windowTitle']);kind=str(phase['kind']).lower()
                    key=(name,title,kind)
                    phase_started.setdefault(key,time.monotonic())
                    if key not in seen and time.monotonic()-phase_started[key]>=.20:
                        current=windows()
                        window=next((identifier for identifier,caption in current if caption==title),None)
                        if window is not None:
                            if kind=='main':main_window=window
                            slug=re.sub(r'[^a-zA-Z0-9_-]+','-',name).strip('-') or 'phase'
                            image=directory/'native'/f'{len(seen):02d}-{slug}.png'
                            size=capture(window,image)
                            observations.append({'run':label,'phase':name,'windowTitle':title,'kind':kind,'windowId':hex(window),'mainWindowId':hex(main_window) if main_window else None,'separateNativeWindow':kind=='main' or (main_window is not None and window!=main_window),'size':size,'screenshot':str(image.relative_to(out))})
                            seen.add(key)
                except (FileNotFoundError,json.JSONDecodeError,KeyError,subprocess.CalledProcessError):
                    pass
                time.sleep(.08)
            return process.returncode
        finally:
            if process.poll() is None:
                process.terminate()
                try:process.wait(timeout=5)
                except subprocess.TimeoutExpired:process.kill();process.wait()

def pass_report(directory):
    candidates=[directory/'application.log',*directory.glob('*check*.txt'),*directory.glob('*check*.json')]
    text='\n'.join(p.read_text(errors='replace') for p in candidates if p.is_file())
    failure=bool(re.search(r'(?m)^(?:FAILED|FAIL:|UI delivery error)',text) or re.search(r'"(?:state|status|result|guiSmoke)"\s*:\s*"(?:FAIL|FAILED|ERROR)"',text,re.I))
    passed=bool('PASS:' in text or re.search(r'"PASS"|"passed"\s*:\s*true',text,re.I))
    return passed and not failure

try:
    xvfb=str(args.xvfb) if args.xvfb else env.get('KAFKA_READER_XVFB') or shutil.which('Xvfb')
    if not xvfb:
        candidate=root.parent/'kafka-reader-toolchains/xvfb/usr/bin/Xvfb'
        if candidate.is_file():xvfb=str(candidate)
    if not xvfb:raise RuntimeError('Install Xvfb or supply --xvfb.')
    authority=out/'xvfb.auth';authority.touch(mode=0o600);authority.chmod(0o600)
    subprocess.run(['xauth','-f',str(authority),'add','127.0.0.1:99','.',secrets.token_hex(16)],check=True)
    env.update(DISPLAY='127.0.0.1:99',XAUTHORITY=str(authority))
    os.environ['XAUTHORITY']=str(authority)
    serverlog=(out/'xvfb.log').open('w')
    server=subprocess.Popen([xvfb,':99','-screen','0','1800x1300x24','-nolisten','local','-nolisten','unix','-listen','tcp','-auth',str(authority)],stdout=serverlog,stderr=subprocess.STDOUT)
    deadline=time.monotonic()+10
    while True:
        if server.poll() is not None:raise RuntimeError('Xvfb exited')
        try:
            with socket.create_connection(('127.0.0.1',6099),timeout=.2):break
        except OSError:
            if time.monotonic()>deadline:raise TimeoutError('Xvfb startup timed out')
            time.sleep(.1)
    empty_code=run(['--screenshot',str(empty/'kafka-reader-empty.png')],empty,'empty',empty_data)
    command=['--gui-smoke','--screenshot-dir',str(smoke)]
    smoke_code=run(command,smoke,'smoke')
    reopen_code=run(['--gui-check-reopen',str(reopen)],reopen,'reopen') if smoke_code==0 else None
    sentinels=['KAFKA_UI_QA_SECRET_2026','qa-reader','PRIVATE KEY']
    leaks=[]
    for path in data.rglob('*'):
        if path.is_file():
            contents=path.read_bytes()
            for index,sentinel in enumerate(sentinels):
                forms=[sentinel.encode(),sentinel.encode('utf-16-le'),base64.b64encode(sentinel.encode())]
                if any(form in contents for form in forms):leaks.append({'file':str(path.relative_to(data)),'sentinelIndex':index})
    native=[entry for entry in observations if entry['run']=='smoke']
    server_phases={entry['phase'].lower() for entry in native if entry['kind']=='server'}
    required={'connection':any('connection' in phase for phase in server_phases),'authentication':any('auth' in phase for phase in server_phases),'tls':any('tls' in phase for phase in server_phases),'advanced':any('advanced' in phase for phase in server_phases),'settings':any(entry['kind']=='settings' for entry in native),'main':any(entry['kind']=='main' for entry in native)}
    clean_workspace=json.loads((empty_data/'workspace.json').read_text()) if (empty_data/'workspace.json').exists() else {}
    saved_workspace=json.loads((data/'workspace.json').read_text()) if (data/'workspace.json').exists() else {}
    checks={'normalStartupExitZero':empty_code==0,'normalStartupScreenshotExists':(empty/'kafka-reader-empty.png').is_file(),'normalStartupHasNoSavedServersOrTabs':not clean_workspace.get('servers') and not clean_workspace.get('tabs'),'syntheticServerNotPersisted':bool(saved_workspace.get('servers')) and all(not server.get('offline',False) for server in saved_workspace.get('servers',[])),'smokeExitZero':smoke_code==0,'smokeReportsPass':pass_report(smoke),'reopenExitZero':reopen_code==0,'reopenReportsPass':pass_report(reopen),'reopenReportExists':any((reopen/name).is_file() for name in ['reopen-check.json','gui-reopen-report.json']),'requiredNativeWindowsObserved':all(required.values()),'configurationWindowsSeparate':all(entry['separateNativeWindow'] for entry in native),'sentinelsAbsentFromPersistedFiles':not leaks}
    report={'appVersion':'0.3.0','kind':args.kind,'executableSha256':exe_sha256,'completedAtUtc':datetime.now(timezone.utc).isoformat(),'status':'PASS' if all(checks.values()) else 'FAIL','output':str(out),'dataDirectory':str(data),'checks':checks,'observedPhases':required,'secretLeaks':leaks,'nativeWindows':observations,'persistedFiles':[str(path.relative_to(data)) for path in sorted(data.rglob('*')) if path.is_file()]}
    (out/'observer-report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({key:value for key,value in report.items() if key!='nativeWindows'},indent=2))
    raise SystemExit(0 if all(checks.values()) else 1)
finally:
    if server is not None:
        server.terminate()
        try:server.wait(timeout=5)
        except subprocess.TimeoutExpired:server.kill();server.wait()
    if serverlog:serverlog.close()
