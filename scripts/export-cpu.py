#!/usr/bin/env python3
"""Export per-Pod cumulative CPU samples without printing cluster credentials."""
import argparse,datetime,json,subprocess,urllib.parse
p=argparse.ArgumentParser()
p.add_argument('--start',required=True);p.add_argument('--end',required=True);p.add_argument('--out',required=True)
a=p.parse_args()
start=datetime.datetime.fromisoformat(a.start.replace('Z','+00:00'));end=datetime.datetime.fromisoformat(a.end.replace('Z','+00:00'))
if end<=start or (end-start).total_seconds()>3600:raise SystemExit('CPU export interval must be positive and <=1h')
query='container_cpu_usage_seconds_total{namespace=~"team-[abc]",container="worker"}'
params=urllib.parse.urlencode({'query':query,'start':start.timestamp(),'end':end.timestamp(),'step':'1s'})
url='/api/v1/namespaces/kubefleet-monitoring/services/http:prometheus:9090/proxy/api/v1/query_range?'+params
result=subprocess.run(['kubectl','--request-timeout=30s','get','--raw',url],check=True,capture_output=True,text=True,timeout=35)
data=json.loads(result.stdout)
with open(a.out,'x') as f:json.dump({'schemaVersion':'v1','query':query,'start':a.start,'end':a.end,'prometheus':data},f,indent=2);f.write('\n')
