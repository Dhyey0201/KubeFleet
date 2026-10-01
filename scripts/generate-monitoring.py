import json,pathlib
root=pathlib.Path('config/monitoring');ns='kubefleet-monitoring'
docs=[]
def add(kind,name,spec=None,api='v1',**kwargs):
 d={'apiVersion':api,'kind':kind,'metadata':{'name':name,'namespace':ns}};d.update(kwargs)
 if spec is not None:d['spec']=spec
 if kind in ['ClusterRole','ClusterRoleBinding']:d['metadata'].pop('namespace')
 docs.append(d);return d
add('Namespace',ns)['metadata']={'name':ns,'labels':{'pod-security.kubernetes.io/enforce':'restricted'}}
for name in ['prometheus','kube-state-metrics']:add('ServiceAccount',name)
add('ClusterRole','kubefleet-prometheus',api='rbac.authorization.k8s.io/v1',rules=[{'apiGroups':[''],'resources':['nodes','pods','services','endpoints'],'verbs':['get','list','watch']},{'apiGroups':[''],'resources':['nodes/proxy'],'verbs':['get']},{'nonResourceURLs':['/metrics'],'verbs':['get']}])
add('ClusterRole','kubefleet-kube-state-metrics',api='rbac.authorization.k8s.io/v1',rules=[{'apiGroups':[''],'resources':['nodes','pods','namespaces'],'verbs':['get','list','watch']},{'apiGroups':['batch'],'resources':['jobs'],'verbs':['get','list','watch']},{'apiGroups':['apps'],'resources':['deployments','daemonsets','statefulsets'],'verbs':['get','list','watch']}])
for name in ['prometheus','kube-state-metrics']:add('ClusterRoleBinding','kubefleet-'+name,api='rbac.authorization.k8s.io/v1',roleRef={'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':'kubefleet-'+name},subjects=[{'kind':'ServiceAccount','name':name,'namespace':ns}])
prom='''global:
  scrape_interval: 5s
  evaluation_interval: 10s
rule_files: [/etc/prometheus/alerts.yaml]
scrape_configs:
- job_name: prometheus
  static_configs: [{targets: [localhost:9090]}]
- job_name: kubefleet-manager
  kubernetes_sd_configs: [{role: pod}]
  relabel_configs:
  - {source_labels: [__meta_kubernetes_namespace], action: keep, regex: kubefleet-system}
  - {source_labels: [__meta_kubernetes_pod_label_app], action: keep, regex: kubefleet-manager}
  - {source_labels: [__meta_kubernetes_pod_container_port_number], action: keep, regex: '8081'}
  - {source_labels: [__meta_kubernetes_pod_ip], target_label: __address__, replacement: '$1:8080'}
  - {source_labels: [__meta_kubernetes_pod_name], target_label: pod}
- job_name: kueue
  scheme: https
  bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
  tls_config: {insecure_skip_verify: true}
  kubernetes_sd_configs: [{role: pod}]
  relabel_configs:
  - {source_labels: [__meta_kubernetes_namespace], action: keep, regex: kueue-system}
  - {source_labels: [__meta_kubernetes_pod_container_port_number], action: keep, regex: '8443'}
- job_name: kube-state-metrics
  static_configs: [{targets: [kube-state-metrics:8080]}]
- job_name: kubelet-cadvisor
  scheme: https
  bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
  tls_config: {ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt}
  kubernetes_sd_configs: [{role: node}]
  relabel_configs:
  - {target_label: __address__, replacement: kubernetes.default.svc:443}
  - {source_labels: [__meta_kubernetes_node_name], target_label: __metrics_path__, replacement: '/api/v1/nodes/$1/proxy/metrics/cadvisor'}
  - {source_labels: [__meta_kubernetes_node_name], target_label: node}
'''
alerts='''groups:
- name: kubefleet
  rules:
  - alert: KubeFleetControllerUnavailable
    expr: sum(up{job="kubefleet-manager"}) < 1 or absent(up{job="kubefleet-manager"})
    for: 1m
    labels: {severity: critical}
    annotations: {summary: 'No manager metrics endpoint is healthy; inspect readiness and leader Lease'}
  - alert: KubeFleetCleanupDelayed
    expr: max by(namespace)(kubefleet_cleanup_pending_current) > 0
    for: 2m
    labels: {severity: warning}
    annotations: {summary: 'Durable stop cleanup has remained pending; inspect owned resource finalization'}
  - alert: KubeFleetGateWaiting
    expr: max by(namespace)(kubefleet_gate_pending_current) > 0
    for: 2m
    labels: {severity: warning}
    annotations: {summary: 'Prerequisite gate remains pending; inspect preparation and backoff'}
'''
add('ConfigMap','prometheus',data={'prometheus.yaml':prom,'alerts.yaml':alerts})
queries=[('Runs by phase','sum by(phase)(max by(namespace,phase)(kubefleet_batchruns_current))'),('Gate pending','max by(namespace)(kubefleet_gate_pending_current)'),('Cleanup pending','max by(namespace)(kubefleet_cleanup_pending_current)'),('Manager reconcile rate','sum by(controller)(rate(controller_runtime_reconcile_total[1m]))'),('Worker CPU actually consumed','sum by(namespace)(rate(container_cpu_usage_seconds_total{namespace=~"team-[abc]",container="worker"}[1m]))'),('Queue wait p95','histogram_quantile(0.95,sum by(le,queue)(rate(kubefleet_queue_wait_observed_seconds_bucket[5m])))'),('Manager up','up{job="kubefleet-manager"}'),('Kueue up','up{job="kueue"}'),('Worker Pods','sum by(namespace,phase)(kube_pod_status_phase{namespace=~"team-[abc]"})')]
dash={'title':'KubeFleet operations','uid':'kubefleet','schemaVersion':41,'version':1,'refresh':'5s','time':{'from':'now-15m','to':'now'},'panels':[{'id':i+1,'type':'timeseries','title':title,'datasource':{'type':'prometheus','uid':'prometheus'},'gridPos':{'x':(i%2)*12,'y':(i//2)*8,'w':12,'h':8},'targets':[{'expr':query,'refId':'A'}]} for i,(title,query) in enumerate(queries)]}
(root/'dashboard.json').write_text(json.dumps(dash,indent=2)+'\n')
(root/'queries.json').write_text(json.dumps([{'title':x,'query':q} for x,q in queries],indent=2)+'\n')
add('ConfigMap','grafana-provisioning',data={'datasource.yaml':'apiVersion: 1\ndatasources:\n- name: Prometheus\n  uid: prometheus\n  type: prometheus\n  url: http://prometheus:9090\n  access: proxy\n  isDefault: true\n','dashboards.yaml':'apiVersion: 1\nproviders:\n- name: KubeFleet\n  type: file\n  options:\n    path: /var/lib/grafana/dashboards\n'})
add('ConfigMap','grafana-dashboard',data={'dashboard.json':json.dumps(dash)})
images={'prometheus':'quay.io/prometheus/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996','grafana':'grafana/grafana:12.1.1@sha256:a1701c2180249361737a99a01bc770db39381640e4d631825d38ff4535efa47d','kube-state-metrics':'registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.16.0@sha256:e750cd4b43f782e3106537026c2995cac85d921aedea334e1d16caad7877c360'}
for name,port,uid in [('prometheus',9090,65534),('grafana',3000,472),('kube-state-metrics',8080,65534)]:
 container={'name':name,'image':images[name],'ports':[{'containerPort':port}],'resources':{'requests':{'cpu':'50m','memory':'96Mi'},'limits':{'cpu':'500m','memory':'384Mi'}},'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},'readinessProbe':{'httpGet':{'path':'/-/ready' if name=='prometheus' else '/api/health' if name=='grafana' else '/healthz','port':port},'initialDelaySeconds':3}}
 volumes=[]
 if name=='prometheus':
  container['args']=['--config.file=/etc/prometheus/prometheus.yaml','--storage.tsdb.path=/prometheus','--storage.tsdb.retention.time=6h','--storage.tsdb.retention.size=512MB']
  container['volumeMounts']=[{'name':'config','mountPath':'/etc/prometheus','readOnly':True},{'name':'data','mountPath':'/prometheus'}]
  volumes=[{'name':'config','configMap':{'name':'prometheus'}},{'name':'data','emptyDir':{'sizeLimit':'768Mi'}}]
 elif name=='grafana':
  container['env']=[{'name':'GF_SECURITY_ADMIN_PASSWORD','valueFrom':{'secretKeyRef':{'name':'grafana-auth','key':'password'}}},{'name':'GF_ANALYTICS_REPORTING_ENABLED','value':'false'},{'name':'GF_ANALYTICS_CHECK_FOR_UPDATES','value':'false'},{'name':'GF_AUTH_ANONYMOUS_ENABLED','value':'false'}]
  container['volumeMounts']=[{'name':'data','mountPath':'/var/lib/grafana'},{'name':'tmp','mountPath':'/tmp'},{'name':'datasource','mountPath':'/etc/grafana/provisioning/datasources','readOnly':True},{'name':'provision','mountPath':'/etc/grafana/provisioning/dashboards','readOnly':True},{'name':'dashboard','mountPath':'/var/lib/grafana/dashboards','readOnly':True}]
  volumes=[{'name':'data','emptyDir':{'sizeLimit':'128Mi'}},{'name':'tmp','emptyDir':{'sizeLimit':'64Mi'}},{'name':'datasource','configMap':{'name':'grafana-provisioning','items':[{'key':'datasource.yaml','path':'datasource.yaml'}]}},{'name':'provision','configMap':{'name':'grafana-provisioning','items':[{'key':'dashboards.yaml','path':'dashboards.yaml'}]}},{'name':'dashboard','configMap':{'name':'grafana-dashboard'}}]
 else:container['args']=['--resources=nodes,pods,namespaces,jobs,deployments,daemonsets,statefulsets']
 pod={'securityContext':{'runAsNonRoot':True,'runAsUser':uid,'runAsGroup':uid,'fsGroup':uid,'seccompProfile':{'type':'RuntimeDefault'}},'containers':[container],'volumes':volumes,'automountServiceAccountToken':name!='grafana'}
 if name!='grafana':pod['serviceAccountName']=name
 add('Deployment',name,{'replicas':1,'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':{'app':name}},'spec':pod}},api='apps/v1')
 add('Service',name,{'selector':{'app':name},'ports':[{'port':port,'targetPort':port}],'type':'ClusterIP'})
(root/'resources.yaml').write_text('\n---\n'.join(json.dumps(d,indent=2) for d in docs)+'\n')
