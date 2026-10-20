{{/*
Render the Beacon container environment. Maps chart values to BEACON_* / AWS_*
environment variables. Only emits keys that are set (non-null / non-empty),
letting the server fall back to its own defaults otherwise.
*/}}
{{- define "beacon.env" -}}
{{- $b := .Values.beacon -}}
- name: BEACON_PORT
  value: {{ $b.port | quote }}
- name: BEACON_HOST
  value: {{ $b.host | quote }}
{{- if $b.basePath }}
- name: BEACON_BASE_PATH
  value: {{ $b.basePath | quote }}
{{- end }}
{{- if $b.logLevel }}
- name: BEACON_LOG_LEVEL
  value: {{ $b.logLevel | quote }}
{{- end }}
{{- if $b.dataDir }}
- name: BEACON_DATA_DIR
  value: {{ $b.dataDir | quote }}
{{- end }}
{{/* Admin credentials from the resolved Secret */}}
- name: BEACON_ADMIN_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ include "beacon.adminSecretName" . }}
      key: BEACON_ADMIN_USERNAME
- name: BEACON_ADMIN_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "beacon.adminSecretName" . }}
      key: BEACON_ADMIN_PASSWORD
{{/* Flight SQL */}}
- name: BEACON_FLIGHT_SQL_ENABLE
  value: {{ $b.flightSql.enabled | quote }}
{{- if $b.flightSql.enabled }}
- name: BEACON_FLIGHT_SQL_PORT
  value: {{ $b.flightSql.port | quote }}
- name: BEACON_FLIGHT_SQL_HOST
  value: {{ $b.flightSql.host | quote }}
- name: BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS
  value: {{ $b.flightSql.allowAnonymous | quote }}
{{- end }}
{{/* Optional runtime tuning */}}
{{- if ne $b.workerThreads nil }}
- name: BEACON_WORKER_THREADS
  value: {{ $b.workerThreads | quote }}
{{- end }}
{{- if ne $b.apiThreads nil }}
- name: BEACON_API_THREADS
  value: {{ $b.apiThreads | quote }}
{{- end }}
{{- if ne $b.batchSize nil }}
- name: BEACON_BATCH_SIZE
  value: {{ $b.batchSize | quote }}
{{- end }}
{{- if ne $b.maxUploadBytes nil }}
- name: BEACON_MAX_UPLOAD_BYTES
  value: {{ $b.maxUploadBytes | quote }}
{{- end }}
{{- if ne $b.vmMemorySize nil }}
- name: BEACON_VM_MEMORY_SIZE
  value: {{ $b.vmMemorySize | quote }}
{{- end }}
{{/* Crawler / file-stats */}}
{{- if ne $b.crawler.enabled nil }}
- name: BEACON_CRAWLER_ENABLE
  value: {{ $b.crawler.enabled | quote }}
{{- end }}
{{- if ne $b.crawler.defaultIntervalSecs nil }}
- name: BEACON_CRAWLER_DEFAULT_INTERVAL_SECS
  value: {{ $b.crawler.defaultIntervalSecs | quote }}
{{- end }}
{{- if ne $b.fileStats.enabled nil }}
- name: BEACON_FILE_STATS_ENABLE
  value: {{ $b.fileStats.enabled | quote }}
{{- end }}
{{/* CORS */}}
{{- if $b.cors.allowedOrigins }}
- name: BEACON_CORS_ALLOWED_ORIGINS
  value: {{ $b.cors.allowedOrigins | quote }}
{{- end }}
{{- if $b.cors.allowedMethods }}
- name: BEACON_CORS_ALLOWED_METHODS
  value: {{ $b.cors.allowedMethods | quote }}
{{- end }}
{{- if $b.cors.allowedHeaders }}
- name: BEACON_CORS_ALLOWED_HEADERS
  value: {{ $b.cors.allowedHeaders | quote }}
{{- end }}
{{/* OIDC */}}
{{- if $b.oidc.enabled }}
- name: BEACON_OIDC_ENABLED
  value: "true"
- name: BEACON_OIDC_ISSUER
  value: {{ $b.oidc.issuer | quote }}
- name: BEACON_OIDC_AUDIENCE
  value: {{ $b.oidc.audience | quote }}
{{- if $b.oidc.jwksUrl }}
- name: BEACON_OIDC_JWKS_URL
  value: {{ $b.oidc.jwksUrl | quote }}
{{- end }}
{{- if $b.oidc.rolesClaim }}
- name: BEACON_OIDC_ROLES_CLAIM
  value: {{ $b.oidc.rolesClaim | quote }}
{{- end }}
{{- if $b.oidc.usernameClaim }}
- name: BEACON_OIDC_USERNAME_CLAIM
  value: {{ $b.oidc.usernameClaim | quote }}
{{- end }}
{{- end }}
{{/* S3-backed datasets store (uses the standard AWS_* credential chain) */}}
{{- if $b.s3.enabled }}
- name: BEACON_S3_ENABLE
  value: "true"
{{- if $b.s3.bucket }}
- name: BEACON_S3_BUCKET
  value: {{ $b.s3.bucket | quote }}
{{- end }}
- name: BEACON_S3_ALLOW_HTTP
  value: {{ $b.s3.allowHttp | quote }}
- name: BEACON_S3_VIRTUAL_HOSTED_STYLE
  value: {{ $b.s3.virtualHostedStyle | quote }}
{{- if $b.s3.region }}
- name: AWS_REGION
  value: {{ $b.s3.region | quote }}
{{- end }}
{{- if $b.s3.endpoint }}
- name: AWS_ENDPOINT
  value: {{ $b.s3.endpoint | quote }}
{{- end }}
{{- if or $b.s3.accessKeyId $b.s3.existingSecret }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "beacon.s3SecretName" . }}
      key: AWS_ACCESS_KEY_ID
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "beacon.s3SecretName" . }}
      key: AWS_SECRET_ACCESS_KEY
{{- end }}
{{- end }}
{{/* User-supplied extra env */}}
{{- with $b.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}
