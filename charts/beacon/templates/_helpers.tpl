{{/*
Expand the name of the chart.
*/}}
{{- define "beacon.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name.
*/}}
{{- define "beacon.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "beacon.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "beacon.labels" -}}
helm.sh/chart: {{ include "beacon.chart" . }}
{{ include "beacon.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "beacon.selectorLabels" -}}
app.kubernetes.io/name: {{ include "beacon.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "beacon.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "beacon.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Name of the Secret holding admin credentials.
*/}}
{{- define "beacon.adminSecretName" -}}
{{- if .Values.beacon.admin.existingSecret -}}
{{- .Values.beacon.admin.existingSecret -}}
{{- else -}}
{{- printf "%s-admin" (include "beacon.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Name of the Secret holding S3 static credentials (only when created).
*/}}
{{- define "beacon.s3SecretName" -}}
{{- if .Values.beacon.s3.existingSecret -}}
{{- .Values.beacon.s3.existingSecret -}}
{{- else -}}
{{- printf "%s-s3" (include "beacon.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Name of the Secret holding BEACON_SECRETS_KEY.
*/}}
{{- define "beacon.secretsKeySecretName" -}}
{{- if .Values.beacon.secretsKey.existingSecret -}}
{{- .Values.beacon.secretsKey.existingSecret -}}
{{- else -}}
{{- printf "%s-secrets-key" (include "beacon.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Resolve the PVC name (existing or chart-managed).
*/}}
{{- define "beacon.pvcName" -}}
{{- if .Values.persistence.existingClaim -}}
{{- .Values.persistence.existingClaim -}}
{{- else -}}
{{- printf "%s-data" (include "beacon.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
True when a StatefulSet creates one claim per pod through volumeClaimTemplates.
*/}}
{{- define "beacon.useClaimTemplate" -}}
{{- if and (eq .Values.kind "StatefulSet") .Values.persistence.enabled (not .Values.persistence.existingClaim) -}}
true
{{- end -}}
{{- end -}}

{{/*
beacon.basePath normalized as the server does it: "" or "/prefix".
*/}}
{{- define "beacon.basePath" -}}
{{- $p := .Values.beacon.basePath | default "" | trim | trimAll "/" -}}
{{- if $p -}}/{{ $p }}{{- end -}}
{{- end -}}

{{/*
Render a probe. An httpGet probe with an empty path checks {basePath}/api/health.
Call with (dict "probe" .Values.livenessProbe "root" .).
*/}}
{{- define "beacon.probe" -}}
{{- $probe := deepCopy .probe -}}
{{- if and $probe.httpGet (not $probe.httpGet.path) -}}
{{- $_ := set $probe.httpGet "path" (printf "%s/api/health" (include "beacon.basePath" .root)) -}}
{{- end -}}
{{- toYaml $probe -}}
{{- end -}}

{{/*
Reject values the server cannot run with.
*/}}
{{- define "beacon.validate" -}}
{{- if gt (int .Values.replicaCount) 1 -}}
{{- fail "replicaCount must be 0 or 1: beacon.db takes an exclusive file lock, so only one Beacon process can open it" -}}
{{- end -}}
{{- with .Values.autoscaling -}}
{{- if .enabled -}}
{{- fail "autoscaling is not supported: beacon.db takes an exclusive file lock, so Beacon runs as a single replica. Scale it with resources, beacon.workerThreads and beacon.vmMemorySize" -}}
{{- end -}}
{{- end -}}
{{- if and .Values.beacon.oidc.enabled (not .Values.beacon.oidc.jwksUrl) -}}
{{- fail "beacon.oidc.jwksUrl is required when beacon.oidc.enabled is true: Beacon does not discover it from the issuer" -}}
{{- end -}}
{{- if and .Values.beacon.s3.enabled (not .Values.beacon.s3.bucket) -}}
{{- fail "beacon.s3.bucket is required when beacon.s3.enabled is true" -}}
{{- end -}}
{{- end -}}
