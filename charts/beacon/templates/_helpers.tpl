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
Resolve the PVC name (existing or chart-managed).
*/}}
{{- define "beacon.pvcName" -}}
{{- if .Values.persistence.existingClaim -}}
{{- .Values.persistence.existingClaim -}}
{{- else -}}
{{- printf "%s-data" (include "beacon.fullname" .) -}}
{{- end -}}
{{- end -}}
