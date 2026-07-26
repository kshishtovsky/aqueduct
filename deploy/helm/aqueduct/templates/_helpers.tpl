{{/*
Expand the name of the chart.
*/}}
{{- define "aqueduct.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "aqueduct.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "aqueduct.labels" -}}
helm.sh/chart: {{ include "aqueduct.name" . }}-{{ .Chart.Version | replace "+" "_" }}
{{ include "aqueduct.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "aqueduct.selectorLabels" -}}
app.kubernetes.io/name: {{ include "aqueduct.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Headless service FQDN for DNS discovery
*/}}
{{- define "aqueduct.headlessFQDN" -}}
{{ include "aqueduct.fullname" . }}-headless.{{ .Release.Namespace }}.svc.cluster.local
{{- end }}
