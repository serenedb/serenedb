{{- define "serenedb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "serenedb.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "serenedb.labels" -}}
app.kubernetes.io/name: {{ include "serenedb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "serenedb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "serenedb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "serenedb.secretName" -}}
{{- if .Values.auth.existingSecret -}}
{{- .Values.auth.existingSecret -}}
{{- else -}}
{{- include "serenedb.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
serened is an abseil-flags binary: repeated --listen is last-wins, so ALL
listeners must be comma-separated inside a single --listen flag.
*/}}
{{- define "serenedb.listenValue" -}}
{{- $urls := list -}}
{{- $pg := printf "postgres://0.0.0.0:%d" (int .Values.listeners.postgres.port) -}}
{{- if .Values.listeners.postgres.sslmode -}}
{{- $pg = printf "%s?sslmode=%s" $pg .Values.listeners.postgres.sslmode -}}
{{- end -}}
{{- $urls = append $urls $pg -}}
{{- if .Values.listeners.http.enabled -}}
{{- $scheme := ternary "https" "http" .Values.tls.enabled -}}
{{- $urls = append $urls (printf "%s://0.0.0.0:%d?api=es" $scheme (int .Values.listeners.http.port)) -}}
{{- end -}}
{{- join "," $urls -}}
{{- end -}}
