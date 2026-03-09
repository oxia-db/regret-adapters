{{- define "okk.namespace" -}}
{{ .Release.Namespace }}
{{- end -}}

{{- define "okk.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: okk
{{- end -}}
