{{- define "tmfc043.image" -}}
{{ if and .Values.global .Values.global.registry }}{{ .Values.global.registry }}{{ end }}{{ .Values.image }}:{{ .Values.version }}{{ if .Values.prereleaseSuffix }}-{{ .Values.prereleaseSuffix }}{{ end }}
{{- end -}}

{{- define "tmfc043.imagePullPolicy" -}}
{{ if .Values.prereleaseSuffix }}Always{{ else }}{{ .Values.imagePullPolicy }}{{ end }}
{{- end -}}

{{/* One A2A/asyncapi event entry of the Component. Arguments: dict "name" "desc" "topic" "api" "root" [ "publisher" ] */}}
{{- define "tmfc043.event" -}}
- name: {{ .name }}
  description: {{ .desc | quote }}
  topic: {{ .topic }}
  specification:
  - apiType: {{ .api }}
    url: {{ .root.Values.specUrl }}
    version: 1.0.0
{{- if .publisher }}
  transport: native
  publisher: {{ .publisher }}
{{- end }}
{{- end -}}
