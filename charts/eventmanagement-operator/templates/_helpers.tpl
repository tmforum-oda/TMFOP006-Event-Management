{{/* Registry prefix, empty unless global.registry is set. */}}
{{- define "eventmanagement-operator.registry" -}}
{{- if and .Values.global .Values.global.registry -}}{{ .Values.global.registry }}{{- end -}}
{{- end -}}

{{/* image + version (+ -prereleaseSuffix) */}}
{{- define "eventmanagement-operator.image" -}}
{{ include "eventmanagement-operator.registry" . }}{{ .Values.operator.image }}:{{ .Values.operator.version }}{{- if .Values.operator.prereleaseSuffix -}}-{{ .Values.operator.prereleaseSuffix }}{{- end -}}
{{- end -}}

{{/* Always when a prerelease suffix is set, otherwise the configured policy */}}
{{- define "eventmanagement-operator.imagePullPolicy" -}}
{{- if .Values.operator.prereleaseSuffix -}}Always{{- else -}}{{ .Values.operator.imagePullPolicy }}{{- end -}}
{{- end -}}

{{/* adapter image, used by the operator when it deploys an adapter */}}
{{- define "eventmanagement-operator.adapterImage" -}}
{{ include "eventmanagement-operator.registry" . }}{{ .Values.adapter.image }}:{{ .Values.adapter.version }}
{{- end -}}
