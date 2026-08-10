'use strict'

const api = require('@opentelemetry/api')
const createId = require('../../../dd-trace/src/id')
const DatadogSpanContext = require('../../../dd-trace/src/opentracing/span_context')
const OtelSpan = require('../../../dd-trace/src/opentelemetry/span')
const OtelSpanContext = require('../../../dd-trace/src/opentelemetry/span_context')
const { extractContext } = require('./azure-trace-context')
const { getTracer, spanAttributes, endSpan } = require('./otel-azure-span')
const { normalizeSpanId, normalizeTraceId } = require('./otel-orchestration-meta')

function getParentFromTraceContext (traceContext) {
  const traceParent = traceContext?.traceParent
  if (!traceParent) return undefined

  const parts = traceParent.split('-')
  if (parts.length < 4) return undefined

  return {
    traceId: normalizeTraceId(parts[1]),
    parentId: normalizeSpanId(parts[2]),
  }
}

function createOrchestrationMeta (instanceId, invocationContext, functionName) {
  const traceContext = invocationContext?.traceContext
  const fromHeader = getParentFromTraceContext(traceContext)
  const parentContext = extractContext(traceContext)
  const parentSpan = api.trace.getSpan(parentContext)
  const parentDdContext = parentSpan?.spanContext()?._ddContext

  let traceId = fromHeader?.traceId
  let parentId = fromHeader?.parentId

  if (!traceId && parentDdContext) {
    traceId = normalizeTraceId(parentDdContext._traceId)
    parentId = normalizeSpanId(parentDdContext._spanId)
  }

  if (!traceId) {
    traceId = normalizeTraceId(createId())
  }

  return {
    instanceId,
    functionName,
    traceId,
    spanId: normalizeSpanId(createId()),
    parentId,
    startTime: Date.now(),
    status: 'open',
  }
}

function exportOrchestrationSpanFromMeta (tracerName, meta, { error, endTime } = {}) {
  if (!meta?.traceId || !meta?.spanId) return false

  const tracer = getTracer(tracerName)
  const ddContext = new DatadogSpanContext({
    traceId: createId(meta.traceId, 16),
    spanId: createId(meta.spanId, 16),
    parentId: meta.parentId ? createId(meta.parentId, 16) : null,
  })

  const span = new OtelSpan(
    tracer,
    api.ROOT_CONTEXT,
    `orchestration ${meta.functionName || 'orchestration'}`,
    new OtelSpanContext(ddContext),
    api.SpanKind.INTERNAL,
    [],
    meta.startTime,
    spanAttributes(meta.functionName || 'orchestration', 'durable-orchestration'),
  )

  if (error) {
    endSpan(span, error)
  } else {
    span.end(endTime ?? Date.now())
  }

  return true
}

module.exports = {
  createOrchestrationMeta,
  exportOrchestrationSpanFromMeta,
  getParentFromTraceContext,
}
