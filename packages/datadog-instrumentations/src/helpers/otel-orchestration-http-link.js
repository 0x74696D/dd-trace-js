'use strict'

const api = require('@opentelemetry/api')
const { getSpanMeta } = require('./otel-orchestration-meta')

const httpParentByInstance = new Map()
const pendingHttpParentByTraceId = new Map()

function publishHttpParentMeta (instanceId, meta) {
  if (!instanceId || !meta?.traceId || !meta?.spanId) return
  httpParentByInstance.set(String(instanceId), meta)
}

function publishPendingHttpParent (meta) {
  if (!meta?.traceId || !meta?.spanId) return
  pendingHttpParentByTraceId.set(meta.traceId, meta)
}

function consumeHttpParentForInstance (instanceId) {
  if (!instanceId) return undefined

  const key = String(instanceId)
  const meta = httpParentByInstance.get(key)
  if (meta) {
    httpParentByInstance.delete(key)
    pendingHttpParentByTraceId.delete(meta.traceId)
    return meta
  }

  return undefined
}

function consumePendingHttpParent (traceId) {
  if (!traceId) return undefined

  const meta = pendingHttpParentByTraceId.get(traceId)
  if (meta) {
    pendingHttpParentByTraceId.delete(traceId)
    return meta
  }

  return undefined
}

function resolveHttpParentForOrchestration (instanceId, traceContext) {
  const fromInstance = consumeHttpParentForInstance(instanceId)
  if (fromInstance) return fromInstance

  const traceParent = traceContext?.traceParent
  if (!traceParent) return undefined

  const traceId = traceParent.split('-')[1]
  return consumePendingHttpParent(traceId)
}

function patchDurableClient () {
  const shimmer = require('../../../datadog-shimmer')
  let DurableClient
  try {
    DurableClient = require('durable-functions/lib/src/durableClient/DurableClient').DurableClient
  } catch {
    return
  }

  shimmer.wrap(DurableClient.prototype, 'startNew', startNew => {
    return async function (...args) {
      const activeSpan = api.trace.getActiveSpan()
      const spanMeta = activeSpan ? getSpanMeta(activeSpan) : undefined

      if (spanMeta) {
        publishPendingHttpParent(spanMeta)
      }

      const instanceId = await startNew.apply(this, args)

      if (instanceId && spanMeta) {
        publishHttpParentMeta(instanceId, spanMeta)
      }

      return instanceId
    }
  })
}

module.exports = {
  consumeHttpParentForInstance,
  consumePendingHttpParent,
  patchDurableClient,
  publishHttpParentMeta,
  publishPendingHttpParent,
  resolveHttpParentForOrchestration,
}
