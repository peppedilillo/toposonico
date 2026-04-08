/**
 * Creates an abort-on-next-call function for a single UI state slot.
 *
 * Racing-request policy:
 * - Requests that represent replaceable user intent must be abortable.
 * - Only the latest request for a UI state slot may update that state.
 * - Aborted requests are expected and must not surface as errors.
 * - Non-2xx responses must be handled explicitly.
 *
 * Each call aborts the previous controller and returns a fresh AbortSignal.
 * Pass the signal to `fetch`; in the catch, filter out `AbortError`.
 */
export function makeAbortable() {
  let controller: AbortController | null = null
  return {
    nextSignal() {
      controller?.abort()
      controller = new AbortController()
      return controller.signal
    },
    cancel() {
      controller?.abort()
      controller = null
    },
  }
}