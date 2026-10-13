export const HLS_MEDIA_SESSION_POOL_COUNT = 6

const DEFAULT_MEDIA_SESSION_PARTITION = 'persist:session'
const HLS_MEDIA_SESSION_PARTITION_PREFIX = 'persist:streamwall-hls'

export function isIowaDotHlsURL(value: string | undefined) {
  if (!value) return false
  try {
    const url = new URL(value)
    return (
      (url.protocol === 'http:' || url.protocol === 'https:') &&
      (url.hostname === 'iowadot.gov' ||
        url.hostname.endsWith('.iowadot.gov')) &&
      url.pathname.toLowerCase().endsWith('.m3u8')
    )
  } catch {
    return false
  }
}

export function getMediaSessionPartition(
  url: string | undefined,
  poolIndex: number,
) {
  if (!isIowaDotHlsURL(url)) return DEFAULT_MEDIA_SESSION_PARTITION
  const normalizedIndex =
    Math.abs(Math.trunc(poolIndex)) % HLS_MEDIA_SESSION_POOL_COUNT
  return `${HLS_MEDIA_SESSION_PARTITION_PREFIX}-${normalizedIndex}`
}
