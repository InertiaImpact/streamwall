import type { StreamDataContent } from './types.ts'

/** Reject executable/local URLs before they reach an embedded graphics page. */
export function normalizeOverlayUrl(value: unknown): string | undefined {
  if (typeof value !== 'string' || !value.trim()) return undefined
  try {
    const url = new URL(value.trim())
    return url.protocol === 'https:' || url.protocol === 'http:'
      ? url.href : undefined
  } catch {
    return undefined
  }
}

/** Survives restarts, generated ID changes, and stream label edits. */
export function streamOverlayKey(stream: Pick<StreamDataContent, 'link' | 'crop'>): string {
  const crop = stream.crop
  return JSON.stringify([stream.link, crop ? [crop.x, crop.y, crop.width, crop.height] : null])
}
