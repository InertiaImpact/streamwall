import type { StreamDataContent } from './types.ts'

export function normalizeOverlayScale(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value)
    ? Math.min(300, Math.max(10, Math.round(value))) : 100
}

export function isTileOverlayVisible(
  hiddenCells: Record<string, boolean> | undefined,
  gridId: string | undefined,
  spaces: number[],
): boolean {
  return !spaces.some((space) => hiddenCells?.[`${gridId ?? 'grid-1'}:${space}`] === true)
}

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
