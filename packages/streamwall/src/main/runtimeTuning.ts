export const STREAMWALL_RENDERER_PROCESS_LIMIT = 16

export interface CommandLineLike {
  appendSwitch(name: string, value?: string): void
}

export function applyStreamwallRuntimeTuning(commandLine: CommandLineLike) {
  commandLine.appendSwitch(
    'renderer-process-limit',
    String(STREAMWALL_RENDERER_PROCESS_LIMIT),
  )
}
