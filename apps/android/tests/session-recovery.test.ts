import { runScenarios, scenarioName } from '../../../scripts/test-scenarios.mjs'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { ChatItem, HistoryEntry, MuxStreamFrame, RemoteSession } from '../src/types'
import { applyMuxFrame, foldHistory } from '../src/state/event-reducer'
import { mergeHistoryAndLive, prependHistory, reconcileDeliveryStates } from '../src/state/message-helpers'

const { proxy, transport, storage, authenticate, acp, codex } = vi.hoisted(() => ({
  proxy: { sessionHistory: vi.fn(), messageFeedbackList: vi.fn(), sessionModels: vi.fn(),
    sessionList: vi.fn(), sessionCreate: vi.fn(), hostDescribe: vi.fn(), workspaceList: vi.fn(), sessionSelectPermission: vi.fn(), sessionPrompt: vi.fn() },
  transport: { connect: vi.fn(), close: vi.fn(), hasCursor: vi.fn(), hasAntigravity: vi.fn() },
  acp: { call: vi.fn(), transferCall: vi.fn(), openStream: vi.fn(), prompt: vi.fn() },
  storage: { saveRecentWorkspaces: vi.fn(), saveLastConnectedDeviceId: vi.fn(), loadCodexPermissionPresets: vi.fn(), saveCodexPermissionPreset: vi.fn() },
  codex: { request: vi.fn() },
  authenticate: vi.fn(),
}))
vi.mock('expo-haptics', () => ({ impactAsync: async () => undefined, ImpactFeedbackStyle: { Light: 'light' } }))
vi.mock('../src/services/network-route', () => ({ resolveAutomaticPreferredTransports: async () => ['relay'] }))
vi.mock('../src/services/connection', () => ({ AndroidRemoteConnection: class {
  requireProxy() { return proxy }
  connect = transport.connect
  close = transport.close
  hasCodex() { return false }
  requireCodex() { return codex }
  hasCursor = transport.hasCursor
  hasAntigravity = transport.hasAntigravity
  requireCursor() { return acp }
  requireAntigravity() { return acp }
  getNetworkDetails() { return Promise.resolve(undefined) }
  getStats() { return { mode: 'Relay', connected: true } }
} }))
vi.mock('../src/services/storage', () => storage)
vi.mock('../src/services/login', () => ({}))
vi.mock('../src/services/server-session', () => ({ serverSession: { authenticate } }))
import { useAppStore } from '../src/state/store'

const session: RemoteSession = { sessionId: 's1', updatedAt: 1, running: false, blank: false,
  projections: { values: { permissions: { currentValue: 'default' } } } }
const entry = (type: string, seq: number, data: Record<string, unknown>): HistoryEntry => ({
  event: { type, seq, time: 1000 + seq, data },
})
const call = (seq = 2, turn = 1, callId = 'c1') => entry('tool/call', seq, { turn, callId, name: 'bash' })
const result = (seq = 3, turn = 1, callId = 'c1') => entry('tool/result', seq, { turn, message: { source: { callId }, content: [] } })
const frame = (row: HistoryEntry): MuxStreamFrame => ({ rpcId: '', payload: { type: 'session/event', sessionId: 's1', event: row.event } })
const page = (events: HistoryEntry[], throughSeq = 10) => ({ events, throughSeq, hasMore: false })

const prepareScenario1 = () => {
  vi.resetAllMocks()
  transport.hasCursor.mockReturnValue(false)
  transport.hasAntigravity.mockReturnValue(false)
  acp.call.mockResolvedValue([])
  acp.transferCall.mockResolvedValue({ events: [] })
  acp.prompt.mockResolvedValue({ accepted: true, stopReason: 'in_progress' })
  acp.openStream.mockImplementation(async () => ({ streamId: 'acp-test', close: async () => undefined }))
  useAppStore.setState({
    ...useAppStore.getInitialState(),
    selectedSession: session,
    sessions: [session],
    connection: { phase: 'connected', stats: { mode: 'Relay', connected: true } },
  })
  proxy.sessionHistory.mockResolvedValue(page([]))
  proxy.messageFeedbackList.mockResolvedValue([])
  proxy.sessionList.mockResolvedValue([session])
  proxy.workspaceList.mockResolvedValue({ items: [], archivedSessionIds: [] })
  proxy.hostDescribe.mockResolvedValue({})
  storage.loadCodexPermissionPresets.mockResolvedValue({})
  authenticate.mockResolvedValue({ api: {}, credentials: { accessToken: 'synthetic' } })
}
beforeEach(prepareScenario1)

describe('ordered history recovery', () => {
  it('adopts only undelivered rows across any ordering of the inbox, prompt acknowledgement and echo', async () => {
    await runScenarios(['inbox-first', 'ack-first', 'echo-first'].map(order => ({ name: order, run: async () => {
      prepareScenario1()
      const running = { ...session, running: true }
      useAppStore.setState({ selectedSession: running, sessions: [running] })
      let acknowledge!: () => void
      proxy.sessionPrompt.mockImplementation(() => new Promise<void>(resolve => { acknowledge = resolve }))
      const rpcId = `adoption-${order}`
      const sending = useAppStore.getState().sendMessage('queued prompt', [], rpcId)
      const projectInbox = (value: unknown, seq: number) => useAppStore.getState().handleMuxFrame({ rpcId: '',
        payload: { type: 'session/projection', sessionId: 's1', key: 'inbox', seq, value } } as MuxStreamFrame)
      const inbox = { 'next-turn': [{ id: 'host-item', source: { kind: 'user', rpcId },
        content: [{ type: 'text', text: 'queued prompt' }] }] }
      // session/follow (echo) and session/control (inbox) are pumped
      // independently: a delayed inbox can still list an already-echoed prompt,
      // and the acknowledgement may land after both.
      if (order === 'echo-first') useAppStore.getState().handleMuxFrame(frame(entry('user/message', 4, { turn: 2,
        message: { id: 'host-delivered', source: { kind: 'user', rpcId }, content: [{ type: 'text', text: 'queued prompt' }] } })))
      if (order !== 'ack-first') projectInbox(inbox, 1)
      acknowledge()
      expect(await sending).toBe('sent')
      if (order === 'ack-first') projectInbox(inbox, 1)
      const delivery = () => {
        const row = useAppStore.getState().messages.s1?.[0]
        return row?.kind === 'message'
          ? { id: row.id, requestRpcId: row.requestRpcId, sendState: row.sendState, queuedAdopted: row.queuedAdopted }
          : row
      }
      const queued = { id: `local:${rpcId}`, requestRpcId: rpcId, sendState: 'queued', queuedAdopted: true }
      // The echo replaced the local row: the delivered Host row must take neither
      // the adoption mark nor a verdict (with no sendState the mark alone hides it).
      const delivered = { id: 'host-delivered', requestRpcId: rpcId, sendState: undefined, queuedAdopted: undefined }
      expect(delivery()).toEqual(order === 'echo-first' ? delivered : queued)
      // The prompt leaves the inbox (removed elsewhere, or consumed after its
      // echo); one projection of grace prevents racing its echo.
      projectInbox({ 'next-turn': [] }, 2)
      expect(delivery()).toEqual(order === 'echo-first' ? delivered : queued)
      projectInbox({ 'next-turn': [] }, 3)
      expect(delivery()).toEqual(order === 'echo-first' ? delivered : { ...queued, sendState: 'canceled' })
      expect(useAppStore.getState().messages.s1).toHaveLength(1)
    } })))
  })

  it('reconciles local delivery states by marking, never by deleting', async () => {
    const row = (id: string, requestRpcId: string, sendState: 'queued' | 'failed' | 'unconfirmed', createdAt = 1): ChatItem => ({
      kind: 'message', id, sessionId: 's1', role: 'user', text: 'hello', createdAt, requestRpcId, sendState,
    })
    await runScenarios([
      {
        name: 'marks pre-connection queued orphans canceled and unconfirmed orphans failed',
        run: () => {
          const merged = reconcileDeliveryStates(
            [row('local:a', 'rpc-a', 'queued'), row('local:b', 'rpc-b', 'unconfirmed')],
            new Set<string>(), new Set<string>(), 100)
          expect(merged.map(item => item.kind === 'message' ? item.sendState : undefined)).toEqual(['canceled', 'failed'])
          expect(merged.map(item => item.id)).toEqual(['local:a', 'local:b'])
        },
      },
      {
        name: 'keeps rows still sitting in the Host inbox',
        run: () => {
          const merged = reconcileDeliveryStates([row('local:a', 'rpc-a', 'queued')], new Set(['rpc-a']), new Set<string>(), 100)
          expect(merged.map(item => item.kind === 'message' ? item.sendState : undefined)).toEqual(['queued'])
        },
      },
      {
        name: 'keeps failed rows as user retry material',
        run: () => {
          const merged = reconcileDeliveryStates([row('local:c', 'rpc-c', 'failed')], new Set<string>(), new Set<string>(), 100)
          expect(merged.map(item => item.kind === 'message' ? item.sendState : undefined)).toEqual(['failed'])
        },
      },
      {
        name: 'keeps canceled rows terminal — user removal is never re-judged',
        run: () => {
          const removed: ChatItem = {
            kind: 'message', id: 'local:g', sessionId: 's1', role: 'user', text: 'gone',
            createdAt: 1, requestRpcId: 'rpc-g', sendState: 'canceled',
          }
          const merged = reconcileDeliveryStates([removed], new Set<string>(), new Set<string>(), 100)
          expect(merged.map(item => item.kind === 'message' ? item.sendState : undefined)).toEqual(['canceled'])
        },
      },
      {
        name: 'settles adopted rows whose echo never landed (lost-echo hole)',
        run: () => {
          const lost: ChatItem = {
            kind: 'message', id: 'local:lost', sessionId: 's1', role: 'user', text: 'queued then lost',
            createdAt: 1, requestRpcId: 'rpc-lost', queuedAdopted: true,
          }
          const merged = reconcileDeliveryStates([lost], new Set<string>(), new Set<string>(), 100)
          expect(merged.map(item => item.kind === 'message' ? item.sendState : undefined)).toEqual(['canceled'])
        },
      },
      {
        name: 'keeps adopted rows echoed in history or still queued host-side',
        run: () => {
          const echoed: ChatItem = {
            kind: 'message', id: 'local:echo', sessionId: 's1', role: 'user', text: 'x',
            createdAt: 1, requestRpcId: 'rpc-echo', queuedAdopted: true,
          }
          const merged = reconcileDeliveryStates([echoed], new Set<string>(), new Set(['rpc-echo']), 100)
          expect(merged.map(item => item.kind === 'message' ? item.sendState : undefined)).toEqual([undefined])
        },
      },
      {
        name: 'keeps rows sent during the current connection for live-frame reconciliation',
        run: () => {
          const merged = reconcileDeliveryStates([row('local:d', 'rpc-d', 'unconfirmed', 200)], new Set<string>(), new Set<string>(), 100)
          expect(merged.map(item => item.kind === 'message' ? item.sendState : undefined)).toEqual(['unconfirmed'])
        },
      },
      {
        name: 'keeps rows whose prompt is echoed in the loaded history',
        run: () => {
          const merged = reconcileDeliveryStates([row('local:e', 'rpc-e', 'queued')], new Set<string>(), new Set(['rpc-e']), 100)
          expect(merged.map(item => item.kind === 'message' ? item.sendState : undefined)).toEqual(['queued'])
        },
      },
    ])
  })

  it('merges cached tool results using snapshot and live ordering', async () => {
    await runScenarios(
      [
        {
          name: 'does not overwrite a completed history tool with its older running cache',
          run: () => {
            const live = foldHistory([call()], 's1')
            const history = foldHistory([call(), result()], 's1')
            expect(mergeHistoryAndLive(history, live)).toMatchObject([{ state: 'finished', nativeSeq: 3 }])
          },
        },
        {
          name: 'preserves a live result newer than the snapshot and rejects an older replayed call',
          run: () => {
            const history = foldHistory([call()], 's1')
            const live = foldHistory([call(), result()], 's1')
            expect(mergeHistoryAndLive(history, live)).toMatchObject([{ state: 'finished' }])
            expect(applyMuxFrame(live, frame(call()))).toEqual(live)
          },
        },
        {
          name: 'retains tool position and owner when a delayed result arrives in a later turn',
          run: () => {
            const rows = [
              call(),
              entry('user/message', 5, {
                turn: 2,
                message: { id: 'u2', source: { kind: 'user' }, content: [{ type: 'text', text: 'next' }] },
              }),
              result(6, 2),
            ]
            expect(foldHistory(rows, 's1')[0]).toMatchObject({ turn: '1', nativeOrderSeq: 2, nativeSeq: 6 })
          },
        },
        {
          name: 'inserts cached older rows before new history instead of appending them after the answer',
          run: () => {
            const older = foldHistory([call()], 's1')
            const newer = foldHistory([call(20, 2, 'c2')], 's1')
            expect(mergeHistoryAndLive(newer, older).map((item) => item.id)).toEqual(['c1', 'c2'])
          },
        },
        {
          name: 'recovers a missing older result during pagination without replaying a stale stream',
          run: () => {
            const incomplete = foldHistory([call(), entry('turn/end', 5, { turn: 1 })], 's1')
            expect(prependHistory(foldHistory([call(), result()], 's1'), incomplete)).toMatchObject([
              { state: 'finished' },
            ])
            const stream = foldHistory(
              [
                entry('assistant/chunk', 10, { turn: 2, step: 1, chunk: { type: 'text-delta', text: 'a' } }),
                entry('assistant/chunk', 10.5, { turn: 2, step: 1, chunk: { type: 'text-delta', text: 'b' } }),
              ],
              's1',
            )
            expect(mergeHistoryAndLive([], stream, 10)).toMatchObject([{ text: 'ab' }])
          },
        },
      ],
      async () => {
        await prepareScenario1()
      },
    )
  }, 25000)

  it('settles history turns without replaying stale streams', async () => {
    await runScenarios(
      [
        {
          name: 'drops covered transient streams but keeps new deltas beyond the snapshot cursor',
          run: () => {
            const chunk = (seq: number) =>
              entry('assistant/chunk', seq, {
                turn: 1,
                step: 1,
                chunk: { type: 'text-delta', text: 'partial' },
              })
            expect(mergeHistoryAndLive([], foldHistory([chunk(2.5)], 's1'), 10)).toEqual([])
            expect(mergeHistoryAndLive([], foldHistory([chunk(11.5)], 's1'), 10)).toHaveLength(1)
          },
        },
        {
          name: 'settles closed turns without claiming a missing tool result succeeded',
          run: () => {
            const rows = [
              call(),
              entry('assistant/chunk', 4, { turn: 1, step: 1, chunk: { type: 'text-delta', text: 'partial' } }),
              entry('turn/end', 5, { turn: 1 }),
            ]
            expect(foldHistory(rows, 's1')).toMatchObject([{ state: 'failed' }, { streaming: false }])
          },
        },
        {
          name: 'does not close current-turn tools when an earlier turn ends',
          run: () => {
            expect(
              foldHistory([call(2, 1), call(4, 2, 'c2'), entry('turn/end', 5, { turn: 1 })], 's1'),
            ).toMatchObject([{ state: 'failed' }, { state: 'running' }])
          },
        },
      ],
      async () => {
        await prepareScenario1()
      },
    )
  }, 15000)

})

describe('session restore state machine', () => {
  it('recovers only current turn lifecycle from snapshots', async () => {
    await runScenarios(
      [
        {
          name: 'restores a closed turn from history and clears stale generating rows',
          run: async () => {
            useAppStore.setState({
              selectedSession: { ...session, running: true },
              sessions: [{ ...session, running: true }],
              messages: { s1: foldHistory([call()], 's1') },
            })
            proxy.sessionHistory.mockResolvedValue(page([call(), result(), entry('turn/end', 4, { turn: 1 })]))
            expect(await useAppStore.getState().openSession({ ...session, running: true })).toBe(true)
            expect(useAppStore.getState().selectedSession?.running).toBe(false)
            expect(useAppStore.getState().messages.s1).toMatchObject([{ state: 'finished' }])
          },
        },
        {
          name: 'keeps live lifecycle events newer than the snapshot',
          run: async () => {
            proxy.sessionHistory.mockImplementation(async () => {
              useAppStore.getState().handleMuxFrame(frame(entry('turn/start', 11, { turn: 2 })))
              useAppStore.getState().handleMuxFrame(frame(call(12, 2, 'c2')))
              return page([entry('turn/end', 10, { turn: 1 })])
            })
            await useAppStore.getState().openSession(session)
            expect(useAppStore.getState().selectedSession?.running).toBe(true)
            expect(useAppStore.getState().messages.s1).toMatchObject([{ state: 'running', turn: '2' }])
          },
        },
        {
          name: 'does not let an older turn-start reopen a completed turn',
          run: () => {
            useAppStore.getState().handleMuxFrame(frame(entry('turn/end', 20, { turn: 1 })))
            useAppStore.getState().handleMuxFrame(frame(entry('turn/start', 10, { turn: 1 })))
            expect(useAppStore.getState().selectedSession?.running).toBe(false)
          },
        },
      ],
      async () => {
        await prepareScenario1()
      },
    )
  }, 15000)

  it('ignores an earlier session load completing after a later navigation', async () => {
    let finish!: (value: ReturnType<typeof page>) => void
    proxy.sessionHistory.mockReturnValueOnce(new Promise(resolve => { finish = resolve }))
    const old = useAppStore.getState().openSession(session)
    await vi.waitFor(() => expect(proxy.sessionHistory).toHaveBeenCalled())
    const next = { ...session, sessionId: 's2' }
    expect(await useAppStore.getState().openSession(next)).toBe(true)
    finish(page([call()]))
    expect(await old).toBe(false)
    expect(useAppStore.getState().selectedSession?.sessionId).toBe('s2')
  })
  it('restores the selected conversation once for overlapping reconnect requests', async () => {
    useAppStore.setState({ selectedDevice: { deviceId: 'host' } as never, config: { baseUrl: 'https://example.invalid' } as never, identity: {} as never })
    const first = useAppStore.getState().reconnect()
    const second = useAppStore.getState().reconnect()
    expect(await first).toBe(true)
    expect(await second).toBe(true)
    expect(transport.connect).toHaveBeenCalledTimes(1)
    expect(proxy.sessionHistory).toHaveBeenCalledTimes(1)
    expect(useAppStore.getState().selectedSession?.sessionId).toBe('s1')
  })
  it('restores ACP sessions once across overlapping reconnects and respects restoreSession=false', async () => {
    const rows = ['cursor', 'antigravity'] as const
    await runScenarios(
      rows.map((row, index) => {
        const backend = row
        return {
          name: scenarioName(
            'restores %s once for overlapping reconnects and respects restoreSession=false',
            row,
            index,
          ),
          run: async () => {
            const selected = {
              ...session,
              sessionId: `${backend}:native-session`,
              nativeId: 'native-session',
              backend,
            }
            useAppStore.setState({
              selectedSession: selected,
              sessions: [selected],
              selectedDevice: { deviceId: 'host' } as never,
              config: { baseUrl: 'https://example.invalid' } as never,
              identity: {} as never,
            })
            transport.hasCursor.mockReturnValue(backend === 'cursor')
            transport.hasAntigravity.mockReturnValue(backend === 'antigravity')
            const first = useAppStore.getState().reconnect()
            const second = useAppStore.getState().reconnect()
            expect(await first).toBe(true)
            expect(await second).toBe(true)
            expect(transport.connect).toHaveBeenCalledTimes(1)
            expect(acp.openStream).toHaveBeenCalledTimes(1)
            expect(acp.transferCall).toHaveBeenCalledTimes(backend === 'antigravity' ? 1 : 0)
            expect(await useAppStore.getState().reconnect({ restoreSession: false })).toBe(true)
            expect(acp.openStream).toHaveBeenCalledTimes(1)
            expect(acp.transferCall).toHaveBeenCalledTimes(backend === 'antigravity' ? 1 : 0)
          },
        }
      }),
      async () => {
        await prepareScenario1()
      },
    )
  })
  it('ignores AGY history completing after navigation to another conversation', async () => {
    let finish!: (result: { events: unknown[] }) => void
    acp.transferCall.mockReturnValueOnce(new Promise(resolve => { finish = resolve }))
    const old = useAppStore.getState().openSession({ ...session, sessionId: 'antigravity:old', nativeId: 'old', backend: 'antigravity' })
    await vi.waitFor(() => expect(acp.transferCall).toHaveBeenCalled())
    expect(await useAppStore.getState().openSession({ ...session, sessionId: 'new' })).toBe(true)
    finish({ events: [] })
    expect(await old).toBe(false)
    expect(useAppStore.getState().selectedSession?.sessionId).toBe('new')
    expect(useAppStore.getState().messages['antigravity:old']).toBeUndefined()
  })

  it('closes late ACP subscriptions after navigation', async () => {
    const rows = ['cursor', 'antigravity'] as const
    await runScenarios(
      rows.map((row, index) => {
        const backend = row
        return {
          name: scenarioName('closes a late %s subscription after navigation', row, index),
          run: async () => {
            let finish!: (stream: { streamId: string; close: () => Promise<void> }) => void
            acp.openStream.mockReturnValueOnce(
              new Promise((resolve) => {
                finish = resolve
              }),
            )
            const old = useAppStore
              .getState()
              .openSession({ ...session, sessionId: `${backend}:old`, nativeId: 'old', backend })
            await vi.waitFor(() => expect(acp.openStream).toHaveBeenCalled())
            expect(
              await useAppStore
                .getState()
                .openSession({ ...session, sessionId: `${backend}:new`, nativeId: 'new', backend }),
            ).toBe(true)
            const close = vi.fn(async () => undefined)
            finish({ streamId: 'old-stream', close })
            expect(await old).toBe(false)
            expect(close).toHaveBeenCalledOnce()
            expect(useAppStore.getState().selectedSession?.sessionId).toBe(`${backend}:new`)
          },
        }
      }),
      async () => {
        await prepareScenario1()
      },
    )
  })

  it('isolates late ACP prompt subscriptions from the new conversation', async () => {
    const rows = ['cursor', 'antigravity'] as const
    await runScenarios(
      rows.map((row, index) => {
        const backend = row
        return {
          name: scenarioName('keeps a late %s prompt subscription out of the new conversation', row, index),
          run: async () => {
            const selected = { ...session, sessionId: `${backend}:old`, nativeId: 'old', backend }
            useAppStore.setState({ selectedSession: selected, sessions: [selected] })
            let finish!: (stream: { streamId: string; close: () => Promise<void> }) => void
            acp.openStream.mockReturnValueOnce(
              new Promise((resolve) => {
                finish = resolve
              }),
            )
            const sending = useAppStore.getState().sendMessage('hello')
            await vi.waitFor(() => expect(acp.openStream).toHaveBeenCalled())
            expect(
              await useAppStore
                .getState()
                .openSession({ ...selected, sessionId: `${backend}:new`, nativeId: 'new' }),
            ).toBe(true)
            const close = vi.fn(async () => undefined)
            finish({ streamId: 'old-prompt-stream', close })
            expect(await sending).toBe('sent')
            expect(close).toHaveBeenCalledOnce()
            expect(useAppStore.getState().selectedSession?.sessionId).toBe(`${backend}:new`)
            expect(acp.prompt).toHaveBeenCalledWith('old', 'hello', undefined, [], backend)
          },
        }
      }),
      async () => {
        await prepareScenario1()
      },
    )
  })

})

describe('authoritative permission projection', () => {
  it('applies new Harness chat defaults only after Host confirmation', async () => {
    await runScenarios([
      ...(['default', 'workspace-write', 'danger-full-access'] as const).map(preset => ({
        name: `creates with ${preset} without changing the existing conversation`,
        run: async () => {
          const created = { ...session, sessionId: 'new', blank: true,
            projections: { values: { permissions: { currentValue: preset } } } }
          useAppStore.setState({ defaultPermissionPreset: preset })
          proxy.sessionCreate.mockResolvedValue({ sessionId: 'new' })
          proxy.sessionList.mockResolvedValue([session, created])
          expect(await useAppStore.getState().createSession()).toBe(true)
          expect(proxy.sessionSelectPermission.mock.calls).toEqual(preset === 'default' ? [] : [['new', preset]])
          expect(useAppStore.getState().selectedSession?.projections?.values?.permissions).toEqual({ currentValue: preset })
          expect(useAppStore.getState().sessions.find(item => item.sessionId === 's1')).toEqual(session)
        },
      })),
      ...(['rejected', 'unconfirmed', 'disconnected'] as const).map(reason => ({
        name: `does not open a new conversation when permissions are ${reason}`,
        run: async () => {
          useAppStore.setState({ defaultPermissionPreset: 'danger-full-access' })
          proxy.sessionCreate.mockResolvedValue({ sessionId: 'new' })
          proxy.sessionList.mockResolvedValue([session, { ...session, sessionId: 'new' }])
          if (reason === 'rejected') proxy.sessionSelectPermission.mockRejectedValue(new Error('refused'))
          if (reason === 'disconnected') proxy.sessionCreate.mockImplementation(async () => {
            await useAppStore.getState().disconnect()
            return { sessionId: 'new' }
          })
          expect(await useAppStore.getState().createSession()).toBe(false)
          expect(useAppStore.getState().selectedSession?.sessionId).not.toBe('new')
          if (reason === 'disconnected') expect(proxy.sessionSelectPermission).not.toHaveBeenCalled()
        },
      })),
      {
        name: 'opening an existing conversation never applies the configured default',
        run: async () => {
          useAppStore.setState({ defaultPermissionPreset: 'danger-full-access' })
          expect(await useAppStore.getState().openSession(session)).toBe(true)
          expect(proxy.sessionSelectPermission).not.toHaveBeenCalled()
          expect(useAppStore.getState().selectedSession?.projections?.values?.permissions).toEqual({ currentValue: 'default' })
        },
      },
    ], prepareScenario1)
  })

  it('starts CodeX with the default and remembers the actual permission for that Host and Thread', async () => {
    await runScenarios([
      ...(['default', 'workspace-write', 'danger-full-access'] as const).flatMap(preset =>
        (preset === 'danger-full-access' ? ['workspace-write', 'danger-full-access'] as const : ['workspace-write'] as const).map(effective => ({
          name: `requests ${preset} and preserves the returned ${effective} policy`,
          run: async () => {
            const workspace = { workspaceId: 'codex:project', backend: 'codex' as const, title: 'project', path: '/project', sessionIds: [], createdAt: '', updatedAt: '' }
            const openSession = vi.fn(async () => true)
            useAppStore.setState({ defaultPermissionPreset: preset, workspaces: [workspace], openSession,
              selectedDevice: { deviceId: 'host', name: 'Host', platform: 'linux', identityKey: 'key', membershipId: 'member', online: true, trusted: true } })
            codex.request.mockResolvedValue({ thread: { id: 'thread', cwd: '/project' },
              approvalPolicy: effective === 'danger-full-access' ? 'never' : 'on-request', sandbox: effective })
            expect(await useAppStore.getState().createSession(workspace.workspaceId)).toBe(true)
            expect(codex.request).toHaveBeenCalledWith('thread/start', {
              cwd: '/project', permissionPreset: preset === 'default' ? 'workspace-write' : preset,
            })
            expect(storage.saveCodexPermissionPreset).toHaveBeenCalledWith('host', 'thread', effective)
            expect(openSession).toHaveBeenCalledWith(expect.objectContaining({
              nativeId: 'thread', projections: { values: expect.objectContaining({ permissions: expect.objectContaining({ currentValue: effective }) }) },
            }))
            expect(proxy.sessionSelectPermission).not.toHaveBeenCalled()
          },
        }))),
    ], prepareScenario1)
  })

  const projection = (value: string, seq: number): MuxStreamFrame => ({ rpcId: '', payload: { type: 'session/projection', sessionId: 's1', key: 'permissions', value: { currentValue: value }, seq } })
  it('requires authoritative permission projections and change confirmation', async () => {
    await runScenarios(
      [
        {
          name: 'updates the selected session and rejects older permission projections',
          run: () => {
            useAppStore.getState().handleMuxFrame(projection('read-only', 20))
            useAppStore.getState().handleMuxFrame(projection('default', 10))
            expect(useAppStore.getState().selectedSession?.projections?.values?.permissions).toEqual({
              currentValue: 'read-only',
            })
          },
        },
        {
          name: 'reads back a successful permission change from the Host',
          run: async () => {
            proxy.sessionList.mockResolvedValue([
              { ...session, projections: { values: { permissions: { currentValue: 'read-only' } } } },
            ])
            expect(await useAppStore.getState().selectPermission('read-only')).toBe(true)
            expect(useAppStore.getState().selectedSession?.projections?.values?.permissions).toEqual({
              currentValue: 'read-only',
            })
          },
        },
        {
          name: 'does not invent a grant when the command succeeded but the Host did not confirm it',
          run: async () => {
            expect(await useAppStore.getState().selectPermission('full-access')).toBe(false)
            expect(useAppStore.getState().selectedSession?.projections?.values?.permissions).toEqual({
              currentValue: 'default',
            })
          },
        },
      ],
      async () => {
        await prepareScenario1()
      },
    )
  }, 15000)

})
