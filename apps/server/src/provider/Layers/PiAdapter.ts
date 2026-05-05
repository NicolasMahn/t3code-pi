/**
 * PiAdapter — translates Pi RPC events into t3code's ProviderRuntimeEvent model.
 *
 * Wraps a PiRpcClient and implements the ProviderAdapterShape contract so
 * Pi sessions can be driven through t3code's standard orchestration layer.
 *
 * @module PiAdapter
 */
import {
  type CanonicalItemType,
  type CanonicalRequestType,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  EventId,
  IsoDateTime,
  ThreadId,
  TurnId,
  RuntimeItemId,
} from "@t3tools/contracts";
import { Effect, Fiber, Queue, Scope, Stream } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import type {
  ProviderSession,
  ProviderSessionStartInput,
  ProviderSendTurnInput,
  ProviderTurnStartResult,
} from "@t3tools/contracts";
import { makePiRpcClient, type PiRpcClientShape, type PiRpcEvent } from "./PiRpcClient.ts";

const PROVIDER = ProviderDriverKind.make("pi");

// ── Event translation ─────────────────────────────────────────────────

let eventIdCounter = 0;
function makeEventId(): EventId {
  return EventId.make(`pi-${Date.now()}-${++eventIdCounter}`);
}

function makeIsoNow(): IsoDateTime {
  return IsoDateTime.make(new Date().toISOString());
}

function runtimeEventBase(threadId: ThreadId, rawEvent: PiRpcEvent) {
  return {
    eventId: makeEventId(),
    provider: PROVIDER,
    threadId,
    createdAt: makeIsoNow(),
    raw: {
      source: "pi.rpc" as const,
      method: rawEvent.type,
      payload: rawEvent,
    },
  };
}

function mapPiEventToRuntimeEvents(
  rawEvent: PiRpcEvent,
  threadId: ThreadId,
): ReadonlyArray<ProviderRuntimeEvent> {
  const base = runtimeEventBase(threadId, rawEvent);

  switch (rawEvent.type) {
    // ── Agent lifecycle ────────────────────────────────────────────
    case "agent_start": {
      return [
        {
          ...base,
          type: "turn.started" as const,
          payload: {},
        },
      ];
    }

    case "agent_end": {
      return [
        {
          ...base,
          type: "turn.completed" as const,
          payload: { state: "completed" as const },
        },
      ];
    }

    // ── Turn lifecycle (Pi's internal turns) ───────────────────────
    case "turn_start": {
      // Pi's turn_start is one LLM response cycle. We don't emit a
      // separate turn.started here — the agent_start/end pair covers
      // the full cycle in t3code's model.
      return [];
    }

    case "turn_end": {
      return [];
    }

    // ── Message lifecycle ──────────────────────────────────────────
    case "message_start": {
      const message = rawEvent.message as { role?: string } | undefined;
      const role = message?.role;
      if (role === "assistant") {
        return [
          {
            ...base,
            type: "item.started" as const,
            payload: {
              itemType: "assistant_message" as CanonicalItemType,
              title: "Assistant message",
            },
          },
        ];
      }
      if (role === "user") {
        return [
          {
            ...base,
            type: "item.started" as const,
            payload: {
              itemType: "user_message" as CanonicalItemType,
              title: "User message",
            },
          },
        ];
      }
      return [];
    }

    case "message_end": {
      const message = rawEvent.message as { role?: string } | undefined;
      const role = message?.role;
      if (role === "assistant") {
        return [
          {
            ...base,
            type: "item.completed" as const,
            payload: {
              itemType: "assistant_message" as CanonicalItemType,
              status: "completed" as const,
            },
          },
        ];
      }
      return [];
    }

    case "message_update": {
      const aevt = rawEvent.assistantMessageEvent as { type?: string; delta?: string } | undefined;
      if (!aevt) return [];

      switch (aevt.type) {
        case "text_delta":
          return [
            {
              ...base,
              type: "content.delta" as const,
              payload: {
                streamKind: "assistant_text" as const,
                delta: aevt.delta ?? "",
              },
            },
          ];
        case "thinking_delta":
          return [
            {
              ...base,
              type: "content.delta" as const,
              payload: {
                streamKind: "reasoning_text" as const,
                delta: aevt.delta ?? "",
              },
            },
          ];
        case "toolcall_start":
          return [
            {
              ...base,
              type: "item.started" as const,
              payload: {
                itemType: "dynamic_tool_call" as CanonicalItemType,
                title: "Tool call",
              },
            },
          ];
        case "toolcall_delta":
          return [
            {
              ...base,
              type: "content.delta" as const,
              payload: {
                streamKind: "assistant_text" as const,
                delta: aevt.delta ?? "",
              },
            },
          ];
        case "toolcall_end": {
          // The full tool call object is in the message; emit item.completed
          return [
            {
              ...base,
              type: "item.completed" as const,
              payload: {
                itemType: "dynamic_tool_call" as CanonicalItemType,
                status: "completed" as const,
              },
            },
          ];
        }
        default:
          return [];
      }
    }

    // ── Tool execution ─────────────────────────────────────────────
    case "tool_execution_start": {
      const toolName = rawEvent.toolName as string | undefined;
      const itemType = toolNameToItemType(toolName);
      return [
        {
          ...base,
          type: "item.started" as const,
          payload: {
            itemType,
            title: toolTitle(toolName),
            detail: JSON.stringify(rawEvent.args ?? {}),
          },
        },
      ];
    }

    case "tool_execution_update": {
      return [
        {
          ...base,
          type: "tool.progress" as const,
          payload: {
            summary: extractToolProgressSummary(rawEvent),
          },
        },
      ];
    }

    case "tool_execution_end": {
      const toolName = rawEvent.toolName as string | undefined;
      const itemType = toolNameToItemType(toolName);
      const isError = rawEvent.isError === true;
      return [
        {
          ...base,
          type: "item.completed" as const,
          payload: {
            itemType,
            status: isError ? ("failed" as const) : ("completed" as const),
            ...(isError ? { detail: extractToolError(rawEvent) } : {}),
          },
        },
      ];
    }

    // ── Compaction ─────────────────────────────────────────────────
    case "compaction_start": {
      return [
        {
          ...base,
          type: "item.started" as const,
          payload: {
            itemType: "context_compaction" as CanonicalItemType,
            title: "Compaction",
          },
        },
      ];
    }

    case "compaction_end": {
      return [
        {
          ...base,
          type: "item.completed" as const,
          payload: {
            itemType: "context_compaction" as CanonicalItemType,
            status: "completed" as const,
          },
        },
      ];
    }

    // ── Auto-retry ─────────────────────────────────────────────────
    case "auto_retry_start": {
      return [
        {
          ...base,
          type: "runtime.warning" as const,
          payload: {
            message: `Auto-retry attempt ${rawEvent.attempt}/${rawEvent.maxAttempts}: ${rawEvent.errorMessage}`,
          },
        },
      ];
    }

    case "auto_retry_end": {
      if (rawEvent.success === false) {
        return [
          {
            ...base,
            type: "runtime.error" as const,
            payload: {
              message: `Auto-retry failed after ${rawEvent.attempt} attempts: ${rawEvent.finalError}`,
              class: "provider_error" as const,
            },
          },
        ];
      }
      return [];
    }

    // ── Queue updates (steering/follow-up) ─────────────────────────
    case "queue_update": {
      // Informational; not mapped to a runtime event
      return [];
    }

    // ── Extension UI requests (approval/input) ─────────────────────
    case "extension_ui_request": {
      const method = rawEvent.method as string | undefined;
      if (method === "confirm" || method === "select") {
        return [
          {
            ...base,
            type: "request.opened" as const,
            payload: {
              requestType: "tool_user_input" as CanonicalRequestType,
              detail: (rawEvent.title as string) ?? method,
              args: rawEvent,
            },
          },
        ];
      }
      if (method === "input" || method === "editor") {
        return [
          {
            ...base,
            type: "user-input.requested" as const,
            payload: {
              questions: [
                {
                  id: (rawEvent.id as string) ?? "input",
                  header: (rawEvent.title as string) ?? "Input",
                  question: (rawEvent.title as string) ?? "Provide input",
                  options: [],
                  multiSelect: false,
                },
              ],
            },
          },
        ];
      }
      // Fire-and-forget methods (notify, setStatus, setWidget, setTitle, set_editor_text)
      return [];
    }

    default:
      return [];
  }
}

function toolNameToItemType(toolName: string | undefined): CanonicalItemType {
  switch (toolName) {
    case "bash":
      return "command_execution";
    case "edit":
    case "write":
      return "file_change";
    case "read":
    case "grep":
    case "find":
    case "ls":
      return "file_change"; // closest match
    default:
      return "dynamic_tool_call";
  }
}

function toolTitle(toolName: string | undefined): string {
  switch (toolName) {
    case "bash":
      return "Ran command";
    case "edit":
      return "Edited file";
    case "write":
      return "Wrote file";
    case "read":
      return "Read file";
    case "grep":
      return "Searched files";
    case "find":
      return "Found files";
    case "ls":
      return "Listed files";
    default:
      return toolName ? `Tool: ${toolName}` : "Tool call";
  }
}

function extractToolProgressSummary(event: PiRpcEvent): string {
  const partial = event.partialResult as { content?: Array<{ text?: string }> } | undefined;
  if (partial?.content?.[0]?.text) {
    const text = partial.content[0].text;
    return text.length > 200 ? text.slice(0, 200) + "..." : text;
  }
  return "Running...";
}

function extractToolError(event: PiRpcEvent): string {
  const result = event.result as { content?: Array<{ text?: string }> } | undefined;
  if (result?.content?.[0]?.text) {
    return result.content[0].text;
  }
  return "Tool execution failed";
}

// ── Adapter session context ───────────────────────────────────────────

interface PiAdapterSessionContext {
  readonly threadId: ThreadId;
  readonly client: PiRpcClientShape;
  readonly eventFiber: ReturnType<ReturnType<typeof Effect.forkChild>["pipe"]>;
  stopped: boolean;
}

// ── Adapter factory ───────────────────────────────────────────────────

export interface PiAdapterOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly binaryPath: string;
  readonly cwd?: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly model?: string;
}

export const makePiAdapter = Effect.fn("makePiAdapter")(function* (options: PiAdapterOptions) {
  const boundInstanceId = options.instanceId ?? ProviderInstanceId.make("pi");
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const runtimeEventQueue = yield* Effect.acquireRelease(
    Queue.unbounded<ProviderRuntimeEvent>(),
    Queue.shutdown,
  );
  const sessions = new Map<ThreadId, PiAdapterSessionContext>();

  const requireSession = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const session = sessions.get(threadId);
      if (!session || session.stopped) {
        return yield* new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId,
        });
      }
      return session;
    });

  const startSession: PiAdapterShape["startSession"] = (input) =>
    Effect.scoped(
      Effect.gen(function* () {
        const threadId = input.threadId;

        // Stop existing session if any
        const existing = sessions.get(threadId);
        if (existing && !existing.stopped) {
          yield* stopSessionInternal(existing);
        }

        // Spawn Pi RPC process
        const client = yield* makePiRpcClient({
          binaryPath: options.binaryPath,
          cwd: input.cwd ?? options.cwd ?? process.cwd(),
          ...(options.environment ? { environment: options.environment } : {}),
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "startSession",
                detail: cause.message,
                cause,
              }),
          ),
        );

        // Fork event processing fiber
        const eventFiber = yield* Stream.runForEach(client.events, (event) =>
          Effect.sync(() => {
            const runtimeEvents = mapPiEventToRuntimeEvents(event, threadId);
            for (const re of runtimeEvents) {
              Effect.runSync(Queue.offer(runtimeEventQueue, re));
            }
          }),
        ).pipe(Effect.forkChild);

        // Set model if configured
        const model = options.model;
        if (model) {
          yield* client
            .sendCommand({ type: "set_model", model })
            .pipe(
              Effect.mapError(
                (e) =>
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "set_model",
                    detail: e.message,
                    cause: e,
                  }),
              ),
            );
        }

        sessions.set(threadId, {
          threadId,
          client,
          eventFiber,
          stopped: false,
        });

        const now = makeIsoNow();
        return {
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          status: "ready" as const,
          runtimeMode: input.runtimeMode,
          threadId,
          createdAt: now,
          updatedAt: now,
        } satisfies ProviderSession;
      }),
    );

  const sendTurn: PiAdapterShape["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const session = yield* requireSession(input.threadId);
      const text = input.input;
      if (!text) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "prompt",
          detail: "No input text provided.",
        });
      }

      yield* session.client.sendCommand({ type: "prompt", message: text }).pipe(
        Effect.mapError(
          (e) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "prompt",
              detail: e.message,
              cause: e,
            }),
        ),
      );

      const turnId = TurnId.make(`pi-turn-${Date.now()}`);
      return {
        threadId: input.threadId,
        turnId,
      } satisfies ProviderTurnStartResult;
    });

  const interruptTurn: PiAdapterShape["interruptTurn"] = (threadId) =>
    Effect.gen(function* () {
      const session = yield* requireSession(threadId);
      yield* session.client.sendCommand({ type: "abort" }).pipe(
        Effect.mapError(
          (e) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "abort",
              detail: e.message,
              cause: e,
            }),
        ),
      );
    });

  const respondToRequest: PiAdapterShape["respondToRequest"] = (threadId, requestId, decision) =>
    Effect.gen(function* () {
      const session = yield* requireSession(threadId);
      yield* session.client
        .sendCommand({
          type: "extension_ui_response",
          id: requestId,
          ...(decision === "accept" || decision === "acceptForSession"
            ? { confirmed: true }
            : decision === "decline"
              ? { confirmed: false }
              : { cancelled: true }),
        })
        .pipe(
          Effect.mapError(
            (e) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "extension_ui_response",
                detail: e.message,
                cause: e,
              }),
          ),
        );
    });

  const respondToUserInput: PiAdapterShape["respondToUserInput"] = (threadId, requestId, answers) =>
    Effect.gen(function* () {
      const session = yield* requireSession(threadId);
      // Pi's extension UI input expects { id, value } or { id, cancelled }
      const firstAnswer = Object.values(answers)[0];
      yield* session.client
        .sendCommand({
          type: "extension_ui_response",
          id: requestId,
          ...(firstAnswer !== undefined ? { value: String(firstAnswer) } : { cancelled: true }),
        })
        .pipe(
          Effect.mapError(
            (e) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "extension_ui_response",
                detail: e.message,
                cause: e,
              }),
          ),
        );
    });

  const stopSessionInternal = (session: PiAdapterSessionContext) =>
    Effect.gen(function* () {
      if (session.stopped) return;
      session.stopped = true;
      sessions.delete(session.threadId);
      yield* session.client.close.pipe(Effect.ignore);
      yield* Fiber.interrupt(session.eventFiber).pipe(Effect.ignore);
    });

  const stopSession: PiAdapterShape["stopSession"] = (threadId) =>
    Effect.gen(function* () {
      const session = sessions.get(threadId);
      if (!session) return;
      yield* stopSessionInternal(session);
    });

  const listSessions = () =>
    Effect.succeed(
      Array.from(sessions.values())
        .filter((s) => !s.stopped)
        .map(
          (s): ProviderSession => ({
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready" as const,
            runtimeMode: "full-access" as const,
            threadId: s.threadId,
            createdAt: makeIsoNow(),
            updatedAt: makeIsoNow(),
          }),
        ),
    );

  const hasSession: PiAdapterShape["hasSession"] = (threadId) =>
    Effect.succeed(Boolean(sessions.get(threadId) && !sessions.get(threadId)?.stopped));

  const readThread: PiAdapterShape["readThread"] = (threadId) =>
    Effect.gen(function* () {
      yield* requireSession(threadId);
      // Pi owns the tree — we return an empty snapshot.
      // Messages arrive via the event stream.
      return {
        threadId,
        turns: [],
      };
    });

  const rollbackThread: PiAdapterShape["rollbackThread"] = (threadId, _numTurns) =>
    Effect.gen(function* () {
      yield* requireSession(threadId);
      // Pi doesn't support rollback — it uses branching.
      // Return empty for now.
      return {
        threadId,
        turns: [],
      };
    });

  const stopAll: PiAdapterShape["stopAll"] = () =>
    Effect.forEach(Array.from(sessions.values()), stopSessionInternal, {
      concurrency: 1,
      discard: true,
    }).pipe(Effect.asVoid);

  yield* Effect.addFinalizer(() =>
    stopAll().pipe(Effect.andThen(Queue.shutdown(runtimeEventQueue)), Effect.ignore),
  );

  return {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "unsupported" as const,
    },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    get streamEvents() {
      return Stream.fromQueue(runtimeEventQueue);
    },
  } satisfies PiAdapterShape;
});

// Adapter shape type (same as ProviderAdapterShape but with Pi error type)
type PiAdapterShape = ProviderAdapterShape<ProviderAdapterError>;
