/**
 * PiRpcClient — communicates with a Pi agent process via JSON-RPC over stdin/stdout.
 *
 * Spawns `pi --mode rpc` and provides typed methods for sending commands
 * and receiving events. Pi's RPC protocol is documented at:
 * https://github.com/badlogic/pi-mono/blob/main/pi/docs/rpc.md
 *
 * @module PiRpcClient
 */
import { Cause, Deferred, Effect, Fiber, Queue, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

// ── Pi RPC Command Types ──────────────────────────────────────────────

interface PiRpcCommand {
  type: string;
  id?: string;
  [key: string]: unknown;
}

// ── Pi RPC Event Types ────────────────────────────────────────────────

export interface PiRpcEvent {
  type: string;
  [key: string]: unknown;
}

export interface PiRpcResponse {
  type: "response";
  id?: string;
  command: string;
  success: boolean;
  data?: unknown;
  error?: string;
}

// ── Client Shape ──────────────────────────────────────────────────────

export interface PiRpcClientShape {
  readonly sendCommand: (command: PiRpcCommand) => Effect.Effect<PiRpcResponse, PiRpcClientError>;
  readonly events: Stream.Stream<PiRpcEvent>;
  readonly close: Effect.Effect<void>;
}

export class PiRpcClientError extends Error {
  readonly _tag = "PiRpcClientError";
  override cause?: unknown;
  constructor(message: string, cause?: unknown) {
    super(message);
    this.cause = cause;
  }
}

// ── Implementation ────────────────────────────────────────────────────

const encoder = new TextEncoder();

let commandIdCounter = 0;

export const makePiRpcClient = Effect.fn("makePiRpcClient")(function* (options: {
  readonly binaryPath: string;
  readonly cwd?: string;
  readonly args?: ReadonlyArray<string>;
  readonly environment?: NodeJS.ProcessEnv;
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const eventQueue = yield* Effect.acquireRelease(Queue.unbounded<PiRpcEvent>(), Queue.shutdown);
  const outgoingQueue = yield* Effect.acquireRelease(
    Queue.unbounded<string, Cause.Done<void>>(),
    Queue.shutdown,
  );
  const pendingRequests = new Map<string, Deferred.Deferred<PiRpcResponse, PiRpcClientError>>();

  const child = yield* spawner
    .spawn(
      ChildProcess.make(
        options.binaryPath,
        ["--mode", "rpc", "--no-session", ...(options.args ?? [])],
        {
          cwd: options.cwd,
          env: options.environment,
          shell: process.platform === "win32",
        },
      ),
    )
    .pipe(
      Effect.mapError(
        (cause) => new PiRpcClientError(`Failed to spawn Pi process: ${cause.message}`, cause),
      ),
    );

  // Parse JSONL from stdout (child.stdout is a Stream<Uint8Array>)
  const stdoutParserFiber = yield* child.stdout
    .pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Effect.sync(() => {
          const lines = chunk.split("\n");
          for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed) continue;
            try {
              const parsed = JSON.parse(trimmed) as PiRpcEvent | PiRpcResponse;
              if (parsed.type === "response" && "command" in parsed) {
                const response = parsed as PiRpcResponse;
                const id = response.id;
                if (id && pendingRequests.has(id)) {
                  const deferred = pendingRequests.get(id)!;
                  pendingRequests.delete(id);
                  Effect.runSync(Deferred.succeed(deferred, response));
                }
              } else {
                // It's an event — offer to the queue
                Effect.runSync(Queue.offer(eventQueue, parsed as PiRpcEvent));
              }
            } catch {
              // Ignore malformed JSON lines
            }
          }
        }),
      ),
    )
    .pipe(Effect.forkChild);

  // Log stderr for debugging (child.stderr is a Stream<Uint8Array>)
  const stderrParserFiber = yield* child.stderr
    .pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Effect.sync(() => {
          if (chunk.trim()) {
            console.error(`[pi-rpc stderr] ${chunk.trim()}`);
          }
        }),
      ),
    )
    .pipe(Effect.forkChild);

  // Write outgoing messages to child's stdin via the Sink
  const writeFiber = yield* Stream.fromQueue(outgoingQueue).pipe(
    Stream.map((line) => encoder.encode(line)),
    Stream.run(child.stdin),
    Effect.forkChild,
  );

  const sendCommand = (command: PiRpcCommand): Effect.Effect<PiRpcResponse, PiRpcClientError> =>
    Effect.gen(function* () {
      const id = `t3-${++commandIdCounter}`;
      const commandWithId = { ...command, id };
      const jsonLine = JSON.stringify(commandWithId) + "\n";

      const deferred = yield* Deferred.make<PiRpcResponse, PiRpcClientError>();
      pendingRequests.set(id, deferred);

      // Offer to the outgoing queue — the write fiber will send it to stdin
      yield* Queue.offer(outgoingQueue, jsonLine).pipe(
        Effect.mapError(() => new PiRpcClientError("Failed to queue command")),
      );

      return yield* Deferred.await(deferred);
    });

  const close = Effect.gen(function* () {
    // Kill the child process
    yield* Effect.ignore(child.kill());
    yield* Fiber.interrupt(stdoutParserFiber).pipe(Effect.ignore);
    yield* Fiber.interrupt(stderrParserFiber).pipe(Effect.ignore);
    yield* Fiber.interrupt(writeFiber).pipe(Effect.ignore);
    yield* Queue.shutdown(outgoingQueue);
  });

  yield* Effect.addFinalizer(() => Effect.ignore(close));

  return {
    sendCommand,
    events: Stream.fromQueue(eventQueue),
    close,
  } satisfies PiRpcClientShape;
});
