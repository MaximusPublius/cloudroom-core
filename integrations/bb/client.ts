export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json };
export type Harness = "codex" | "pi";
export type Workspace = { id: string; path: string };
type Upload = { body: ReadableStream<Uint8Array>; length: number };
export type Receipt = {
  request_id: string;
  command: string;
  state: string;
  input: Json;
  model?: string;
  provider?: string;
};
export type Acceptance = { session_id: string; receipt: Receipt; saving: Json };
export type SessionRecord = {
  sequence: number;
  session_id: string;
  kind: string;
  data: Json;
  native?: string;
  timestamp_ms?: number;
};
export type Session = {
  session_id: string;
  harness: Harness;
  state: string;
  native_id: string | null;
  current_request: string | null;
  last_sequence: number;
  queue: string[];
  receipts: { [id: string]: Receipt };
};

export class CloudroomError extends Error {
  readonly status: number | null;

  constructor(message: string, status: number | null = null) {
    super(message);
    this.name = "CloudroomError";
    this.status = status;
  }
}

function object(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new CloudroomError("Invalid Cloudroom response");
  }
  return value as Record<string, unknown>;
}

function text(value: unknown): string {
  if (typeof value !== "string")
    throw new CloudroomError("Invalid Cloudroom response");
  return value;
}

function sequence(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new CloudroomError("Invalid Cloudroom sequence");
  }
  return value;
}

function json(value: unknown): Json {
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const items = Array.isArray(value) ? value : Object.values(object(value));
  for (const item of items) json(item);
  return value as Json;
}

function receipt(value: unknown): Receipt {
  const item = object(value);
  return {
    request_id: text(item.request_id),
    command: text(item.command),
    state: text(item.state),
    input: json(item.input),
    ...(item.model === undefined ? {} : { model: text(item.model) }),
    ...(item.provider === undefined ? {} : { provider: text(item.provider) }),
  };
}

function record(value: unknown, sessionId: string): SessionRecord {
  const item = object(value);
  if (item.session_id !== sessionId)
    throw new CloudroomError("Cloudroom session mismatch");
  return {
    sequence: sequence(item.sequence),
    session_id: sessionId,
    kind: text(item.kind),
    data: json(item.data),
    ...(item.native === undefined ? {} : { native: text(item.native) }),
    ...(item.timestamp_ms === undefined
      ? {}
      : { timestamp_ms: sequence(item.timestamp_ms) }),
  };
}

function requestId(value: string): string {
  if (!/^[a-zA-Z0-9_-]{1,64}$/.test(value)) {
    throw new CloudroomError(
      "request_id must be 1–64 letters, digits, underscores or hyphens",
    );
  }
  return value;
}

function sessionPath(id: string): string {
  if (!/^[a-zA-Z0-9_-]{1,128}$/.test(id))
    throw new CloudroomError("A valid Cloudroom session ID is required");
  return `/v1/sessions/${encodeURIComponent(id)}`;
}

const MAX_RESPONSE_BYTES = 16 * 1024 * 1024;

export class CloudroomClient {
  #base: string;
  #token: string;
  #gateToken?: string;
  #timeoutMs: number;

  constructor(options: { url: string; token: string; gateToken?: string; timeoutMs?: number }) {
    let url: URL;
    try {
      url = new URL(options.url);
    } catch {
      throw new CloudroomError("Invalid Cloudroom URL");
    }
    const local = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
    if (
      (url.protocol !== "https:" && !(url.protocol === "http:" && local)) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    ) {
      throw new CloudroomError(
        "Use an HTTPS core URL without credentials, query or fragment",
      );
    }
    if (!options.token || /[^\x21-\x7e]/.test(options.token)) {
      throw new CloudroomError("A core access token is required");
    }
    if (options.gateToken !== undefined && !/^[a-zA-Z0-9._~-]{1,4096}$/.test(options.gateToken)) {
      throw new CloudroomError("Invalid Boat gate credential");
    }
    this.#base = url.href.replace(/\/$/, "");
    this.#token = options.token;
    this.#gateToken = options.gateToken;
    this.#timeoutMs = options.timeoutMs ?? 30_000;
    if (!Number.isSafeInteger(this.#timeoutMs) || this.#timeoutMs <= 0) {
      throw new CloudroomError("timeoutMs must be a positive integer");
    }
  }

  async #request(
    path: string,
    body?: Json,
    signal?: AbortSignal,
    upload?: Upload,
  ): Promise<Response> {
    let response: Response;
    try {
      response = await fetch(`${this.#base}${path}`, {
        method: body === undefined && !upload ? "GET" : "POST",
        headers: {
          Authorization: `Bearer ${this.#token}`,
          ...(this.#gateToken ? { Cookie: `_port_auth=${this.#gateToken}` } : {}),
          Accept: path.includes("/stream?")
            ? "text/event-stream"
            : "application/json",
          ...(body === undefined ? {} : { "Content-Type": "application/json" }),
          ...(upload ? { "Content-Type": "application/gzip", "Content-Length": String(upload.length) } : {}),
        },
        ...(upload ? { body: upload.body, duplex: "half" } : body === undefined ? {} : { body: JSON.stringify(body) }),
        redirect: "error",
        signal,
      });
    } catch {
      if (signal?.aborted)
        throw new DOMException(
          "Cloudroom request cancelled or timed out",
          "AbortError",
        );
      throw new CloudroomError(
        "Cloudroom is unreachable. For an unconfirmed command, retry the same request_id.",
      );
    }
    if (!response.ok) {
      await response.body?.cancel();
      const message =
        response.status === 401 || response.status === 403
          ? "Cloudroom authentication failed"
          : `Cloudroom rejected the request (HTTP ${response.status})`;
      throw new CloudroomError(message, response.status);
    }
    return response;
  }

  async #json(
    path: string,
    body?: Json,
    signal?: AbortSignal,
    upload?: Upload,
  ): Promise<Record<string, unknown>> {
    const timeout = AbortSignal.timeout(this.#timeoutMs);
    const response = await this.#request(
      path,
      body,
      signal ? AbortSignal.any([signal, timeout]) : timeout,
      upload,
    );
    if ((body !== undefined || upload) && response.status !== (upload ? 201 : 202)) {
      await response.body?.cancel();
      throw new CloudroomError(
        "Cloudroom did not acknowledge command acceptance",
      );
    }
    if (!response.headers.get("content-type")?.includes("application/json")) {
      await response.body?.cancel();
      throw new CloudroomError("Expected a Cloudroom JSON response");
    }
    const reader = response.body?.getReader();
    if (!reader) throw new CloudroomError("Empty Cloudroom response");
    const chunks: Uint8Array[] = [];
    let length = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        length += value.byteLength;
        if (length > MAX_RESPONSE_BYTES)
          throw new CloudroomError("Cloudroom response exceeds the size limit");
        chunks.push(value);
      }
      const bytes = new Uint8Array(length);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.length;
      }
      return object(JSON.parse(new TextDecoder().decode(bytes)));
    } catch (error) {
      if (error instanceof CloudroomError) throw error;
      throw new CloudroomError(
        "Incomplete or invalid Cloudroom response. Retry unconfirmed commands with the same request_id.",
      );
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  }

  async #command(
    path: string,
    command: string,
    body: { request_id: string } & Record<string, Json>,
    sessionId?: string,
  ): Promise<Acceptance> {
    requestId(body.request_id);
    const value = await this.#json(path, body);
    const accepted = {
      session_id: text(value.session_id),
      receipt: receipt(value.receipt),
      saving: json(value.saving),
    };
    if (
      (sessionId !== undefined && accepted.session_id !== sessionId) ||
      accepted.receipt.request_id !== body.request_id ||
      accepted.receipt.command !== command
    ) {
      throw new CloudroomError(
        "Cloudroom acceptance does not match the command",
      );
    }
    return accepted;
  }

  health(signal?: AbortSignal) {
    return this.#json("/v1/health", undefined, signal);
  }
  ready(signal?: AbortSignal) {
    return this.#json("/v1/ready", undefined, signal);
  }
  dashboard(signal?: AbortSignal) {
    return this.#json("/v1/dashboard", undefined, signal);
  }
  capabilities(signal?: AbortSignal) {
    return this.#json("/v1/capabilities", undefined, signal);
  }

  async workspace(id: string, signal?: AbortSignal): Promise<Workspace | null> {
    requestId(id);
    try {
      const value = await this.#json(`/v1/workspaces/${id}`, undefined, signal);
      if (value.id !== id) throw new CloudroomError("Cloudroom workspace mismatch");
      return { id, path: text(value.path) };
    } catch (error) {
      if (error instanceof CloudroomError && error.status === 404) return null;
      throw error;
    }
  }

  async importWorkspace(id: string, name: string, upload: Upload, signal?: AbortSignal): Promise<Workspace> {
    requestId(id);
    if (!Number.isSafeInteger(upload.length) || upload.length <= 0 || upload.length > 4 * 1024 ** 3) throw new CloudroomError("Project archive exceeds the 4 GiB limit");
    const value = await this.#json(`/v1/workspaces/${id}?name=${encodeURIComponent(name)}`, undefined, signal, upload);
    if (value.id !== id) throw new CloudroomError("Cloudroom workspace mismatch");
    return { id, path: text(value.path) };
  }

  start(id: string, harness: Harness = "codex", options: { model?: string; reasoning?: string; workspace?: string } = {}) {
    if (harness !== "codex" && harness !== "pi")
      throw new CloudroomError("Unsupported Cloudroom harness");
    return this.#command("/v1/sessions", "start", { request_id: id, harness, ...options });
  }

  prompt(sessionId: string, id: string, prompt: string) {
    if (!prompt.trim() || new TextEncoder().encode(prompt).length > 32768) {
      throw new CloudroomError("Prompt must contain 1–32768 bytes of text");
    }
    return this.#command(
      `${sessionPath(sessionId)}/prompts`,
      "prompt",
      { request_id: id, text: prompt },
      sessionId,
    );
  }

  interrupt(sessionId: string, id: string, targetRequestId: string) {
    return this.#command(
      `${sessionPath(sessionId)}/interrupt`,
      "interrupt",
      { request_id: id, target_request_id: requestId(targetRequestId) },
      sessionId,
    );
  }

  stop(sessionId: string, id: string) {
    return this.#command(`${sessionPath(sessionId)}/stop`, "stop", { request_id: id }, sessionId);
  }

  resume(sessionId: string, id: string) {
    return this.#command(`${sessionPath(sessionId)}/resume`, "resume", { request_id: id }, sessionId);
  }

  close(sessionId: string, id: string) {
    return this.#command(
      `${sessionPath(sessionId)}/close`,
      "close",
      { request_id: id },
      sessionId,
    );
  }

  async session(sessionId: string, signal?: AbortSignal): Promise<Session> {
    const value = object(
      (await this.#json(sessionPath(sessionId), undefined, signal)).session,
    );
    if (
      value.session_id !== sessionId ||
      (value.harness !== "codex" && value.harness !== "pi")
    ) {
      throw new CloudroomError(
        "Cloudroom session mismatch or unsupported harness",
      );
    }
    if (!Array.isArray(value.queue))
      throw new CloudroomError("Invalid Cloudroom queue");
    return {
      session_id: sessionId,
      harness: value.harness,
      state: text(value.state),
      native_id: value.native_id === null ? null : text(value.native_id),
      current_request:
        value.current_request === null ? null : text(value.current_request),
      last_sequence: sequence(value.last_sequence),
      queue: value.queue.map(text),
      receipts: Object.fromEntries(
        Object.entries(object(value.receipts)).map(([id, item]) => [
          id,
          receipt(item),
        ]),
      ),
    };
  }

  async events(
    sessionId: string,
    after = 0,
    signal?: AbortSignal,
  ): Promise<SessionRecord[]> {
    let cursor = sequence(after);
    const timeout = AbortSignal.timeout(this.#timeoutMs);
    const replaySignal = signal ? AbortSignal.any([signal, timeout]) : timeout;
    const lastSequence = (await this.session(sessionId, replaySignal))
      .last_sequence;
    if (cursor >= lastSequence) return [];
    const records: SessionRecord[] = [];
    for await (const item of this.#stream(sessionId, {
      after,
      signal: replaySignal,
    })) {
      if (item.sequence <= cursor)
        throw new CloudroomError("Cloudroom history is out of order");
      if (item.sequence > lastSequence) break;
      records.push(item);
      cursor = item.sequence;
      if (cursor === lastSequence) return records;
    }
    throw new CloudroomError("Incomplete Cloudroom history");
  }

  async *stream(
    sessionId: string,
    options: { after?: number; signal: AbortSignal },
  ): AsyncGenerator<SessionRecord> {
    let cursor = sequence(options.after ?? 0);
    for await (const item of this.#stream(sessionId, options)) {
      if (item.sequence > cursor) {
        cursor = item.sequence;
        yield item;
      }
    }
  }

  async *#stream(
    sessionId: string,
    options: { after?: number; signal: AbortSignal },
  ): AsyncGenerator<SessionRecord> {
    const cursor = sequence(options.after ?? 0);
    const response = await this.#request(
      `${sessionPath(sessionId)}/stream?after=${cursor}`,
      undefined,
      options.signal,
    );
    if (!response.headers.get("content-type")?.includes("text/event-stream")) {
      await response.body?.cancel();
      throw new CloudroomError("Expected a Cloudroom event stream");
    }
    const reader = response.body?.getReader();
    if (!reader) throw new CloudroomError("Empty Cloudroom stream");
    const decoder = new TextDecoder();
    let buffer = "";
    let data: string[] = [];
    let event = "";
    let id = "";
    let frameSize = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) return;
        buffer += decoder.decode(value, { stream: true });
        if (buffer.length + frameSize > MAX_RESPONSE_BYTES)
          throw new CloudroomError("Cloudroom event exceeds the size limit");
        let newline: number;
        while ((newline = buffer.indexOf("\n")) !== -1) {
          const line = buffer.slice(0, newline).replace(/\r$/, "");
          buffer = buffer.slice(newline + 1);
          if (line === "") {
            if (event === "record" && data.length > 0) {
              let parsed: unknown;
              try {
                parsed = JSON.parse(data.join("\n"));
              } catch {
                throw new CloudroomError("Invalid Cloudroom event JSON");
              }
              const item = record(parsed, sessionId);
              if (id !== String(item.sequence))
                throw new CloudroomError("Cloudroom event cursor mismatch");
              yield item;
            }
            data = [];
            event = "";
            id = "";
            frameSize = 0;
          } else if (!line.startsWith(":")) {
            frameSize += line.length;
            if (frameSize > MAX_RESPONSE_BYTES)
              throw new CloudroomError(
                "Cloudroom event exceeds the size limit",
              );
            const separator = line.indexOf(":");
            const field = separator === -1 ? line : line.slice(0, separator);
            const content =
              separator === -1
                ? ""
                : line.slice(separator + 1).replace(/^ /, "");
            if (field === "data") data.push(content);
            if (field === "event") event = content;
            if (field === "id") id = content;
          }
        }
      }
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  }
}
