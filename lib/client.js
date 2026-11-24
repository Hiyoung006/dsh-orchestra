window.__ModuleLoader__.load({
	id: "dsh-orchestra",
	factory: (require) => {
		var module = { exports: {} };
		var exports = module.exports;
		Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
		let react = require("react");
		let react_jsx_runtime = require("react/jsx-runtime");
		// src/client/api/client.ts
		var ApiError = class extends Error {
			code;
			retryable;
			details;
			constructor(code, message, retryable, details) {
				super(message);
				this.name = "ApiError";
				this.code = code;
				this.retryable = retryable;
				this.details = details;
			}
		};
		async function unwrap(res) {
			let body;
			try {
				body = await res.json();
			} catch {
				throw new ApiError("SERVICE_UNAVAILABLE", "服务返回内容无法读取", true, { status: res.status });
			}
			if (body.schema_version !== 1) throw new ApiError("PROTOCOL_VERSION_MISMATCH", "服务与面板协议版本不一致", false, { schema_version: body.schema_version });
			if (!body.ok) throw new ApiError(body.error?.code ?? "SERVICE_UNAVAILABLE", body.error?.message ?? "服务没回应", body.error?.retryable ?? true, body.error?.details);
			if (typeof body.as_of_seq !== "number" || !Number.isSafeInteger(body.as_of_seq) || body.as_of_seq < 0) throw new ApiError("PROTOCOL_VERSION_MISMATCH", "服务返回的事件位置无效", false);
			return {
				data: body.data,
				as_of_seq: body.as_of_seq
			};
		}
		function createApiClient(baseUrl, fetcher = globalThis.fetch.bind(globalThis)) {
			const urlFor = (path, query) => {
				const url = new URL(path.replace(/^\//, ""), baseUrl.endsWith("/") ? baseUrl : baseUrl + "/");
				for (const [key, value] of Object.entries(query ?? {})) if (value !== void 0) url.searchParams.set(key, String(value));
				return url.toString();
			};
			const send = async (path, init, query) => {
				init.signal?.throwIfAborted();
				let response;
				try {
					response = await fetcher(urlFor(path, query), init);
				} catch (error) {
					init.signal?.throwIfAborted();
					const timedOut = error instanceof DOMException && (error.name === "AbortError" || error.name === "TimeoutError");
					const commandOutcomeUnknown = init.method === "POST";
					throw new ApiError(timedOut ? "OPERATION_TIMEOUT" : commandOutcomeUnknown ? "OPERATION_OUTCOME_UNKNOWN" : "SERVICE_UNAVAILABLE", timedOut || commandOutcomeUnknown ? "请求结果暂不可确认" : "服务未连接", true);
				}
				const result = await unwrap(response);
				init.signal?.throwIfAborted();
				return result;
			};
			return {
				get: (path, query) => send(path, {
					method: "GET",
					headers: { accept: "application/json" }
				}, query),
				post: (path, payload, signal) => send(path, {
					method: "POST",
					headers: {
						accept: "application/json",
						"content-type": "application/json"
					},
					body: JSON.stringify(payload),
					...signal ? { signal } : {}
				})
			};
		}
		// src/client/api/commands.ts
		/** For inbox-backed commands, look up the original operation if the response was lost. */
		async function postWithOperationReceipt(client, path, kind, body) {
			try {
				await client.post(path, body);
			} catch (error) {
				if (!(error instanceof ApiError) || ![
					"OPERATION_TIMEOUT",
					"OPERATION_OUTCOME_UNKNOWN",
					"SERVICE_UNAVAILABLE"
				].includes(error.code)) throw error;
				try {
					const { data } = await client.get("/v1/operations/get", { operation_id: body.operation_id });
					if (data.status === "applied" && data.kind === kind) return;
				} catch {}
				throw new ApiError("OPERATION_OUTCOME_UNKNOWN", "操作结果暂不可确认，请刷新查看回执", true, { operation_id: body.operation_id });
			}
		}
		/** Ambiguous command transport failures are resolved through the receipt; never blindly resubmit here. */
		async function submitThenReceipt(post, getOperation, operationId) {
			try {
				return {
					replayed: false,
					receipt: (await unwrap(await post(operationId))).data
				};
			} catch (error) {
				if (!(error instanceof ApiError) || ![
					"OPERATION_TIMEOUT",
					"OPERATION_OUTCOME_UNKNOWN",
					"SERVICE_UNAVAILABLE"
				].includes(error.code)) throw error;
				try {
					return {
						replayed: true,
						receipt: (await unwrap(await getOperation(operationId))).data
					};
				} catch (receiptError) {
					if (!(receiptError instanceof ApiError) || [
						"NOT_FOUND",
						"SERVICE_UNAVAILABLE",
						"OPERATION_TIMEOUT",
						"OPERATION_OUTCOME_UNKNOWN"
					].includes(receiptError.code)) throw new ApiError("OPERATION_OUTCOME_UNKNOWN", "操作结果暂不可确认；请稍后查询回执，不要重新提交", true, {
						operation_id: operationId,
						receipt_error: receiptError instanceof ApiError ? receiptError.code : "NETWORK_ERROR"
					});
					throw receiptError;
				}
			}
		}
		// src/client/api/sse.ts
		const SEEN_LIMIT = 5e3;
		/** SSE is only an invalidation channel; callers refresh authoritative views separately. */
		var EventStream = class {
			options;
			seen = /* @__PURE__ */ new Set();
			lastSeq = 0;
			stopSubscription = null;
			pollTimer = null;
			coalesceTimer = null;
			reconnectTimer = null;
			started = false;
			awaitingSnapshot = false;
			transportGeneration = 0;
			reading = false;
			pendingTypes = /* @__PURE__ */ new Set();
			constructor(options) {
				this.options = options;
			}
			start(asOfSeq) {
				this.stop();
				this.started = true;
				this.lastSeq = asOfSeq;
				this.awaitingSnapshot = false;
				this.openStream();
			}
			stop() {
				this.started = false;
				this.awaitingSnapshot = false;
				this.closeTransport();
				this.clearInvalidations();
				this.seen.clear();
			}
			clearInvalidations() {
				if (this.coalesceTimer !== null) clearTimeout(this.coalesceTimer);
				this.coalesceTimer = null;
				this.pendingTypes.clear();
			}
			/** Accept a refreshed snapshot only after the stream requested a resync. */
			setSnapshot(asOfSeq) {
				if (!this.started || !this.awaitingSnapshot || !Number.isSafeInteger(asOfSeq) || asOfSeq < 0) return;
				this.lastSeq = asOfSeq;
				this.awaitingSnapshot = false;
				this.seen.clear();
				this.openStream();
			}
			get lastEventSeq() {
				return this.lastSeq;
			}
			closeTransport() {
				this.transportGeneration += 1;
				this.stopSubscription?.();
				this.stopSubscription = null;
				if (this.pollTimer !== null) clearInterval(this.pollTimer);
				if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
				this.pollTimer = null;
				this.reconnectTimer = null;
				this.reading = false;
			}
			openStream() {
				if (!this.started || this.awaitingSnapshot) return;
				const generation = this.transportGeneration;
				const active = () => generation === this.transportGeneration;
				try {
					this.options.onStatus("stale");
					if (!active()) return;
					const unsubscribe = this.options.subscribe(this.lastSeq, (event) => {
						if (active()) this.accept(event);
					}, () => {
						if (active()) this.requireSnapshot();
					}, () => {
						if (active()) this.fallbackToPolling();
					}, () => {
						if (active()) this.options.onStatus("live");
					});
					if (active()) this.stopSubscription = unsubscribe;
					else unsubscribe();
				} catch {
					if (active()) this.fallbackToPolling();
				}
			}
			accept(event) {
				if (!this.started || this.awaitingSnapshot) return;
				if (this.seen.has(event.event_id)) return;
				if (!Number.isSafeInteger(event.seq) || event.seq !== this.lastSeq + 1) {
					this.requireSnapshot();
					return;
				}
				this.seen.add(event.event_id);
				if (this.seen.size > SEEN_LIMIT) {
					const oldest = this.seen.values().next().value;
					if (oldest !== void 0) this.seen.delete(oldest);
				}
				this.lastSeq = event.seq;
				this.pendingTypes.add(event.type);
				if (this.coalesceTimer === null) this.coalesceTimer = setTimeout(() => {
					this.coalesceTimer = null;
					for (const type of this.pendingTypes) this.options.onInvalidate(type);
					this.pendingTypes.clear();
				}, this.options.coalesceMs ?? 100);
			}
			requireSnapshot() {
				if (!this.started || this.awaitingSnapshot) return;
				this.awaitingSnapshot = true;
				this.closeTransport();
				this.clearInvalidations();
				this.options.onStatus("stale");
				this.options.onInvalidate("resync_required");
			}
			fallbackToPolling() {
				if (!this.started || this.awaitingSnapshot || this.pollTimer !== null) return;
				this.closeTransport();
				const generation = this.transportGeneration;
				this.options.onStatus("stale");
				if (generation !== this.transportGeneration) return;
				this.pollTimer = setInterval(() => {
					this.pollOnce(generation);
				}, this.options.pollIntervalMs ?? 5e3);
				this.reconnectTimer = setTimeout(() => {
					if (generation !== this.transportGeneration) return;
					this.closeTransport();
					this.openStream();
				}, 3e4);
				this.pollOnce(generation);
			}
			async pollOnce(generation) {
				if (generation !== this.transportGeneration || this.reading) return;
				this.reading = true;
				try {
					const events = await this.options.readAfter(this.lastSeq, 200);
					for (const event of events) {
						if (generation !== this.transportGeneration) return;
						this.accept(event);
					}
					if (generation === this.transportGeneration) this.options.onStatus("polling");
				} catch {
					if (generation === this.transportGeneration) this.options.onStatus("stale");
				} finally {
					if (generation === this.transportGeneration) this.reading = false;
				}
			}
		};
		// src/client/api/sse-fetch.ts
		/** Fetch based SSE transport for hosts that inject authenticated fetch without exposing a secret to the panel. */
		function createSseSubscribe(fetcher, baseUrl, requestInit = {}) {
			return (afterSeq, onEvent, onResyncRequired, onError, onOpen) => {
				const controller = new AbortController();
				const url = new URL("v1/events/stream", baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`);
				url.searchParams.set("after_seq", String(afterSeq));
				pump(url, fetcher, requestInit, controller.signal, onEvent, onResyncRequired, onError, onOpen);
				return () => controller.abort();
			};
		}
		async function pump(url, fetcher, requestInit, signal, onEvent, onResyncRequired, onError, onOpen) {
			try {
				const response = await fetcher(url, {
					...requestInit,
					method: "GET",
					signal,
					headers: {
						...Object.fromEntries(new Headers(requestInit.headers)),
						accept: "text/event-stream"
					}
				});
				if (!response.ok || !response.body || !response.headers.get("content-type")?.includes("text/event-stream")) {
					onError();
					return;
				}
				onOpen();
				const reader = response.body.getReader();
				const decoder = new TextDecoder();
				let buffer = "";
				try {
					while (!signal.aborted) {
						const { value, done } = await reader.read();
						if (done) {
							if (!signal.aborted) onError();
							return;
						}
						buffer += decoder.decode(value, { stream: true });
						buffer = buffer.replace(/\r\n/g, "\n");
						if (buffer.length > 65536) {
							onResyncRequired();
							await reader.cancel();
							return;
						}
						let boundary;
						while ((boundary = buffer.indexOf("\n\n")) >= 0) {
							const frame = buffer.slice(0, boundary).replace(/\r/g, "");
							buffer = buffer.slice(boundary + 2);
							if (!dispatchFrame(frame, onEvent, onResyncRequired)) {
								await reader.cancel();
								return;
							}
						}
					}
				} finally {
					if (signal.aborted) await reader.cancel().catch(() => void 0);
					reader.releaseLock();
				}
			} catch {
				if (!signal.aborted) onError();
			}
		}
		/** Returns false when the server explicitly requires a snapshot resync. */
		function dispatchFrame(frame, onEvent, onResyncRequired) {
			let eventName = "message";
			const data = [];
			for (const line of frame.split("\n")) {
				if (!line || line.startsWith(":")) continue;
				const colon = line.indexOf(":");
				const field = colon < 0 ? line : line.slice(0, colon);
				const value = colon < 0 ? "" : line.slice(colon + 1).replace(/^ /, "");
				if (field === "event") eventName = value;
				else if (field === "data") data.push(value);
			}
			if (eventName === "resync_required") {
				onResyncRequired();
				return false;
			}
			if (eventName !== "invalidation" || data.length === 0) return true;
			try {
				const value = JSON.parse(data.join("\n"));
				if (Number.isSafeInteger(value.seq) && typeof value.event_id === "string" && typeof value.type === "string") onEvent({
					seq: value.seq,
					event_id: value.event_id,
					type: value.type
				});
				else {
					onResyncRequired();
					return false;
				}
			} catch {
				onResyncRequired();
				return false;
			}
			return true;
		}
		// src/client/api/event-stream.ts
		/** Connects invalidation events to authoritative list_events reads via the injected API client and host fetch. */
		function createConnectedEventStream(options) {
			return new EventStream({
				subscribe: createSseSubscribe(options.fetcher, options.baseUrl, options.requestInit),
				readAfter: async (afterSeq, limit) => {
					return (await options.client.get("/v1/events", {
						from_seq: afterSeq,
						limit
					})).data.items;
				},
				onInvalidate: options.onInvalidate,
				onStatus: options.onStatus,
				pollIntervalMs: options.pollIntervalMs,
				coalesceMs: options.coalesceMs
			});
		}
		// src/client/api/resources.ts
		const compact = (params) => params;
		/** Typed read-only API surface used by the panel; transport and envelope handling stay in ApiClient. */
		function createResourcesApi(client) {
			return {
				getReworkTopology: (run_id) => client.get("/v1/rework/topology", { run_id }),
				listRework: (run_id) => client.get("/v1/rework", { run_id }),
				getRework: (request_id) => client.get("/v1/rework/get", { request_id }),
				getWorkspaceOverview: () => client.get("/v1/workspace/overview"),
				listTasks: (params = {}) => client.get("/v1/tasks", compact(params)),
				getTask: (task_id) => client.get("/v1/tasks/get", { task_id }),
				getTaskTimeline: (task_id, limit = 200) => client.get("/v1/tasks/timeline", { task_id, limit }),
				listTaskAttempts: (task_id, limit = 100) => client.get("/v1/tasks/attempts", { task_id, limit }),
				getTaskAcceptance: (task_id) => client.get("/v1/tasks/acceptance", { task_id }),
				getTaskWorkspace: (task_id) => client.get("/v1/tasks/workspace", { task_id }),
				getWorkspaceDiff: (workspace_id) => client.get("/v1/workspaces/diff", { workspace_id }),
				getTaskPlanCapability: () => client.get("/v1/task-plans/capability"),
				getTaskPlan: (plan_id) => client.get("/v1/task-plans/get", { plan_id }),
				getTaskPlanForTask: (task_id) => client.get("/v1/task-plans/by-task", { task_id }),
				listTaskTriggers: (params = {}) => client.get("/v1/task-triggers", compact(params)),
				listRuns: (params = {}) => client.get("/v1/runs", compact(params)),
				getRun: (run_id) => client.get("/v1/runs/get", { run_id }),
				listNodes: (run_id, limit) => client.get("/v1/node-runs", {
					run_id,
					limit
				}),
				getNode: (node_run_id) => client.get("/v1/node-runs/get", { node_run_id }),
				listDecisions: (params = {}) => client.get("/v1/decisions", compact(params)),
				listMemoryBatches: (params = {}) => client.get("/v1/memory/batches", compact(params)),
				getDecision: (decision_id) => client.get("/v1/decisions/get", { decision_id }),
				listUnverifiedEffects: (params = {}) => client.get("/v1/effects/unverified", compact(params)),
				getEffectReview: (node_run_id) => client.get("/v1/effects/review", { node_run_id }),
				getEvidence: (evidence_id) => client.get("/v1/evidence/get", { evidence_id }),
				getArtifact: (artifact_id) => client.get("/v1/artifacts/get", { artifact_id }),
				getArtifactContent: (run_id, artifact_id, mode) => client.get("/v1/artifacts/content", {
					run_id,
					artifact_id,
					mode
				}),
				listRunArtifacts: (run_id) => client.get("/v1/artifacts", { run_id }),
				getAttestation: (attestation_id) => client.get("/v1/attestations/get", { attestation_id }),
				verifyLedger: () => client.get("/v1/ledger/verify"),
				listEvents: (params = {}) => client.get("/v1/events", compact(params)),
				listActivity: (params = {}) => client.get("/v1/activity", compact(params)),
				listExecutionHistory: (params) => client.get("/v1/execution-history", compact(params)),
				listWorkflows: (params = {}) => client.get("/v1/workflows", compact(params)),
				getWorkflow: (workflow_id) => client.get("/v1/workflows/get", { workflow_id }),
				getWorkflowVersion: (workflow_id, version) => client.get("/v1/workflows/versions/get", {
					workflow_id,
					version
				}),
				listWorkflowTemplates: () => client.get("/v1/workflow-templates"),
				getWorkflowTemplate: (template_id) => client.get("/v1/workflow-templates/get", { template_id }),
				getOperation: (operation_id) => client.get("/v1/operations/get", { operation_id })
			};
		}
		// src/client/api/workflow-commands.ts
		const AMBIGUOUS_CODES$1 = /* @__PURE__ */ new Set([
			"OPERATION_TIMEOUT",
			"OPERATION_OUTCOME_UNKNOWN",
			"SERVICE_UNAVAILABLE"
		]);
		const HEX = "0123456789abcdef";
		function randomBytes(length) {
			const bytes = new Uint8Array(length);
			const source = globalThis.crypto;
			if (source && typeof source.getRandomValues === "function") {
				source.getRandomValues(bytes);
				return bytes;
			}
			for (let index = 0; index < bytes.length; index += 1) bytes[index] = Math.floor(Math.random() * 256);
			return bytes;
		}
		/** 客户端自造 UUIDv7：服务端幂等层只认 v7 形状（小写、版本位 7、变体位 8-b）。 */
		function newOperationId(now = Date.now()) {
			const bytes = randomBytes(16);
			let timestamp = now;
			for (let index = 5; index >= 0; index -= 1) {
				bytes[index] = timestamp % 256;
				timestamp = Math.floor(timestamp / 256);
			}
			bytes[6] = 112 | bytes[6] & 15;
			bytes[8] = 128 | bytes[8] & 63;
			let hex = "";
			for (let index = 0; index < 16; index += 1) {
				hex += HEX.charAt(bytes[index] >> 4 & 15) + HEX.charAt(bytes[index] & 15);
				if (index === 3 || index === 5 || index === 7 || index === 9) hex += "-";
			}
			return hex;
		}
		function isAmbiguous(error) {
			return error instanceof ApiError && AMBIGUOUS_CODES$1.has(error.code);
		}
		function receiptResult$1(row) {
			if (!row.receipt_json) return null;
			try {
				const parsed = JSON.parse(row.receipt_json);
				if (parsed && typeof parsed === "object" && "result" in parsed) return parsed.result;
				return parsed;
			} catch {
				return null;
			}
		}
		async function fetchReceipt(client, operationId) {
			try {
				return (await client.get("/v1/operations/get", { operation_id: operationId })).data;
			} catch {
				return null;
			}
		}
		/** 结果不可确认时按同一 operation_id 查回执；查不到就报未知，绝不换 id 重发。 */
		async function recoverResult$1(client, operationId) {
			try {
				const row = await client.get("/v1/operations/get", { operation_id: operationId });
				if (row.data.status !== "applied") return null;
				return receiptResult$1(row.data);
			} catch {
				return null;
			}
		}
		async function submitCommand(client, kind, path, body) {
			const operationId = newOperationId();
			let result;
			let replayed = false;
			try {
				const posted = await client.post(path, {
					...body,
					operation_id: operationId
				});
				result = posted.data;
				replayed = posted.data?.replayed === true;
			} catch (error) {
				if (!isAmbiguous(error)) throw error;
				const recovered = await recoverResult$1(client, operationId);
				if (recovered === null) throw new ApiError("OPERATION_OUTCOME_UNKNOWN", "操作结果暂不可确认；请稍后查询回执，不要重新提交", true, { operation_id: operationId });
				result = recovered;
				replayed = true;
			}
			const receipt = await fetchReceipt(client, operationId);
			return {
				operation_id: operationId,
				kind,
				replayed,
				result,
				receipt
			};
		}
		function createWorkflowCommandsApi(client) {
			return {
				setWorkflowStatus: (input) => submitCommand(client, "workflow.set_status", "/v1/workflows/status", input),
				testWorkflow: async (workflow_id, revision) => (await client.post("/v1/workflows/test", {
					workflow_id,
					revision
				})).data,
				getWorkflowTest: async (workflow_id) => (await client.get("/v1/workflows/test", { workflow_id })).data,
				canGenerateWorkflow: async () => (await client.get("/v1/workflows/generate/capability")).data.enabled,
				generateWorkflow: async (goal) => (await client.post("/v1/workflows/generate", { goal })).data.definition,
				compileWorkflowTemplate: async (input) => (await client.post("/v1/workflow-templates/compile", input)).data,
				validateWorkflowTemplate: async (input) => (await client.post("/v1/workflow-templates/validate", input)).data,
				createWorkflow: (input) => submitCommand(client, "workflow.create", "/v1/workflows", {
					workflow_id: input.workflow_id,
					name: input.name,
					definition: input.definition,
					...input.scope ? { scope: input.scope } : {}
				}),
				openWorkflowDraft: (input) => submitCommand(client, "workflow.open_draft", "/v1/workflows/open-draft", {
					workflow_id: input.workflow_id,
					base_version: input.base_version
				}),
				updateWorkflowDraft: (input) => submitCommand(client, "workflow.update", "/v1/workflows/update", {
					workflow_id: input.workflow_id,
					revision: input.revision,
					definition: input.definition
				}),
				publishWorkflow: (input) => submitCommand(client, "workflow.publish", "/v1/workflows/publish", {
					workflow_id: input.workflow_id,
					revision: input.revision,
					base_version: input.base_version,
					publish_note: input.publish_note
				}),
				getOperation: async (operationId) => (await client.get("/v1/operations/get", { operation_id: operationId })).data
			};
		}
		// src/client/api/memory-recovery.ts
		function createMemoryRecoveryApi(client) {
			return {
				async inspect(batchId) {
					return (await client.post("/v1/memory/recovery/inspect", { batch_id: batchId })).data;
				},
				async resolve(proof, resolution) {
					const { batch_id, decision_id, revision, stopped_at, evidence_id } = proof;
					const operationId = newOperationId();
					try {
						await client.post("/v1/memory/recovery/resolve", {
							batch_id,
							decision_id,
							revision,
							stopped_at,
							evidence_id,
							resolution,
							operation_id: operationId
						});
					} catch (error) {
						if (!(error instanceof ApiError) || ![
							"OPERATION_TIMEOUT",
							"OPERATION_OUTCOME_UNKNOWN",
							"SERVICE_UNAVAILABLE"
						].includes(error.code)) throw error;
						try {
							if ((await client.get("/v1/operations/get", { operation_id: operationId })).data.status === "applied") return;
						} catch {}
						throw new ApiError("OPERATION_OUTCOME_UNKNOWN", "恢复结果暂不可确认，请刷新查看", true, { operation_id: operationId });
					}
				}
			};
		}
		// src/core/operator-language.ts
		/** Labels for human-facing execution and approval messages. Keep protocol values in the ledger. */
		const STATUS = {
			reserved: "已接收",
			queued: "排队中",
			waiting_capacity: "等待执行位",
			waiting_dependencies: "等待上游完成",
			ready: "等待派发",
			queued_unknown: "排队原因未确认",
			leased: "已分配执行位，等待接手",
			dispatched: "已派发，等待启动",
			started: "执行中",
			running: "执行中",
			verifying: "核验中",
			waiting: "等待条件满足",
			paused: "已暂停",
			settling: "正在收尾",
			timing_out: "已超时，正在确认是否停止",
			stopping: "正在确认是否停止",
			startup_unknown: "启动情况待确认",
			paused_bridge_missing: "执行连接已断开，跟踪暂停",
			blocked: "执行受阻",
			blocked_pre: "执行前受阻，等待处理",
			blocked_post: "执行后受阻，等待处理",
			indeterminate_side_effect: "外部操作待核验",
			done: "执行完成",
			succeeded: "执行完成",
			succeeded_with_unverified: "执行完成，外部操作待核验",
			failed: "执行失败",
			failed_with_unverified: "执行失败，外部操作待核验",
			abandoned_unverified: "已停止跟踪，外部操作待核验",
			orphaned: "等待接手",
			superseded: "已由新的执行尝试接替",
			cancelled: "已取消",
			timeout: "已超时",
			skipped: "已跳过",
			pending: "待处理",
			approval_pending: "待审批",
			acknowledged: "已接手",
			blocked_for_human: "等待人工处理",
			resolved: "已处理",
			approved: "已批准",
			rejected: "未批准",
			expired: "已到期",
			passed: "验收通过",
			unverifiable: "交付待核验",
			not_applicable: "无需验收"
		};
		const REASON = {
			evidence_missing: "现有记录缺少完成核验所需的执行证据。",
			model_assignment_unavailable: "暂时没有可用于这一步的模型。请在模型设置中核对可用模型和这一步要求的能力。",
			model_assignment_unconfirmed: "执行端尚未确认使用了指定模型。请在步骤详情中核对实际使用的模型。",
			model_assignment_requeued: "主脑已为这一步重新分配模型，正在等待执行。",
			evidence_unverifiable: "现有记录无法核实这一步的执行结果。",
			evidence_rejected: "这次提交的执行证据未通过校验。",
			verification_too_weak: "现有证据不足以确认这一步的执行结果。",
			tool_execution_failed: "记录显示，这次工具执行未成功。",
			verdict_unmet: "现有证据未能满足已登记的判定条件。",
			verdict_policy_unmet: "当前计划或结果未通过要求的检查。",
			verdict_unsupported: "当前环境暂无法按所需条件进行判断。",
			artifact_not_registered: "没有找到验收要求的成果。",
			artifact_source_node_unbound: "成果缺少可核验的步骤来源。",
			artifact_source_node_mismatch: "成果来源与验收规则指定的步骤不符。",
			artifact_below_minimum_bytes: "成果大小未达到验收规则的最低要求。",
			artifact_retain_until_expired: "成果在验收时已超过保留期限。",
			tool_evidence_missing: "没有找到验收指定步骤的工具执行记录。",
			tool_exit_code_mismatch: "工具的实际退出结果与验收规则要求不符。",
			proposal_json_not_object: "现有计划记录无法用于验收。",
			proposal_json_unparsable: "现有计划记录无法用于验收。",
			deliverables_missing: "现有计划记录未列出要验收的交付物。",
			deliverable_not_object: "有一项交付物的验收记录不完整。",
			deliverable_title_missing: "有一项交付物缺少名称。",
			deliverable_node_id_missing: "有一项交付物缺少负责的步骤。",
			acceptance_kind_unknown: "这项交付物的验收方式无法识别。",
			acceptance_file_name_missing: "验收规则缺少要求核对的成果文件名。",
			acceptance_minimum_bytes_missing: "验收规则缺少成果大小的最低要求。",
			acceptance_tool_missing: "验收规则缺少要求核对的工具。",
			pre_execution: "执行前需要审批。",
			verify_human: "这一步的执行结果需要人工核验。",
			abandon_ack: "请确认是否停止跟踪这项操作。",
			audit_result: "核验结果等待审批。",
			action_overrun: "操作超过等待时限，当前结果需要核对。",
			startup_unknown: "现有记录无法确认这一步是否已启动。",
			report_contradicted: "这一步的回报与现有记录不一致，需要人工核对。",
			decision_required: "这一步需要人工确认后才能继续。",
			rule_ambiguous: "同时匹配到多条规则，需要确认应走哪一条。",
			ambiguous: "同时匹配到多条规则，需要确认应走哪一条。",
			no_match: "没有找到符合当前条件的规则。",
			attempt_mismatch: "这次回报对应的执行尝试已不是当前版本。",
			status_mismatch: "这次回报与步骤当前状态不符。",
			illegal_transition: "这一步在当前状态下无法接受此次更新。",
			control_node_report_not_allowed: "此类步骤需要使用编排服务记录的执行结果。",
			already_applied: "这项更新已记录。",
			already_replayed: "这次回报此前已处理。",
			replay_stale_attempt: "补报所属的执行尝试已失效。",
			replay_chain_broken: "现有记录无法验证这次补报的来源。",
			replay_not_head: "已有更新的执行记录，不能应用这次补报。",
			replay_after_reject: "这次回报已被拒绝，无法直接补报。",
			uphold_decision: "已保留原有审批结论。",
			uphold_report: "已采纳这次回报。",
			report_applied: "这次回报已记录。",
			boot_changed: "服务已重启，原执行时限待核对。",
			clock_drift: "计时发生偏差，原执行时限待核对。",
			checkpoint_missing: "缺少可信的计时记录，需先核对当前执行状态。",
			startup_timeout: "启动时限待核对。",
			action_deadline: "执行时限待核对。",
			unknown_watchdog: "跟踪时限待核对。",
			verify_timeout: "核验时限待核对。",
			timing_out_grace: "确认停止的时限待核对。",
			fence_armed: "该次执行已被限制继续提交结果。",
			fence_already_armed: "该次执行已被限制继续提交结果。",
			fence_pending: "正在限制该次执行继续提交结果。",
			timeout_escalated: "已到期，需要人工处理。",
			expired_final: "已到期并结束。",
			grace_expired: "等待停止确认的时限已过。",
			cancelled: "运行已取消。",
			run_cancelled: "运行已取消。",
			cancelled_before_dispatch: "步骤派发前已取消。",
			control_node_cancelled: "等待或流程控制步骤已取消。",
			cancel_confirmed: "已确认停止。",
			not_interruptible: "当前操作无法直接中止。",
			already_stopped: "这项操作此前已停止。",
			no_executable_compensation: "当前没有可执行的补救操作。",
			no_node_spec: "缺少步骤执行配置。",
			wait_registered: "已登记要等待的条件。",
			task_archived: "任务已归档。",
			unauthorized: "无法确认当前身份，请重新连接后再试。",
			forbidden: "当前权限不允许执行这项操作。",
			not_found: "没有找到这项操作对应的记录。",
			protocol_version_mismatch: "服务版本不匹配，请重启 DSH 后再试。",
			schema_version_unsupported: "当前版本无法读取这项配置或记录。",
			cursor_expired: "页面所依据的查询已到期，请刷新后再试。",
			cursor_filter_mismatch: "查询条件已变化，请刷新后再试。",
			range_too_large: "这次提交或查询的范围超过限制。",
			query_timeout: "查询已超时，请稍后重试。",
			service_unavailable: "服务暂时无法处理这项操作。",
			operation_id_conflict: "同一操作的请求内容已变化，请核对后再试。",
			operation_expired: "这项请求已到期，请重新查看当前状态。",
			operation_timeout: "等待操作结果超时，请查询回执确认。",
			operation_outcome_unknown: "操作结果暂待确认，请查询原请求的回执。",
			command_busy: "当前有操作正在处理，请稍后查看。",
			confirmation_required: "需要人工确认后才能继续。",
			confirmation_mismatch: "当前材料与确认时的内容不符，请重新核对。",
			confirmation_expired: "本次确认已到期，请重新核对后确认。",
			decision_revision_conflict: "审批内容已更新，请重新查看。",
			resolution_not_available: "当前状态下无法执行这项处理。",
			stale_generation: "状态已更新，请刷新后再试。",
			stale_hang: "导致这一步暂停的情况已变化，请刷新后再试。",
			tool_not_registered: "当前环境未配置这一步需要的工具。",
			tool_not_allowed: "当前授权不允许使用这一步需要的工具。",
			run_tool_not_read_only: "这项工具操作超出当前步骤的只读权限。",
			side_effect_undeclared: "这一步涉及未登记的外部操作。",
			compensation_unexecutable: "当前没有可执行的补救操作。",
			compensation_tool_not_compensating: "所选工具无法执行已登记的补救操作。",
			compensation_tool_in_forward_path: "补救工具只能用于补救操作。",
			verifier_tool_policy_mutating: "核验步骤只能使用只读工具。",
			evidence_source_unavailable: "当前缺少可用于核验的记录来源。",
			effect_resolution_conflict: "这项外部操作已有核验结果，请刷新后核对。",
			dispatch_settle_failed: "暂未确认派发后的执行状态，请先查询这一步的记录。",
			lease_expired: "本次执行的授权时限已过。",
			bridge_not_subscribed: "当前没有可用的执行连接。",
			bridge_ack_lag: "执行连接尚未确认收到最新记录。",
			bridge_seq_conflict: "执行连接返回了不一致的记录，请核对当前状态。",
			payload_schema_invalid: "提交的内容不符合这项操作的要求。",
			workflow_not_found: "没有找到对应的流程。",
			workflow_version_not_found: "没有找到对应的流程版本。",
			workflow_revision_conflict: "流程内容已更新，请重新查看。",
			workflow_version_conflict: "流程版本已变化，请核对后再试。",
			workflow_version_immutable: "当前操作要求使用已固定的流程版本。",
			workflow_draft_not_open: "当前没有可编辑的流程草稿。",
			workflow_draft_exists: "已有待处理的流程草稿。",
			workflow_not_published: "请先选择一个已发布的流程版本。",
			workflow_validation_failed: "流程未通过检查，请核对步骤与依赖。",
			workflow_title_invalid: "请检查流程标题是否符合要求。",
			workflow_name_invalid: "请检查流程名称是否符合要求。",
			workflow_definition_invalid: "流程定义未通过检查，请核对步骤与依赖。",
			workflow_definition_too_large: "流程定义超过大小限制。",
			workflow_edge_self_loop: "步骤不能将自己作为前置依赖。",
			workflow_edge_duplicate: "同一对步骤间有重复连接。",
			workflow_node_id_invalid: "有步骤缺少有效的标识。",
			workflow_node_id_duplicate: "有步骤使用了相同的标识。",
			workflow_edge_endpoint_missing: "有流程连接指向不存在的步骤。",
			workflow_layout_invalid: "有步骤的画布位置无效。",
			workflow_no_trigger: "流程缺少启动入口。",
			workflow_multiple_triggers: "流程只能有一个启动入口。",
			workflow_trigger_has_inbound: "启动入口不能依赖其他步骤。",
			workflow_trigger_fanout: "启动入口连接的后续步骤超过限制。",
			workflow_node_fanout_invalid: "有步骤连接的后续步骤不符合要求。",
			workflow_node_config_invalid: "有步骤的执行配置未通过检查。",
			workflow_graph_cycle: "流程步骤存在循环依赖。",
			workflow_node_unreachable: "有步骤无法从流程入口到达。",
			workflow_branch_route_invalid: "有流程分支缺少有效的去向。",
			workflow_branch_priority_duplicate: "有流程分支使用了相同的优先级。",
			workflow_branch_default_duplicate: "有流程分支设置了多个默认去向。",
			workflow_subworkflow_version_missing: "有子流程缺少可用的固定版本。",
			branch_ambiguous: "需要核对同时匹配到的流程分支。",
			node_not_supported: "当前环境不支持执行此类步骤。",
			subworkflow_depth_exceeded: "子流程嵌套超过允许的层数。",
			subworkflow_cycle: "子流程之间存在循环引用。",
			memory_batch_too_large: "本次整理的记忆内容超过大小或数量限制。",
			memory_proposal_invalid: "记忆提议未通过检查，请核对内容。",
			memory_batch_conflict: "同一批次里有互相冲突的记忆提议。",
			memory_batch_not_found: "没有找到这次记忆整理的记录。",
			memory_batch_not_approved: "这批记忆仍需审批。",
			memory_run_active: "已有记忆整理正在处理。",
			memory_source_run_not_settled: "请先完成来源运行的收尾。",
			memory_eval_timeout: "记忆评估超时，请稍后查看。",
			memory_eval_unavailable: "当前没有可用的记忆评估服务。",
			memory_eval_disabled: "当前未启用记忆评估。",
			memory_eval_invalid_output: "本次记忆评估没有返回可用结论。",
			memory_apply_binding_invalid: "待写入内容与已批准的记忆提议不符。",
			memory_apply_conflict: "当前记忆记录已有变化，请核对后再处理。",
			memory_apply_failed: "这次记忆写入未成功，请先核对写入记录。",
			memory_write_tool_forbidden: "记忆写入需要经由已授权的操作入口。"
		};
		const UNKNOWN_REASON = "现有记录未能说明具体原因。";
		const UNKNOWN_STATUS = "当前状态未确认";
		function lookup(labels, key) {
			return Object.hasOwn(labels, key) ? labels[key] : void 0;
		}
		function isPlainReason(text) {
			if (text.length > 1800 || text.startsWith("\"") || /[\p{Cc}\p{Cf}{}\[\]`<>]/u.test(text)) return false;
			if (/(?:\b\w+\s*=|\b(?:args?|params?|payload|token|secret|password|stack)\s*[:：]|--[a-z]|:\/\/|\\|(?:^|\s)\/\S)/i.test(text)) return false;
			if (/(?:[a-f\d]{8}-(?:[a-f\d]{4}-){3}[a-f\d]{12}|[a-f\d]{24,}|\b(?:nrun|run|tsk|dec|evt|tex|att|plan|lease|artifact|scope|finding|verification|rework|memrun|membatch|memitem)[_-][a-z\d-]+|\b[A-Z\d]+(?:_[A-Z\d]+)+\b)/i.test(text)) return false;
			return /\p{Script=Han}/u.test(text) || /^[A-Z][a-z]+(?:[ ,]+[a-zA-Z][a-zA-Z'’-]*){2,}[.!?]?$/.test(text);
		}
		function operatorReason(value) {
			if (typeof value !== "string") return UNKNOWN_REASON;
			const text = value.trim();
			const key = text.toLowerCase();
			const label = lookup(REASON, key);
			if (label) return label;
			if (key.startsWith("verdict_unmet:")) return REASON.verdict_unmet;
			if (key.startsWith("child_run:")) return "已启动子流程，等待子流程完成。";
			if (key.startsWith("final_status_")) {
				const status = lookup(STATUS, key.slice(13));
				return status ? `步骤结束时的状态为「${status}」。` : UNKNOWN_REASON;
			}
			return isPlainReason(text) ? text : UNKNOWN_REASON;
		}
		function operatorStatus(value) {
			return typeof value === "string" ? lookup(STATUS, value.trim().toLowerCase()) ?? UNKNOWN_STATUS : UNKNOWN_STATUS;
		}
		// src/client/api/decision-commands.ts
		/** 面板控制面只允许这两条决定写命令。 */
		const DECISION_COMMAND_KINDS = ["decision.acknowledge", "decision.resolve"];
		var DecisionConfirmationRequired = class extends Error {
			confirmation;
			constructor(confirmation) {
				super("请确认这次裁决");
				this.confirmation = confirmation;
			}
		};
		/** 错误分型的中文提示：每类一条可辨识的话，面板直接显示。 */
		const DECISION_ERROR_TEXT = {
			DECISION_REVISION_CONFLICT: "状态已变，请刷新后重试",
			COMMAND_BUSY: "命令正在处理中，请稍后再试",
			OPERATION_ID_CONFLICT: "命令编号冲突，请稍后重试",
			OPERATION_EXPIRED: "命令已过期，请重新发起",
			CONFIRMATION_EXPIRED: "确认已过期，请重新选择处理方式",
			CONFIRMATION_MISMATCH: "确认与本次处理不再匹配，请重新选择",
			FORBIDDEN: "没有权限处理这条决定",
			RESOLUTION_NOT_AVAILABLE: "当前状态下无法执行这项处理，请刷新后查看可用选项",
			NOT_FOUND: "找不到这条决定，可能已被处理",
			OPERATION_TIMEOUT: "结果未知，正在查回执",
			OPERATION_OUTCOME_UNKNOWN: "结果未知，正在查回执",
			SERVICE_UNAVAILABLE: "结果未知，正在查回执"
		};
		/** 把服务端错误翻译成面板上的一句中文。 */
		function describeDecisionError(error) {
			if (error instanceof ApiError) return Object.hasOwn(DECISION_ERROR_TEXT, error.code) ? DECISION_ERROR_TEXT[error.code] : operatorReason(error.message || error.code);
			return "操作没成功";
		}
		const AMBIGUOUS_CODES = /* @__PURE__ */ new Set([
			"OPERATION_TIMEOUT",
			"OPERATION_OUTCOME_UNKNOWN",
			"SERVICE_UNAVAILABLE"
		]);
		function receiptResult(row) {
			if (!row.receipt_json) return null;
			try {
				const parsed = JSON.parse(row.receipt_json);
				if (parsed && typeof parsed === "object" && "result" in parsed) return parsed.result;
				return parsed;
			} catch {
				return null;
			}
		}
		/** 结果不可确认时按同一 operation_id 查回执；查不到就报未知，绝不换 id 重发。 */
		async function recoverResult(client, operationId) {
			try {
				const row = await client.get("/v1/operations/get", { operation_id: operationId });
				if (row.data.status !== "applied") return null;
				return receiptResult(row.data);
			} catch {
				return null;
			}
		}
		async function submitDecisionCommand(client, kind, path, body, operationId = newOperationId()) {
			try {
				const result = (await client.post(path, {
					...body,
					operation_id: operationId
				})).data;
				return {
					operation_id: operationId,
					kind,
					replayed: result.replayed === true,
					result
				};
			} catch (error) {
				if (kind === "decision.resolve" && error instanceof ApiError && error.code === "CONFIRMATION_REQUIRED") {
					const challenge = error.details?.confirm_challenge;
					if (typeof challenge === "string" && challenge.length > 0) throw new DecisionConfirmationRequired({
						operationId,
						challenge
					});
				}
				if (!(error instanceof ApiError) || !AMBIGUOUS_CODES.has(error.code)) throw error;
				const recovered = await recoverResult(client, operationId);
				if (recovered === null) throw new ApiError("OPERATION_OUTCOME_UNKNOWN", "结果未知，正在查回执", true, {
					operation_id: operationId,
					kind
				});
				return {
					operation_id: operationId,
					kind,
					replayed: true,
					result: recovered
				};
			}
		}
		function createDecisionCommandsApi(client) {
			return {
				acknowledgeDecision: (input) => submitDecisionCommand(client, "decision.acknowledge", "/v1/decisions/acknowledge", {
					decisionId: input.decisionId,
					revision: input.revision
				}),
				resolveDecision: (input) => submitDecisionCommand(client, "decision.resolve", "/v1/decisions/resolve", {
					decisionId: input.decisionId,
					revision: input.revision,
					resolution: input.resolution,
					...input.contentHash ? { contentHash: input.contentHash } : {},
					...input.evidenceId ? { payload: { evidence_ids: [input.evidenceId] } } : input.attestationId ? { payload: { attestation_id: input.attestationId } } : input.selectedNodeId ? { payload: { selected_node_id: input.selectedNodeId } } : {},
					...input.confirmation ? { confirm_challenge: input.confirmation.challenge } : {}
				}, input.confirmation?.operationId)
			};
		}
		// src/client/panel/copy.ts
		const COPY = {
			emptyTasks: "还没有任务，新建一个就能开始",
			emptyDecisions: "暂无待处理审批",
			emptyEffects: "没有待核实的操作",
			serviceDown: "服务未连接",
			staleNotice: "显示的是最后一次拿到的数据",
			protocolMismatch: "服务与面板版本不一致，请重启 DSH",
			queryFailed: "查询失败",
			commandUnknown: "结果未知",
			retry: "重试",
			refresh: "刷新后重试",
			oldPanel: "当前状态未确认",
			awaitingDecision: "待审批",
			pendingIrreversible: "仍有未结算不可逆动作",
			heartbeatStopped: "心跳已停",
			succeededWithUnverified: "执行完成，外部操作待核验",
			failedWithUnverified: "执行失败，外部操作待核验",
			deliveryAcceptanceFailed: "交付验收失败",
			deliveryAcceptanceUnverifiable: "交付待核验",
			unverifiedBadge: "项未验证"
		};
		// src/client/panel/tokens.ts
		const COLOR = {
			canvas: "#ffffff",
			surface: "#ffffff",
			sidebar: "#f8fafc",
			line: "#e6e7ea",
			ink: "#111316",
			inkMuted: "#5a616b",
			success: "#1c7c54",
			warning: "#8a5c17",
			danger: "#a83239",
			link: "#1f5fbf"
		};
		const FONT_PX = {
			body: 14,
			helper: 12,
			menu: 13,
			section: 16,
			title: 24
		};
		const FONT_FLOOR_PX = {
			body: 13,
			helper: 12,
			menu: 12.5
		};
		// src/client/panel/panel-layout.ts
		const PANEL_LAYOUT_CSS = `
.ops-panel-viewport {
  display: flex; flex: 1 1 auto; flex-direction: column;
  block-size: 100dvh; max-block-size: 100%; min-block-size: 0; min-inline-size: 0;
  container-type: inline-size; container-name: orchestra-panel;
}
.ops-shell { display: flex; flex: 1; min-height: 0; min-width: 0; }
.ops-shell-sidebar {
  box-sizing: border-box; flex: none; width: 236px; min-height: 0; min-width: 0;
  padding: 22px 14px; overflow: auto; scrollbar-gutter: stable;
  border-right: 1px solid ${COLOR.line};
}
.ops-shell-brand { padding: 4px 10px 22px; }
.ops-main-menu { display: grid; gap: 4px; }
.ops-settings-menu {
  display: grid; gap: 2px; margin: 4px 0 0 13px; padding-inline-start: 7px;
  border-inline-start: 1px solid ${COLOR.line};
}
.ops-settings-menu button {
  min-height: 36px; border: 0; border-radius: 6px; padding: 4px 8px;
  background: transparent; color: ${COLOR.inkMuted}; text-align: start;
  font: inherit; font-size: ${FONT_PX.menu}px; cursor: pointer;
}
.ops-settings-menu button:hover { background: #eef3f9; }
.ops-settings-menu button[aria-current=location] { background: #e5efff; color: #1e55a1; font-weight: 700; }
.ops-settings-menu button:focus-visible, .ops-settings-anchor:focus-visible { outline: 2px solid #225fba; outline-offset: 2px; }
.ops-shell-main { display: flex; flex: 1; flex-direction: column; min-width: 0; min-height: 0; }
.ops-shell-main > header, .ops-shell-main > [role=status] { flex: none; }
.ops-shell-main > main {
  flex: 1; min-width: 0; min-height: 0; overflow: auto;
  scrollbar-gutter: stable; overscroll-behavior: contain;
}
.ops-shell-main > main:focus-visible { outline: 2px solid #225fba; outline-offset: -2px; }
.ops-panel-content-inner { box-sizing: border-box; max-width: 1780px; margin-inline: auto; }
.ops-panel-protocol-error {
  flex: 1; min-height: 0; overflow: auto; display: grid;
  align-content: safe center; justify-content: safe center;
  padding: 24px; text-align: center; overflow-wrap: anywhere;
}
.ops-page { display: grid; align-content: start; gap: 12px; min-width: 0; }
.ops-page-header { display: flex; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; gap: 8px 14px; min-width: 0; }
.ops-page h1 { margin: 0; font-size: ${FONT_PX.title}px; line-height: 1.3; letter-spacing: -.02em; overflow-wrap: anywhere; }
.ops-page h2 { font-size: ${FONT_PX.section}px; line-height: 1.4; }
.ops-meta { color: #546275; font-size: ${FONT_PX.menu}px; margin: 4px 0 0; }
.ops-page button, .ops-page input, .ops-page select, .ops-page textarea { font-family: inherit; }
.ops-actions, .ops-toolbar { display: flex; align-items: center; flex-wrap: wrap; gap: 7px; }
.ops-toolbar { justify-content: space-between; }
.ops-button, .ops-field { box-sizing: border-box; min-height: 34px; border: 1px solid #d8e0ea; border-radius: 7px; color: #1d2939; background: #fff; font: inherit; font-size: ${FONT_PX.menu}px; }
.ops-field { padding: 5px 9px; max-width: 100%; min-width: 0; }
.ops-button { padding: 5px 11px; font-weight: 600; cursor: pointer; white-space: nowrap; }
.ops-button:hover:not(:disabled) { background: #f0f5fb; }
.ops-button:disabled { color: #64748b; background: #f8fafc; cursor: default; }
.ops-button--primary { color: #fff; background: #225fba; border-color: #225fba; }
.ops-button--primary:hover:not(:disabled) { background: #174c9a; }
.ops-button--link { color: #2057a1; border-color: transparent; background: transparent; }
.ops-surface { min-width: 0; border: 1px solid #dde4ed; border-radius: 9px; background: #fff; }
.ops-surface-head { display: flex; justify-content: space-between; align-items: center; gap: 7px; padding: 9px 12px; background: #f8fafc; border-bottom: 1px solid #e3e9f0; border-radius: 8px 8px 0 0; }
.ops-surface-head h2 { margin: 0; }
.ops-table-scroll { min-width: 0; overflow-x: auto; }
.ops-table { width: 100%; border-collapse: collapse; text-align: left; font-size: ${FONT_PX.menu}px; }
.ops-table--wide { min-width: 640px; }
.ops-table th, .ops-table td { vertical-align: middle; padding: 8px 12px; }
.ops-table th { color: #526274; font-weight: 600; white-space: nowrap; background: #f4f7fb; }
.ops-table td { border-top: 1px solid #e8edf3; }
.ops-table tbody tr:hover { background: #f8fbff; }
.ops-table td:first-child { overflow-wrap: anywhere; }
.ops-table time, .ops-number { font-variant-numeric: tabular-nums; white-space: nowrap; }
.ops-status { display: inline-flex; align-items: center; gap: 5px; padding: 2px 7px; border: 1px solid #dce3eb; border-radius: 5px; font-size: ${FONT_PX.helper}px; line-height: 1.45; font-weight: 600; background: #f2f5f8; color: #45546a; white-space: nowrap; }
.ops-status[data-tone=on] { color: #146048; background: #eaf5ef; border-color: #cce7da; }
.ops-status[data-tone=off] { color: #46546a; background: #f0f3f8; border-color: #dce3eb; }
.ops-status[data-tone=info] { color: #23569e; background: #edf4ff; border-color: #d8e6fa; }
.ops-empty { margin: 0; padding: 14px 12px; color: #546275; }
.ops-tabs { display: flex; flex-wrap: wrap; gap: 5px; align-items: flex-end; border-bottom: 1px solid #dbe3ed; }
.ops-tabs button { color: #526274; font: inherit; font-weight: 550; min-height: 36px; border: 0; border-bottom: 2px solid transparent; background: none; padding: 5px 10px; cursor: pointer; }
.ops-tabs [aria-pressed=true] { color: #1e55a1; border-bottom-color: #225fba; }
.ops-stack { display: grid; gap: 10px; min-width: 0; align-content: start; }
.ops-context-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(300px, 100%), 1fr)); align-items: start; gap: 10px; }
.ops-settings-page { width: 100%; max-width: 1100px; margin-inline: auto; }
.ops-settings-section { display: grid; grid-template-columns: minmax(0, 1fr); gap: 10px; }
.ops-settings-section fieldset { min-width: 0; }
.ops-card-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(min(400px, 100%), 1fr)); align-items: start; gap: 10px; }
.ops-card-grid:empty { display: none; }
.ops-inset { padding: 10px 12px; }
.ops-inset > details + details { border-top: 1px solid #e8edf3; margin-top: 6px; padding-top: 6px; }
.ops-entry-form { display: grid; gap: 7px; padding: 10px 12px; justify-items: start; }
.ops-entry-form textarea { width: min(100%, 660px); }
.ops-entry-form label { font-weight: 600; }
.ops-page details > summary { cursor: pointer; }
.ops-workflow-stage { display: grid; grid-template-columns: minmax(0, 1fr) minmax(205px, 26%); min-width: 0; align-items: stretch; gap: 8px; }
.ops-workflow-stage aside p { margin: 3px 0; overflow-wrap: anywhere; }
.ops-workflow-controls { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(255px, 100%), 1fr)); gap: 8px; }
.ops-workflow-controls > section { padding: 9px 11px; min-width: 0; background: #f7f9fc; border: 1px solid #e1e7ef; border-radius: 7px; }
.ops-workflow-controls > section > :first-child { margin-top: 0; }
.ops-workflow-controls ol, .ops-workflow-controls ul { padding-left: 20px; }
.ops-shell-main:not([data-panel-route=tasks]) { background: #f4f7fb; }
.ops-shell-main[data-panel-route=tasks] .ops-panel-content-inner { padding: 28px 32px 80px; }
.ops-shell-main:not([data-panel-route=tasks]) .ops-panel-content-inner { padding: 16px 22px 36px; }
@container orchestra-panel (max-width: 980px) {
  .ops-shell-sidebar { width: 164px; padding: 11px 8px; }
}
@container orchestra-panel (max-width: 680px) {
  .ops-shell { flex-direction: column; }
  .ops-shell-sidebar { width: 100%; padding: 5px 8px; border-right: 0; overflow: visible; scrollbar-gutter: auto; }
  .ops-shell-brand { padding: 4px; }
  .ops-main-menu { display: flex; overflow-x: auto; }
  .ops-main-menu button { white-space: nowrap; }
  .ops-settings-menu {
    display: flex; flex-wrap: wrap; gap: 2px 4px; margin: 3px 0 0; padding: 3px 0 0;
    border-inline-start: 0; border-block-start: 1px solid ${COLOR.line};
  }
  .ops-shell-main[data-panel-route] .ops-panel-content-inner { padding: 10px 10px 24px; }
  .ops-workflow-stage { grid-template-columns: minmax(0, 1fr); }
  .ops-table th, .ops-table td { padding: 7px 8px; }
}
`;
		// src/client/panel/shell.tsx
		const PANEL_ROUTES = [
			{
				id: "overview",
				label: "总览"
			},
			{
				id: "tasks",
				label: "任务"
			},
			{
				id: "decisions",
				label: "审批中心"
			},
			{
				id: "effects",
				label: "操作核验"
			},
			{
				id: "workflows",
				label: "Workflow 管理"
			},
			{
				id: "activity",
				label: "事件日志"
			},
			{
				id: "collaboration",
				label: "协作核验"
			},
			{
				id: "memory",
				label: "记忆"
			},
			{
				id: "settings",
				label: "设置"
			}
		];
		const CommandsEnabledContext = (0, react.createContext)(null);
		/** Write controls default to disabled outside a connected panel shell. */
		function usePanelCommandsEnabled(standalone = false) {
			return (0, react.useContext)(CommandsEnabledContext)?.enabled ?? standalone;
		}
		function PanelCommandButton(props) {
			const { enabled, reason } = (0, react.useContext)(CommandsEnabledContext) ?? {
				enabled: false,
				reason: COPY.serviceDown
			};
			const { disabled, title, type, ...rest } = props;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
				...rest,
				type: type ?? "button",
				disabled: disabled || !enabled,
				title: !enabled ? reason : title
			});
		}
		function UnknownState(_props) {
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
				role: "status",
				style: { color: COLOR.warning },
				children: COPY.oldPanel
			});
		}
		function PanelShell(props) {
			const content = (0, react.useRef)(null);
			const settingsMenuId = (0, react.useId)();
			const [activeSettingsSection, setActiveSettingsSection] = (0, react.useState)();
			const settingsOpen = props.activeRoute === "settings" && !!props.settingsSections?.length;
			(0, react.useLayoutEffect)(() => {
				if (content.current) {
					content.current.scrollTop = 0;
					content.current.scrollLeft = 0;
				}
				setActiveSettingsSection(void 0);
			}, [props.activeRoute]);
			(0, react.useLayoutEffect)(() => {
				if (activeSettingsSection && !props.settingsSections?.some(({ id }) => id === activeSettingsSection)) setActiveSettingsSection(void 0);
			}, [activeSettingsSection, props.settingsSections]);
			function openSettingsSection(id) {
				const scroller = content.current;
				const heading = scroller?.querySelector(`#${id}`);
				if (!scroller || !heading) return;
				const offset = heading.getBoundingClientRect().top - scroller.getBoundingClientRect().top;
				scroller.scrollTop += offset - 12;
				heading.focus({ preventScroll: true });
				setActiveSettingsSection(id);
			}
			if (props.protocolMismatch) return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				className: "ops-panel-viewport",
				style: appStyle,
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("style", { children: PANEL_LAYOUT_CSS }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("main", {
					role: "alert",
					className: "ops-panel-protocol-error",
					children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", {
						style: { fontSize: FONT_PX.title },
						children: COPY.protocolMismatch
					})
				})]
			});
			const stateLabel = !props.serviceConnected ? COPY.serviceDown : props.streamStatus === "polling" ? "轮询更新" : props.streamStatus === "stale" ? "数据可能已过期" : props.streamStatus === "live" ? "实时更新" : "服务已连接";
			const isStale = props.snapshotStale || props.streamStatus === "stale";
			const commandsEnabled = props.serviceConnected && !isStale;
			const commandsUnavailableReason = !props.serviceConnected ? COPY.serviceDown : COPY.staleNotice;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(CommandsEnabledContext.Provider, {
				value: {
					enabled: commandsEnabled,
					reason: commandsUnavailableReason
				},
				children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
					className: "ops-panel-viewport",
					style: appStyle,
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("style", { children: PANEL_LAYOUT_CSS }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "ops-shell",
						"data-panel-route": props.activeRoute,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("aside", {
							"aria-label": "主导航",
							className: "ops-shell-sidebar",
							style: sidebarStyle,
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
									className: "ops-shell-brand",
									style: brandStyle,
									children: "Orchestra"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("nav", {
									"aria-label": "主菜单",
									className: "ops-main-menu",
									children: PANEL_ROUTES.filter((route) => route.id !== "collaboration" || props.collaborationEnabled).map((route) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										"aria-current": props.activeRoute === route.id ? "page" : void 0,
										"aria-expanded": route.id === "settings" && props.settingsSections?.length ? settingsOpen : void 0,
										"aria-controls": route.id === "settings" && settingsOpen ? settingsMenuId : void 0,
										onClick: () => props.onNavigate(route.id),
										style: {
											...navButtonStyle,
											...props.activeRoute === route.id ? activeNavStyle : {}
										},
										children: route.label
									}, route.id))
								}),
								settingsOpen && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("nav", {
									id: settingsMenuId,
									"aria-label": "设置子菜单",
									className: "ops-settings-menu",
									children: props.settingsSections?.map(({ id, label }) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										"aria-current": activeSettingsSection === id ? "location" : void 0,
										"aria-controls": id,
										onClick: () => openSettingsSection(id),
										children: label
									}, id))
								})
							]
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "ops-shell-main",
							"data-panel-route": props.activeRoute,
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
									style: props.activeRoute === "tasks" ? headerStyle : {
										...headerStyle,
										height: 44,
										paddingInline: 22
									},
									children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: PANEL_ROUTES.find((route) => route.id === props.activeRoute)?.label }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
										role: "status",
										style: { color: props.serviceConnected ? COLOR.success : COLOR.warning },
										children: stateLabel
									})]
								}),
								(!props.serviceConnected || isStale) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
									role: "status",
									style: noticeStyle$1,
									children: [COPY.staleNotice, "。刷新成功后可继续写入。"]
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("main", {
									id: "panel-content",
									ref: content,
									tabIndex: 0,
									children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
										className: "ops-panel-content-inner",
										children: props.children
									})
								})
							]
						})]
					})]
				})
			});
		}
		const appStyle = {
			background: COLOR.canvas,
			color: COLOR.ink,
			fontSize: FONT_PX.body,
			lineHeight: 1.55
		};
		const sidebarStyle = { background: COLOR.sidebar };
		const brandStyle = {
			fontSize: 16,
			fontWeight: 700
		};
		const navButtonStyle = {
			minHeight: 40,
			padding: "0 12px",
			border: 0,
			borderRadius: 7,
			textAlign: "left",
			background: "transparent",
			color: COLOR.ink,
			fontFamily: "system-ui, sans-serif",
			fontSize: FONT_PX.menu,
			fontWeight: 500,
			cursor: "pointer"
		};
		const activeNavStyle = {
			background: "#e9edf2",
			fontWeight: 700
		};
		const headerStyle = {
			height: 58,
			padding: "0 28px",
			display: "flex",
			alignItems: "center",
			justifyContent: "space-between",
			borderBottom: `1px solid ${COLOR.line}`,
			fontSize: FONT_PX.menu
		};
		const noticeStyle$1 = {
			padding: "10px 28px",
			background: "#fff8e9",
			color: COLOR.warning,
			borderBottom: `1px solid ${COLOR.line}`,
			fontSize: FONT_PX.helper
		};
		// src/core/task-workspace.ts
		const TASK_LANES = [
			{
				id: "upcoming",
				label: "待执行"
			},
			{
				id: "running",
				label: "执行中"
			},
			{
				id: "attention",
				label: "待处理"
			},
			{
				id: "finished",
				label: "已结束"
			}
		];
		// src/client/panel/workflow-canvas-nodes.tsx
		/** 九种节点类型（规格 8.5 行 657 的「九种节点」）。 */
		const CANVAS_NODE_TYPES = [
			"trigger",
			"agent",
			"tool",
			"policy",
			"branch",
			"approval",
			"parallel",
			"wait",
			"subworkflow"
		];
		/** 节点类型的中文说法：节点条、画布标签、检查提示都用它。 */
		const NODE_TYPE_LABEL = {
			trigger: "触发",
			agent: "智能体",
			tool: "工具",
			policy: "策略",
			branch: "分支",
			approval: "审批",
			parallel: "并行",
			wait: "等待",
			subworkflow: "子流程"
		};
		/** branch / parallel 可以有多条出边，其余节点最多一条。 */
		const MULTI_OUT_TYPES = /* @__PURE__ */ new Set(["branch", "parallel"]);
		/** position 的取值范围与 PositionSchema 一致。 */
		const POSITION_MAX = 2e4;
		function clampCoordinate(value) {
			return Math.min(POSITION_MAX, Math.max(0, Math.round(typeof value === "number" && Number.isFinite(value) ? value : 0)));
		}
		function readPosition(value) {
			if (!value || typeof value !== "object" || Array.isArray(value)) return {
				x: 0,
				y: 0
			};
			const raw = value;
			return {
				x: clampCoordinate(raw.x),
				y: clampCoordinate(raw.y)
			};
		}
		function readRoute(value) {
			if (!value || typeof value !== "object" || Array.isArray(value)) return void 0;
			const raw = value;
			const route = {};
			if (typeof raw.priority === "number" && Number.isFinite(raw.priority)) route.priority = Math.round(raw.priority);
			if (raw.default === true) route.default = true;
			return Object.keys(route).length > 0 ? route : void 0;
		}
		/** 把定义读成画布能画的形状；不是对象就返回 null（画布先不显示）。 */
		function readCanvasDoc(definition) {
			if (!definition || typeof definition !== "object" || Array.isArray(definition)) return null;
			const source = definition;
			const nodes = [];
			if (Array.isArray(source.nodes)) for (const item of source.nodes) {
				if (!item || typeof item !== "object" || Array.isArray(item)) continue;
				const raw = item;
				const nodeId = typeof raw.node_id === "string" ? raw.node_id : "";
				if (!nodeId) continue;
				nodes.push({
					node_id: nodeId,
					type: typeof raw.type === "string" ? raw.type : "agent",
					title: typeof raw.title === "string" ? raw.title : nodeId,
					position: readPosition(raw.position),
					raw
				});
			}
			const edges = [];
			if (Array.isArray(source.edges)) for (const item of source.edges) {
				if (!item || typeof item !== "object" || Array.isArray(item)) continue;
				const raw = item;
				const from = typeof raw.from === "string" ? raw.from : "";
				const to = typeof raw.to === "string" ? raw.to : "";
				if (!from || !to) continue;
				edges.push({
					from,
					to,
					route: readRoute(raw.route),
					raw
				});
			}
			const extra = {};
			for (const [key, value] of Object.entries(source)) {
				if (key === "nodes" || key === "edges") continue;
				extra[key] = value;
			}
			return {
				name: typeof source.name === "string" ? source.name : "",
				nodes,
				edges,
				extra
			};
		}
		/** 画布改动写回定义：只换 nodes / edges，其余字段原样带回。 */
		function serializeCanvasDoc(doc) {
			return {
				...doc.extra,
				nodes: doc.nodes.map((node) => ({
					...node.raw,
					node_id: node.node_id,
					type: node.type,
					title: node.title,
					position: {
						x: node.position.x,
						y: node.position.y
					}
				})),
				edges: doc.edges.map((edge) => {
					const next = {
						...edge.raw,
						from: edge.from,
						to: edge.to
					};
					if (edge.route) next.route = { ...edge.route };
					else delete next.route;
					return next;
				})
			};
		}
		/** 新节点的身份：node_id 合 NODE_ID_RE 且唯一，标题说人话。 */
		function nextNodeIdentity(doc, type) {
			const base = type.replace(/[^a-z0-9_]/g, "") || "node";
			let index = 1;
			while (doc.nodes.some((node) => node.node_id === base + "_" + String(index))) index += 1;
			const label = NODE_TYPE_LABEL[type] ?? type;
			const serial = doc.nodes.filter((node) => node.type === type).length + 1;
			return {
				nodeId: base + "_" + String(index),
				title: label + " " + String(serial)
			};
		}
		/** 连线能不能建：自环、重复边、单出边越界都在前端拦住。 */
		function canConnect(doc, from, to) {
			if (from === to) return {
				ok: false,
				message: "不能连到自己：节点之间不许自环。"
			};
			if (doc.edges.some((edge) => edge.from === from && edge.to === to)) return {
				ok: false,
				message: "这两个节点之间已经有一条连线了。"
			};
			const source = doc.nodes.find((node) => node.node_id === from);
			if (!source) return {
				ok: false,
				message: "找不到起点节点。"
			};
			if (!doc.nodes.some((node) => node.node_id === to)) return {
				ok: false,
				message: "找不到终点节点。"
			};
			if (!MULTI_OUT_TYPES.has(source.type)) {
				if (doc.edges.filter((edge) => edge.from === from).length >= 1) return {
					ok: false,
					message: source.title + " 最多只能有一条出边。"
				};
			}
			return { ok: true };
		}
		/** branch 的出边必须带 route；画布给新边补一个不重复的 priority，免得定义发不出去。 */
		function routeForNewEdge(doc, from) {
			const source = doc.nodes.find((node) => node.node_id === from);
			if (!source || source.type !== "branch") return void 0;
			const taken = /* @__PURE__ */ new Set();
			for (const edge of doc.edges) {
				if (edge.from !== from) continue;
				const priority = edge.route?.priority;
				if (typeof priority === "number") taken.add(priority);
			}
			let priority = 0;
			while (taken.has(priority)) priority += 1;
			return { priority };
		}
		/** 拓扑序 + 分层：迭代实现，不递归；图里有环就明确拒绝，不静默乱排。 */
		function topologicalOrder(doc) {
			const index = /* @__PURE__ */ new Map();
			doc.nodes.forEach((node, position) => index.set(node.node_id, position));
			const indegree = /* @__PURE__ */ new Map();
			for (const node of doc.nodes) indegree.set(node.node_id, 0);
			const adjacency = /* @__PURE__ */ new Map();
			for (const edge of doc.edges) {
				if (!index.has(edge.from) || !index.has(edge.to)) continue;
				const list = adjacency.get(edge.from) ?? [];
				list.push(edge.to);
				adjacency.set(edge.from, list);
				indegree.set(edge.to, (indegree.get(edge.to) ?? 0) + 1);
			}
			const ready = doc.nodes.map((node) => node.node_id).filter((id) => (indegree.get(id) ?? 0) === 0);
			const order = [];
			while (ready.length > 0) {
				ready.sort((left, right) => (index.get(left) ?? 0) - (index.get(right) ?? 0));
				const current = ready.shift();
				order.push(current);
				for (const next of adjacency.get(current) ?? []) {
					const rest = (indegree.get(next) ?? 0) - 1;
					indegree.set(next, rest);
					if (rest === 0) ready.push(next);
				}
			}
			if (order.length !== doc.nodes.length) return {
				ok: false,
				message: "图里有环，没法按拓扑整理：先断开环上的连线再整理。"
			};
			const layer = /* @__PURE__ */ new Map();
			for (const id of order) {
				let depth = 0;
				for (const edge of doc.edges) {
					if (edge.to !== id) continue;
					const from = layer.get(edge.from);
					if (from === void 0) continue;
					depth = Math.max(depth, from + 1);
				}
				layer.set(id, depth);
			}
			return {
				ok: true,
				order,
				layer
			};
		}
		/** 整理：按拓扑序重新生成 position。有环就拒绝并给提示。 */
		function arrangePositions(doc) {
			const topology = topologicalOrder(doc);
			if (!topology.ok) return {
				ok: false,
				message: topology.message
			};
			const rows = /* @__PURE__ */ new Map();
			for (const id of topology.order) {
				const depth = topology.layer.get(id) ?? 0;
				const list = rows.get(depth) ?? [];
				list.push(id);
				rows.set(depth, list);
			}
			const positions = /* @__PURE__ */ new Map();
			for (const [depth, ids] of rows) ids.forEach((id, row) => {
				positions.set(id, {
					x: 40 + depth * 240,
					y: 40 + row * 140
				});
			});
			return {
				ok: true,
				nodes: doc.nodes.map((node) => ({
					...node,
					position: positions.get(node.node_id) ?? node.position
				}))
			};
		}
		// src/client/panel/runtime-topology.ts
		/** Selection is independent of query order and shared by the graph and step inspector. */
		function currentAttempts(nodes) {
			const selected = /* @__PURE__ */ new Map();
			for (const node of nodes) {
				if (node.replacement_node_run_id) continue;
				const previous = selected.get(node.node_id);
				if (!previous || (node.attempt ?? 0) > (previous.attempt ?? 0)) selected.set(node.node_id, node);
			}
			return selected;
		}
		/** Runtime additions only: never mutate the pinned workflow object used by the editor. */
		function runtimeCanvasDoc(definition, topology) {
			const base = readCanvasDoc(definition);
			if (!base || !topology) return base;
			const nodes = [...base.nodes];
			for (const [index, step] of topology.steps.entries()) {
				if (nodes.some((n) => n.node_id === step.node_id)) continue;
				const source = base.nodes.find((n) => n.node_id === step.before_node_id);
				nodes.push({
					node_id: step.node_id,
					type: "agent",
					title: `新增步骤 ${index + 1}`,
					position: {
						x: 40,
						y: 40
					},
					raw: {
						contract: { description: step.instruction },
						runtime_step: {
							...step,
							source_title: source?.title || "来源步骤名称暂不可用"
						}
					}
				});
			}
			const edges = [...base.edges];
			for (const edge of topology.edges) if (!edges.some((e) => e.from === edge.from && e.to === edge.to)) edges.push({
				...edge,
				raw: { runtime_added: true }
			});
			const merged = {
				...base,
				nodes,
				edges
			};
			if (topology.edges.length || topology.steps.length) {
				const arranged = arrangePositions(merged);
				if (arranged.ok) return {
					...merged,
					nodes: arranged.nodes
				};
			}
			return merged;
		}
		function separateOverlappingNodes(doc, width, height) {
			const { nodes } = doc;
			if (!nodes.some(({ position: left }, index) => {
				for (let other = index + 1; other < nodes.length; other++) {
					const right = nodes[other].position;
					if (Math.abs(left.x - right.x) < width && Math.abs(left.y - right.y) < height) return true;
				}
				return false;
			})) return doc;
			const arranged = arrangePositions(doc);
			return arranged.ok ? {
				...doc,
				nodes: arranged.nodes
			} : doc;
		}
		function useRunTopology(api, runId, enabled, reloadVersion) {
			const [loaded, setLoaded] = (0, react.useState)();
			(0, react.useEffect)(() => {
				if (!runId || !enabled) return;
				let active = true;
				api.getReworkTopology(runId).then(({ data }) => {
					if (active) setLoaded({
						key: runId,
						data,
						failed: false
					});
				}).catch(() => {
					if (active) setLoaded({
						key: runId,
						data: null,
						failed: true
					});
				});
				return () => {
					active = false;
				};
			}, [
				api,
				runId,
				enabled,
				reloadVersion
			]);
			return enabled && runId ? loaded?.key === runId ? {
				...loaded,
				loading: false
			} : {
				data: null,
				failed: false,
				loading: true
			} : {
				data: null,
				failed: false,
				loading: false
			};
		}
		// src/client/panel/artifact-content.tsx
		function saveArtifact(artifact) {
			const binary = atob(artifact.base64);
			const bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0));
			if (bytes.byteLength !== artifact.size_bytes) throw new Error("下载内容大小与登记不一致");
			const url = URL.createObjectURL(new Blob([bytes], { type: "application/octet-stream" }));
			const link = document.createElement("a");
			link.href = url;
			link.download = artifact.file_name;
			link.hidden = true;
			document.body.append(link);
			try {
				link.click();
			} finally {
				link.remove();
				window.setTimeout(() => URL.revokeObjectURL(url), 1e3);
			}
		}
		function artifactContentKey(artifact) {
			return JSON.stringify([
				artifact.run_id,
				artifact.artifact_id,
				artifact.sha256,
				artifact.file_name,
				artifact.retain_until
			]);
		}
		function matchesArtifactContent(content, artifact) {
			return content.sha256 === artifact.sha256 && content.file_name === artifact.file_name;
		}
		function useArtifactRetention(artifact) {
			const [, refresh] = (0, react.useState)(0);
			const deadline = artifact?.retain_until == null ? Infinity : Date.parse(artifact.retain_until);
			const markedExpired = artifact?.expired === true;
			(0, react.useEffect)(() => {
				if (markedExpired || !Number.isFinite(deadline) || deadline <= Date.now()) return;
				let timer;
				const check = () => {
					const remaining = deadline - Date.now();
					if (remaining <= 0) refresh((tick) => tick + 1);
					else timer = window.setTimeout(check, Math.min(remaining, 2147483647));
				};
				check();
				return () => window.clearTimeout(timer);
			}, [markedExpired, deadline]);
			if (markedExpired || deadline <= Date.now()) return "expired";
			return Number.isNaN(deadline) ? "invalid" : "available";
		}
		function ArtifactContent({ api, artifact }) {
			const retention = useArtifactRetention(artifact);
			if (retention === "expired") return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				style: noteStyle,
				children: "已超过保留期限。仍可查看登记记录。"
			});
			if (retention === "invalid") return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				style: noteStyle,
				children: "产物保留期限未通过校验。仍可查看登记记录。"
			});
			if (!artifact.file_name) return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				style: noteStyle,
				children: "这份产物仅有登记记录，内容尚未入库。"
			});
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ArtifactContentReader, {
				api,
				artifact
			}, artifactContentKey(artifact));
		}
		function ArtifactContentReader({ api, artifact }) {
			const [pending, setPending] = (0, react.useState)(null);
			const [preview, setPreview] = (0, react.useState)(null);
			const [notice, setNotice] = (0, react.useState)("");
			const active = (0, react.useRef)(false);
			(0, react.useLayoutEffect)(() => {
				active.current = true;
				return () => {
					active.current = false;
				};
			}, []);
			async function load(mode) {
				setNotice("");
				setPending(mode);
				try {
					const { data } = await api.getArtifactContent(artifact.run_id, artifact.artifact_id, mode);
					if (!active.current) return;
					if (artifact.retain_until !== null && Date.parse(artifact.retain_until) <= Date.now()) return;
					if (data.kind !== mode || !matchesArtifactContent(data, artifact)) throw new Error("返回的内容与登记记录不一致，请刷新");
					if (data.kind === "preview") setPreview(data);
					else saveArtifact(data);
				} catch (error) {
					if (active.current) setNotice(error instanceof Error ? error.message : "未能读取产物。");
				} finally {
					if (active.current) setPending(null);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-label": artifact.file_name + "的内容",
				style: { marginTop: 10 },
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: {
							display: "flex",
							flexWrap: "wrap",
							gap: 8
						},
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							style: buttonStyle$8,
							disabled: pending !== null,
							"aria-expanded": preview !== null,
							onClick: () => preview ? setPreview(null) : void load("preview"),
							children: preview ? "收起预览" : "预览内容"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							style: buttonStyle$8,
							disabled: pending !== null,
							onClick: () => {
								load("download");
							},
							children: "下载产物"
						})]
					}),
					pending && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: noteStyle,
						children: "正在读取并核验产物…"
					}),
					notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "alert",
						style: noteStyle,
						children: notice
					}),
					preview && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [preview.truncated && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: noteStyle,
						children: "预览仅显示前 32 KiB，可下载完整产物。"
					}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("pre", {
						style: previewStyle,
						children: preview.text
					})] })
				]
			});
		}
		const buttonStyle$8 = {
			minHeight: 36,
			padding: "0 12px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 7,
			background: COLOR.surface,
			color: COLOR.ink,
			fontSize: FONT_PX.menu,
			cursor: "pointer"
		};
		const noteStyle = {
			color: COLOR.inkMuted,
			fontSize: FONT_PX.helper,
			margin: "6px 0"
		};
		const previewStyle = {
			maxHeight: 280,
			overflow: "auto",
			whiteSpace: "pre-wrap",
			overflowWrap: "anywhere",
			padding: 10,
			border: `1px solid ${COLOR.line}`,
			borderRadius: 6,
			color: COLOR.ink,
			fontSize: FONT_PX.body
		};
		// src/client/panel/task-node-status.ts
		const ACTIVE_NODE_STATES = /* @__PURE__ */ new Set([
			"leased",
			"dispatched",
			"started",
			"verifying",
			"stopping",
			"timing_out"
		]);
		const BLOCKED_NODE_STATES = /* @__PURE__ */ new Set([
			"blocked_pre",
			"blocked_post",
			"indeterminate_side_effect",
			"startup_unknown",
			"paused_bridge_missing"
		]);
		const FAILED = /* @__PURE__ */ new Set([
			"failed",
			"timeout",
			"orphaned"
		]);
		const UNVERIFIED = /* @__PURE__ */ new Set(["failed_with_unverified", "abandoned_unverified"]);
		const QUEUE_PHASES = {
			waiting_capacity: {
				label: "等待执行位",
				tone: "waiting"
			},
			waiting_dependencies: {
				label: "等待上游完成",
				tone: "waiting"
			},
			blocked_dependencies: {
				label: "上游失败，步骤受阻",
				tone: "blocked"
			},
			ready: {
				label: "就绪待派发",
				tone: "waiting"
			},
			paused: {
				label: "已暂停",
				tone: "blocked"
			},
			queued_unknown: {
				label: "排队原因未知",
				tone: "waiting"
			},
			cancelled: {
				label: "已取消",
				tone: "waiting"
			}
		};
		const STEP_STATUS = /* @__PURE__ */ new Map([
			["verifying", "核验中"],
			["blocked_post", "执行结束，待人工处理"],
			["orphaned", "执行者失联，尚未接手"]
		]);
		function supersededLabel(node) {
			return node.replacement_node_run_id ? "已由新尝试接替" : "本次尝试已停止";
		}
		function executionState(node) {
			return node.execution_state ?? node.status;
		}
		function nodeExecutionPhase(node) {
			if (!node) return {
				label: "未运行",
				tone: "waiting"
			};
			const state = executionState(node);
			if (Object.hasOwn(QUEUE_PHASES, state)) return QUEUE_PHASES[state];
			if (state === "done") return {
				label: "已完成",
				tone: "done"
			};
			if (BLOCKED_NODE_STATES.has(state)) return {
				label: "待处理",
				tone: "blocked"
			};
			if (ACTIVE_NODE_STATES.has(state)) return {
				label: state === "verifying" ? "核验中" : "进行中",
				tone: "active"
			};
			if (UNVERIFIED.has(state)) return {
				label: "待核实",
				tone: "blocked"
			};
			if (FAILED.has(state)) return {
				label: "未成功",
				tone: "failed"
			};
			if (state === "queued") return {
				label: "等待调度",
				tone: "waiting"
			};
			if (state === "waiting") return {
				label: "等待条件",
				tone: "waiting"
			};
			if (state === "superseded") return {
				label: supersededLabel(node),
				tone: "waiting"
			};
			return {
				label: operatorStatus(state),
				tone: "waiting"
			};
		}
		function nodeExecutionLabel(node) {
			const state = node.execution_state;
			return (state && Object.hasOwn(QUEUE_PHASES, state) ? QUEUE_PHASES[state].label : void 0) ?? (node.status === "superseded" ? supersededLabel(node) : STEP_STATUS.get(node.status)) ?? (/\p{Script=Han}/u.test(node.status_label) ? operatorReason(node.status_label) : operatorStatus(node.status));
		}
		/** Only the persisted execution projection identifies which predecessors are still blocking a node. */
		function waitingFor(node, titles) {
			if (!node.waiting_on?.length) return void 0;
			const names = [...new Set(node.waiting_on)].map((id) => titles.get(id) || "未命名步骤").join("、");
			if (node.execution_state === "waiting_dependencies") return `等待：${names}`;
			if (node.execution_state === "blocked_dependencies") return `上游未成功：${names}`;
			return `待核对的上游：${names}`;
		}
		// src/client/panel/rework-panel.tsx
		const labels = {
			proposed: "等待授权",
			evaluating: "正在评估",
			needs_human_approval: "等待人工授权",
			authorized: "已授权",
			scheduled: "等待返工",
			executing: "正在返工",
			awaiting_review: "等待重新验收",
			accepted: "验收通过",
			closed: "已完成",
			declined: "未批准",
			escalated: "等待人工处理",
			cancelled: "已取消"
		};
		const scopes = {
			targeted_revision: "针对性修改",
			rerun_node: "重新执行当前节点",
			replan_subgraph: "局部重规划"
		};
		function ReworkPanel({ api, commands, runId, reloadVersion = 0, plannerEnabled = false, onSnapshot, compact = false }) {
			const [items, setItems] = (0, react.useState)([]);
			const [failed, setFailed] = (0, react.useState)(false);
			const [loading, setLoading] = (0, react.useState)(true);
			const [retry, setRetry] = (0, react.useState)(0);
			(0, react.useEffect)(() => {
				let current = true;
				setLoading(true);
				setFailed(false);
				api.listRework(runId).then(({ data, as_of_seq }) => {
					if (current) {
						setItems(data.items);
						onSnapshot?.(as_of_seq);
					}
				}).catch(() => {
					if (current) setFailed(true);
				}).finally(() => {
					if (current) setLoading(false);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				runId,
				reloadVersion,
				retry,
				onSnapshot
			]);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-label": "成果审核与返工",
				style: box,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
						style: {
							fontSize: FONT_PX.section,
							margin: "0 0 8px"
						},
						children: "成果审核与返工"
					}),
					loading && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取返工记录…"
					}),
					failed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: "alert",
						children: ["返工记录读取失败。", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							onClick: () => setRetry((n) => n + 1),
							children: "重试"
						})]
					}),
					!loading && !failed && items.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "暂无返工记录。" }),
					items.filter((item) => !runId || item.run_id === runId).map((item) => compact ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
						labels[item.status] ?? operatorStatus(item.status),
						" · 第 ",
						item.round_no,
						" 轮 · ",
						item.reason
					] }, item.request_id) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReworkCard, {
						api,
						item,
						commands,
						plannerEnabled,
						onChanged: () => setRetry((n) => n + 1)
					}, item.request_id + ":" + item.revision)),
					runId && commands && !compact && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReworkRequestForm, {
						api,
						runId,
						commands,
						plannerEnabled,
						onChanged: () => setRetry((n) => n + 1)
					}, runId)
				]
			});
		}
		function errorText(error) {
			return error instanceof Error ? operatorReason(error.message) : "操作未完成";
		}
		function artifactVersionLabel(artifact, index) {
			return `${artifact.file_name || "未命名产物"} · 登记记录 ${index + 1} · ${new Date(artifact.created_at).toLocaleString()}`;
		}
		function receiptStatus(status) {
			if (status === "applied") return "操作已生效";
			if (status === "received") return "已收件，等待处理";
			return operatorStatus(status);
		}
		function ReworkCard({ api, item, commands, plannerEnabled, onChanged }) {
			const canWrite = usePanelCommandsEnabled(true);
			const [note, setNote] = (0, react.useState)("");
			const [busy, setBusy] = (0, react.useState)(false);
			const [notice, setNotice] = (0, react.useState)("");
			const [unknownOperation, setUnknownOperation] = (0, react.useState)();
			const [artifacts, setArtifacts] = (0, react.useState)([]);
			const [history, setHistory] = (0, react.useState)();
			const [plan, setPlan] = (0, react.useState)(null);
			const [terminationConfirm, setTerminationConfirm] = (0, react.useState)(false);
			(0, react.useEffect)(() => {
				let live = true;
				api.listRunArtifacts(item.run_id).then(({ data }) => {
					if (live) setArtifacts(data.items);
				}).catch(() => {
					if (live) setNotice("成果列表读取失败，可刷新后重试");
				});
				api.getRework(item.request_id).then(({ data }) => {
					if (live) setHistory(data.item);
				}).catch(() => {
					if (live) setNotice("审核历史读取失败，可刷新后重试");
				});
				if (plannerEnabled && commands) commands.getPlan(item.request_id).then(({ data }) => {
					if (live) setPlan(data.plan);
				}).catch(() => {});
				return () => {
					live = false;
				};
			}, [
				api,
				item.request_id,
				item.run_id,
				commands,
				plannerEnabled
			]);
			const old = artifacts.find((a) => a.run_id === item.run_id && a.artifact_id === item.source_artifact_id && a.sha256 === item.source_artifact_hash);
			const currentAttempt = history?.attempts.find((a) => a.round_no === item.round_no);
			const review = item.status === "awaiting_review";
			const candidates = review && !history ? [] : artifacts.filter((a) => a.run_id === item.run_id && a.source_node_run_id === item.new_node_run_id && (!currentAttempt?.result_artifact_id || a.artifact_id === currentAttempt.result_artifact_id && a.sha256 === currentAttempt.result_artifact_hash));
			const [selected, setSelected] = (0, react.useState)("");
			const revised = candidates.find((a) => a.artifact_id === selected) ?? candidates[0];
			const oldRetention = useArtifactRetention(old);
			const revisedRetention = useArtifactRetention(revised);
			const left = old && oldRetention === "available" && old.file_name ? old : void 0;
			const right = revised && revisedRetention === "available" && revised.file_name ? revised : void 0;
			const comparisonKey = JSON.stringify([left && artifactContentKey(left), right && artifactContentKey(right)]);
			const [comparison, setComparison] = (0, react.useState)({ key: comparisonKey });
			if (comparison.key !== comparisonKey) setComparison({ key: comparisonKey });
			const interactionBusy = busy || comparison.pending;
			async function act(action) {
				if (!canWrite || !commands) return;
				setBusy(true);
				setNotice("");
				try {
					await commands.review({
						request_id: item.request_id,
						expected_revision: item.revision,
						action,
						note: note.trim() || "人工验收通过",
						...revised ? {
							artifact_id: revised.artifact_id,
							artifact_hash: revised.sha256
						} : {}
					});
					setNotice(action === "reject_final" ? "已记录终止请求；未确认的执行和副作用继续按原协议核实。" : "已记录处理结果");
					onChanged();
				} catch (error) {
					setNotice(errorText(error));
					if (error instanceof ApiError && error.code === "OPERATION_OUTCOME_UNKNOWN") setUnknownOperation(error.details.operation_id);
				} finally {
					setBusy(false);
				}
			}
			async function planAction(approve = false) {
				if (!canWrite || !commands) return;
				setBusy(true);
				try {
					if (approve && plan) await commands.approvePlan({
						request_id: item.request_id,
						expected_revision: item.revision,
						plan_hash: plan.plan_hash
					});
					else await commands.plan({
						request_id: item.request_id,
						expected_revision: item.revision
					});
					setPlan((await commands.getPlan(item.request_id)).data.plan);
					onChanged();
				} catch (error) {
					setNotice(errorText(error));
					if (error instanceof ApiError && error.code === "OPERATION_OUTCOME_UNKNOWN") setUnknownOperation(error.details.operation_id);
				} finally {
					setBusy(false);
				}
			}
			async function compare() {
				if (!left || !right) return;
				const reading = {
					key: comparisonKey,
					pending: true
				};
				setComparison(reading);
				try {
					const [{ data: before }, { data: after }] = await Promise.all([api.getArtifactContent(left.run_id, left.artifact_id, "preview"), api.getArtifactContent(right.run_id, right.artifact_id, "preview")]);
					if (before.kind !== "preview" || after.kind !== "preview" || !matchesArtifactContent(before, left) || !matchesArtifactContent(after, right)) throw new Error("成果版本已变化，请刷新");
					const result = {
						before: before.text,
						after: after.text,
						truncated: before.truncated || after.truncated
					};
					setComparison((current) => current === reading ? {
						key: comparisonKey,
						result
					} : current);
				} catch (error) {
					setComparison((current) => current === reading ? {
						key: comparisonKey,
						notice: errorText(error)
					} : current);
				}
			}
			const pending = ["proposed", "needs_human_approval"].includes(item.status);
			const escalated = item.status === "escalated";
			const mayDecline = (pending || escalated) && history?.attempts.length === 0;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("article", {
				style: box,
				"aria-label": `第 ${item.round_no} 轮返工`,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h3", { children: [
						labels[item.status] ?? operatorStatus(item.status),
						" · 第 ",
						item.round_no,
						" / ",
						item.max_rounds,
						" 轮"
					] }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
						"原步骤的第 ",
						item.target_attempt,
						" 次尝试",
						old?.file_name && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [" · ", old.file_name] })
					] }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: item.reason }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["期望结果：", item.expected_result] }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["范围：", scopes[item.suggested_scope] ?? "范围未确认"] }),
					history?.dispatch_blocks?.filter((block) => !block.resolved_at).map((block) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
						"aria-label": "自动派发受阻",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h4", { children: [block.node_id, " 的自动派发已暂停"] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: operatorReason(block.message || block.reason) }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看暂停记录" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["原因代码 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: block.reason })] })] })
						]
					}, block.node_run_id)),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "修改前后成果与版本" }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							style: {
								display: "grid",
								gridTemplateColumns: "repeat(auto-fit,minmax(220px,1fr))",
								gap: 12
							},
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h4", { children: "修改前" }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: old?.file_name ?? "原成果记录" }),
								old && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ArtifactContent, {
									api,
									artifact: old
								})
							] }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h4", { children: "修订成果" }),
								candidates.length > 1 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["选择修订成果", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("select", {
									value: revised?.artifact_id ?? "",
									onChange: (e) => setSelected(e.target.value),
									children: candidates.map((a, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: a.artifact_id,
										children: artifactVersionLabel(a, index)
									}, a.artifact_id))
								})] }),
								revised ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: revised.file_name || "未命名产物" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ArtifactContent, {
									api,
									artifact: revised
								})] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "尚无修订成果。" })
							] })]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "请核对选中的修订文件。本次验收会对应这一份文件。" }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看成果版本记录" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
								"原成果编号 ",
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: item.source_artifact_id }),
								" · SHA-256 ",
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: item.source_artifact_hash })
							] }),
							revised && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
								"修订成果编号 ",
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: revised.artifact_id }),
								" · SHA-256 ",
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: revised.sha256 })
							] })
						] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							disabled: interactionBusy || !left || !right,
							onClick: () => void compare(),
							children: "逐行对比修改"
						}),
						comparison.key === comparisonKey && comparison.notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "status",
							children: comparison.notice
						}),
						left && right && comparison.key === comparisonKey && comparison.result && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [comparison.result.truncated && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "仅对比内容预览，完整成果请分别下载核对。" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(LineComparison, {
							before: comparison.result.before,
							after: comparison.result.after
						})] })
					] }),
					history?.impacts && history.impacts.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h4", { children: "受影响步骤" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: history.impacts.map((i) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
						i.node_id,
						" · ",
						i.released_at ? "已恢复" : "等待返工或验收",
						i.replacement_node_run_id && " · 已有后续执行"
					] }, i.node_run_id)) })] }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "返工与验收历史" }), history ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("ol", { children: history.attempts.map((a) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
						"第 ",
						a.round_no,
						" 轮 · ",
						labels[a.status] ?? a.status,
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: a.feedback }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
							a.source_artifact_id,
							" → ",
							a.result_artifact_id ?? "等待修订成果"
						] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: a.created_at })
					] }, a.attempt_id)) }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ol", { children: history.reviews.map((r) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
						"第 ",
						r.round_no,
						" 轮 · ",
						r.reviewer_actor,
						" · ",
						r.action === "accept" ? "验收通过" : r.action === "request_changes" ? "要求修改" : r.action === "authorize" ? "授权返工" : r.action === "reject" ? "不批准" : "升级处理",
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: r.note }),
						r.artifact_id && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
							"验收版本 ",
							r.artifact_id,
							" · ",
							r.artifact_hash
						] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: r.created_at })
					] }, r.review_id)) })] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "尚未读取历史。" })] }),
					commands && (pending || review || escalated) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("fieldset", {
						disabled: !canWrite || interactionBusy || Boolean(unknownOperation),
						style: {
							border: 0,
							padding: 0
						},
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
								style: field$1,
								children: ["具体修改意见或处理说明", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
									value: note,
									onChange: (e) => setNote(e.target.value),
									rows: 3
								})]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								style: actions$1,
								children: [
									pending && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										disabled: !note.trim() || item.suggested_scope === "replan_subgraph",
										onClick: () => void act("authorize"),
										children: "授权返工"
									}),
									review && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										disabled: !revised || !note.trim(),
										onClick: () => void act("accept"),
										children: "通过验收"
									}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										disabled: !note.trim(),
										onClick: () => void act("request_changes"),
										children: "要求修改"
									})] }),
									!escalated && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										disabled: !note.trim(),
										onClick: () => void act("escalate"),
										children: "升级处理"
									}),
									mayDecline && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										disabled: !note.trim(),
										onClick: () => void act("reject"),
										children: "不批准返工"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										disabled: !note.trim(),
										onClick: () => setTerminationConfirm(true),
										children: "终止本次运行"
									})
								]
							}),
							terminationConfirm && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								role: "group",
								"aria-label": "确认终止运行",
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "此操作终止整个当前运行，并保留全部成果及未确认副作用。确定继续？" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										disabled: !note.trim(),
										onClick: () => void act("reject_final"),
										children: "确认终止运行"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										onClick: () => setTerminationConfirm(false),
										children: "继续审核"
									})
								]
							}),
							escalated && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "此反馈等待人工处理。未启动返工可不批准；已执行的返工保留依赖保护，可明确终止当前运行。" }),
							plannerEnabled && pending && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: () => void planAction(),
								children: "请主脑提出局部方案"
							}),
							plan && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h4", { children: "主脑候选方案" }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: plan.proposal.reason }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: plan.proposal.changes.map((c) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
									c.node_id,
									"：",
									c.instruction
								] }, c.node_id)) }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["保留节点：", plan.proposal.preserved_node_ids.join("、") || "无"] }),
								Boolean(plan.proposal.additional_edges?.length) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: ["新增执行依赖", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: plan.proposal.additional_edges?.map((e) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
									e.from,
									" 完成后执行 ",
									e.to
								] }, e.from + "/" + e.to)) })] }),
								Boolean(plan.proposal.insert_steps?.length) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
									"aria-label": "候选新增步骤",
									children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h4", { children: "新增步骤（授权后执行）" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: plan.proposal.insert_steps?.map((step) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
										step.node_id,
										" · 插入 ",
										step.before_node_id,
										" 之前 · 来源角色：",
										step.before_node_id,
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: step.instruction })
									] }, step.node_id)) })]
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看方案版本记录" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", {
									style: hash,
									children: plan.plan_hash
								})] }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									disabled: plan.revision !== item.revision,
									onClick: () => void planAction(true),
									children: "授权此方案"
								})
							] })
						]
					}),
					notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: notice
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看返工登记明细" }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
							"请求编号 ",
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: item.request_id }),
							" · 原步骤尝试编号 ",
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: item.target_node_run_id }),
							" · 提出方 ",
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: item.requester_actor })
						] }),
						unknownOperation && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["回执编号 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: unknownOperation })] })
					] }),
					unknownOperation && commands && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						disabled: interactionBusy,
						onClick: async () => {
							setBusy(true);
							try {
								const { data } = await commands.getOperation(unknownOperation);
								setNotice(`回执状态：${receiptStatus(data.status)}`);
								if (data.status === "applied") {
									setUnknownOperation(void 0);
									onChanged();
								}
							} catch (e) {
								setNotice(errorText(e));
							} finally {
								setBusy(false);
							}
						},
						children: "查询原操作回执"
					})
				]
			});
		}
		function ReworkRequestForm({ api, commands, runId, onChanged, plannerEnabled }) {
			const canWrite = usePanelCommandsEnabled(true);
			const [nodes, setNodes] = (0, react.useState)([]);
			const [artifacts, setArtifacts] = (0, react.useState)([]);
			const [holds, setHolds] = (0, react.useState)([]);
			const [refresh, setRefresh] = (0, react.useState)(0);
			const [nodeId, setNodeId] = (0, react.useState)("");
			const [artifactId, setArtifactId] = (0, react.useState)("");
			const [reason, setReason] = (0, react.useState)("");
			const [expected, setExpected] = (0, react.useState)("");
			const [preserve, setPreserve] = (0, react.useState)("");
			const [scope, setScope] = (0, react.useState)("targeted_revision");
			const [busy, setBusy] = (0, react.useState)(false);
			const [notice, setNotice] = (0, react.useState)("");
			const [unknownOperation, setUnknownOperation] = (0, react.useState)();
			const [inspectionCriteria, setInspectionCriteria] = (0, react.useState)("");
			const [inspections, setInspections] = (0, react.useState)([]);
			(0, react.useEffect)(() => {
				let live = true;
				Promise.all([
					api.listNodes(runId, 200),
					api.listRunArtifacts(runId),
					commands.listHolds(runId)
				]).then(([n, a, h]) => {
					if (live) {
						setNodes(n.data.items.filter((node) => node.node_type === "agent" && !node.replacement_node_run_id));
						setArtifacts(a.data.items);
						setHolds(h.data.items);
					}
				}).catch((e) => {
					if (live) setNotice(errorText(e));
				});
				if (plannerEnabled) commands.listInspections(runId).then(({ data }) => {
					if (live) setInspections(data.items ?? []);
				}).catch((e) => {
					if (live) setNotice(errorText(e));
				});
				return () => {
					live = false;
				};
			}, [
				api,
				commands,
				runId,
				refresh,
				plannerEnabled
			]);
			const node = nodes.find((n) => n.node_run_id === nodeId);
			const artifact = artifacts.find((a) => a.artifact_id === artifactId && a.source_node_run_id === nodeId);
			const held = holds.some((h) => h.node_id === node?.node_id && h.status === "held");
			async function inspect() {
				if (!canWrite || !node || !artifact) return;
				setBusy(true);
				setNotice("");
				try {
					const result = await commands.inspect({
						run_id: runId,
						target_node_run_id: node.node_run_id,
						source_artifact_id: artifact.artifact_id,
						source_artifact_hash: artifact.sha256,
						expected_revision: node.revision,
						criteria: inspectionCriteria.trim()
					});
					setNotice((result.verdict === "pass" ? "主脑检查意见：" : "已建立主脑反馈：") + result.summary);
					setRefresh((n) => n + 1);
					onChanged();
				} catch (e) {
					setNotice(errorText(e));
					if (e instanceof ApiError && e.code === "OPERATION_OUTCOME_UNKNOWN") setUnknownOperation(e.details.operation_id);
				} finally {
					setBusy(false);
				}
			}
			async function submit(action) {
				if (!canWrite || !node) return;
				setBusy(true);
				setNotice("");
				try {
					if (action === "request" && artifact) await commands.request({
						run_id: runId,
						target_node_run_id: nodeId,
						source_artifact_id: artifact.artifact_id,
						source_artifact_hash: artifact.sha256,
						expected_revision: node.revision,
						reason: reason.trim(),
						expected_result: expected.trim(),
						preserve_requirements: preserve.split("\n").map((s) => s.trim()).filter(Boolean),
						suggested_scope: scope,
						evidence_ids: []
					});
					else if (action !== "request") await commands.hold({
						run_id: runId,
						node_run_id: nodeId,
						action,
						...action === "accept" && artifact ? {
							artifact_id: artifact.artifact_id,
							sha256: artifact.sha256
						} : {}
					});
					setNotice("已保存");
					setRefresh((n) => n + 1);
					setHolds((await commands.listHolds(runId)).data.items);
					onChanged();
				} catch (e) {
					setNotice(errorText(e));
					if (e instanceof ApiError && e.code === "OPERATION_OUTCOME_UNKNOWN") setUnknownOperation(e.details.operation_id);
				} finally {
					setBusy(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
				/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "审核成果或提出修改" }),
				/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("fieldset", {
					disabled: busy || Boolean(unknownOperation),
					style: {
						border: 0,
						padding: 0
					},
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
							style: field$1,
							children: ["来源节点", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
								value: nodeId,
								onChange: (e) => {
									setNodeId(e.target.value);
									setArtifactId("");
								},
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
									value: "",
									children: "选择节点"
								}), nodes.map((n) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
									value: n.node_run_id,
									children: [
										n.title || n.node_id,
										" · 第 ",
										n.attempt,
										" 次 · ",
										nodeExecutionLabel(n)
									]
								}, n.node_run_id))]
							})]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
							style: field$1,
							children: ["成果版本", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
								value: artifactId,
								onChange: (e) => setArtifactId(e.target.value),
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
									value: "",
									children: "选择成果"
								}), artifacts.filter((a) => a.source_node_run_id === nodeId).map((a, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
									value: a.artifact_id,
									children: artifactVersionLabel(a, index)
								}, a.artifact_id))]
							})]
						}),
						artifact && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ArtifactContent, {
							api,
							artifact
						}),
						artifact && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看所选成果版本记录" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
							"成果编号 ",
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: artifact.artifact_id }),
							" · SHA-256 ",
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: artifact.sha256 })
						] })] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							style: actions$1,
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									disabled: !canWrite || !node || held,
									onClick: () => void submit("hold"),
									children: "等待人工验收后结束"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									disabled: !canWrite || !held || !artifact,
									onClick: () => void submit("accept"),
									children: "通过成果验收"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									disabled: !canWrite || !held,
									onClick: () => void submit("withdraw"),
									children: "撤销留验"
								})
							]
						}),
						held && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "status",
							children: "此节点等待人工验收，运行暂不结束。"
						}),
						plannerEnabled && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
							"aria-label": "主脑成果检查",
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
									style: field$1,
									children: ["主脑检查标准", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
										value: inspectionCriteria,
										onChange: (e) => setInspectionCriteria(e.target.value),
										rows: 2,
										maxLength: 2500
									})]
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									disabled: !canWrite || !artifact || !node || node.status !== "done" || !inspectionCriteria.trim(),
									onClick: () => void inspect(),
									children: "请主脑检查成果"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "主脑通过意见只留存记录；发现缺陷后按返工策略建立反馈。检查完整 UTF-8 文本，最大 64 KiB。" }),
								inspections.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "主脑检查记录" }), inspections.map((item) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("article", { children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
										item.verdict === "pass" ? "通过意见" : "要求修改",
										" · ",
										item.created_at,
										" · ",
										item.artifact_id
									] }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", {
										style: hash,
										children: item.artifact_hash
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: item.summary }),
									item.request_id && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["反馈编号：", item.request_id] })
								] }, item.inspection_id))] })
							]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
							style: field$1,
							children: ["具体修改意见", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
								value: reason,
								onChange: (e) => setReason(e.target.value),
								rows: 3
							})]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
							style: field$1,
							children: ["期望结果", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
								value: expected,
								onChange: (e) => setExpected(e.target.value),
								rows: 2
							})]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
							style: field$1,
							children: ["保留要求（每行一条）", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
								value: preserve,
								onChange: (e) => setPreserve(e.target.value),
								rows: 2
							})]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
							style: field$1,
							children: ["修改方式", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
								value: scope,
								onChange: (e) => setScope(e.target.value),
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "targeted_revision",
										children: "针对性修改"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "rerun_node",
										children: "重新执行当前节点"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "replan_subgraph",
										children: "提交局部重规划需求"
									})
								]
							})]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							disabled: !canWrite || !artifact || !node || node.status !== "done" || !reason.trim() || !expected.trim(),
							onClick: () => void submit("request"),
							children: "要求修改"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "意见提交后等待授权，旧成果与执行记录保留。" })
					]
				}),
				notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					role: "status",
					children: notice
				}),
				unknownOperation && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
					type: "button",
					onClick: async () => {
						try {
							const { data } = await commands.getOperation(unknownOperation);
							setNotice(`回执状态：${receiptStatus(data.status)}`);
							if (data.status === "applied") {
								setUnknownOperation(void 0);
								onChanged();
							}
						} catch (e) {
							setNotice(errorText(e));
						}
					},
					children: "查询原操作回执"
				})
			] });
		}
		function LineComparison({ before, after }) {
			const oldLines = before.split("\n"), newLines = after.split("\n");
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
				style: {
					overflowX: "auto",
					maxHeight: 360
				},
				children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("table", {
					"aria-label": "修改前后逐行对比",
					style: {
						width: "100%",
						borderCollapse: "collapse"
					},
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("thead", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
							scope: "col",
							children: "行"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
							scope: "col",
							children: "修改前"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
							scope: "col",
							children: "修订后"
						})
					] }) }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("tbody", { children: Array.from({ length: Math.max(oldLines.length, newLines.length) }, (_, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
							scope: "row",
							children: index + 1
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", {
							style: {
								whiteSpace: "pre-wrap",
								background: oldLines[index] !== newLines[index] ? "#fff0ed" : void 0
							},
							children: oldLines[index] ?? ""
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", {
							style: {
								whiteSpace: "pre-wrap",
								background: oldLines[index] !== newLines[index] ? "#edf8ee" : void 0
							},
							children: newLines[index] ?? ""
						})
					] }, index)) })]
				})
			});
		}
		const box = {
			padding: 14,
			margin: "12px 0",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 8,
			overflowWrap: "anywhere"
		};
		const field$1 = {
			display: "grid",
			gap: 5,
			margin: "10px 0"
		};
		const actions$1 = {
			display: "flex",
			flexWrap: "wrap",
			gap: 8,
			margin: "10px 0"
		};
		const hash = {
			display: "block",
			overflowWrap: "anywhere",
			fontSize: FONT_PX.helper
		};
		// src/executor/model-routing.ts
		const MODEL_COMPLEXITIES = [
			"routine",
			"standard",
			"advanced",
			"complex",
			"expert"
		];
		const MODEL_LEVELS = {
			routine: {
				label: "常规",
				description: "规则明确的整理、提取或格式转换"
			},
			standard: {
				label: "中等",
				description: "根据少量资料做独立分析或核对"
			},
			advanced: {
				label: "进阶",
				description: "串联多项依赖或综合多份证据"
			},
			complex: {
				label: "复杂",
				description: "处理难以验证的证据、冲突判断或较高的失败代价"
			},
			expert: {
				label: "专家",
				description: "解决高度不确定的跨领域问题，或作出高代价的关键判断"
			}
		};
		// src/client/panel/task-workspace-styles.ts
		const TASK_WORKSPACE_CSS = `
.task-workspace { min-width: 0; }
.task-list { display: grid; gap: 16px; min-width: 0; }
.task-page-header { display: flex; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; gap: 8px; }
.task-launch-form { display: flex; align-items: end; flex-wrap: wrap; gap: 8px; border: 1px solid ${COLOR.line}; border-radius: 8px; padding: 10px; }
.task-launch-form label { display: grid; gap: 3px; font-size: ${FONT_PX.menu}px; }
.task-launch-form input, .task-launch-form select, .task-launch-form button { max-width: 100%; min-height: 32px; padding: 4px 8px; font: inherit; background: ${COLOR.surface}; border: 1px solid ${COLOR.line}; border-radius: 5px; }
.task-launch-form p { margin: 0; flex-basis: 100%; color: ${COLOR.inkMuted}; }
.task-toolbar { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; padding: 8px 10px; border: 1px solid ${COLOR.line}; border-radius: 8px; color: ${COLOR.inkMuted}; font-size: ${FONT_PX.menu}px; }
.task-toolbar > span { margin-right: auto; }
.task-toolbar label, .task-date-range { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.task-toolbar select, .task-toolbar input[type=date], .task-view-group button, .task-flow-view button { padding: 6px 9px; background: ${COLOR.surface}; border: 1px solid ${COLOR.line}; border-radius: 6px; color: ${COLOR.ink}; font: inherit; }
.task-archive-toggle { margin-left: auto; }
.task-view-group { display: flex; flex-wrap: wrap; gap: 4px; }
.task-view-group button, .task-flow-view button { cursor: pointer; }
.task-view-group [aria-pressed=true], .task-flow-picker [aria-pressed=true] { background: #eff3f8; border-color: #c2d0df; font-weight: 650; }
.task-search { display: flex; gap: 6px; min-width: 0; max-width: 100%; }
.task-search input { box-sizing: border-box; max-width: 100%; }
.task-board-scroll { container-type: inline-size; overflow-x: auto; min-width: 0; padding: 1px 1px 5px; }
.task-board { display: grid; grid-template-columns: repeat(4, minmax(140px, 1fr)); gap: 9px; align-items: stretch; }
.task-column { min-width: 0; min-height: 258px; display: flex; flex-direction: column; gap: 8px; padding: 9px; background: #f7f8fa; border: 1px solid ${COLOR.line}; border-radius: 10px; }
.task-column-title { display: flex; align-items: center; justify-content: space-between; gap: 5px; min-height: 27px; margin: 0 0 1px; padding-bottom: 7px; border-bottom: 1px solid ${COLOR.line}; font-size: ${FONT_PX.body}px; }
.task-column-title span { min-width: 20px; text-align: center; padding: 0 3px; border: 1px solid ${COLOR.line}; border-radius: 5px; color: ${COLOR.inkMuted}; background: ${COLOR.surface}; font-size: ${FONT_PX.helper}px; font-variant-numeric: tabular-nums; }
.task-column-empty { margin: 12px 0; text-align: center; color: ${COLOR.inkMuted}; font-size: ${FONT_PX.helper}px; }
.task-card { position: relative; min-width: 0; border: 1px solid ${COLOR.line}; border-radius: 8px; padding: 10px; background: ${COLOR.surface}; overflow-wrap: anywhere; }
.task-card:hover { border-color: #92a8c0; }
.task-card[data-selected] { background: #f1f6fd; border-color: #7f9ebf; }
.task-card-title { margin: 7px 0 5px; }
.task-card-title button::after { content: ''; position: absolute; inset: 0; border-radius: 8px; }
.task-card-title button:focus-visible::after { outline: 2px solid ${COLOR.link}; outline-offset: 1px; }
.task-card-status, .task-card-meta { display: flex; align-items: center; flex-wrap: wrap; gap: 4px 9px; color: ${COLOR.inkMuted}; font-size: ${FONT_PX.helper}px; }
.task-card-status { justify-content: space-between; }
.task-card-status > :first-child { padding: 1px 5px; border-radius: 4px; background: #f2f4f6; font-weight: 650; }
.task-card-reason { margin: 5px 0 0; font-size: ${FONT_PX.menu}px; color: ${COLOR.inkMuted}; }
.task-card-type { margin: 3px 0; font-size: ${FONT_PX.helper}px; color: ${COLOR.inkMuted}; }
.task-card-attention { display: grid; gap: 2px; margin-top: 8px; padding: 5px 6px; border: 1px solid #eee2cc; border-radius: 5px; background: #fff9ee; color: ${COLOR.warning}; font-size: ${FONT_PX.helper}px; }
.task-card-meta { margin-top: 8px; padding-top: 7px; border-top: 1px solid ${COLOR.line}; }
.task-card-steps { position: relative; z-index: 1; margin-top: 8px; }
.task-card-steps button:focus-visible { outline: 2px solid ${COLOR.link}; outline-offset: 2px; }
.task-board-step-groups details { border-top: 1px solid ${COLOR.line}; }
.task-board-step-groups summary { padding: 6px 0; cursor: pointer; overflow-wrap: anywhere; }
.task-board-step-groups summary:focus-visible { outline: 2px solid ${COLOR.link}; outline-offset: 2px; }
.task-board-step-groups summary small { display: block; color: ${COLOR.inkMuted}; font-size: ${FONT_PX.helper}px; }
.task-board-step-groups details ul { margin-inline-start: 10px; }
.task-step-overview { display: grid; gap: 0; list-style: none; margin: 0; padding: 0; }
.task-step-overview li { display: flex; flex-wrap: wrap; align-items: baseline; gap: 2px 7px; min-width: 0; padding: 6px 0; border-top: 1px solid ${COLOR.line}; overflow-wrap: anywhere; }
.task-step-overview li > * { min-width: 0; max-width: 100%; }
.task-step-overview-status, .task-step-overview small { color: ${COLOR.inkMuted}; font-size: ${FONT_PX.helper}px; }
.task-step-overview [data-tone=active] .task-step-overview-status { color: ${COLOR.link}; }
.task-step-overview [data-tone=blocked] .task-step-overview-status { color: ${COLOR.warning}; }
.task-step-overview [data-tone=failed] .task-step-overview-status { color: ${COLOR.danger}; }
.task-step-waiting { flex-basis: 100%; }
.task-card-steps > div { margin-top: 6px; }
.task-queue-groups { display: grid; gap: 10px; }
.task-queue-groups h3 { margin: 0; font-size: ${FONT_PX.body}px; }
.task-queue-groups .task-step-overview li { padding: 7px 12px; }
@container (max-width: 720px) { .task-board { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@container (max-width: 420px) { .task-board { grid-template-columns: minmax(0, 1fr); } .task-column { min-height: 0; } }
.task-table-scroll { overflow-x: auto; border: 1px solid ${COLOR.line}; border-radius: 9px; }
.task-table { width: 100%; min-width: 530px; border-collapse: collapse; font-size: ${FONT_PX.menu}px; text-align: left; }
.task-table th, .task-table td { padding: 10px 12px; border-bottom: 1px solid ${COLOR.line}; overflow-wrap: anywhere; }
.task-table th { background: #f5f7f9; }
.task-table tr[data-selected] { background: #f1f6fd; }
.task-table small { display: block; font-size: ${FONT_PX.helper}px; color: ${COLOR.inkMuted}; }
.task-link { border: 0; padding: 0; color: ${COLOR.link}; background: transparent; font: inherit; font-weight: 650; cursor: pointer; text-align: left; }
.task-page-actions { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; }
.task-plan-entry { display: grid; justify-items: start; min-width: 0; }
.task-plan-entry > details[open] { width: min(100%, 720px); box-sizing: border-box; }
.task-plan-entry summary { cursor: pointer; }
.task-flow-view { display: grid; grid-template-columns: minmax(156px, 22%) minmax(0, 1fr); gap: 14px; align-items: start; container-type: inline-size; }
.task-flow-main { min-width: 0; display: grid; gap: 12px; }
.task-flow-picker, .task-flow-run-head, .task-flow-graph { min-width: 0; border: 1px solid ${COLOR.line}; border-radius: 10px; background: ${COLOR.surface}; }
.task-flow-picker { display: grid; overflow: auto; max-height: calc(100dvh - 40px); }
.task-flow-picker button.task-flow-pick { min-width: 0; display: grid; gap: 4px; text-align: left; overflow-wrap: anywhere; padding: 10px 12px; border: 0; border-top: 1px solid ${COLOR.line}; border-radius: 0; }
.task-flow-pick > span:last-child, .task-flow-pick-meta, .task-flow-section-head span, .task-run-facts dt { color: ${COLOR.inkMuted}; font-size: ${FONT_PX.menu}px; }
.task-flow-pick-meta { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 5px 8px; }
.task-type-badge { display: inline-flex; padding: 1px 6px; align-items: center; background: #eef2f7; color: #34465f; border-radius: 5px; }
.task-flow-section-head { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 6px; padding: 10px 12px; }
.task-flow-run-head { display: grid; justify-items: start; gap: 6px; padding: 15px 17px; }
.task-flow-run-head h2 { margin: 0; font-size: ${FONT_PX.title}px; line-height: 1.3; }
.task-flow-run-head p { margin: 0; color: ${COLOR.inkMuted}; }
.task-run-facts { display: flex; flex-wrap: wrap; gap: 7px 16px; margin: 0; padding: 7px 0; }
.task-run-facts dd { margin: 0; font-weight: 600; }
.task-flow-empty { margin: 0; padding: 9px 12px; overflow-wrap: anywhere; color: ${COLOR.inkMuted}; }
.task-flow-empty button { margin-inline-start: 7px; }
.task-quiet-button { min-height: 33px; padding: 5px 10px; border-radius: 6px; border: 1px solid ${COLOR.line}; background: ${COLOR.surface}; color: ${COLOR.ink}; font: inherit; cursor: pointer; }
.task-flow-pick:focus-visible { outline: 2px solid ${COLOR.link}; outline-offset: -2px; }
.task-quiet-button:hover { border-color: #91a5be; }
.task-flow-graph { overflow: hidden; }
.task-flow-graph > [data-canvas-root] { min-width: 0; max-width: 100%; grid-template-columns: minmax(0, 1fr); }
.task-flow-graph summary { padding: 10px 12px; cursor: pointer; font-weight: 600; }
.task-flow-graph[open] { padding-bottom: 8px; }
@media (max-width: 900px) {
  .task-flow-view { grid-template-columns: minmax(0, 1fr); }
  .task-flow-picker { display: flex; max-height: none; }
  .task-flow-picker > header { display: none; }
  .task-flow-picker button.task-flow-pick { min-width: 174px; border-top: 0; border-right: 1px solid ${COLOR.line}; }
}
`;
		const TASK_RUN_STEPS_CSS = `
.task-run-steps {
  container: task-run-steps / inline-size; min-width: 0; align-self: start;
  border: 1px solid ${COLOR.line}; border-radius: 9px; background: ${COLOR.surface};
}
.task-run-steps-head {
  display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 10px;
  padding: 9px 12px; border-bottom: 1px solid ${COLOR.line};
}
.task-run-steps-head h3, .task-run-steps-head p { margin: 0; }
.task-run-steps-head h3 { font-size: ${FONT_PX.section}px; }
.task-run-steps-head p, .task-run-steps-head > span { color: ${COLOR.inkMuted}; font-size: ${FONT_PX.menu}px; }
.task-run-steps-head > span { margin-inline-start: auto; }
.task-step-totals { display: flex; flex-wrap: wrap; gap: 4px 10px; margin: 0; padding: 6px 12px; list-style: none; border-bottom: 1px solid ${COLOR.line}; }
.task-step-totals li { font-size: ${FONT_PX.menu}px; color: ${COLOR.inkMuted}; }
.task-step-totals [data-tone=blocked], .task-step-head [data-tone=blocked], .task-flow-step[data-tone=blocked] .task-flow-step-result { color: ${COLOR.warning}; }
.task-step-totals [data-tone=active], .task-step-head [data-tone=active], .task-flow-step[data-tone=active] .task-flow-step-result { color: ${COLOR.link}; }
.task-step-head [data-tone=failed], .task-flow-step[data-tone=failed] .task-flow-step-result { color: ${COLOR.danger}; }
.task-step-head [data-tone=done] { color: ${COLOR.success}; }
.task-run-grid { min-width: 0; display: grid; grid-template-columns: minmax(0, 40%) minmax(0, 1fr); align-items: start; }
.task-run-grid[data-single=true] { grid-template-columns: minmax(0, 1fr); }
.task-flow-steps {
  margin: 0; padding: 0; list-style: none; min-width: 0; max-block-size: min(35vh, 350px);
  overflow: auto; scrollbar-gutter: stable; border-inline-end: 1px solid ${COLOR.line};
}
.task-flow-trigger-record { margin: 0; padding: 6px 12px; border-bottom: 1px solid ${COLOR.line}; color: ${COLOR.inkMuted}; }
.task-flow-steps .task-flow-step {
  display: grid; grid-template-columns: 25px minmax(0, 1fr) fit-content(40%); align-items: center;
  min-block-size: 34px; gap: 6px; width: 100%; text-align: left; color: ${COLOR.ink};
  background: ${COLOR.surface}; cursor: pointer; overflow-wrap: anywhere; padding: 5px 8px;
  border: 0; border-bottom: 1px solid ${COLOR.line}; border-radius: 0; font: inherit;
}
.task-flow-steps .task-flow-step[aria-pressed=true] { background: #eef4fc; box-shadow: inset 3px 0 #6385b2; }
.task-flow-step:focus-visible, .task-step-more summary:focus-visible { outline: 2px solid ${COLOR.link}; outline-offset: -2px; }
.task-flow-step-number { text-align: center; color: ${COLOR.inkMuted}; font-variant-numeric: tabular-nums; }
.task-flow-step-number, .task-flow-step-result, .task-step-meta, .task-step-model span { color: ${COLOR.inkMuted}; font-size: ${FONT_PX.helper}px; }
.task-step-inspector { min-width: 0; }
.task-step-inspect-body { display: grid; justify-items: stretch; gap: 7px; padding: 9px 12px; overflow-wrap: anywhere; }
.task-step-inspect-body p, .task-step-inspect-body h4, .task-step-inspect-body h5 { margin: 0; }
.task-step-head { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 8px; }
.task-step-head h4 { font-size: ${FONT_PX.section}px; }
.task-step-head span { color: ${COLOR.inkMuted}; font-weight: 600; }
.task-step-attention { padding: 5px 8px; border-radius: 5px; background: #f3f6fa; }
.task-step-attention[data-tone=blocked] { background: #fff7e8; color: ${COLOR.warning}; }
.task-step-attention[data-tone=failed] { background: #fff1f1; color: ${COLOR.danger}; }
.task-step-dependencies { display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 2px 8px; margin: 0; }
.task-step-dependencies dt { color: ${COLOR.inkMuted}; }
.task-step-dependencies dd { margin: 0; }
.task-step-model { display: flex; flex-wrap: wrap; align-items: baseline; gap: 2px 10px; }
.task-step-more { border-top: 1px solid ${COLOR.line}; }
.task-step-more summary { padding: 6px 0; cursor: pointer; color: ${COLOR.link}; }
.task-step-more-body { display: grid; gap: 8px; padding: 3px 0 6px; }
.task-step-more-body h5 { font-size: ${FONT_PX.menu}px; }
.task-step-inspect-body > button { justify-self: start; }
@container task-run-steps (max-width: 640px) {
  .task-run-grid { grid-template-columns: minmax(0, 1fr); }
  .task-flow-steps { max-block-size: min(32vh, 176px); border-inline-end: 0; border-block-end: 1px solid ${COLOR.line}; }
}
`;
		const TASK_INSPECTOR_CSS = `
.task-inspector { box-sizing: border-box; position: fixed; inset: 0; margin: auto; width: min(1160px, calc(100vw - 48px)); height: min(880px, calc(100dvh - 48px)); padding: 0; overflow: hidden; background: ${COLOR.surface}; color: ${COLOR.ink}; border: 1px solid #d8dee8; border-radius: 14px; box-shadow: 0 24px 72px #172c4933; overflow-wrap: anywhere; }
.task-inspector[open] { display: flex; flex-direction: column; }
.task-inspector:has(.task-run-grid[data-single=true]) { height: fit-content; max-height: min(880px, calc(100dvh - 48px)); }
.task-inspector::backdrop { background: #111c36a0; }
.task-inspector-top { flex: none; background: #f8fafc; border-bottom: 1px solid ${COLOR.line}; padding: 22px 24px 0; }
.task-inspector-heading { display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; }
.task-inspector-content { min-height: 0; flex: 1; overflow: auto; overscroll-behavior: contain; padding: 22px 24px; display: grid; gap: 16px; align-content: start; }
.task-inspector-kicker { color: ${COLOR.inkMuted}; font-size: ${FONT_PX.helper}px; margin: 0 0 5px; }
.task-inspector-badges { display: flex; flex-wrap: wrap; gap: 4px 7px; align-items: center; margin-top: 7px; font-size: ${FONT_PX.menu}px; }
.task-inspector-tabs { display: flex; flex-wrap: wrap; gap: 2px; margin-top: 10px; }
.task-inspector-tabs button { min-height: 39px; padding: 0 9px; border: 0; border-bottom: 2px solid transparent; background: transparent; color: ${COLOR.inkMuted}; font-size: ${FONT_PX.body}px; cursor: pointer; }
.task-inspector-tabs [aria-pressed=true] { color: ${COLOR.ink}; border-bottom-color: ${COLOR.link}; font-weight: 650; }
.task-inspector-select { display: grid; gap: 4px; margin-top: 9px; color: ${COLOR.inkMuted}; font-size: ${FONT_PX.helper}px; }
.task-inspector-select select { min-width: 0; max-width: 100%; background: ${COLOR.surface}; border: 1px solid ${COLOR.line}; border-radius: 6px; padding: 5px; font: inherit; color: ${COLOR.ink}; }
.task-archive-actions { margin-top: 8px; display: flex; align-items: center; flex-wrap: wrap; gap: 6px; }
.task-archive-actions p { flex-basis: 100%; margin: 3px 0; color: ${COLOR.inkMuted}; }
.task-drawer-section { min-width: 0; display: grid; gap: 7px; }
.task-drawer-section > header { display: flex; align-items: center; justify-content: space-between; gap: 6px; flex-wrap: wrap; }
.task-drawer-section h3 { margin: 0; font-size: ${FONT_PX.section}px; }
.task-drawer-card { min-width: 0; padding: 11px; border: 1px solid ${COLOR.line}; border-radius: 8px; background: ${COLOR.surface}; }
.task-schedule-card { background: #f4f7fb; }
.task-drawer-card .task-run-facts { padding: 0; }
.task-schedule-card .task-run-facts { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); }
.task-run-history { min-width: 0; display: grid; overflow: hidden; border: 1px solid ${COLOR.line}; border-radius: 8px; }
.task-run-history button { display: grid; grid-template-columns: minmax(0, 1.5fr) minmax(0, 1fr) auto; gap: 7px; align-items: center; min-width: 0; padding: 8px 10px; min-height: 43px; text-align: left; overflow-wrap: anywhere; border: 0; border-bottom: 1px solid ${COLOR.line}; background: ${COLOR.surface}; color: ${COLOR.ink}; font: inherit; cursor: pointer; }
.task-run-history button:last-child { border-bottom: 0; }
.task-run-history button:hover, .task-run-history button[aria-current=true] { background: #eef4fc; }
.task-run-history button small { display: block; color: ${COLOR.inkMuted}; }
.task-inspector-back { margin-bottom: 5px; }
.task-inspector-status { padding: 4px 0; color: ${COLOR.inkMuted}; }
@media (max-width: 660px) {
  .task-inspector { width: calc(100vw - 18px); height: calc(100dvh - 18px); }
  .task-inspector:has(.task-run-grid[data-single=true]) { max-height: calc(100dvh - 18px); }
  .task-inspector-top, .task-inspector-content { padding-inline: 12px; }
}
`;
		// src/client/panel/task-run-steps.tsx
		function stepsFor(nodes, doc) {
			const actual = currentAttempts(nodes);
			const documented = new Map(doc?.nodes.map((node) => [node.node_id, node]));
			if (actual.size) {
				const order = doc ? topologicalOrder(doc) : null;
				const rank = new Map(order?.ok ? order.order.map((id, index) => [id, index]) : []);
				return Array.from(actual.values()).sort((a, b) => (rank.get(a.node_id) ?? Number.MAX_SAFE_INTEGER) - (rank.get(b.node_id) ?? Number.MAX_SAFE_INTEGER)).map((node, index) => ({
					id: node.node_run_id,
					nodeId: node.node_id,
					title: node.title || documented.get(node.node_id)?.title || `未命名步骤 ${index + 1}`,
					type: node.node_type,
					attempt: node,
					definition: documented.get(node.node_id)
				}));
			}
			return (doc?.nodes ?? []).filter((node) => node.type !== "trigger").map((node) => ({
				id: node.node_id,
				nodeId: node.node_id,
				title: node.title,
				type: node.type,
				attempt: null,
				definition: node
			}));
		}
		function usePinnedWorkflow(api, workflowId, version) {
			const [loaded, setLoaded] = (0, react.useState)();
			const key = workflowId && version != null ? `${workflowId}:${version}` : null;
			(0, react.useEffect)(() => {
				if (!workflowId || version == null || !key) return;
				let active = true;
				api.getWorkflowVersion(workflowId, version).then(({ data }) => {
					if (active) setLoaded({
						key,
						doc: readCanvasDoc(data.definition),
						definition: data.definition,
						error: false
					});
				}).catch(() => {
					if (active) setLoaded({
						key,
						doc: null,
						definition: null,
						error: true
					});
				});
				return () => {
					active = false;
				};
			}, [
				api,
				key,
				version,
				workflowId
			]);
			return key === loaded?.key ? loaded : void 0;
		}
		function TaskRunSteps({ nodes, doc, run, runDetails = false, onOpenRun, onOpenResult, onOpenDecision, renderEvidence }) {
			const steps = (0, react.useMemo)(() => stepsFor(nodes, doc), [nodes, doc]);
			const titles = (0, react.useMemo)(() => new Map([...(doc?.nodes ?? []).map((node) => [node.node_id, node.title]), ...steps.map((step) => [step.nodeId, step.title])]), [steps, doc]);
			const [selectedId, setSelectedId] = (0, react.useState)();
			const attention = steps.filter(({ attempt }) => needsAttention(attempt));
			const active = steps.filter(({ attempt }) => nodeExecutionPhase(attempt).tone === "active");
			const waiting = steps.filter(({ attempt }) => attempt?.execution_state === "waiting_capacity" || attempt?.execution_state === "waiting_dependencies");
			const selected = steps.find((step) => step.nodeId === selectedId) ?? attention[0] ?? active[0] ?? waiting[0] ?? steps[0];
			const done = steps.filter((step) => step.attempt?.status === "done").length;
			const parallel = (0, react.useMemo)(() => sameLayerWidth(doc, steps), [doc, steps]);
			const progress = steps.length > 1 ? nodes.length ? `${done} / ${steps.length} 步完成` : `${steps.length} 个计划步骤` : null;
			const picker = (0, react.useRef)(null);
			const selectedNodeId = selected?.nodeId;
			(0, react.useLayoutEffect)(() => {
				const list = picker.current;
				const button = list?.querySelector("[aria-pressed=true]");
				if (!list || !button) return;
				const box = list.getBoundingClientRect();
				const row = button.getBoundingClientRect();
				if (row.top < box.top) list.scrollTop -= box.top - row.top;
				else if (row.bottom > box.bottom) list.scrollTop += row.bottom - box.bottom;
			}, [selectedNodeId]);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "task-run-steps",
				"aria-label": "执行步骤",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("style", { children: TASK_RUN_STEPS_CSS }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "task-run-steps-head",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h3", { children: ["执行步骤", runDetails && ` · ${steps.length}`] }),
							runDetails && run?.settling && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "正在收尾" }),
							progress && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: progress })
						]
					}),
					(attention.length > 0 || active.length > 1 || waiting.length > 0 || parallel > 1) && steps.length > 1 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("ul", {
						className: "task-step-totals",
						"aria-label": "步骤状态概况",
						children: [
							attention.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", {
								"data-tone": "blocked",
								children: [attention.length, " 步需关注"]
							}),
							active.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", {
								"data-tone": "active",
								children: [active.length, " 步执行中"]
							}),
							waiting.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [waiting.length, " 步等待"] }),
							parallel > 1 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
								"同层最多 ",
								parallel,
								" 步"
							] })
						]
					}),
					!runDetails && run?.scheduled_for && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						className: "task-flow-trigger-record",
						children: ["定时触发 · 计划时间 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", {
							dateTime: run.scheduled_for,
							children: formatTaskDate(run.scheduled_for)
						})]
					}),
					steps.length === 0 ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						className: "task-flow-empty",
						children: runDetails ? "这次运行没有执行步骤。" : "执行步骤会在运行排入后出现在这里。"
					}) : /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-run-grid",
						"data-single": steps.length === 1,
						children: [steps.length > 1 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ol", {
							className: "task-flow-steps",
							ref: picker,
							"aria-label": "选择执行步骤",
							children: steps.map((step, index) => {
								const phase = nodeExecutionPhase(step.attempt);
								const type = NODE_TYPE_LABEL[step.type] ?? "步骤类型未记录";
								return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("button", {
									className: "task-flow-step",
									type: "button",
									"data-tone": phase.tone,
									"aria-pressed": selected === step,
									onClick: () => setSelectedId(step.nodeId),
									children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
											className: "task-flow-step-number",
											"aria-hidden": "true",
											children: index + 1
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", {
											title: type,
											children: step.title
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
											className: "task-flow-step-result",
											children: step.attempt ? nodeExecutionLabel(step.attempt) : "尚无执行记录"
										})
									]
								}) }, step.id);
							})
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("aside", {
							className: "task-step-inspector",
							"aria-label": "步骤执行详情",
							children: selected && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(StepInspection, {
								step: selected,
								steps,
								doc,
								titles,
								renderEvidence,
								onOpenRun,
								onOpenResult,
								onOpenDecision
							}, selected.id)
						})]
					})
				]
			});
		}
		function sameLayerWidth(doc, steps) {
			const topology = doc && steps.length > 1 ? topologicalOrder(doc) : null;
			if (!topology?.ok) return 0;
			const counts = /* @__PURE__ */ new Map();
			for (const { nodeId } of steps) {
				const layer = topology.layer.get(nodeId);
				if (layer !== void 0) counts.set(layer, (counts.get(layer) ?? 0) + 1);
			}
			return Math.max(0, ...counts.values());
		}
		function StepInspection({ step, steps, doc, titles, onOpenRun, onOpenResult, onOpenDecision, renderEvidence }) {
			const node = step.attempt;
			const name = (id) => titles.get(id) || "未命名步骤";
			const incomingIds = node?.depends_on ?? doc?.edges.filter((edge) => edge.to === step.nodeId).map((edge) => edge.from);
			const incoming = incomingIds?.map(name);
			const downstream = new Set(doc?.edges.filter((edge) => edge.from === step.nodeId).map((edge) => edge.to));
			for (const { nodeId, attempt } of steps) if (attempt?.depends_on?.includes(step.nodeId)) downstream.add(nodeId);
			else if (attempt?.depends_on) downstream.delete(nodeId);
			const outgoing = [...downstream].map(name);
			const assignment = node?.model_assignment;
			const model = assignment?.usage;
			const metering = model?.metering;
			const contract = step.definition?.raw.contract;
			const description = typeof contract?.description === "string" ? contract.description : null;
			const waiting = node && waitingFor(node, titles);
			const allUpstreamWaiting = waiting && incomingIds?.length && incomingIds.length === node?.waiting_on?.length && node.waiting_on.every((id) => incomingIds.includes(id));
			const heartbeat = node && heartbeatNotice(node);
			const phase = nodeExecutionPhase(node);
			const status = node ? nodeExecutionLabel(node) : "尚无执行记录";
			const decisions = node?.active_decision_ids ?? [];
			const verifying = node?.status === "verifying";
			const waitingReview = node?.status === "blocked_post";
			const modelLabel = model ? "实际执行模型" : assignment?.state === "confirmed" ? "宿主确认模型" : assignment?.state === "rejected" ? "模型未获宿主确认" : "拟用模型";
			const modelName = assignment && (model ? `${model.provider} / ${model.model}` : assignment.provider && assignment.model ? `${assignment.provider} / ${assignment.model}` : assignment.model_ref);
			const [open, setOpen] = (0, react.useState)(false);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				className: "task-step-inspect-body",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "task-step-head",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h4", { children: step.title }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
							"data-tone": phase.tone,
							children: status
						})]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						className: "task-step-meta",
						children: [
							NODE_TYPE_LABEL[step.type] ?? "步骤类型未记录",
							node && ` · 第 ${node.attempt} 次尝试`,
							node?.started_at && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [" · 开始于 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", {
								dateTime: node.started_at,
								children: formatTaskDate(node.started_at)
							})] }),
							node && node.execution_count > 0 && ` · ${node.execution_count} 条工具执行记录`,
							node && Boolean(node.projection_missing?.length) && " · 部分信息暂不可用"
						]
					}),
					(waiting || verifying || waitingReview || node?.hang_reason || heartbeat || decisions.length > 0) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-step-attention",
						"data-tone": phase.tone,
						children: [
							waiting && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: waiting }),
							verifying && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "系统正在核验这一步的执行结果，核验结束后会更新步骤状态。已登记的产物可在运行的「结果」中查看。" }),
							waitingReview && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: decisions.length ? "这一步正在等待人工处理，打开审批可查看需要核对的内容。" : "这一步的结果仍待人工处理，可在运行的「证据」中查看现有记录。" }),
							node?.hang_reason && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: operatorReason(node.hang_reason) }),
							heartbeat && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: heartbeat }),
							decisions.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
								decisions.length,
								" 项",
								COPY.awaitingDecision
							] }),
							decisions.map((id, index) => onOpenDecision && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								className: "task-quiet-button",
								onClick: () => onOpenDecision(id),
								children: decisions.length === 1 ? "查看待处理审批" : `查看第 ${index + 1} 项审批`
							}, id)),
							verifying && onOpenResult && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								className: "task-quiet-button",
								onClick: onOpenResult,
								children: "查看已登记产物"
							})
						]
					}),
					(!allUpstreamWaiting && !!incoming?.length || outgoing.length > 0) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
						className: "task-step-dependencies",
						children: [!allUpstreamWaiting && !!incoming?.length && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "上游" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: incoming.join("、") })] }), outgoing.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "下游" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: outgoing.join("、") })] })]
					}),
					(assignment || node?.node_type === "agent") && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-step-model",
						children: [
							assignment && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: modelLabel }),
								" · ",
								modelName
							] }),
							assignment?.reasoning_effort !== void 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: ["派发思考深度 · ", assignment.reasoning_effort ?? "模型默认"] }),
							model?.reasoning_effort && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: ["实际请求思考深度 · ", model.reasoning_effort] }),
							metering?.settlement_incomplete && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "执行提前结束，仅显示模型已报告的用量。" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["本次尝试模型用量（Token）", metering ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
								" · 输入（未缓存）",
								metering.tokens.uncached_input_tokens ?? "不可用",
								" · 输出",
								metering.tokens.output_tokens ?? "不可用",
								" · 缓存读取",
								metering.tokens.cache_read_tokens ?? "不可用",
								" · 缓存写入",
								metering.tokens.cache_write_tokens ?? "不可用"
							] }) : " · 不可用"] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["上下文压缩", metering?.context_compactions ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
								" · 摘要 ",
								metering.context_compactions.summaries,
								" 次 · 工具结果裁剪 ",
								metering.context_compactions.tool_result_prunes,
								" 次"
							] }) : " · 不可用"] })
						]
					}),
					(assignment || description || step.definition?.raw.runtime_step != null || node) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
						className: "task-step-more",
						onToggle: (event) => setOpen(event.currentTarget.open),
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: node && renderEvidence ? "更多详情与执行日志" : "更多步骤详情" }), open && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "task-step-more-body",
							children: [
								step.definition?.raw.runtime_step != null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["运行中新增 · 来源角色：", String(step.definition.raw.runtime_step.source_title)] }),
								description && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h5", { children: "步骤职责" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: description })] }),
								assignment && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h5", { children: "模型诊断" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
										MODEL_LEVELS[assignment.complexity]?.label,
										" · ",
										assignment.reason
									] }),
									assignment.settings_revision != null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["按用户配置 · 修订 ", assignment.settings_revision] })
								] }),
								node && renderEvidence?.(node),
								node && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看技术记录" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
										"步骤编号 ",
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: node.node_id }),
										" · 尝试编号 ",
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: node.node_run_id })
									] }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
										"状态 ",
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: node.status }),
										node.hang_reason && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [" · 原因 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: node.hang_reason })] })
									] })
								] })
							]
						})]
					}),
					onOpenRun && node && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						className: "task-quiet-button",
						onClick: (event) => onOpenRun(event.currentTarget),
						children: "查看这次运行"
					})
				]
			});
		}
		const terminal = /* @__PURE__ */ new Set([
			"done",
			"failed",
			"timeout",
			"abandoned_unverified",
			"failed_with_unverified",
			"orphaned",
			"superseded"
		]);
		function heartbeatNotice(node) {
			if (!node.heartbeat_at || terminal.has(node.status)) return null;
			const seconds = Math.floor((Date.now() - Date.parse(node.heartbeat_at)) / 1e3);
			return seconds > 30 ? `${COPY.heartbeatStopped} ${seconds} 秒` : null;
		}
		function needsAttention(node) {
			if (!node) return false;
			const { tone } = nodeExecutionPhase(node);
			return tone === "blocked" || tone === "failed" || Boolean(node.hang_reason || node.active_decision_ids.length || heartbeatNotice(node));
		}
		function formatTaskDate(value, timeZone) {
			const at = Date.parse(value);
			if (!Number.isFinite(at)) return "时间未知";
			return new Intl.DateTimeFormat("zh-CN", {
				year: "numeric",
				month: "2-digit",
				day: "2-digit",
				hour: "2-digit",
				minute: "2-digit",
				hourCycle: "h23",
				timeZone
			}).format(at);
		}
		function runDuration(start, end) {
			const ms = Date.parse(end ?? "") - Date.parse(start ?? "");
			if (!Number.isFinite(ms) || ms < 0) return null;
			if (ms < 1e3) return `${ms} 毫秒`;
			const seconds = Math.floor(ms / 1e3);
			const minutes = Math.floor(seconds / 60);
			return minutes ? `${minutes} 分 ${seconds % 60} 秒` : `${seconds} 秒`;
		}
		// src/client/panel/execution-history.tsx
		const statusFields = /* @__PURE__ */ new Set([
			"原状态",
			"新状态",
			"状态",
			"报告状态",
			"记录状态",
			"业务结果"
		]);
		const reasonFields = /* @__PURE__ */ new Set([
			"原因",
			"错误代码",
			"处理结论"
		]);
		const plainFields = /* @__PURE__ */ new Set([
			"工具",
			"退出码",
			"摘要",
			"报告摘要",
			"模型配置",
			"提供方",
			"实际模型",
			"文件名称",
			"节点数量",
			"计划时间",
			"计划触发时间",
			"开始时间",
			"完成时间",
			"尝试次数",
			"Workflow 版本"
		]);
		function Fields({ entries }) {
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dl", {
				style: {
					display: "grid",
					gridTemplateColumns: "minmax(72px, max-content) minmax(0, 1fr)",
					gap: "4px 10px",
					margin: "8px 0 0",
					fontSize: FONT_PX.helper,
					overflowWrap: "anywhere"
				},
				children: entries.map(({ label, value }, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
					style: { display: "contents" },
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", {
						style: { color: COLOR.inkMuted },
						children: label
					}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", {
						style: {
							margin: 0,
							whiteSpace: "pre-wrap"
						},
						children: value
					})]
				}, label + index))
			});
		}
		function EventDetails({ entries }) {
			if (!entries?.length) return null;
			const readable = entries.flatMap(({ label, value }) => {
				if (statusFields.has(label)) return [{
					label,
					value: operatorStatus(value)
				}];
				if (reasonFields.has(label)) return [{
					label: label === "错误代码" ? "错误说明" : label,
					value: operatorReason(value)
				}];
				return plainFields.has(label) ? [{
					label,
					value
				}] : [];
			});
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [readable.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(Fields, { entries: readable }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看技术记录" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(Fields, { entries })] })] });
		}
		function ExecutionHistory({ api, taskId, runId, nodeId, reloadVersion }) {
			const [items, setItems] = (0, react.useState)();
			const [next, setNext] = (0, react.useState)(null);
			const [loading, setLoading] = (0, react.useState)(false);
			const [failed, setFailed] = (0, react.useState)(false);
			const [retry, refresh] = (0, react.useState)(0);
			const [requested, setRequested] = (0, react.useState)();
			const [category, setCategory] = (0, react.useState)("");
			const [pageGeneration, setPageGeneration] = (0, react.useState)(reloadVersion);
			const before = pageGeneration === reloadVersion ? requested : void 0;
			(0, react.useEffect)(() => {
				let active = true;
				setLoading(true);
				setFailed(false);
				api.listExecutionHistory({
					task_id: taskId,
					run_id: runId,
					node_run_id: nodeId,
					before
				}).then(({ data }) => {
					if (!active) return;
					setItems((previous) => before === void 0 ? data.items : [...previous ?? [], ...data.items]);
					setNext(data.next_before);
				}).catch(() => {
					if (active) setFailed(true);
				}).finally(() => {
					if (active) setLoading(false);
				});
				return () => {
					active = false;
				};
			}, [
				api,
				taskId,
				runId,
				nodeId,
				before,
				retry,
				reloadVersion
			]);
			const shown = items?.filter((item) => category === "" || (category === "tool" ? item.type.includes("tool") : category === "report" ? item.type.includes("report") || item.type === "bridge.turn" : item.type.startsWith("executor.")));
			const summary = (nodeId ? items?.find((item) => item.type === "bridge.turn") : void 0)?.details.find((detail) => detail.label === "报告摘要")?.value;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-label": "执行日志",
				style: {
					display: "grid",
					gap: 8
				},
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h4", {
						style: {
							fontSize: FONT_PX.section,
							margin: "4px 0"
						},
						children: "执行日志"
					}),
					summary && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: {
							background: COLOR.sidebar,
							border: `1px solid ${COLOR.line}`,
							borderRadius: 6,
							padding: "8px 10px"
						},
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "智能体报告摘要" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							style: {
								margin: "5px 0 0",
								whiteSpace: "pre-wrap"
							},
							children: summary
						})]
					}),
					!nodeId && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["筛选已加载记录 ", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
						value: category,
						onChange: (event) => setCategory(event.target.value),
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
								value: "",
								children: "全部"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
								value: "tool",
								children: "工具调用"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
								value: "report",
								children: "执行报告"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
								value: "model",
								children: "模型派发"
							})
						]
					})] }),
					loading && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取执行日志…"
					}),
					failed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: "alert",
						children: ["执行日志读取失败。 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							onClick: () => refresh((value) => value + 1),
							children: "重试"
						})]
					}),
					!failed && !loading && items?.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "此范围内没有已保存的节点日志。" }),
					!failed && !loading && items?.length && shown?.length === 0 ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "已加载的记录没有符合筛选条件的事件。" }) : null,
					shown?.length ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ol", {
						style: {
							listStyle: "none",
							margin: 0,
							padding: 0
						},
						children: shown.map((item, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", {
							style: {
								padding: "9px 0",
								borderTop: `1px solid ${COLOR.line}`
							},
							children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
								open: nodeId && index < 3 ? true : void 0,
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", { children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: item.title === item.type ? "其他执行记录" : item.title }),
									" ",
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", {
										dateTime: item.created_at,
										children: new Date(item.created_at).toLocaleString()
									})
								] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(EventDetails, { entries: [
									...item.details,
									{
										label: "事件类型",
										value: item.type
									},
									{
										label: "步骤 ID",
										value: item.node_id
									},
									{
										label: "运行 ID",
										value: item.run_id
									}
								] })]
							})
						}, item.id))
					}) : null,
					next !== null && !failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						disabled: loading,
						style: { justifySelf: "start" },
						onClick: () => {
							setPageGeneration(reloadVersion);
							setRequested(next);
						},
						children: "加载更早的日志"
					})
				]
			});
		}
		// src/client/panel/task-run-evidence.tsx
		const TOOL_ACTIONS = /* @__PURE__ */ new Map([["workspace_read_sample", "读取已授权的工作区资料"], ["workspace_list", "列出获准查看的工作区目录"]]);
		function evidenceToolLabel(tool, fallback) {
			return TOOL_ACTIONS.get(tool) ?? fallback;
		}
		function NodeEffectFacts({ node, onOpenDecision }) {
			const effectLabel = (value) => value === "applied" ? "外部操作已发生" : value === "not_applied" ? "外部操作未发生" : null;
			const declared = effectLabel(node.declared_effect);
			const verified = effectLabel(node.verified_effect);
			const settled = node.evidence_state === "settled" && verified;
			const readOnly = node.evidence_state === "not_required" && node.side_effect_policy === "pure";
			const mismatch = declared && settled && declared !== verified;
			const decisionId = node.effect_decision_id;
			const reason = node.evidence_state_reason && operatorReason(node.evidence_state_reason);
			if (readOnly && !declared) return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				style: mutedStyle$10,
				children: reason || "此步骤按只读策略执行，无需核验外部操作。"
			});
			const headline = mismatch ? "声明与核实结果不一致" : readOnly ? "只读步骤" : settled ? `已核验 · ${verified}` : node.evidence_state === "unverified" ? "外部操作待核验" : "缺少外部操作核验记录";
			const basis = node.effect_resolution_basis === "decision" ? "依据已批准的决定及其引用的系统证据。" : node.effect_resolution_basis === "verified_none" ? "依据账本中的停止或未发生记录。" : node.effect_resolution_basis === "evidence" ? "依据系统核验证据。" : null;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				style: mismatch ? {
					...evidenceStyle,
					background: "#fff8e9",
					color: COLOR.warning
				} : evidenceStyle,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: headline }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
						className: "task-run-facts",
						children: [(!readOnly || declared) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "步骤声明" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: declared ?? "无声明记录" })] }), mismatch && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "核验结果" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: verified })] })]
					}),
					(basis ?? reason) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$10,
						children: basis ?? reason
					}),
					mismatch && decisionId && onOpenDecision && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						className: "ops-button",
						onClick: () => onOpenDecision(decisionId),
						children: "查看相关决定"
					})
				]
			});
		}
		function NodeEvidence({ api, node, reloadVersion, onOpenDecision }) {
			const refs = (node.execution_ids ?? []).map((id, index) => ({
				kind: "tool",
				id,
				label: `工具执行记录 ${index + 1}`
			}));
			if (node.attestation_id) refs.push({
				kind: "human",
				id: node.attestation_id,
				label: "人工签认"
			});
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
				/* @__PURE__ */ (0, react_jsx_runtime.jsx)(NodeEffectFacts, {
					node,
					onOpenDecision
				}),
				!refs.length && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					style: mutedStyle$10,
					children: "这次尝试没有关联工具记录或人工签认。"
				}),
				refs.map((ref) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)(EvidenceRecord, {
					api,
					node,
					record: ref,
					reloadVersion
				}, `${ref.kind}-${ref.id}`))
			] });
		}
		function EvidenceRecord({ api, node, record, reloadVersion }) {
			const [open, setOpen] = (0, react.useState)(false);
			const [verified, setVerified] = (0, react.useState)(null);
			const [failure, setFailure] = (0, react.useState)(null);
			(0, react.useEffect)(() => {
				if (!open) return;
				let current = true;
				setVerified(null);
				setFailure(null);
				(record.kind === "tool" ? api.getEvidence(record.id) : api.getAttestation(record.id)).then(({ data }) => {
					if ("evidence_id" in data) {
						if (record.kind !== "tool" || data.strength !== "strong" || data.source !== "run_tool" || data.evidence_id !== record.id || data.run_id !== node.run_id || data.node_run_id !== node.node_run_id || data.attempt !== node.attempt) throw new Error("证据与当前步骤不匹配");
					} else if (record.kind !== "human" || data.attestation_id !== record.id || data.action_start_source !== "human_attested" || data.run_id !== node.run_id || data.node_run_id !== node.node_run_id || data.attempt !== node.attempt) throw new Error("签认与当前记录不匹配");
					if (current) setVerified(data);
				}).catch((error) => {
					if (!current) return;
					const unverified = error instanceof ApiError && error.code === "EVIDENCE_SOURCE_UNAVAILABLE";
					const missing = error instanceof ApiError && error.code === "NOT_FOUND";
					setFailure(record.kind === "human" ? "暂时无法读取这条签认。" : missing ? "未找到这条工具执行记录。" : unverified ? "这条执行记录尚未通过系统核验。" : "暂时无法确认这条系统证据。");
				});
				return () => {
					current = false;
				};
			}, [
				api,
				node.attempt,
				node.node_run_id,
				node.run_id,
				open,
				record.id,
				record.kind,
				reloadVersion
			]);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
				style: evidenceStyle,
				onToggle: (event) => setOpen(event.currentTarget.open),
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: verified && "evidence_id" in verified ? evidenceToolLabel(verified.tool, `已核对的${record.label}`) : record.label }),
					open && failure && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: mutedStyle$10,
						children: failure
					}),
					open && !verified && !failure && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: mutedStyle$10,
						children: "正在读取…"
					}),
					verified && ("evidence_id" in verified ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: verified.exit_code === 0 ? "只读工具正常退出" : "只读工具返回非零退出码" }),
							" · 退出码 ",
							verified.exit_code
						] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							style: mutedStyle$10,
							children: "系统已核对这次调用的记录。交付产物是否符合要求，请查看交付验收。"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看核验明细" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$10,
								children: [
									"工具 ",
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: verified.tool }),
									" · 记录于 ",
									formatDate$6(verified.observed_at)
								]
							}),
							verified.execution_boundary === "dsh_workspace_rpc" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								style: mutedStyle$10,
								children: "执行环境为 DSH 工作区固定读取接口。"
							}),
							verified.execution_boundary === "windows_appcontainer_bfs" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								style: mutedStyle$10,
								children: "执行环境为 Windows AppContainer 只读工作区。"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$10,
								children: ["证据编号 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: verified.evidence_id })]
							}),
							verified.stdout_hash && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$10,
								children: ["标准输出哈希 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: verified.stdout_hash })]
							}),
							verified.stderr_hash && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$10,
								children: ["标准错误哈希 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: verified.stderr_hash })]
							})
						] })
					] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: verified.claim === "action_started" ? "有人确认操作已启动" : "已记录人工签认" }) }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$10,
							children: [
								"签认人 ",
								verified.attested_by,
								" · ",
								formatDate$6(verified.attested_at)
							]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$10,
							children: [
								"签认覆盖本步骤的第 ",
								node.attempt,
								" 次尝试，外部操作结果仍以对应核验记录为准。"
							]
						}),
						verified.summary && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							style: mutedStyle$10,
							children: verified.summary
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看签认明细" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["签认编号 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: verified.attestation_id })] })] })
					] }))
				]
			});
		}
		function formatDate$6(value) {
			const at = Date.parse(value);
			return Number.isFinite(at) ? new Date(at).toLocaleString() : "时间未知";
		}
		const evidenceStyle = {
			padding: "6px 0",
			borderTop: `1px solid ${COLOR.line}`,
			overflowWrap: "anywhere"
		};
		const mutedStyle$10 = {
			margin: "6px 0 0",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.body
		};
		// src/client/panel/workflow-canvas-view.tsx
		const REWORK_STATUS = {
			proposed: "等待授权",
			needs_human_approval: "等待人工授权",
			scheduled: "等待返工",
			awaiting_review: "等待重新验收",
			escalated: "等待处理",
			closed: "验收通过",
			declined: "未批准",
			cancelled: "已取消"
		};
		const NODE_WIDTH = 160;
		const NODE_HEIGHT = 56;
		const TASK_NODE_HEIGHT = 88;
		const MIN_CANVAS_WIDTH = 1100;
		const MIN_CANVAS_HEIGHT = 620;
		function WorkflowCanvasView({ definition, readOnly, onChange, canvasId, inspectNodes = false, editRecoveryRetry = false, runtimeNodes, runtimeNodesPartial = false, runtimeTopology, runtimeCompact = false, workspace = false }) {
			const compact = readOnly && runtimeCompact;
			const repairOverlaps = compact && !workspace;
			const nodeHeight = readOnly && runtimeTopology ? 108 : repairOverlaps ? TASK_NODE_HEIGHT : NODE_HEIGHT;
			const doc = (0, react.useMemo)(() => {
				const base = runtimeCanvasDoc(definition, readOnly ? runtimeTopology : null);
				return base && repairOverlaps ? separateOverlappingNodes(base, NODE_WIDTH, nodeHeight) : base;
			}, [
				definition,
				readOnly,
				runtimeTopology,
				repairOverlaps,
				nodeHeight
			]);
			const [alert, setAlert] = (0, react.useState)(null);
			const [selectedId, setSelectedId] = (0, react.useState)(null);
			const [zoom, setZoom] = (0, react.useState)(1);
			const [layoutVersion, setLayoutVersion] = (0, react.useState)(0);
			const [groupMode, setGroupMode] = (0, react.useState)(false);
			const [collapsedGroups, setCollapsedGroups] = (0, react.useState)(() => /* @__PURE__ */ new Set());
			const [collapsedNodes, setCollapsedNodes] = (0, react.useState)(() => /* @__PURE__ */ new Set());
			const [showFailureChain, setShowFailureChain] = (0, react.useState)(true);
			const [showMinimap, setShowMinimap] = (0, react.useState)(true);
			const viewportRef = (0, react.useRef)(null);
			const zoomRef = (0, react.useRef)(zoom);
			zoomRef.current = zoom;
			const displayDoc = (0, react.useMemo)(() => {
				if (!layoutVersion || !doc) return doc;
				const result = arrangePositions(doc);
				return result.ok ? { ...doc, nodes: result.nodes } : doc;
			}, [doc, layoutVersion]);
			const retryHelpId = (0, react.useId)();
			const docRef = (0, react.useRef)(doc);
			const onChangeRef = (0, react.useRef)(onChange);
			const readOnlyRef = (0, react.useRef)(readOnly);
			docRef.current = doc;
			onChangeRef.current = onChange;
			readOnlyRef.current = readOnly;
			const canvasRef = (0, react.useRef)(null);
			const dragRef = (0, react.useRef)({ kind: "none" });
			const size = (0, react.useMemo)(() => {
				let width = compact || workspace ? NODE_WIDTH : MIN_CANVAS_WIDTH;
				let height = workspace ? readOnly ? 130 : 220 : compact ? NODE_HEIGHT : MIN_CANVAS_HEIGHT;
				const padding = workspace ? 44 : compact ? 32 : 120;
				for (const node of displayDoc?.nodes ?? []) {
					width = Math.max(width, node.position.x + NODE_WIDTH + padding);
					height = Math.max(height, node.position.y + nodeHeight + padding);
				}
				return {
					width,
					height
				};
			}, [
				compact,
				displayDoc,
				readOnly,
				workspace,
				nodeHeight
			]);
			function emit(next) {
				if (readOnlyRef.current) return;
				onChangeRef.current?.(serializeCanvasDoc(next));
			}
			function setRecoveryRetry(node, checked) {
				const current = docRef.current;
				if (!current || !editRecoveryRetry) return;
				const config = { ...nodeConfig(node) };
				if (checked) config.recovery_retry_idempotent = true;
				else delete config.recovery_retry_idempotent;
				const raw = { ...node.raw };
				if (Object.keys(config).length) raw.config = config;
				else delete raw.config;
				emit({
					...current,
					nodes: current.nodes.map((item) => item === node ? {
						...item,
						raw
					} : item)
				});
			}
			function addNode(type, x, y) {
				const current = docRef.current;
				if (!current) return;
				const identity = nextNodeIdentity(current, type);
				const node = {
					node_id: identity.nodeId,
					type,
					title: identity.title,
					position: {
						x,
						y
					},
					raw: {}
				};
				emit({
					...current,
					nodes: [...current.nodes, node]
				});
				setAlert(null);
			}
			function connect(from, to) {
				const current = docRef.current;
				if (!current) return;
				const check = canConnect(current, from, to);
				if (!check.ok) {
					setAlert(check.message);
					return;
				}
				const route = routeForNewEdge(current, from);
				emit({
					...current,
					edges: [...current.edges, {
						from,
						to,
						...route ? { route } : {},
						raw: {}
					}]
				});
				setAlert(null);
			}
			function deleteNode(nodeId) {
				const current = docRef.current;
				if (!current) return;
				emit({
					...current,
					nodes: current.nodes.filter((node) => node.node_id !== nodeId),
					edges: current.edges.filter((edge) => edge.from !== nodeId && edge.to !== nodeId)
				});
				setAlert(null);
			}
			function deleteEdge(from, to) {
				const current = docRef.current;
				if (!current) return;
				emit({
					...current,
					edges: current.edges.filter((edge) => !(edge.from === from && edge.to === to))
				});
				setAlert(null);
			}
			function arrange() {
				const current = docRef.current;
				if (!current) return;
				const result = arrangePositions(current);
				if (!result.ok) {
					setAlert(result.message);
					return;
				}
				emit({
					...current,
					nodes: result.nodes
				});
				setAlert(null);
			}
			function startMove(event, node) {
				if (readOnlyRef.current) return;
				if (event.button !== 0) return;
				event.preventDefault();
				dragRef.current = {
					kind: "move",
					nodeId: node.node_id,
					startClientX: event.clientX,
					startClientY: event.clientY,
					originX: node.position.x,
					originY: node.position.y,
					zoom: zoomRef.current
				};
			}
			function startPalette(event, nodeType) {
				if (readOnlyRef.current) return;
				if (event.button !== 0) return;
				event.preventDefault();
				dragRef.current = {
					kind: "palette",
					nodeType
				};
			}
			function startLink(event, node) {
				if (readOnlyRef.current) return;
				if (event.button !== 0) return;
				event.preventDefault();
				event.stopPropagation();
				dragRef.current = {
					kind: "link",
					fromNodeId: node.node_id
				};
			}
			function handleDoubleClick(event) {
				if (readOnlyRef.current) return;
				const target = event.target;
				if (target && typeof target.closest === "function" && target.closest("[data-node-id]")) return;
				const canvas = canvasRef.current;
				if (!canvas) return;
				const rect = canvas.getBoundingClientRect();
				addNode("agent", clampCoordinate((event.clientX - rect.left) / (zoomRef.current || 1)), clampCoordinate((event.clientY - rect.top) / (zoomRef.current || 1)));
			}
			(0, react.useEffect)(() => {
				const onMove = (event) => {
					const drag = dragRef.current;
					if (drag.kind !== "move") return;
					const current = docRef.current;
					if (!current) return;
					const x = clampCoordinate(drag.originX + (event.clientX - drag.startClientX) / (drag.zoom || 1));
					const y = clampCoordinate(drag.originY + (event.clientY - drag.startClientY) / (drag.zoom || 1));
					emit({
						...current,
						nodes: current.nodes.map((node) => node.node_id === drag.nodeId ? {
							...node,
							position: {
								x,
								y
							}
						} : node)
					});
				};
				const onUp = (event) => {
					const drag = dragRef.current;
					dragRef.current = { kind: "none" };
					if (drag.kind === "palette") {
						const canvas = canvasRef.current;
						const target = event.target;
						if (!canvas || !target || !canvas.contains(target)) return;
						const rect = canvas.getBoundingClientRect();
						addNode(drag.nodeType, clampCoordinate((event.clientX - rect.left) / (zoomRef.current || 1)), clampCoordinate((event.clientY - rect.top) / (zoomRef.current || 1)));
						return;
					}
					if (drag.kind === "link") {
						const target = event.target;
						const targetId = (target && typeof target.closest === "function" ? target.closest("[data-node-id]") : null)?.getAttribute("data-node-id") ?? null;
						if (!targetId) {
							setAlert("连线要落在另一个节点上。");
							return;
						}
						connect(drag.fromNodeId, targetId);
					}
				};
				document.addEventListener("mousemove", onMove);
				document.addEventListener("mouseup", onUp);
				return () => {
					document.removeEventListener("mousemove", onMove);
					document.removeEventListener("mouseup", onUp);
				};
			}, []);
			const nodes = displayDoc?.nodes ?? [];
			const edges = displayDoc?.edges ?? [];
			const selected = nodes.find((node) => node.node_id === selectedId) ?? (workspace ? nodes[0] : void 0);
			const selectedConfig = selected?.type === "agent" ? nodeConfig(selected) : void 0;
			const retryEnabled = selectedConfig?.recovery_retry_idempotent === true;
			const irreversible = selectedConfig?.side_effect_policy === "irreversible";
			const runtimeByNode = currentAttempts(runtimeNodes ?? []);
			const runtimeLabel = (id) => {
				const attempt = runtimeByNode.get(id);
				if (attempt) return nodeExecutionLabel(attempt);
				return runtimeNodesPartial ? "执行状态暂不可用" : "尚无运行记录";
			};
			const hiddenNodeIds = (0, react.useMemo)(() => {
				if (!groupMode || collapsedGroups.size === 0) return /* @__PURE__ */ new Set();
				return new Set(nodes.filter((node) => collapsedGroups.has(node.type)).map((node) => node.node_id));
			}, [
				groupMode,
				collapsedGroups,
				nodes
			]);
			const dependencyNodeIds = (0, react.useMemo)(() => {
				const result = /* @__PURE__ */ new Set();
				if (!selectedId) return result;
				result.add(selectedId);
				let changed = true;
				while (changed) {
					changed = false;
					for (const edge of edges) {
						if (result.has(edge.from) && !result.has(edge.to)) {
							result.add(edge.to);
							changed = true;
						}
						if (result.has(edge.to) && !result.has(edge.from)) {
							result.add(edge.from);
							changed = true;
						}
					}
				}
				return result;
			}, [selectedId, edges]);
			const failureNodeIds = (0, react.useMemo)(() => {
				const result = /* @__PURE__ */ new Set();
				if (!showFailureChain) return result;
				const failed = nodes.filter((node) => {
					const status = runtimeByNode.get(node.node_id)?.status;
					return status === "failed" || status === "timeout" || status === "failed_with_unverified";
				});
				const queue = failed.map((node) => node.node_id);
				while (queue.length > 0) {
					const id = queue.shift();
					if (result.has(id)) continue;
					result.add(id);
					for (const edge of edges) if (edge.to === id && !result.has(edge.from)) queue.push(edge.from);
				}
				return result;
			}, [
				showFailureChain,
				nodes,
				edges,
				runtimeNodes
			]);
			const nodeGroups = (0, react.useMemo)(() => {
				if (!groupMode) return [];
				const byType = /* @__PURE__ */ new Map();
				for (const node of nodes) {
					const list = byType.get(node.type) ?? [];
					list.push(node);
					byType.set(node.type, list);
				}
				return [...byType.entries()].map(([type, list]) => {
					const minX = Math.min(...list.map((node) => node.position.x));
					const minY = Math.min(...list.map((node) => node.position.y));
					const maxX = Math.max(...list.map((node) => node.position.x));
					const maxY = Math.max(...list.map((node) => node.position.y));
					const collapsed = collapsedGroups.has(type);
					return {
						type,
						label: NODE_TYPE_LABEL[type] ?? type,
						count: list.length,
						collapsed,
						x: minX - 18,
						y: minY - 34,
						width: maxX - minX + NODE_WIDTH + 36,
						height: collapsed ? 44 : maxY - minY + nodeHeight + 48
					};
				});
			}, [
				groupMode,
				nodes,
				collapsedGroups,
				nodeHeight
			]);
			function setZoomClamped(next) {
				setZoom(Math.max(0.4, Math.min(1.6, Math.round(next * 100) / 100)));
			}
			function fitCanvas() {
				const viewport = viewportRef.current;
				if (!viewport) return;
				const next = Math.min(viewport.clientWidth / Math.max(1, size.width), viewport.clientHeight / Math.max(1, size.height));
				setZoomClamped(Number.isFinite(next) ? next : 1);
				viewport.scrollTo({ left: 0, top: 0, behavior: "smooth" });
			}
			function focusMinimap(event) {
				const viewport = viewportRef.current;
				if (!viewport) return;
				const rect = event.currentTarget.getBoundingClientRect();
				const ratioX = (event.clientX - rect.left) / Math.max(1, rect.width);
				const ratioY = (event.clientY - rect.top) / Math.max(1, rect.height);
				viewport.scrollTo({
					left: Math.max(0, ratioX * size.width * zoom - viewport.clientWidth / 2),
					top: Math.max(0, ratioY * size.height * zoom - viewport.clientHeight / 2),
					behavior: "smooth"
				});
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				"data-canvas-id": canvasId ?? "workflow-canvas",
				"data-canvas-root": "true",
				"data-canvas-readonly": readOnly ? "true" : "false",
				className: "ops-canvas-shell",
				style: rootStyle,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("style", { children: OPS_MONITOR_CSS }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "ops-canvas-toolbar",
						role: "group",
						"aria-label": "画布视图控制",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"data-canvas-action": "auto-layout",
								onClick: () => {
									if (readOnly) setLayoutVersion((value) => value + 1);
									else arrange();
								},
								children: "自动布局"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"aria-label": "缩小",
								onClick: () => setZoomClamped(zoom - 0.1),
								children: "−"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
								"aria-live": "polite",
								children: [Math.round(zoom * 100), "%"]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"aria-label": "放大",
								onClick: () => setZoomClamped(zoom + 0.1),
								children: "+"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: fitCanvas,
								children: "适应画布"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"aria-pressed": groupMode,
								onClick: () => setGroupMode((value) => !value),
								children: "任务分组"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"aria-pressed": collapsedGroups.size > 0,
								disabled: !groupMode,
								onClick: () => setCollapsedGroups((current) => current.size > 0 ? /* @__PURE__ */ new Set() : new Set(nodes.map((node) => node.type))),
								children: collapsedGroups.size > 0 ? "展开分组" : "折叠分组"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"aria-pressed": collapsedNodes.size > 0,
								onClick: () => setCollapsedNodes((current) => current.size > 0 ? /* @__PURE__ */ new Set() : new Set(nodes.map((node) => node.node_id))),
								children: collapsedNodes.size > 0 ? "展开节点" : "折叠节点"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"aria-pressed": showFailureChain,
								onClick: () => setShowFailureChain((value) => !value),
								children: "失败链路"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"aria-pressed": showMinimap,
								onClick: () => setShowMinimap((value) => !value),
								children: "小地图"
							})
						]
					}),
					!compact && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: workspace ? workspaceToolbar : toolbarStyle,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [!workspace && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
							style: sectionTitleStyle$4,
							children: "节点条"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							style: mutedStyle$9,
							children: "按住节点拖到画布空白处就新建一个节点。"
						})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
							style: paletteStyle,
							children: CANVAS_NODE_TYPES.map((type) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"data-node-palette": type,
								"aria-label": `拖出${NODE_TYPE_LABEL[type] ?? type}节点`,
								"aria-disabled": readOnly || !doc ? "true" : "false",
								disabled: readOnly || !doc,
								title: "拖到画布添加节点",
								onMouseDown: (event) => startPalette(event, type),
								style: paletteButtonStyle,
								children: NODE_TYPE_LABEL[type] ?? type
							}, type))
						})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							"data-canvas-action": "arrange",
							"aria-label": "按拓扑整理节点",
							"aria-disabled": readOnly || !doc ? "true" : "false",
							disabled: readOnly || !doc,
							onClick: arrange,
							style: buttonStyle$7,
							children: "整理"
						}), !workspace && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							style: mutedStyle$9,
							children: readOnly ? "已发布版本只读，不能改。" : "整理按拓扑序重排节点位置。"
						})] })]
					}),
					alert && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "alert",
						"data-canvas-alert": "true",
						style: alertStyle,
						children: alert
					}),
					doc === null && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$9,
						children: "流程定义 JSON 还没解析成对象，画布暂时是空的。"
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: workspace && inspectNodes ? "ops-workflow-stage" : void 0,
						style: { position: "relative", ...inspectNodes && !workspace ? repairOverlaps ? taskInspectionLayout : inspectionLayout : {} },
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
							ref: viewportRef,
							className: "ops-canvas-stage",
							style: workspace ? workspaceViewport : viewportStyle,
							onWheel: (event) => {
								if (!event.ctrlKey && !event.metaKey) return;
								event.preventDefault();
								setZoomClamped(zoom + (event.deltaY < 0 ? 0.1 : -0.1));
							},
							children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								style: {
									position: "relative",
									width: size.width * zoom,
									height: size.height * zoom
								},
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
									ref: canvasRef,
									"data-canvas-surface": "true",
									className: "ops-canvas-scale",
									"aria-label": "流程画布",
									role: "group",
									onDoubleClick: handleDoubleClick,
									style: {
										...surfaceStyle,
										...workspace ? workspaceSurface : {},
										width: size.width,
										height: size.height,
										transform: `scale(${zoom})`,
										transformOrigin: "0 0"
									},
									children: [
										nodeGroups.map((group) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
											className: "ops-canvas-group",
											"data-canvas-group": group.type,
											"data-collapsed": group.collapsed ? "true" : void 0,
											style: {
												left: group.x,
												top: group.y,
												width: group.width,
												height: group.height
											},
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
												type: "button",
												className: "ops-canvas-group-label",
												"aria-expanded": !group.collapsed,
												onClick: (event) => {
													event.stopPropagation();
													setCollapsedGroups((current) => {
														const next = new Set(current);
														if (next.has(group.type)) next.delete(group.type);
														else next.add(group.type);
														return next;
													});
												},
												children: `${group.label} · ${group.count}${group.collapsed ? " · 已折叠" : ""}`
											})]
										}, group.type)),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("svg", {
									width: size.width,
									height: size.height,
									style: edgeLayerStyle,
									"aria-hidden": "true",
									children: edges.filter((edge) => !hiddenNodeIds.has(edge.from) && !hiddenNodeIds.has(edge.to)).map((edge) => {
										const from = nodes.find((node) => node.node_id === edge.from);
										const to = nodes.find((node) => node.node_id === edge.to);
										if (!from || !to) return null;
											const dependencyActive = Boolean(selectedId) && dependencyNodeIds.has(edge.from) && dependencyNodeIds.has(edge.to);
											const failureActive = failureNodeIds.has(edge.from) && failureNodeIds.has(edge.to);
										return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("line", {
											"data-edge-from": edge.from,
											"data-edge-to": edge.to,
											"data-runtime-edge": edge.raw.runtime_added ? "true" : void 0,
												className: "ops-canvas-edge",
												"data-edge-active": dependencyActive ? "true" : void 0,
												"data-failure-chain": failureActive ? "true" : void 0,
											x1: from.position.x + NODE_WIDTH,
											y1: from.position.y + nodeHeight / 2,
											x2: to.position.x,
											y2: to.position.y + nodeHeight / 2,
											stroke: failureActive ? COLOR.danger : edge.raw.runtime_added ? COLOR.link : workspace ? "#8c9ebb" : COLOR.line,
											strokeDasharray: edge.raw.runtime_added ? "6 3" : void 0,
											strokeWidth: 2
										}, `${edge.from}->${edge.to}`);
									})
								}), nodes.filter((node) => !hiddenNodeIds.has(node.node_id)).map((node) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
									"data-node-id": node.node_id,
									"data-node-type": node.type,
										className: "ops-canvas-node",
										"data-collapsed": collapsedNodes.has(node.node_id) ? "true" : void 0,
										"data-dependency-highlight": selectedId && dependencyNodeIds.has(node.node_id) ? "true" : void 0,
										"data-failure-chain": failureNodeIds.has(node.node_id) ? "true" : void 0,
									"data-runtime-status": runtimeByNode.get(node.node_id)?.status,
									"data-runtime-execution-state": runtimeByNode.get(node.node_id)?.execution_state,
									"data-runtime-attempt": runtimeByNode.get(node.node_id)?.attempt,
									"data-runtime-added": node.raw.runtime_step ? "true" : void 0,
									"data-runtime-held": runtimeTopology?.held_node_ids.includes(node.node_id) ? "true" : void 0,
									"data-node-x": node.position.x,
									"data-node-y": node.position.y,
									"data-node-readonly": readOnly ? "true" : "false",
									"aria-label": compact ? `步骤 ${node.title}` : `节点 ${node.node_id} ${node.title}`,
									"aria-disabled": readOnly ? "true" : "false",
									onMouseDown: (event) => startMove(event, node),
									onClick: () => setSelectedId(node.node_id),
									onDoubleClick: (event) => {
										event.stopPropagation();
										setCollapsedNodes((current) => {
											const next = new Set(current);
											if (next.has(node.node_id)) next.delete(node.node_id);
											else next.add(node.node_id);
											return next;
										});
									},
									style: {
										...nodeStyle,
										height: nodeHeight,
										left: node.position.x,
										top: node.position.y,
										boxSizing: workspace || repairOverlaps ? "border-box" : void 0,
										...workspace ? {
											cursor: readOnly ? "default" : "move",
											borderColor: selected === node ? "#83a9dc" : "#d2deec",
											background: selected === node ? "#edf5ff" : COLOR.surface
										} : {}
									},
									children: [
										inspectNodes ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
											type: "button",
											"aria-pressed": selected === node,
											onClick: () => setSelectedId(node.node_id),
											style: nodeSelectStyle,
											children: node.title
										}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
											style: nodeTitleStyle,
											children: node.title
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
											style: nodeMetaStyle,
											children: runtimeNodes ? runtimeLabel(node.node_id) : /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [NODE_TYPE_LABEL[node.type] ?? node.type, MULTI_OUT_TYPES.has(node.type) ? " · 可多条出边" : ""] })
										}),
										runtimeNodes && runtimeByNode.get(node.node_id)?.attempt != null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
											style: nodeMetaStyle,
											children: [
												"第 ",
												runtimeByNode.get(node.node_id)?.attempt,
												" 次执行"
											]
										}),
										node.raw.runtime_step != null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
											style: nodeMetaStyle,
											children: ["来源角色：", String(node.raw.runtime_step.source_title)]
										}),
										runtimeTopology?.held_node_ids.includes(node.node_id) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
											style: {
												...nodeMetaStyle,
												color: COLOR.warning
											},
											children: "等待返工或验收"
										}),
										runtimeTopology?.requests.filter((r) => r.target_node_id === node.node_id).slice(-1).map((r) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
											style: {
												...nodeMetaStyle,
												color: COLOR.warning
											},
											children: [
												"成果反馈 · 第 ",
												r.round_no,
												" 轮"
											]
										}, r.request_id)),
										!compact && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
											type: "button",
											"data-node-handle": node.node_id,
											"aria-label": `从 ${node.node_id} 拉出连线`,
											"aria-disabled": readOnly ? "true" : "false",
											disabled: readOnly,
											onMouseDown: (event) => startLink(event, node),
											style: handleStyle
										}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
											type: "button",
											"data-node-delete": node.node_id,
											"aria-label": `删除节点 ${node.node_id}`,
											"aria-disabled": readOnly ? "true" : "false",
											disabled: readOnly,
											onMouseDown: (event) => event.stopPropagation(),
											onClick: (event) => {
												event.stopPropagation();
												deleteNode(node.node_id);
											},
											style: deleteStyle,
											children: "×"
										})] })
									]
								}, node.node_id))]
							})
						]})}), inspectNodes && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("aside", {
							style: inspectionPanel,
							"aria-label": "节点详情",
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
									style: sectionTitleStyle$4,
									children: "节点详情"
								}),
								!selected && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$9,
									children: "点选画布中的节点查看依赖。"
								}),
								selected && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: selected.title }) }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
										style: mutedStyle$9,
										children: [NODE_TYPE_LABEL[selected.type] ?? "步骤类型未记录", !compact && ` · ${selected.node_id}`]
									}),
									selectedConfig && !readOnly && onChange && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
										type: "checkbox",
										disabled: !editRecoveryRetry || irreversible && !retryEnabled,
										"aria-describedby": retryHelpId,
										checked: retryEnabled,
										onChange: (event) => setRecoveryRetry(selected, event.currentTarget.checked)
									}), "核实后允许重试"] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										id: retryHelpId,
										style: mutedStyle$9,
										children: irreversible ? "这一步含不可逆操作，发布前请关闭重试。" : "仅为可安全重复执行的步骤开启。发布后，新运行中确认上次未启动或副作用未发生时，才可创建新尝试。"
									})] }),
									runtimeByNode.get(selected.node_id) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
										runtimeLabel(selected.node_id),
										" · 第 ",
										runtimeByNode.get(selected.node_id)?.attempt ?? "未知",
										" 次执行"
									] }),
									selected.raw.runtime_step != null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["来源角色：", String(selected.raw.runtime_step.source_title)] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: String(selected.raw.runtime_step.instruction) })] }),
									runtimeTopology?.requests.filter((r) => r.target_node_id === selected.node_id).map((r) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
										"成果反馈 · 第 ",
										r.round_no,
										" 轮 · ",
										REWORK_STATUS[r.status] ?? "返工状态未确认"
									] }, r.request_id)),
									runtimeTopology?.held_node_ids.includes(selected.node_id) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "受影响步骤：等待返工或验收" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["上游 · ", edges.filter((edge) => edge.to === selected.node_id).map((edge) => nodes.find((node) => node.node_id === edge.from)?.title ?? "未命名步骤").join("、") || "无"] }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["下游 · ", edges.filter((edge) => edge.from === selected.node_id).map((edge) => nodes.find((node) => node.node_id === edge.to)?.title ?? "未命名步骤").join("、") || "无"] }),
									compact && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看步骤登记明细" }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["步骤编号 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: selected.node_id })] }),
										runtimeTopology?.requests.filter((request) => request.target_node_id === selected.node_id).map((request) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
											"反馈编号 ",
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: request.request_id }),
											" · 状态 ",
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: request.status })
										] }, request.request_id))
									] })
								] })
							]
						})]
					}),
					showMinimap && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("svg", {
						className: "ops-canvas-minimap",
						viewBox: `0 0 ${size.width} ${size.height}`,
						role: "img",
						"aria-label": "画布小地图，点击定位",
						onClick: focusMinimap,
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("rect", {
								x: 0,
								y: 0,
								width: size.width,
								height: size.height,
								fill: "transparent"
							}),
							nodes.map((node) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("rect", {
								x: node.position.x,
								y: node.position.y,
								width: NODE_WIDTH,
								height: nodeHeight,
								rx: 8,
								fill: failureNodeIds.has(node.node_id) ? "#d1434b" : dependencyNodeIds.has(node.node_id) ? "#4a86d8" : "#9aa7b6",
								opacity: 0.78
							}, node.node_id))
						]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						hidden: workspace && readOnly,
						children: workspace ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", { children: [
							"连线（",
							edges.length,
							"）"
						] }), renderEdges()] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h3", {
							style: sectionTitleStyle$4,
							children: [
								"连线（",
								edges.length,
								"）"
							]
						}), renderEdges()] })
					})
				]
			});
			function renderEdges() {
				return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(react_jsx_runtime.Fragment, { children: edges.length === 0 ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					style: mutedStyle$9,
					children: "还没有连线。"
				}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
					style: edgeListStyle,
					children: edges.map((edge) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", {
						"data-edge-row": `${edge.from}->${edge.to}`,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [
							compact ? nodes.find((node) => node.node_id === edge.from)?.title ?? edge.from : edge.from,
							" → ",
							compact ? nodes.find((node) => node.node_id === edge.to)?.title ?? edge.to : edge.to,
							edge.route?.priority === void 0 ? "" : ` （优先级 ${edge.route.priority}）`
						] }), !compact && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							"data-edge-delete-from": edge.from,
							"data-edge-delete-to": edge.to,
							"aria-label": `删除连线 ${edge.from} 到 ${edge.to}`,
							"aria-disabled": readOnly ? "true" : "false",
							disabled: readOnly,
							onClick: () => deleteEdge(edge.from, edge.to),
							style: edgeDeleteStyle,
							children: "删除"
						})]
					}, `${edge.from}->${edge.to}`))
				}) });
			}
		}
		function nodeConfig(node) {
			const config = node.raw.config;
			if (config === void 0) return {};
			if (config && typeof config === "object" && !Array.isArray(config)) return config;
		}
		const rootStyle = {
			display: "grid",
			gap: 12
		};
		const toolbarStyle = {
			display: "flex",
			flexWrap: "wrap",
			gap: 24,
			alignItems: "flex-start"
		};
		const workspaceToolbar = {
			display: "flex",
			flexWrap: "wrap",
			gap: 8,
			alignItems: "center",
			justifyContent: "space-between"
		};
		const sectionTitleStyle$4 = {
			margin: "0 0 4px",
			fontSize: 14
		};
		const mutedStyle$9 = {
			margin: "0 0 6px",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.helper
		};
		const paletteStyle = {
			display: "flex",
			flexWrap: "wrap",
			gap: 5
		};
		const paletteButtonStyle = {
			minHeight: 30,
			padding: "0 10px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 6,
			background: COLOR.surface,
			color: COLOR.ink,
			fontSize: FONT_PX.helper,
			cursor: "grab"
		};
		const buttonStyle$7 = {
			minHeight: 32,
			padding: "0 12px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 7,
			background: COLOR.surface,
			color: COLOR.ink,
			fontSize: FONT_PX.menu,
			cursor: "pointer"
		};
		const alertStyle = {
			margin: 0,
			padding: 10,
			color: COLOR.danger,
			background: "#fff1f0",
			borderRadius: 8,
			fontSize: FONT_PX.body
		};
		const viewportStyle = {
			maxHeight: 520,
			overflow: "auto",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 8,
			background: COLOR.canvas
		};
		const surfaceStyle = {
			position: "relative",
			minWidth: "100%",
			backgroundImage: "linear-gradient(to right, #f1f2f4 1px, transparent 1px), linear-gradient(to bottom, #f1f2f4 1px, transparent 1px)",
			backgroundSize: "40px 40px"
		};
		const workspaceViewport = {
			...viewportStyle,
			maxHeight: 400,
			padding: 12,
			background: "#f5f8fc"
		};
		const workspaceSurface = {
			backgroundImage: "radial-gradient(#cad6e6 1px, transparent 1px)",
			backgroundSize: "18px 18px"
		};
		const edgeLayerStyle = {
			position: "absolute",
			left: 0,
			top: 0,
			pointerEvents: "none"
		};
		const nodeStyle = {
			position: "absolute",
			width: NODE_WIDTH,
			height: NODE_HEIGHT,
			display: "grid",
			alignContent: "center",
			gap: 2,
			padding: "6px 10px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 8,
			background: COLOR.surface,
			boxShadow: "0 1px 2px rgba(17, 19, 22, 0.08)",
			cursor: "move"
		};
		const nodeTitleStyle = {
			fontSize: FONT_PX.body,
			fontWeight: 600,
			overflow: "hidden",
			textOverflow: "ellipsis",
			whiteSpace: "nowrap"
		};
		const nodeSelectStyle = {
			...nodeTitleStyle,
			background: "transparent",
			border: 0,
			color: COLOR.link,
			textAlign: "left",
			cursor: "pointer",
			padding: 0
		};
		const inspectionLayout = {
			display: "grid",
			gridTemplateColumns: "minmax(0, 1fr) minmax(120px, 24%)",
			gap: 10,
			minWidth: 0
		};
		const taskInspectionLayout = {
			display: "grid",
			gridTemplateColumns: "minmax(0, 1fr)",
			gap: 10
		};
		const inspectionPanel = {
			padding: 10,
			borderRadius: 8,
			border: `1px solid ${COLOR.line}`,
			minWidth: 0,
			overflowWrap: "anywhere"
		};
		const nodeMetaStyle = {
			color: COLOR.inkMuted,
			fontSize: FONT_PX.helper
		};
		const handleStyle = {
			position: "absolute",
			right: -7,
			top: NODE_HEIGHT / 2 - 7,
			width: 14,
			height: 14,
			padding: 0,
			border: `2px solid ${COLOR.link}`,
			borderRadius: "50%",
			background: COLOR.canvas,
			cursor: "crosshair"
		};
		const deleteStyle = {
			position: "absolute",
			right: 2,
			top: 2,
			width: 20,
			height: 20,
			padding: 0,
			border: "none",
			borderRadius: 4,
			background: "transparent",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.helper,
			cursor: "pointer"
		};
		const edgeListStyle = {
			margin: 0,
			paddingLeft: 18,
			display: "grid",
			gap: 4,
			fontSize: FONT_PX.body
		};
		const edgeDeleteStyle = {
			marginLeft: 8,
			minHeight: 24,
			padding: "0 8px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 6,
			background: COLOR.surface,
			color: COLOR.ink,
			fontSize: FONT_PX.helper,
			cursor: "pointer"
		};
		// src/client/panel/workflow-dependency-view.tsx
		function WorkflowDependencyView({ doc, nodes }) {
			const topology = (0, react.useMemo)(() => topologicalOrder(doc), [doc]);
			if (!doc.nodes.length) return null;
			if (!topology.ok) return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				role: "status",
				children: "流程依赖存在环路，无法按层展示。"
			});
			const attempts = currentAttempts(nodes ?? []);
			const byId = new Map(doc.nodes.map((node) => [node.node_id, node]));
			const levels = /* @__PURE__ */ new Map();
			const incoming = /* @__PURE__ */ new Map();
			const outgoing = /* @__PURE__ */ new Map();
			for (const id of topology.order) {
				const layer = topology.layer.get(id);
				const level = levels.get(layer) ?? [];
				level.push(byId.get(id));
				levels.set(layer, level);
			}
			for (const { from, to } of doc.edges) {
				const sources = incoming.get(to) ?? /* @__PURE__ */ new Set();
				sources.add(from);
				incoming.set(to, sources);
				const destinations = outgoing.get(from) ?? /* @__PURE__ */ new Set();
				destinations.add(to);
				outgoing.set(from, destinations);
			}
			const title = (id) => attempts.get(id)?.title || byId.get(id)?.title || "未命名步骤";
			const names = (ids) => [...ids].map(title).join("、");
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "ops-surface ops-inset",
				"aria-label": "流程步骤依赖",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
					style: { margin: "0 0 8px" },
					children: "步骤依赖"
				}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ol", {
					style: layersStyle,
					children: [...levels].sort(([left], [right]) => left - right).map(([layer, peers]) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h4", {
						style: headingStyle$2,
						children: [
							"第 ",
							layer + 1,
							" 层",
							peers.length > 1 && ` · 同层 ${peers.length} 步`
						]
					}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
						style: peersStyle,
						children: peers.map(({ node_id: id, type }) => {
							const before = incoming.get(id);
							const after = outgoing.get(id);
							const attempt = attempts.get(id);
							return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", {
								style: peerStyle,
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: title(id) }),
									attempt && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: nodeExecutionLabel(attempt) }),
									before && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("small", { children: [
										before.size > 1 ? "汇合，依赖" : "依赖",
										" ",
										names(before)
									] }),
									after && after.size > 1 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("small", { children: [
										type === "branch" ? "可选出口" : "分支至",
										" ",
										names(after)
									] }),
									before && !after && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("small", { children: "流程终点" })
								]
							}, id);
						})
					})] }, layer))
				})]
			});
		}
		const layersStyle = {
			display: "grid",
			gap: 12,
			listStyle: "none",
			margin: 0,
			padding: 0
		};
		const headingStyle$2 = {
			margin: "0 0 4px",
			color: COLOR.inkMuted,
			fontSize: "inherit"
		};
		const peersStyle = {
			display: "grid",
			gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 14rem), 1fr))",
			gap: 6,
			listStyle: "none",
			margin: 0,
			padding: 0
		};
		const peerStyle = {
			display: "grid",
			gap: 2,
			alignContent: "start",
			minWidth: 0,
			overflowWrap: "anywhere",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 6,
			padding: "6px 8px"
		};
		// src/client/panel/task-step-overview.tsx
		const PAGE_LIMIT = 200;
		function useRunStepSnapshot(api, runId, reloadVersion, retainPrevious = false) {
			const [snapshot, setSnapshot] = (0, react.useState)();
			const [retry, setRetry] = (0, react.useState)(0);
			(0, react.useEffect)(() => {
				if (!runId) return;
				let active = true;
				const key = {
					api,
					runId,
					reloadVersion,
					retry
				};
				api.listNodes(runId, PAGE_LIMIT).then(({ data }) => {
					if (data.items.some((node) => node.run_id !== runId)) throw new Error("步骤归属与当前运行不符");
					if (active) setSnapshot({
						...key,
						data: {
							nodes: data.items,
							partial: data.has_more ?? data.items.length >= PAGE_LIMIT
						},
						failed: false
					});
				}).catch(() => {
					if (active) setSnapshot((previous) => ({
						...key,
						failed: true,
						data: previous?.api === api && previous.runId === runId ? previous.data : void 0
					}));
				});
				return () => {
					active = false;
				};
			}, [
				api,
				runId,
				reloadVersion,
				retry
			]);
			const sameRun = snapshot?.api === api && snapshot.runId === runId ? snapshot : void 0;
			const current = sameRun?.reloadVersion === reloadVersion && sameRun.retry === retry ? sameRun : void 0;
			const data = retainPrevious ? sameRun?.data : current?.failed ? void 0 : current?.data;
			return {
				nodes: data?.nodes,
				partial: data?.partial,
				failed: current?.failed,
				refresh: () => setRetry((value) => value + 1)
			};
		}
		const GROUPS = [
			{
				id: "active",
				title: "进行中"
			},
			{
				id: "capacity",
				title: "等待执行位"
			},
			{
				id: "dependencies",
				title: "等待上游完成"
			},
			{
				id: "ready",
				title: "就绪待派发"
			},
			{
				id: "attention",
				title: "暂停、重试与异常"
			},
			{
				id: "finished",
				title: "已结束"
			}
		];
		function queueGroup(node) {
			const state = executionState(node);
			if (ACTIVE_NODE_STATES.has(state)) return "active";
			if (state === "waiting_capacity") return "capacity";
			if (state === "waiting_dependencies") return "dependencies";
			if (state === "ready") return "ready";
			if (state === "done" || state === "cancelled" || state === "superseded") return "finished";
			return "attention";
		}
		function StepRows({ nodes, labels, groupTitle, joinParents }) {
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
				className: "task-step-overview",
				children: nodes.map((node) => {
					const waiting = waitingFor(node, labels.references);
					const label = groupTitle && executionState(node) === "queued" ? "排队原因未记录" : nodeExecutionLabel(node);
					return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", {
						"data-tone": nodeExecutionPhase(node).tone,
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: labels.titles.get(node.node_id) }),
							labels.namesakes.has(node.node_id) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("small", { children: labels.namesakes.get(node.node_id) }),
							label !== groupTitle && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
								className: "task-step-overview-status",
								children: label
							}),
							node.attempt > 1 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("small", { children: [
								"第 ",
								node.attempt,
								" 次尝试"
							] }),
							joinParents && joinParents.length > 1 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("small", {
								className: "task-step-waiting",
								children: ["汇合 · 依赖 ", parentTitle(joinParents, labels)]
							}),
							waiting && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("small", {
								className: "task-step-waiting",
								children: waiting
							})
						]
					}, node.node_id);
				})
			});
		}
		function parentTitle(ids, labels) {
			return ids.map((id) => labels.references.get(id)).join("、");
		}
		/** A group has one exact set of direct predecessors. Traversal order expresses dependencies. */
		function boardGroups(steps) {
			const ids = new Set(steps.map(({ node_id }) => node_id));
			const pending = /* @__PURE__ */ new Map();
			const children = /* @__PURE__ */ new Map();
			const groups = /* @__PURE__ */ new Map();
			const groupFor = /* @__PURE__ */ new Map();
			const ordered = [];
			for (const step of steps) {
				if (!step.node_id || !Array.isArray(step.depends_on) || step.depends_on.some((id) => !ids.has(id))) return void 0;
				const parents = [...new Set(step.depends_on)].sort();
				const key = JSON.stringify(parents);
				let group = groups.get(key);
				if (!group) groups.set(key, group = {
					key,
					parents,
					steps: []
				});
				group.steps.push(step);
				groupFor.set(step.node_id, group);
				pending.set(step.node_id, parents.length);
				if (parents.length === 0) ordered.push(step);
				for (const parent of parents) {
					const siblings = children.get(parent) ?? [];
					siblings.push(step);
					children.set(parent, siblings);
				}
			}
			for (let index = 0; index < ordered.length; index++) for (const child of children.get(ordered[index].node_id) ?? []) {
				const remaining = pending.get(child.node_id) - 1;
				pending.set(child.node_id, remaining);
				if (remaining === 0) ordered.push(child);
			}
			if (ordered.length !== steps.length || groups.size === steps.length) return void 0;
			return [...new Set(ordered.map(({ node_id }) => groupFor.get(node_id)))];
		}
		function stepLabels(steps) {
			const titles = new Map(steps.map((node, index) => [node.node_id, node.title || `未命名步骤 ${index + 1}`]));
			const byTitle = /* @__PURE__ */ new Map();
			for (const [id, title] of titles) {
				const matches = byTitle.get(title) ?? [];
				matches.push(id);
				byTitle.set(title, matches);
			}
			const namesakes = /* @__PURE__ */ new Map();
			const references = new Map(titles);
			for (const matches of byTitle.values()) {
				if (matches.length < 2) continue;
				matches.sort().forEach((id, index) => {
					const label = `同名步骤 ${index + 1}`;
					namesakes.set(id, label);
					references.set(id, `${titles.get(id)}（${label}）`);
				});
			}
			return {
				titles,
				namesakes,
				references
			};
		}
		function BoardSteps({ steps, labels, partial, openBranches }) {
			const groups = partial ? void 0 : boardGroups(steps);
			if (!groups) return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(StepRows, {
				nodes: steps,
				labels
			});
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
				className: "task-board-step-groups",
				children: groups.map(({ key, parents, steps: group }) => {
					if (group.length === 1) return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(StepRows, {
						nodes: group,
						labels,
						joinParents: parents
					}, key);
					const active = group.filter((step) => ACTIVE_NODE_STATES.has(executionState(step))).length;
					const done = group.filter((step) => executionState(step) === "done").length;
					const upstream = parentTitle(parents, labels);
					return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
						open: openBranches?.has(key),
						onToggle: (event) => {
							if (event.currentTarget.open) openBranches?.add(key);
							else openBranches?.delete(key);
						},
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", {
							"data-task-card-focus": `branch:${key}`,
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("strong", { children: [
								upstream ? `${upstream}之后` : "运行起点",
								" · ",
								group.length,
								" 个并行步骤"
							] }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("small", { children: [
								done,
								" / ",
								group.length,
								" 步完成",
								active > 0 && ` · ${active} 步执行中`
							] })]
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(StepRows, {
							nodes: group,
							labels
						})]
					}, key);
				})
			});
		}
		function TaskStepOverview({ nodes, queue = false, partial = nodes.length >= PAGE_LIMIT, openBranches }) {
			const steps = [...currentAttempts(nodes).values()];
			const labels = stepLabels(steps);
			const groups = /* @__PURE__ */ new Map();
			if (queue) for (const node of steps) {
				const id = queueGroup(node);
				const group = groups.get(id) ?? [];
				group.push(node);
				groups.set(id, group);
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [!steps.length ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				className: "task-flow-empty",
				children: nodes.length ? "读取到的记录中没有当前尝试。" : "这次运行暂无步骤记录。"
			}) : queue ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
				className: "task-queue-groups",
				children: GROUPS.map(({ id, title }) => {
					const group = groups.get(id);
					return group && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
						"aria-label": title,
						className: "task-flow-steps",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
							className: "task-flow-section-head",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: title }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [group.length, " 步"] })]
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(StepRows, {
							nodes: group,
							labels,
							groupTitle: title
						})]
					}, id);
				})
			}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)(BoardSteps, {
				steps,
				labels,
				partial,
				openBranches
			}), partial && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
				className: "task-flow-empty",
				children: [
					"本次最多读取 ",
					PAGE_LIMIT,
					" 条尝试记录，可能还有步骤或较新的尝试未显示。"
				]
			})] });
		}
		// src/client/panel/task-flow-view.tsx
		function TaskFlowView({ api, tasks, onOpenTask, reloadVersion, reworkEnabled, queue = false }) {
			const [selection, setSelection] = (0, react.useState)("");
			const available = queue ? tasks.filter((task) => task.current_run_id) : tasks;
			const selected = available.find((task) => task.task_id === selection) ?? available.find((task) => task.lane === "running" || task.current_run_status === "running") ?? available[0];
			if (!selected) return queue ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				className: "task-flow-empty",
				children: "当前列表中没有已创建运行的任务。"
			}) : null;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				className: "task-flow-view",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("nav", {
					className: "task-flow-picker",
					"aria-label": queue ? "选择任务查看队列" : "选择任务流程",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "task-flow-section-head",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "任务" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [available.length, " 项"] })]
					}), available.map((task) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("button", {
						className: "task-flow-pick",
						type: "button",
						"aria-pressed": task === selected,
						onClick: () => setSelection(task.task_id),
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
								className: "task-flow-pick-meta",
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
									className: "task-type-badge",
									children: taskOriginLabel(task)
								}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: taskCardStatus(task).label ?? COPY.oldPanel })]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: task.title || "未命名任务" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: task.attention_reason ? operatorReason(task.attention_reason) : `${task.run_count} 次运行` }),
							task.next_trigger_at && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: ["下次 · ", formatTaskDate(task.next_trigger_at)] })
						]
					}, task.task_id))]
				}), queue ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskRunQueue, {
					api,
					task: selected,
					reloadVersion
				}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskRunFlow, {
					reworkEnabled,
					api,
					task: selected,
					reloadVersion,
					onOpenTask: (trigger, runId) => onOpenTask(selected.task_id, trigger, runId)
				}, `${selected.task_id}:${selected.current_run_id ?? ""}:${selected.workflow_version ?? ""}`)]
			});
		}
		function TaskRunQueue({ api, task, reloadVersion }) {
			const { nodes, partial, failed, refresh } = useRunStepSnapshot(api, task.current_run_id, reloadVersion);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "task-flow-main",
				"aria-label": "任务队列",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
					className: "task-flow-run-head",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h2", { children: ["队列 · ", task.title || "未命名任务"] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "按所选任务的当前运行查看步骤。" })]
				}), failed ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
					role: "alert",
					className: "task-flow-empty",
					children: ["队列读取失败。", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						className: "task-quiet-button",
						type: "button",
						onClick: refresh,
						children: "重试"
					})]
				}) : !nodes ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					role: "status",
					className: "task-flow-empty",
					children: "正在读取队列…"
				}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskStepOverview, {
					nodes,
					partial,
					queue: true
				})]
			});
		}
		function TaskRunFlow({ api, task, reloadVersion, onOpenTask, reworkEnabled }) {
			const [run, setRun] = (0, react.useState)();
			const [runError, setRunError] = (0, react.useState)(false);
			const [retry, setRetry] = (0, react.useState)(0);
			const { nodes, partial, failed: nodeError, refresh: refreshNodes } = useRunStepSnapshot(api, task.current_run_id, reloadVersion);
			(0, react.useEffect)(() => {
				if (!task.current_run_id) return;
				let current = true;
				setRunError(false);
				api.getRun(task.current_run_id).then(({ data }) => {
					if (data.run_id !== task.current_run_id || data.task_id !== task.task_id) throw new Error("运行归属已变化");
					if (current) setRun(data);
				}).catch(() => {
					if (current) setRunError(true);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				task.task_id,
				task.current_run_id,
				reloadVersion,
				retry
			]);
			const workflow = usePinnedWorkflow(api, task.current_run_id ? run?.workflow_id : task.workflow_id, task.current_run_id ? run?.workflow_version : task.workflow_version);
			const topology = useRunTopology(api, task.current_run_id ?? void 0, Boolean(reworkEnabled), reloadVersion + retry);
			const runtimeDoc = runtimeCanvasDoc(workflow?.definition, topology.data);
			const outcome = run ? taskStatusView(run.status, run.display_status).label ?? COPY.oldPanel : null;
			const duration = runDuration(run?.started_at, run?.ended_at);
			const hasRun = Boolean(task.current_run_id);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "task-flow-main",
				"aria-label": "任务运行流程",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "task-flow-run-head",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "task-flow-pick-meta",
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
									className: "task-type-badge",
									children: taskOriginLabel(task)
								}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [
									taskAutonomyLabel(task),
									" · ",
									task.run_count,
									" 次运行"
								] })]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", { children: task.title || "未命名任务" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: task.attention_reason ? operatorReason(task.attention_reason) : run ? "查看本次运行的步骤与证据。" : "还没有运行记录。" }),
							(run || task.next_trigger_at || task.trigger_enabled === false) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
								className: "task-run-facts",
								children: [
									run && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "最近运行" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: [outcome, run.settling ? " · 正在收尾" : ""] })] }),
									run && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "开始时间" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: run.started_at ? formatTaskDate(run.started_at) : "暂无开始记录" })] }),
									duration && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "用时" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: duration })] }),
									task.next_trigger_at && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "下次触发" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: formatTaskDate(task.next_trigger_at) })] }),
									task.trigger_enabled === false && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "触发器" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: "已暂停" })] })
								]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								className: "task-quiet-button",
								onClick: (event) => onOpenTask(event.currentTarget),
								children: "打开任务详情"
							})
						]
					}),
					runError && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: "alert",
						className: "task-flow-empty",
						children: ["运行信息读取失败。", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							className: "task-quiet-button",
							type: "button",
							onClick: () => setRetry((n) => n + 1),
							children: "重试"
						})]
					}),
					nodeError && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: "alert",
						className: "task-flow-empty",
						children: ["执行步骤读取失败。", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							className: "task-quiet-button",
							type: "button",
							onClick: refreshNodes,
							children: "重试"
						})]
					}),
					partial && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						className: "task-flow-empty",
						children: "部分步骤或较新的尝试未显示。"
					}),
					runtimeDoc && !topology.failed && !topology.loading && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowDependencyView, {
						doc: runtimeDoc,
						nodes
					}),
					hasRun && !nodes && !nodeError && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						className: "task-flow-empty",
						children: "正在读取执行步骤…"
					}),
					(!hasRun || nodes) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskRunSteps, {
						nodes: nodes ?? [],
						doc: runtimeDoc,
						run,
						onOpenRun: run ? (trigger) => onOpenTask(trigger, run.run_id) : void 0,
						renderEvidence: (node) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(ExecutionHistory, {
							api,
							nodeId: node.node_run_id,
							reloadVersion
						}, node.node_run_id), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h4", { children: "执行证据" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(NodeEvidence, {
							api,
							node,
							reloadVersion
						}, node.node_run_id)] })] })
					}),
					reworkEnabled && task.current_run_id && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReworkPanel, {
						api,
						runId: task.current_run_id,
						reloadVersion,
						compact: true
					}),
					workflow?.error && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						className: "task-flow-empty",
						role: "status",
						children: "当前固定版本的流程定义暂时不可读取。"
					}),
					topology.loading && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取本次运行拓扑…"
					}),
					topology.failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "alert",
						children: "运行拓扑读取失败，请刷新后重试。"
					}),
					workflow?.doc && !topology.failed && !topology.loading && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
						className: "task-flow-graph",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", { children: [
							reworkEnabled ? "查看本次运行流程图" : "查看固定流程图",
							" · ",
							workflow.doc.name || "流程",
							" v",
							run?.workflow_version ?? task.workflow_version
						] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowCanvasView, {
							definition: workflow.definition,
							readOnly: true,
							runtimeCompact: true,
							inspectNodes: true,
							runtimeNodesPartial: partial,
							runtimeTopology: topology.data,
							canvasId: "task-run-flow",
							runtimeNodes: nodes
						})]
					})
				]
			});
		}
		// src/client/panel/task-plan-form.tsx
		const checkLabels = {
			workflow: "流程版本",
			coverage: "目标覆盖",
			deliverables: "交付物验收",
			tools: "工具可用及授权",
			dependencies: "执行依赖",
			risks: "风险及应对"
		};
		const severityLabels = {
			low: "低风险",
			medium: "中风险",
			high: "高风险"
		};
		function proposalFailure(error) {
			if (error instanceof ApiError && error.code === "OPERATION_TIMEOUT" && error.details && typeof error.details === "object" && "boundary" in error.details) {
				if (error.details.boundary === "inactivity") return "等待可用的规划回复超时。已保留输入，可以重试。";
				if (error.details.boundary === "maximum") return "规划已达到服务的处理时限。已保留输入，可以重试。";
			}
			return error instanceof Error ? error.message : "暂未生成任务计划。";
		}
		function Brief({ plan }) {
			const { brief, proposal } = plan;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				style: contentStyle,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "目标" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: descriptionStyle,
						children: brief.goal
					})] }),
					brief.requirements.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "必须满足" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
						style: listStyle$3,
						children: brief.requirements.map(({ id, text }) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
							id,
							" · ",
							text
						] }, id))
					})] }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "交付物与验收条件" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
						style: listStyle$3,
						children: proposal.deliverables.map(({ title, node_id, goal_ids, acceptance }, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
							title,
							" · ",
							goal_ids.join("、"),
							" · 由 ",
							node_id,
							" 提交。",
							acceptance.kind === "artifact" ? `验收文件「${acceptance.file_name}」至少 ${acceptance.minimum_bytes} 字节。` : `验收需有 ${acceptance.tool} 退出码为 ${acceptance.exit_code} 的系统证据。`
						] }, index))
					})] })
				]
			});
		}
		function TaskPlanBrief({ api, taskId }) {
			const [record, setRecord] = (0, react.useState)(null);
			(0, react.useEffect)(() => {
				let current = true;
				api.getTaskPlanForTask(taskId).then(({ data }) => {
					if (current) setRecord(data?.launch?.task_id === taskId && data.brief && data.proposal ? {
						taskId,
						plan: data
					} : null);
				}).catch(() => {
					if (current) setRecord(null);
				});
				return () => {
					current = false;
				};
			}, [api, taskId]);
			if (record?.taskId !== taskId) return null;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-label": "已确认任务简报",
				style: panelStyle,
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "任务简报" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(Brief, { plan: record.plan })]
			});
		}
		function TaskPlanForm({ api, commands, onStarted, onOpenRun, workflowRequest, onClearWorkflow }) {
			const [open, setOpen] = (0, react.useState)(Boolean(workflowRequest));
			const [capability, setCapability] = (0, react.useState)("loading");
			const [goal, setGoal] = (0, react.useState)(workflowRequest?.goal ?? "");
			const [criteria, setCriteria] = (0, react.useState)("");
			const [plan, setPlan] = (0, react.useState)(null);
			const [confirmed, setConfirmed] = (0, react.useState)(false);
			const [busy, setBusy] = (0, react.useState)(null);
			const proposal = (0, react.useRef)(null);
			const [proposalCancelled, setProposalCancelled] = (0, react.useState)(false);
			const [failure, setFailure] = (0, react.useState)("");
			(0, react.useLayoutEffect)(() => {
				setBusy((value) => value === "propose" ? null : value);
				return () => {
					proposal.current?.abort();
					proposal.current = null;
				};
			}, [api, commands]);
			(0, react.useEffect)(() => {
				if (!open) return;
				let current = true;
				setCapability("loading");
				api.getTaskPlanCapability().then(({ data }) => {
					if (current) setCapability(data.enabled ? "enabled" : "unavailable");
				}).catch(() => {
					if (current) setCapability("error");
				});
				return () => {
					current = false;
				};
			}, [api, open]);
			async function propose(event) {
				event.preventDefault();
				if (busy || proposal.current || !goal.trim()) return;
				const requirements = criteria.split("\n").map((entry) => entry.trim()).filter(Boolean);
				if (requirements.length > 12 || requirements.some((entry) => entry.length > 300)) {
					setFailure("最多 12 项要求，每项不超过 300 字。");
					return;
				}
				const request = new AbortController();
				proposal.current = request;
				setProposalCancelled(false);
				setBusy("propose");
				setFailure("");
				setConfirmed(false);
				try {
					const proposed = await commands.proposeTaskPlan(goal.trim(), requirements, request.signal);
					if (proposal.current !== request) return;
					if (workflowRequest && (proposed.proposal.workflow_id !== workflowRequest.id || proposed.proposal.workflow_version !== workflowRequest.version)) {
						setFailure("主脑给出的流程或版本与所选流程不同。请调整目标或重新选择已发布流程。");
						return;
					}
					setPlan(proposed);
				} catch (error) {
					if (proposal.current === request) setFailure(proposalFailure(error));
				} finally {
					if (proposal.current === request) {
						proposal.current = null;
						setBusy(null);
					}
				}
			}
			async function start() {
				if (busy || !confirmed || !plan?.quality.ready || plan.launch) return;
				setBusy("start");
				setFailure("");
				try {
					const launch = await commands.confirmTaskPlan(plan.plan_id, plan.content_hash);
					setPlan({
						...plan,
						launch
					});
					onStarted?.(launch.task_id);
				} catch (error) {
					setFailure(error instanceof Error ? error.message : "暂未获取启动回执。");
				} finally {
					setBusy(null);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
				style: panelStyle,
				open,
				onToggle: (event) => {
					if (event.target !== event.currentTarget) return;
					const opened = event.currentTarget.open;
					if (!opened && proposal.current) {
						proposal.current.abort();
						proposal.current = null;
						setBusy(null);
						setProposalCancelled(true);
					}
					setOpen(opened);
				},
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "用自然语言规划任务" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
					style: contentStyle,
					children: [
						workflowRequest && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							disabled: busy === "start",
							style: editStyle,
							onClick: onClearWorkflow,
							children: "不指定流程，从目标开始"
						}),
						capability === "loading" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "status",
							style: descriptionStyle,
							children: "检查主脑规划服务…"
						}),
						capability === "unavailable" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "status",
							style: descriptionStyle,
							children: "当前宿主尚未启用主脑规划。"
						}),
						capability === "error" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "alert",
							style: descriptionStyle,
							children: "暂时无法获取主脑规划状态。"
						}),
						capability === "enabled" && !plan && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
							onSubmit: (event) => {
								propose(event);
							},
							style: contentStyle,
							"aria-busy": busy === "propose",
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
									style: contentStyle,
									children: ["要完成什么", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
										required: true,
										rows: 3,
										maxLength: 2e3,
										value: goal,
										disabled: busy !== null,
										autoFocus: Boolean(workflowRequest),
										placeholder: "例如，复核本周项目交付清单并整理差异",
										onChange: (event) => setGoal(event.target.value)
									})]
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
									style: contentStyle,
									children: ["必须满足的要求（可选，每行一项）", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
										rows: 2,
										maxLength: 3612,
										value: criteria,
										disabled: busy !== null,
										onChange: (event) => setCriteria(event.target.value)
									})]
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
									type: "submit",
									disabled: busy !== null || !goal.trim(),
									children: busy === "propose" ? "正在规划…" : "生成计划"
								})
							]
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							role: "status",
							"aria-atomic": "true",
							children: [busy === "propose" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								style: descriptionStyle,
								children: "正在生成计划。如果没有合适的已发布流程，可能会先生成流程，再编排步骤。收起表单会取消这次生成，输入会保留。"
							}), proposalCancelled && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								style: descriptionStyle,
								children: "已取消生成，输入已保留。可以重新生成计划。"
							})]
						})] }),
						plan && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
							style: contentStyle,
							"aria-label": "计划复核",
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
									style: headingStyle$1,
									children: plan.proposal.title
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
									style: descriptionStyle,
									children: [
										plan.proposal.workflow_id,
										" · 固定 v",
										plan.proposal.workflow_version
									]
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)(Brief, { plan }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: plan.quality.ready ? "质量检查通过，待你确认" : "质量检查发现阻断项" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
									style: listStyle$3,
									children: plan.quality.checks.map((check) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
										checkLabels[check.check],
										" · ",
										check.passed ? "通过" : "需要调整",
										!check.passed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
											style: listStyle$3,
											children: check.issues.map((issue) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", { children: issue }, issue))
										})
									] }, check.check))
								})] }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "步骤、模型建议与风险" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
										style: listStyle$3,
										children: plan.proposal.steps.map((step, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: step.node_id }),
											" · ",
											step.description,
											" · ",
											MODEL_LEVELS[step.complexity].label,
											"。",
											step.model_reason,
											step.preferred_model_ref && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [" · 建议模型 ", step.preferred_model_ref] }),
											step.depends_on.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
												style: descriptionStyle,
												children: ["依赖 ", step.depends_on.join("、")]
											}),
											step.tools.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
												style: descriptionStyle,
												children: ["工具 ", step.tools.join("、")]
											})
										] }, index))
									}),
									plan.proposal.risks.map((risk, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
										style: descriptionStyle,
										children: [
											severityLabels[risk.severity],
											" · ",
											risk.description,
											"。应对措施为",
											risk.mitigation,
											"。"
										]
									}, index))
								] }),
								!plan.launch && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
									style: contentStyle,
									children: [plan.quality.ready && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
										type: "checkbox",
										checked: confirmed,
										disabled: busy !== null,
										onChange: (event) => setConfirmed(event.target.checked)
									}), " 我已核对目标、交付物、风险和固定流程版本。"] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
										disabled: !confirmed || busy !== null,
										onClick: () => {
											start();
										},
										children: busy === "start" ? "正在取得启动回执…" : "确认后启动任务"
									})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										disabled: busy !== null,
										style: editStyle,
										onClick: () => {
											setPlan(null);
											setConfirmed(false);
											setFailure("");
										},
										children: "调整目标并重新规划"
									})]
								}),
								plan.launch && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									role: "status",
									style: descriptionStyle,
									children: "已建立受监督的任务运行。"
								}), onOpenRun && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									onClick: (event) => {
										if (plan.launch) onOpenRun(plan.launch.task_id, event.currentTarget, plan.launch.run_id);
									},
									children: "查看本次运行"
								})] })
							]
						}),
						failure && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "alert",
							style: {
								...descriptionStyle,
								color: COLOR.danger
							},
							children: failure
						})
					]
				})]
			});
		}
		const panelStyle = {
			padding: 12,
			border: `1px solid ${COLOR.line}`,
			borderRadius: 8,
			background: COLOR.surface,
			color: COLOR.ink
		};
		const contentStyle = {
			display: "grid",
			gap: 8,
			marginTop: 5,
			fontSize: FONT_PX.body,
			minWidth: 0
		};
		const headingStyle$1 = {
			fontSize: FONT_PX.section,
			margin: "4px 0"
		};
		const descriptionStyle = {
			margin: "4px 0",
			color: COLOR.inkMuted,
			overflowWrap: "anywhere"
		};
		const listStyle$3 = {
			margin: "5px 0",
			paddingLeft: 22,
			overflowWrap: "anywhere"
		};
		const editStyle = {
			justifySelf: "start",
			color: COLOR.ink,
			background: COLOR.surface,
			border: `1px solid ${COLOR.line}`,
			borderRadius: 5,
			minHeight: 36
		};
		// src/client/panel/task-start-form.tsx
		const AUTONOMY_HELP = {
			autonomous: "只有必须人工处理或你点名的事项会进入「审批中心 → 待处理」。普通决定可在「全部进行中」查看，超时按默认规则处理。",
			assisted: "普通决定也会进入「审批中心 → 待处理」，你可按权限提前处理。审计专属决定可在「全部进行中」查看。普通决定超时按默认规则处理。",
			supervised: "你可处理的决定会进入「审批中心 → 待处理」。超时仍等待获授权的处理。"
		};
		function TaskStartForm({ api, commands, onCreated, onOpenRun, collaborationEnabled, onPlanWorkflow }) {
			const [workflows, setWorkflows] = (0, react.useState)(null);
			const [failed, setFailed] = (0, react.useState)(false);
			const [title, setTitle] = (0, react.useState)("");
			const [selected, setSelected] = (0, react.useState)("");
			const [autonomy, setAutonomy] = (0, react.useState)("autonomous");
			const autonomyHelpId = (0, react.useId)();
			const [pending, setPending] = (0, react.useState)(null);
			const [notice, setNotice] = (0, react.useState)("");
			const [busy, setBusy] = (0, react.useState)(false);
			const [receipt, setReceipt] = (0, react.useState)(null);
			(0, react.useEffect)(() => {
				let current = true;
				api.listWorkflows({
					status: "active",
					limit: 100
				}).then(({ data }) => {
					if (current) setWorkflows(data.items.filter((item) => item.scope === "reusable" && item.current_version !== null));
				}).catch(() => {
					if (current) setFailed(true);
				});
				return () => {
					current = false;
				};
			}, [api]);
			async function submit(event) {
				event.preventDefault();
				const workflow = workflows?.find((item) => item.workflow_id === selected);
				const trimmedTitle = title.trim();
				if (onPlanWorkflow) {
					if (workflow && workflow.current_version !== null && trimmedTitle) onPlanWorkflow({
						...workflow,
						current_version: workflow.current_version
					}, trimmedTitle);
					return;
				}
				const input = pending ?? (workflow && workflow.current_version !== null && trimmedTitle ? {
					operationId: newOperationId(),
					title: trimmedTitle,
					workflowId: workflow.workflow_id,
					version: String(workflow.current_version),
					autonomy
				} : null);
				if (!input || busy) return;
				setBusy(true);
				setReceipt(null);
				setNotice("");
				try {
					const result = await commands.start(input);
					if (result.task_id && result.run_id) setReceipt({
						taskId: result.task_id,
						runId: result.run_id
					});
					setTitle("");
					setPending(null);
					setNotice("任务已创建，执行步骤已入队。");
					onCreated();
				} catch (error) {
					if (error instanceof ApiError && error.code === "OPERATION_OUTCOME_UNKNOWN") setPending(input);
					else setPending(null);
					setNotice(error instanceof Error ? error.message : "任务创建暂不可确认。");
				} finally {
					setBusy(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
				className: "task-launch-form",
				onSubmit: (event) => {
					submit(event);
				},
				"aria-label": "从已发布流程创建任务",
				children: [
					failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "alert",
						children: "暂时无法读取已发布流程。关闭后重新打开可再试。"
					}),
					!workflows && !failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取已发布流程…"
					}),
					workflows?.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "发布一个可复用流程后，就可以从这里创建任务。" }),
					workflows && workflows.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["任务标题 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
							value: title,
							maxLength: 200,
							disabled: busy || Boolean(pending),
							onChange: (event) => setTitle(event.target.value)
						})] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["已发布流程 ", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
							value: selected,
							disabled: busy || Boolean(pending),
							onChange: (event) => setSelected(event.target.value),
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
								value: "",
								children: "选择流程"
							}), workflows.map((item) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
								value: item.workflow_id,
								children: [
									item.name,
									" · v",
									item.current_version
								]
							}, item.workflow_id))]
						})] }),
						onPlanWorkflow ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "补充本次的交付物和验收条件，确认固定流程版本后再启动。" }) : /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["审批提醒与等待方式 ", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
								value: autonomy,
								"aria-describedby": autonomyHelpId,
								disabled: busy || Boolean(pending),
								onChange: (event) => setAutonomy(event.target.value),
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "autonomous",
										children: "自主执行（默认）"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "assisted",
										children: "协助执行"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "supervised",
										children: "人工监督"
									})
								]
							})] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								id: autonomyHelpId,
								children: AUTONOMY_HELP[autonomy]
							}),
							collaborationEnabled && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "需要两个步骤互相检查时，可在「协作核验」选择参与步骤。" })
						] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
							type: "submit",
							disabled: busy || !pending && (!title.trim() || !selected),
							children: onPlanWorkflow ? "用此流程规划任务" : pending ? "查询或重试" : "创建并排队"
						})
					] }),
					notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: notice
					}),
					receipt && onOpenRun && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						onClick: (event) => onOpenRun(receipt.taskId, event.currentTarget, receipt.runId),
						children: "查看本次运行"
					})
				]
			});
		}
		// src/client/panel/display-preferences.ts
		const TASK_VIEWS = [
			{
				id: "board",
				label: "看板"
			},
			{
				id: "flow",
				label: "流程"
			},
			{
				id: "queue",
				label: "队列"
			},
			{
				id: "list",
				label: "列表"
			}
		];
		const EVENT_PAGE_SIZES = [
			5,
			10,
			20
		];
		const DISPLAY_PREFERENCES_KEY = "dsh-orchestra:display-preferences";
		const defaults = {
			taskView: "board",
			eventPageSize: 5
		};
		const listeners = /* @__PURE__ */ new Set();
		let state = {
			preferences: defaults,
			persistent: true
		};
		let pendingLocalChange = false;
		function decode(stored) {
			try {
				const value = JSON.parse(stored ?? "null");
				return {
					taskView: TASK_VIEWS.find(({ id }) => id === value?.taskView)?.id ?? defaults.taskView,
					eventPageSize: EVENT_PAGE_SIZES.find((size) => size === value?.eventPageSize || String(size) === value?.eventPageSize) ?? defaults.eventPageSize
				};
			} catch {
				return defaults;
			}
		}
		function readDisplayPreferences() {
			if (typeof window === "undefined" || pendingLocalChange) return state;
			try {
				state = {
					preferences: decode(window.localStorage.getItem(DISPLAY_PREFERENCES_KEY)),
					persistent: true
				};
			} catch {
				state = {
					...state,
					persistent: false
				};
			}
			return state;
		}
		function save(preferences) {
			try {
				window.localStorage.setItem(DISPLAY_PREFERENCES_KEY, JSON.stringify(preferences));
				pendingLocalChange = false;
			} catch {
				pendingLocalChange = true;
			}
			state = {
				preferences,
				persistent: !pendingLocalChange
			};
			for (const notify of listeners) notify();
		}
		function updateDisplayPreferences(next) {
			save({
				...readDisplayPreferences().preferences,
				...next
			});
		}
		function resetDisplayPreferences() {
			save(defaults);
		}
		function subscribeDisplayPreferences(notify) {
			listeners.add(notify);
			function onStorage(event) {
				if (event.key !== "dsh-orchestra:display-preferences" && event.key !== null) return;
				notify();
			}
			window.addEventListener("storage", onStorage);
			return () => {
				listeners.delete(notify);
				window.removeEventListener("storage", onStorage);
			};
		}
		// src/client/panel/tasks-screen.tsx
		const ATTENTION_LABEL = {
			decision: "待审批",
			unverified: "待核验",
			failed: "执行失败",
			orphaned: "待接管",
			running: "执行中",
			none: "无待处理事项"
		};
		const STATUS_LABEL$1 = {
			reserved: "已接收",
			queued: "排队中",
			running: "进行中",
			waiting: "等待中",
			succeeded: "已完成",
			failed: "未成功",
			cancelled: "已取消",
			paused: "已暂停",
			settling: "正在收尾",
			orphaned: "等待接手",
			blocked: "执行受阻"
		};
		const EFFECT_LABEL$1 = {
			pure: "只读步骤",
			compensable: "可能需要补偿",
			irreversible: "涉及不可逆操作"
		};
		/** 未验证终态是独立状态：自己的中文文案与色板，不折叠进「已完成」或「未成功」。 */
		const UNVERIFIED_STATUS = {
			succeeded_with_unverified: {
				label: COPY.succeededWithUnverified,
				tone: COLOR.warning
			},
			failed_with_unverified: {
				label: COPY.failedWithUnverified,
				tone: COLOR.danger
			}
		};
		/** 交付验收结论的独立文案与色板：验收没过就不算目标达成，不能显示成「已完成」。 */
		const ACCEPTANCE_STATUS = {
			failed: {
				label: COPY.deliveryAcceptanceFailed,
				tone: COLOR.danger
			},
			unverifiable: {
				label: COPY.deliveryAcceptanceUnverifiable,
				tone: COLOR.warning
			}
		};
		/** 任务状态文案与色板：display_status 优先，未验证终态走独立文案。 */
		function taskStatusView(status, displayStatus = status) {
			const unverified = UNVERIFIED_STATUS[status] ?? UNVERIFIED_STATUS[displayStatus];
			if (unverified) return {
				label: unverified.label,
				tone: unverified.tone
			};
			return {
				label: STATUS_LABEL$1[displayStatus] ?? STATUS_LABEL$1[status],
				tone: void 0
			};
		}
		function taskCardStatus(task) {
			if (task.lane === "upcoming" && task.trigger_enabled === true) return {
				label: task.trigger_kind === "scheduled" ? "已排期" : "等待事件",
				tone: COLOR.link
			};
			const acceptance = task.delivery_acceptance ? ACCEPTANCE_STATUS[task.delivery_acceptance] : void 0;
			if (acceptance) return {
				label: `${taskStatusView(task.status, task.display_status).label ?? COPY.oldPanel} · ${acceptance.label}`,
				tone: acceptance.tone
			};
			return taskStatusView(task.status, task.display_status);
		}
		const ORIGINS = {
			manual: "人工",
			scheduled: "定时",
			event: "事件触发",
			continuous: "持续"
		};
		const AUTONOMY = {
			autonomous: "自主执行",
			assisted: "协助执行",
			supervised: "人工监督"
		};
		const ABSOLUTE_STATUSES = [
			"reserved",
			"queued",
			"running",
			"waiting",
			"paused",
			"succeeded",
			"succeeded_with_unverified",
			"failed",
			"failed_with_unverified",
			"cancelled"
		];
		const taskOriginLabel = ({ trigger_kind }) => trigger_kind ? ORIGINS[trigger_kind] : "来源未记录";
		const taskAutonomyLabel = (task) => AUTONOMY[task.autonomy] ?? "自治档位未知";
		function taskColumn(task) {
			if (task.lane) return task.lane;
			if (task.open_decision_count > 0 || task.unverified_count > 0 || [
				"decision",
				"unverified",
				"failed",
				"orphaned"
			].includes(task.attention) || [
				"paused",
				"orphaned",
				"failed",
				"failed_with_unverified"
			].includes(task.status)) return "attention";
			if ([
				"succeeded",
				"succeeded_with_unverified",
				"cancelled"
			].includes(task.status)) return "finished";
			if (["reserved", "queued"].includes(task.status)) return "upcoming";
			if (task.attention === "running" || [
				"running",
				"settling",
				"waiting"
			].includes(task.status)) return "running";
			return "upcoming";
		}
		var TaskBoardScroll = class extends react.Component {
			board = (0, react.createRef)();
			getSnapshotBeforeUpdate() {
				const board = this.board.current;
				const active = board?.ownerDocument.activeElement;
				if (!active || !board?.contains(active)) return null;
				const card = active.closest("[data-board-task-id]");
				const action = active.getAttribute("data-task-card-focus");
				return card?.dataset.boardTaskId && action ? {
					taskId: card.dataset.boardTaskId,
					runId: card.dataset.boardRunId ?? "",
					action
				} : null;
			}
			componentDidUpdate(_previousProps, _previousState, previous) {
				const board = this.board.current;
				if (!board || !previous) return;
				const doc = board.ownerDocument;
				if (doc.activeElement && doc.activeElement !== doc.body && doc.activeElement !== doc.documentElement) return;
				const card = Array.from(board.querySelectorAll("[data-board-task-id]")).find(({ dataset }) => dataset.boardTaskId === previous.taskId && (dataset.boardRunId ?? "") === previous.runId);
				const matching = Array.from(card?.querySelectorAll("[data-task-card-focus]") ?? []).find((control) => control.dataset.taskCardFocus === previous.action && !control.closest("[hidden]"));
				const fallback = card?.querySelector("[data-task-card-focus=\"steps\"]") ?? card?.querySelector("[data-task-card-focus=\"inspect\"]") ?? board;
				(matching ?? fallback).focus();
			}
			render() {
				return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
					className: "task-board-scroll",
					ref: this.board,
					role: "region",
					"aria-label": "任务看板",
					tabIndex: -1,
					children: this.props.children
				});
			}
		};
		function TaskListScreen({ api, onSelectTask, selectedTaskId, reloadVersion = 0, onSnapshot, commands, triggerManager, reworkEnabled, collaborationEnabled, workflowLaunchRequiresPlan }) {
			const [search, setSearch] = (0, react.useState)("");
			const [view, setView] = (0, react.useState)(() => readDisplayPreferences().preferences.taskView);
			const [expandedTaskId, setExpandedTaskId] = (0, react.useState)(null);
			const [filter, setFilter] = (0, react.useState)({});
			const [status, setStatus] = (0, react.useState)("");
			const [period, setPeriod] = (0, react.useState)("");
			const [dateFrom, setDateFrom] = (0, react.useState)("");
			const [dateTo, setDateTo] = (0, react.useState)("");
			const [dateInvalid, setDateInvalid] = (0, react.useState)(false);
			const [startOpen, setStartOpen] = (0, react.useState)(false);
			const [workflowPlan, setWorkflowPlan] = (0, react.useState)();
			const [cursor, setCursor] = (0, react.useState)();
			const [items, setItems] = (0, react.useState)([]);
			const [nextCursor, setNextCursor] = (0, react.useState)(null);
			const [loading, setLoading] = (0, react.useState)(true);
			const [error, setError] = (0, react.useState)(false);
			const [reload, setReload] = (0, react.useState)(0);
			const [selectedIds, setSelectedIds] = (0, react.useState)(() => /* @__PURE__ */ new Set());
			const [batchBusy, setBatchBusy] = (0, react.useState)(null);
			const [batchNotice, setBatchNotice] = (0, react.useState)("");
			(0, react.useEffect)(() => {
				setSelectedIds((current) => {
					if (current.size === 0) return current;
					const visible = new Set(items.map((task) => task.task_id));
					let changed = false;
					const next = /* @__PURE__ */ new Set();
					for (const id of current) {
						if (visible.has(id)) next.add(id);
						else changed = true;
					}
					return changed ? next : current;
				});
			}, [items]);
			const lastReloadVersion = (0, react.useRef)(reloadVersion);
			const expandedRunId = view === "board" && !filter.archived ? items.find((task) => task.task_id === expandedTaskId)?.current_run_id ?? null : null;
			const boardSteps = useRunStepSnapshot(api, expandedRunId, reload, true);
			const openBranches = (0, react.useMemo)(() => /* @__PURE__ */ new Set(), [
				api,
				expandedTaskId,
				expandedRunId
			]);
			(0, react.useEffect)(() => {
				if (lastReloadVersion.current === reloadVersion) return;
				lastReloadVersion.current = reloadVersion;
				setCursor(void 0);
				setNextCursor(null);
				setReload((value) => value + 1);
			}, [reloadVersion]);
			(0, react.useEffect)(() => {
				let current = true;
				setLoading(true);
				setError(false);
				api.listTasks({
					limit: 50,
					cursor,
					...filter
				}).then((result) => {
					if (!current) return;
					setItems((previous) => mergeByTaskId(cursor ? previous : [], result.data.items));
					setNextCursor(result.data.next_cursor);
					onSnapshot?.(result.as_of_seq);
				}).catch(() => {
					if (!current) return;
					setError(true);
				}).finally(() => {
					if (current) setLoading(false);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				cursor,
				filter,
				reload,
				onSnapshot
			]);
			function updateFilter(next) {
				setCursor(void 0);
				setNextCursor(null);
				setItems([]);
				setLoading(true);
				setFilter((current) => ({
					...current,
					...next
				}));
			}
			function submitSearch(event) {
				event.preventDefault();
				updateFilter({ query: search.trim() || void 0 });
			}
			function changePeriod(value, from = dateFrom, to = dateTo) {
				setPeriod(value);
				const invalid = value === "custom" && Boolean(from && to && from > to);
				setDateInvalid(invalid);
				if (!invalid) updateFilter(dateBounds(value, from, to));
			}
			function refresh() {
				setCursor(void 0);
				setReload((n) => n + 1);
			}
			function toggleSelected(taskId) {
				setSelectedIds((current) => {
					const next = new Set(current);
					if (next.has(taskId)) next.delete(taskId);
					else next.add(taskId);
					return next;
				});
			}
			function toggleAllSelected() {
				setSelectedIds((current) => {
					if (items.every((task) => current.has(task.task_id))) return /* @__PURE__ */ new Set();
					return new Set(items.map((task) => task.task_id));
				});
			}
			async function runBatch(action) {
				if (!commands?.batchTasks || selectedIds.size === 0 || batchBusy) return;
				if (action === "cancel" && !window.confirm(`确认取消选中的 ${selectedIds.size} 个任务？`)) return;
				setBatchBusy(action);
				setBatchNotice("");
				try {
					const result = await commands.batchTasks(action, [...selectedIds], action === "cancel" ? "cancelled_by_user" : "batch_dispatch");
					setBatchNotice(`已处理 ${result.applied ?? 0} 项，失败 ${result.failed ?? 0} 项。`);
					setSelectedIds(/* @__PURE__ */ new Set());
					refresh();
				} catch (batchError) {
					setBatchNotice(batchError instanceof Error ? batchError.message : "批量操作失败。");
				} finally {
					setBatchBusy(null);
				}
			}
			function exportSelected() {
				const chosen = items.filter((task) => selectedIds.has(task.task_id));
				const payload = {
					exported_at: (/* @__PURE__ */ new Date()).toISOString(),
					tasks: chosen
				};
				downloadTextFile("dsh-tasks.json", JSON.stringify(payload, null, 2));
			}
			const grouped = {
				upcoming: [],
				running: [],
				attention: [],
				finished: []
			};
			for (const task of items) grouped[taskColumn(task)].push(task);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "task-list",
				"aria-labelledby": "tasks-title",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("style", { children: TASK_WORKSPACE_CSS }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("style", { children: OPS_MONITOR_CSS }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "task-page-header",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", {
							id: "tasks-title",
							style: titleStyle$1,
							children: filter.archived ? "归档箱" : "任务"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							style: mutedStyle$8,
							children: filter.archived ? "归档任务仍可查看运行记录、结果与证据，也可以取回。" : "在看板展开步骤看进度，打开任务查看结果和证据。"
						})] }), !filter.archived && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "task-page-actions",
							children: [commands && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"aria-expanded": startOpen,
								onClick: () => setStartOpen(!startOpen),
								style: buttonStyle$6,
								children: workflowLaunchRequiresPlan ? "从流程规划任务" : "从流程新建任务"
							}), triggerManager]
						})]
					}),
					!filter.archived && commands && startOpen && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskStartForm, {
						api,
						commands,
						onCreated: refresh,
						onOpenRun: onSelectTask,
						collaborationEnabled,
						onPlanWorkflow: workflowLaunchRequiresPlan ? (workflow, title) => {
							setWorkflowPlan((previous) => ({
								sequence: (previous?.sequence ?? 0) + 1,
								request: {
									id: workflow.workflow_id,
									version: String(workflow.current_version),
									goal: `请使用已发布流程「${workflow.name}」v${workflow.current_version}（${workflow.workflow_id}）完成任务「${title}」。`
								}
							}));
							setStartOpen(false);
						} : void 0
					}),
					!filter.archived && commands && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						className: "task-plan-entry",
						children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskPlanForm, {
							workflowRequest: workflowPlan?.request,
							onClearWorkflow: () => setWorkflowPlan(void 0),
							api,
							commands,
							onStarted: refresh,
							onOpenRun: onSelectTask
						}, workflowPlan?.sequence)
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-toolbar",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["状态 ", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
								value: status,
								onChange: (event) => {
									const value = event.target.value;
									setStatus(value);
									updateFilter({
										lane: value.startsWith("lane:") ? value.slice(5) : void 0,
										status: value && !value.startsWith("lane:") ? value : void 0
									});
								},
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "",
										children: "全部状态"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("optgroup", {
										label: "看板分组",
										children: TASK_LANES.map(({ id, label }) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
											value: `lane:${id}`,
											children: label
										}, id))
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("optgroup", {
										label: "执行状态",
										children: ABSOLUTE_STATUSES.map((id) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
											value: id,
											children: taskStatusView(id).label
										}, id))
									})
								]
							})] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["更新时间 ", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
								value: period,
								onChange: (event) => changePeriod(event.target.value),
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "",
										children: "全部时间"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "today",
										children: "今天"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "7",
										children: "近 7 天"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "30",
										children: "近 30 天"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "custom",
										children: "自选时间段"
									})
								]
							})] }),
							period === "custom" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "task-date-range",
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["从 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									type: "date",
									"aria-label": "开始日期",
									value: dateFrom,
									onChange: (event) => {
										setDateFrom(event.target.value);
										changePeriod("custom", event.target.value, dateTo);
									}
								})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["至 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									type: "date",
									"aria-label": "结束日期",
									value: dateTo,
									onChange: (event) => {
										setDateTo(event.target.value);
										changePeriod("custom", dateFrom, event.target.value);
									}
								})] })]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								className: "task-archive-toggle",
								style: quietButtonStyle$1,
								onClick: () => updateFilter({ archived: filter.archived ? void 0 : "1" }),
								children: filter.archived ? "返回任务" : "归档箱"
							})
						]
					}),
					dateInvalid && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "alert",
						style: mutedStyle$8,
						children: "开始日期应早于或等于结束日期。"
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-toolbar task-viewbar",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["类型 ", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
								value: filter.trigger_kind ?? "",
								onChange: (event) => updateFilter({ trigger_kind: event.target.value || void 0 }),
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "",
										children: "全部类型"
									}),
									Object.entries(ORIGINS).map(([key, label]) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: key,
										children: label
									}, key)),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "unknown",
										children: "来源未记录"
									})
								]
							})] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [
								filter.query ? `“${filter.query}”` : "已加载",
								" · ",
								items.length,
								" 项任务"
							] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
								onSubmit: submitSearch,
								className: "task-search",
								role: "search",
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
										style: visuallyHidden,
										htmlFor: "task-search",
										children: "搜索任务"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
										id: "task-search",
										value: search,
										onChange: (e) => setSearch(e.target.value),
										placeholder: "搜索任务名称",
										style: searchInputStyle
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "submit",
										style: buttonStyle$6,
										children: "搜索"
									})
								]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: refresh,
								disabled: loading,
								style: quietButtonStyle$1,
								children: loading ? "正在更新…" : "刷新"
							}),
							!filter.archived && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
								className: "task-view-group",
								role: "group",
								"aria-label": "任务视图",
								children: TASK_VIEWS.map(({ id, label }) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									"aria-pressed": view === id,
									onClick: () => setView(id),
									children: label
								}, id))
							})
						]
					}),
					!filter.archived && items.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-toolbar task-batchbar",
						role: "group",
						"aria-label": "批量任务操作",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
								type: "checkbox",
								checked: items.length > 0 && items.every((task) => selectedIds.has(task.task_id)),
								onChange: toggleAllSelected
							}), " 全选当前"] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: ["已选 ", selectedIds.size, " 项"] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								style: quietButtonStyle$1,
								disabled: selectedIds.size === 0 || batchBusy !== null || !commands?.batchTasks,
								onClick: () => runBatch("dispatch"),
								children: batchBusy === "dispatch" ? "正在派发…" : "批量派发"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								style: quietButtonStyle$1,
								disabled: selectedIds.size === 0 || batchBusy !== null || !commands?.batchTasks,
								onClick: () => runBatch("cancel"),
								children: batchBusy === "cancel" ? "正在取消…" : "批量取消"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								style: quietButtonStyle$1,
								disabled: selectedIds.size === 0,
								onClick: exportSelected,
								children: "导出所选"
							}),
							batchNotice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
								role: "status",
								style: mutedStyle$8,
								children: batchNotice
							})
						]
					}),
					error && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						role: "alert",
						style: errorStyle$5,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: COPY.queryFailed }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							onClick: refresh,
							style: quietButtonStyle$1,
							children: COPY.retry
						})]
					}),
					loading && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: mutedStyle$8,
						children: "正在读取任务…"
					}),
					!loading && !error && items.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						style: emptyStyle,
						children: filter.archived ? "归档箱没有符合条件的任务。" : Object.values(filter).some(Boolean) ? "没有符合筛选条件的任务。" : COPY.emptyTasks
					}),
					!filter.archived && view === "board" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskBoardScroll, { children: (!loading || items.length > 0) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						className: "task-board",
						children: TASK_LANES.map(({ id, label }) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
							className: "task-column",
							"data-task-column": id,
							"aria-labelledby": `tasks-${id}`,
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h2", {
									id: `tasks-${id}`,
									className: "task-column-title",
									children: [label, /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: grouped[id].length })]
								}),
								grouped[id].length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									className: "task-column-empty",
									children: "暂无任务"
								}),
								grouped[id].map((task) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskCard, {
									task,
									selected: task.task_id === selectedTaskId,
									selectedBatch: selectedIds.has(task.task_id),
									onToggleBatch: () => toggleSelected(task.task_id),
									compact: Boolean(selectedTaskId),
									expanded: expandedTaskId === task.task_id,
									onExpand: () => setExpandedTaskId((selected) => selected === task.task_id ? null : task.task_id),
									steps: expandedTaskId === task.task_id && expandedRunId && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(CardSteps, {
										snapshot: boardSteps,
										openBranches
									}, expandedRunId),
									onSelect: (trigger) => onSelectTask(task.task_id, trigger)
								}, task.task_id))
							]
						}, id))
					}) }),
					items.length > 0 && (filter.archived || view === "list") && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskTable, {
						tasks: items,
						archived: Boolean(filter.archived),
						selectedTaskId,
						selectedIds,
						onToggleBatch: toggleSelected,
						onSelect: onSelectTask
					}),
					items.length > 0 && !filter.archived && (view === "flow" || view === "queue") && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskFlowView, {
						reworkEnabled,
						api,
						tasks: items,
						onOpenTask: onSelectTask,
						reloadVersion: reload,
						queue: view === "queue"
					}),
					nextCursor && !error && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						style: {
							display: "flex",
							justifyContent: "center"
						},
						children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							disabled: loading,
							onClick: () => setCursor(nextCursor),
							style: buttonStyle$6,
							children: "加载更多"
						})
					})
				]
			});
		}
		function dateBounds(period, from, to) {
			if (period === "custom") {
				const end = to ? /* @__PURE__ */ new Date(`${to}T00:00:00`) : null;
				if (end) end.setDate(end.getDate() + 1);
				return {
					updated_from: from ? (/* @__PURE__ */ new Date(`${from}T00:00:00`)).toISOString() : void 0,
					updated_before: end?.toISOString()
				};
			}
			if (!period) return {
				updated_from: void 0,
				updated_before: void 0
			};
			const start = /* @__PURE__ */ new Date();
			start.setHours(0, 0, 0, 0);
			const end = new Date(start);
			end.setDate(end.getDate() + 1);
			start.setDate(start.getDate() - (period === "today" ? 0 : Number(period) - 1));
			return {
				updated_from: start.toISOString(),
				updated_before: end.toISOString()
			};
		}
		function TaskTable({ tasks, archived, selectedTaskId, selectedIds, onToggleBatch, onSelect }) {
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
				className: "task-table-scroll",
				children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("table", {
					className: "task-table",
					"aria-label": archived ? "归档任务" : "任务列表",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("thead", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
							scope: "col",
							style: { width: 32 },
							children: "选"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
							scope: "col",
							children: "任务"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
							scope: "col",
							children: "状态"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
							scope: "col",
							children: "关注事项"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
							scope: "col",
							children: "运行"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
							scope: "col",
							children: archived ? "归档时间" : "更新时间"
						})
					] }) }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("tbody", { children: tasks.map((task) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", {
						"data-selected": selectedTaskId === task.task_id || void 0,
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: selectedIds && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
								type: "checkbox",
								checked: selectedIds.has(task.task_id),
								onChange: () => onToggleBatch?.(task.task_id),
								"aria-label": `选择任务：${task.title || "未命名任务"}`
							}) }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("td", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								className: "task-link",
								type: "button",
								onClick: (event) => onSelect(task.task_id, event.currentTarget),
								children: task.title || "未命名任务"
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("small", { children: [
								taskOriginLabel(task),
								" · ",
								taskAutonomyLabel(task)
							] })] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: taskCardStatus(task).label ?? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(UnknownState, { value: task.status }) }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("td", { children: [
								task.attention_reason ? operatorReason(task.attention_reason) : "—",
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("small", { children: [
									task.open_decision_count,
									" 项",
									COPY.awaitingDecision
								] }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("small", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskWaitAndHeartbeat, { task }) }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("small", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskRisk, { level: task.max_side_effect_level }) })
							] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("td", { children: [task.run_count, " 次"] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: archived && task.archived_at ? new Date(task.archived_at).toLocaleString() : formatDate$5(task.updated_at) })
						]
					}, task.task_id)) })]
				})
			});
		}
		/**
		* 分页途中插入新任务时按 task_id 去重：同 task_id 后页整条覆盖前页，
		* 首次出现的位置不动（不重复行，也不把旧行挪到末尾）。
		*/
		function mergeByTaskId(previous, incoming) {
			const merged = [...previous];
			const position = /* @__PURE__ */ new Map();
			merged.forEach((task, index) => {
				if (!position.has(task.task_id)) position.set(task.task_id, index);
			});
			for (const task of incoming) {
				const at = position.get(task.task_id);
				if (at === void 0) {
					position.set(task.task_id, merged.length);
					merged.push(task);
				} else merged[at] = task;
			}
			return merged;
		}
		function TaskCard({ task, selected, selectedBatch, onToggleBatch, compact, onSelect, expanded, onExpand, steps }) {
			const stepsId = (0, react.useId)();
			const attention = ATTENTION_LABEL[task.attention];
			const { label: status, tone } = taskCardStatus(task);
			const hasUnverified = task.unverified_count > 0;
			const actionable = task.open_decision_count > 0 || hasUnverified || taskColumn(task) === "attention";
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("article", {
				className: "task-card",
				"data-selected": selected || void 0,
				"data-board-task-id": task.task_id,
				"data-board-run-id": task.current_run_id ?? void 0,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
						type: "checkbox",
						className: "task-batch-check",
						checked: Boolean(selectedBatch),
						onChange: onToggleBatch,
						onClick: (event) => event.stopPropagation(),
						"aria-label": `选择任务：${task.title || "未命名任务"}`
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-card-status",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
							style: tone ? { color: tone } : void 0,
							children: status ?? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(UnknownState, { value: task.display_status || task.status })
						}), task.workflow_version !== null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: ["v", task.workflow_version] })]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
						className: "task-card-title",
						children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							"data-task-card-focus": "inspect",
							onClick: (event) => onSelect(event.currentTarget),
							title: task.title,
							style: compact ? compactTitleStyle : taskTitleStyle,
							"aria-expanded": selected,
							"aria-controls": selected ? "task-inspector" : void 0,
							children: task.title || "未命名任务"
						})
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						className: "task-card-type",
						children: [
							taskOriginLabel(task),
							" · ",
							taskAutonomyLabel(task)
						]
					}),
					task.attention_reason && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						className: "task-card-reason",
						children: operatorReason(task.attention_reason)
					}),
					actionable && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-card-attention",
						children: [
							task.open_decision_count > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("strong", { children: [
								task.open_decision_count,
								" 项",
								COPY.awaitingDecision
							] }),
							hasUnverified && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
								style: { color: tone },
								children: [
									task.unverified_count,
									" ",
									tone ? COPY.unverifiedBadge : "项待核实"
								]
							}) }),
							!task.open_decision_count && !hasUnverified && attention && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: attention }),
							hasUnverified && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: COPY.pendingIrreversible })
						]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-card-meta",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [task.run_count, " 次运行"] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskRisk, { level: task.max_side_effect_level }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskWaitAndHeartbeat, { task }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", {
								dateTime: task.updated_at,
								title: formatDate$5(task.updated_at),
								children: formatDate$5(task.updated_at)
							})
						]
					}),
					task.current_run_id && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-card-steps",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							className: "task-quiet-button",
							"data-task-card-focus": "steps",
							"aria-expanded": expanded,
							"aria-controls": stepsId,
							"aria-label": `${expanded ? "收起" : "展开"}步骤：${task.title || "未命名任务"}`,
							onClick: onExpand,
							children: expanded ? "收起步骤" : "展开步骤"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
							id: stepsId,
							hidden: !expanded,
							children: steps
						})]
					})
				]
			});
		}
		function CardSteps({ snapshot: { nodes, partial, failed, refresh }, openBranches }) {
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
				failed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
					role: "alert",
					children: [nodes ? "步骤刷新失败，仍显示上次读取结果。" : "步骤读取失败。", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						className: "task-quiet-button",
						type: "button",
						"data-task-card-focus": "retry-steps",
						onClick: refresh,
						children: "重试"
					})]
				}),
				!nodes && !failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					role: "status",
					children: "正在读取步骤…"
				}),
				nodes && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskStepOverview, {
					nodes,
					partial,
					openBranches
				})
			] });
		}
		function TaskRisk({ level }) {
			if (!level) return null;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
				style: { color: level === "irreversible" ? COLOR.danger : COLOR.inkMuted },
				children: EFFECT_LABEL$1[level] ?? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(UnknownState, { value: level })
			});
		}
		function TaskWaitAndHeartbeat({ task }) {
			const { oldest_open_decision_age_seconds: wait, max_heartbeat_age_ms: heartbeat } = task;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [wait != null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: ["最长等待 ", formatAge(wait)] }), heartbeat != null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [
				wait != null && " · ",
				"最久未更新的心跳 ",
				formatAge(heartbeat / 1e3),
				"前"
			] })] });
		}
		function formatAge(seconds) {
			if (seconds > 0 && seconds < 1) return "不足 1 秒";
			if (seconds < 60) return `${Math.floor(seconds)} 秒`;
			if (seconds < 3600) return `${Math.floor(seconds / 60)} 分 ${Math.floor(seconds % 60)} 秒`;
			return `${Math.floor(seconds / 3600)} 小时 ${Math.floor(seconds / 60) % 60} 分`;
		}
		function formatDate$5(value) {
			const date = new Date(value);
			return Number.isNaN(date.valueOf()) ? "更新时间未知" : `更新于 ${date.toLocaleDateString(void 0, {
				month: "numeric",
				day: "numeric"
			})} ${date.toLocaleTimeString(void 0, {
				hour: "2-digit",
				minute: "2-digit"
			})}`;
		}
		const titleStyle$1 = {
			margin: 0,
			fontSize: FONT_PX.title,
			lineHeight: 1.3
		};
		const mutedStyle$8 = {
			margin: "6px 0 0",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.body
		};
		const taskTitleStyle = {
			display: "block",
			width: "100%",
			overflow: "hidden",
			textOverflow: "ellipsis",
			whiteSpace: "nowrap",
			padding: 0,
			border: 0,
			background: "transparent",
			color: COLOR.ink,
			fontSize: FONT_PX.body,
			fontWeight: 650,
			lineHeight: 1.5,
			textAlign: "left",
			cursor: "pointer"
		};
		const compactTitleStyle = {
			...taskTitleStyle,
			display: "-webkit-box",
			whiteSpace: "normal",
			WebkitBoxOrient: "vertical",
			WebkitLineClamp: 2
		};
		const searchInputStyle = {
			minWidth: 0,
			width: 240,
			height: 38,
			padding: "0 11px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 7,
			color: COLOR.ink,
			fontSize: FONT_PX.body
		};
		const buttonStyle$6 = {
			minHeight: 38,
			padding: "0 14px",
			border: 0,
			borderRadius: 7,
			background: COLOR.ink,
			color: "#ffffff",
			fontSize: FONT_PX.menu,
			cursor: "pointer"
		};
		const quietButtonStyle$1 = {
			minHeight: 36,
			padding: "0 12px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 7,
			background: COLOR.surface,
			color: COLOR.ink,
			fontSize: FONT_PX.menu,
			cursor: "pointer"
		};
		const emptyStyle = {
			border: `1px dashed ${COLOR.line}`,
			borderRadius: 9,
			padding: 36,
			textAlign: "center",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.body
		};
		const errorStyle$5 = {
			display: "flex",
			justifyContent: "space-between",
			alignItems: "center",
			gap: 12,
			padding: 14,
			borderRadius: 8,
			background: "#fff1f0",
			color: COLOR.danger,
			fontSize: FONT_PX.body
		};
		const visuallyHidden = {
			position: "absolute",
			width: 1,
			height: 1,
			padding: 0,
			margin: -1,
			overflow: "hidden",
			clip: "rect(0,0,0,0)",
			whiteSpace: "nowrap",
			border: 0
		};
		// src/client/panel/model-escalation.tsx
		const classifications = {
			model_capability: "模型能力不足",
			model_efficiency: "模型执行效率低",
			waiting_external: "等待外部条件",
			other: "其他原因",
			insufficient: "证据不足"
		};
		const triggers = {
			prolonged_runtime: "运行时间较长",
			execution_blocked: "执行受阻",
			repeated_rework: "两次有效打回"
		};
		function ModelEscalation({ api, node, reloadVersion }) {
			const [open, setOpen] = (0, react.useState)(false);
			const [view, setView] = (0, react.useState)();
			const [error, setError] = (0, react.useState)("");
			const [busy, setBusy] = (0, react.useState)(false);
			const [refresh, setRefresh] = (0, react.useState)(0);
			const operation = (0, react.useRef)();
			(0, react.useEffect)(() => {
				if (!open) return;
				let current = true;
				api.get(node.run_id, node.node_run_id).then(({ data }) => {
					if (current) setView(data);
				}, (err) => {
					if (current) setError(err instanceof ApiError ? err.message : "暂时无法读取主脑诊断");
				});
				return () => {
					current = false;
				};
			}, [
				api,
				node.run_id,
				node.node_run_id,
				open,
				reloadVersion,
				refresh
			]);
			async function assess() {
				if (!view?.precondition || busy) return;
				setBusy(true);
				setError("");
				operation.current ??= newOperationId();
				try {
					const { data } = await api.assess(view.precondition, operation.current);
					setView((previous) => previous ? {
						...previous,
						item: data
					} : previous);
					operation.current = void 0;
				} catch (err) {
					if (err instanceof ApiError && ![
						"OPERATION_TIMEOUT",
						"OPERATION_OUTCOME_UNKNOWN",
						"SERVICE_UNAVAILABLE"
					].includes(err.code)) operation.current = void 0;
					setError(err instanceof ApiError ? err.message : "暂时无法完成主脑诊断");
				} finally {
					setBusy(false);
					setRefresh((value) => value + 1);
				}
			}
			const opinion = view?.item;
			const dispatch = view?.dispatch;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
				onToggle: (event) => setOpen(event.currentTarget.open),
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "主脑模型诊断" }),
					open && !view && !error && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取诊断…"
					}),
					view && !opinion && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: view.precondition ? "可请主脑根据当前执行记录诊断。" : "当前没有模型诊断记录。" }),
					opinion && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
							triggers[opinion.trigger],
							" · ",
							classifications[opinion.classification]
						] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: opinion.reason }),
						opinion.outcome === "kept" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "主脑建议沿用当前模型。" }),
						opinion.outcome === "no_higher_model" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "宿主没有已批准且能力更高的可用模型。" }),
						opinion.outcome === "recommended" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
							"建议下一次尝试使用 ",
							opinion.model_ref,
							"。"
						] }),
						opinion.outcome === "recommended" && (dispatch ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
							dispatch.provider && dispatch.model ? "已回读实际模型" : dispatch.state === "rejected" ? "宿主未确认建议模型" : dispatch.state === "selected" ? "已选定，等待宿主确认" : "宿主已确认配置，等待实际模型回读",
							" · ",
							dispatch.provider && dispatch.model ? dispatch.provider + " / " + dispatch.model : dispatch.model_ref
						] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: view.successor_node_run_id ? "后续尝试尚无采纳记录。" : "等待已授权的重试或返工。" }))
					] }),
					view?.precondition && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
						disabled: busy,
						onClick: () => {
							assess();
						},
						children: busy ? "主脑正在诊断…" : "请主脑诊断模型"
					}),
					error && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: error
					})
				]
			});
		}
		// src/client/panel/activity-screen.tsx
		const LABELS$1 = {
			"task.created": "创建任务",
			"task.reserved": "预留任务",
			"task.archived": "归档任务",
			"task.restored": "取回任务",
			"run.created": "创建运行",
			"run.closed": "结束运行",
			"decision.created": "创建审批",
			"decision.resolved": "记录审批结果",
			"artifact.registered": "登记交付产物",
			"task_plan.proposed": "生成待确认任务计划",
			"workflow.draft_tested": "完成流程结构与模拟流转测试",
			"workflow.published": "发布 Workflow 版本",
			"workflow.status_changed": "变更 Workflow 启用状态",
			"run.terminal": "结束运行",
			"task.run_started": "任务启动运行",
			"task.trigger_created": "配置任务触发器",
			"task.trigger_enabled": "启用任务触发器",
			"task.trigger_paused": "暂停任务触发器",
			"task.trigger_fired": "触发任务运行",
			"task.trigger_skipped": "跳过这次触发",
			"effect.resolved": "核实外部操作",
			"memory.apply.outcome_unknown": "记忆写入结果待核对",
			"executor.model_selected": "选定执行模型",
			"executor.model_confirmed": "宿主确认执行模型",
			"executor.model_rejected": "宿主未确认指定模型",
			"executor.model_used": "记录实际执行模型",
			"model_routing.updated": "更新模型分配",
			"collaboration.finding_reported": "提交核验发现",
			"collaboration.proposed": "请求独立核验",
			"collaboration.authorized": "授权独立核验",
			"collaboration.delivered": "核验请求已送达",
			"collaboration.accepted": "接收方已接手",
			"collaboration.withdrawn": "撤回原核验请求",
			"collaboration.reassigned": "重新指派核验",
			"collaboration.completed": "独立核验完成",
			"collaboration.adopted": "主理人采纳核验结论"
		};
		function activityTitle(type) {
			if (Object.hasOwn(LABELS$1, type)) return LABELS$1[type];
			return /\p{Script=Han}/u.test(type) ? operatorReason(type) : "其他活动记录";
		}
		function ActivityScreen({ api, reloadVersion, onSnapshot }) {
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "activity-title",
				className: "ops-page",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", {
					className: "ops-page-header",
					children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", {
						id: "activity-title",
						children: "事件日志"
					})
				}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ActivityList, {
					api,
					reloadVersion,
					onSnapshot
				})]
			});
		}
		function ActivityList({ api, runId, taskId, reloadVersion, onSnapshot, compact = false }) {
			const [items, setItems] = (0, react.useState)([]);
			const [next, setNext] = (0, react.useState)(null);
			const [before, setBefore] = (0, react.useState)();
			const [retry, setRetry] = (0, react.useState)(0);
			const [loading, setLoading] = (0, react.useState)(true);
			const [failed, setFailed] = (0, react.useState)(false);
			(0, react.useEffect)(() => {
				setBefore(void 0);
			}, [reloadVersion]);
			(0, react.useEffect)(() => {
				let current = true;
				setLoading(true);
				setFailed(false);
				api.listActivity({
					before_seq: before,
					run_id: runId,
					task_id: taskId,
					limit: compact ? 5 : void 0
				}).then(({ data, as_of_seq }) => {
					if (!current) return;
					setItems((previous) => before === void 0 ? data.items : [...previous, ...data.items.filter((entry) => !previous.some((old) => old.seq === entry.seq))]);
					setNext(data.next_before_seq);
					onSnapshot?.(as_of_seq);
				}).catch(() => {
					if (current) setFailed(true);
				}).finally(() => {
					if (current) setLoading(false);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				before,
				runId,
				taskId,
				retry,
				reloadVersion,
				onSnapshot,
				compact
			]);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-label": "账本活动",
				style: {
					display: "grid",
					gap: 9
				},
				children: [
					loading && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: muted,
						children: "正在读取活动…"
					}),
					failed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: "alert",
						children: ["活动记录读取失败。 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							onClick: () => {
								setBefore(void 0);
								setRetry((value) => value + 1);
							},
							children: "重试"
						})]
					}),
					!loading && !failed && items.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: muted,
						children: "当前没有活动记录。"
					}),
					items.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ol", {
						style: timeline,
						children: items.map((event) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", {
							style: row$1,
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: activityTitle(event.type) }), !compact && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(EventDetails, { entries: [
								...event.details ?? [],
								{
									label: "事件类型",
									value: event.type
								},
								{
									label: "账本位置",
									value: String(event.seq)
								},
								...[
									event.run_id,
									event.node_run_id,
									event.decision_id
								].flatMap((value, index) => value ? [{
									label: [
										"运行 ID",
										"步骤 ID",
										"审批 ID"
									][index],
									value
								}] : [])
							] })] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", {
								dateTime: event.occurred_at,
								style: muted,
								children: new Date(event.occurred_at).toLocaleString()
							})]
						}, event.seq))
					}),
					!compact && next !== null && !failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						disabled: loading,
						onClick: () => setBefore(next),
						style: { justifySelf: "start" },
						children: "加载更早活动"
					})
				]
			});
		}
		const muted = {
			color: COLOR.inkMuted,
			fontSize: FONT_PX.helper,
			margin: "4px 0"
		};
		const timeline = {
			display: "grid",
			gap: 0,
			padding: 0,
			margin: 0,
			listStyle: "none",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 8,
			background: COLOR.surface
		};
		const row$1 = {
			display: "flex",
			justifyContent: "space-between",
			flexWrap: "wrap",
			gap: 6,
			padding: "7px 11px",
			borderBottom: `1px solid ${COLOR.line}`,
			overflowWrap: "anywhere",
			minWidth: 0
		};
		// src/client/panel/task-event-timeline.tsx
		const attentionOptions = [["all", "全部记录"], ["attention", "异常与需留意"]];
		const executionOptions = [
			...attentionOptions,
			["tool", "工具调用"],
			["report", "执行报告"],
			["model", "模型派发"]
		];
		const progressTitles = {
			"decision.acknowledged": "已查看审批",
			"decision.blocked": "审批暂无法推进",
			"rework.plan_proposed": "已提出返工方案",
			"rework.plan_approved": "已批准返工方案",
			"rework.authorization_deferred": "返工授权延后",
			"rework.recovery_blocked": "返工恢复受阻",
			"rework.dispatch_blocked": "返工派发受阻",
			"rework.dispatch_unblocked": "返工派发限制已解除"
		};
		const executionTitles = {
			lease_acquired: "已取得执行租约",
			fence_armed: "已记录停止请求"
		};
		const statusTitles = {
			blocked_pre: "执行前已阻塞",
			blocked_post: "执行后待人工处理",
			indeterminate_side_effect: "副作用待核实",
			startup_unknown: "启动结果未知",
			paused_bridge_missing: "执行桥连接缺失",
			failed: "执行失败",
			timeout: "执行超时",
			orphaned: "执行会话已失联",
			failed_with_unverified: "执行失败，有影响待核实",
			succeeded_with_unverified: "运行已结束，有影响待核实",
			abandoned_unverified: "已停止，影响待核实",
			cancelled: "运行已取消",
			succeeded: "运行成功",
			done: "步骤完成",
			started: "步骤开始"
		};
		const dateFormat = new Intl.DateTimeFormat("zh-CN", {
			dateStyle: "short",
			timeStyle: "medium",
			hourCycle: "h23"
		});
		function eventTime(value) {
			const time = Date.parse(value);
			return Number.isFinite(time) ? dateFormat.format(time) : "时间未记录";
		}
		function statusTitle(status) {
			return status && Object.hasOwn(statusTitles, status) ? statusTitles[status] : void 0;
		}
		function ledgerEvent(row) {
			const details = row.details ?? [];
			const label = Object.hasOwn(progressTitles, row.type) ? progressTitles[row.type] : activityTitle(row.type);
			const status = row.type === "run.terminal" ? details.find((detail) => detail.label === "状态")?.value : void 0;
			const outcome = details.find((detail) => detail.label === "业务结果")?.value;
			const title = status === "cancelled" && row.attention && statusTitle(outcome) ? `运行已取消 · ${statusTitle(outcome)}` : statusTitle(status) ?? label;
			return {
				id: row.seq,
				title,
				type: row.type,
				time: row.occurred_at,
				attention: row.attention,
				details: [...details, ...[
					row.run_id,
					row.node_run_id,
					row.decision_id
				].flatMap((value, index) => value ? [{
					label: [
						"运行 ID",
						"节点运行 ID",
						"审批 ID"
					][index],
					value
				}] : [])]
			};
		}
		function executionEvent(row) {
			const status = row.type === "status_changed" ? row.details.find((detail) => detail.label === "新状态")?.value : void 0;
			const exitCode = row.type === "tool_finished" ? row.details.find((detail) => detail.label === "退出码")?.value : void 0;
			const label = Object.hasOwn(executionTitles, row.type) ? executionTitles[row.type] : row.title;
			const title = statusTitle(status) ?? (row.attention && exitCode !== void 0 ? `工具调用结束（退出码 ${exitCode}）` : label && label !== row.type ? label : "其他执行记录");
			return {
				id: row.id,
				title,
				type: row.type,
				time: row.created_at,
				attention: row.attention,
				details: [
					...row.details,
					{
						label: "步骤 ID",
						value: row.node_id
					},
					{
						label: "运行 ID",
						value: row.run_id
					},
					{
						label: "节点运行 ID",
						value: row.node_run_id
					}
				]
			};
		}
		function requireFilter(items, filter) {
			if (filter !== "all" && items.some(({ attention }) => attention === void 0 || filter === "attention" && !attention)) throw new Error("Host did not confirm the event filter");
			return items;
		}
		function TaskEventTimeline(props) {
			const { taskId, runId } = props.scope;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(Timeline, { ...props }, taskId ? `task:${taskId}` : `run:${runId}`);
		}
		function Timeline({ api, scope, reloadVersion }) {
			const heading = (0, react.useId)();
			const [pageSize] = (0, react.useState)(() => readDisplayPreferences().preferences.eventPageSize);
			const pageHeading = (0, react.useRef)(null);
			const head = (0, react.useRef)(void 0);
			const [source, setSource] = (0, react.useState)("activity");
			const [filter, setFilter] = (0, react.useState)("all");
			const [trail, setTrail] = (0, react.useState)({ cursors: [] });
			const [expanded, setExpanded] = (0, react.useState)(/* @__PURE__ */ new Set());
			const [refresh, setRefresh] = (0, react.useState)(0);
			const [retry, setRetry] = (0, react.useState)(0);
			const [page, setPage] = (0, react.useState)();
			const [failed, setFailed] = (0, react.useState)();
			const [latestAttention, setLatestAttention] = (0, react.useState)();
			const { runId, taskId } = scope;
			const { cursors } = trail;
			const before = cursors.at(-1);
			const pageVersion = trail.headVersion ?? reloadVersion;
			const requestKey = JSON.stringify([
				source,
				filter,
				before,
				pageVersion,
				refresh
			]);
			const current = page?.key === requestKey ? page : void 0;
			const loadFailed = !current && failed === requestKey;
			const loading = !current && !loadFailed;
			const attentionKey = JSON.stringify([pageVersion, refresh]);
			const attention = latestAttention?.key === attentionKey ? latestAttention.event : void 0;
			(0, react.useEffect)(() => {
				let active = true;
				if (before === void 0 && head.current?.key === requestKey) {
					setPage(head.current);
					return;
				}
				const reqScope = {
					run_id: runId,
					task_id: taskId,
					limit: pageSize
				};
				(source === "activity" ? api.listActivity({
					...reqScope,
					before_seq: before,
					filter: filter === "attention" ? filter : "all"
				}).then(({ data }) => ({
					events: requireFilter(data.items, filter).map(ledgerEvent),
					next: data.next_before_seq
				})) : api.listExecutionHistory({
					...reqScope,
					before,
					filter
				}).then(({ data }) => ({
					events: requireFilter(data.items, filter).map(executionEvent),
					next: data.next_before
				}))).then((result) => {
					if (!active) return;
					const loaded = {
						key: requestKey,
						...result
					};
					if (before === void 0) head.current = loaded;
					setPage(loaded);
				}).catch(() => {
					if (active) setFailed(requestKey);
				});
				return () => {
					active = false;
				};
			}, [
				api,
				before,
				filter,
				pageSize,
				requestKey,
				retry,
				runId,
				source,
				taskId
			]);
			(0, react.useEffect)(() => {
				if (source !== "activity") return;
				let active = true;
				api.listExecutionHistory({
					run_id: runId,
					task_id: taskId,
					limit: 1,
					filter: "attention"
				}).then(({ data }) => {
					if (active) {
						const newest = data.items[0];
						setLatestAttention({
							key: attentionKey,
							event: newest?.attention === true ? executionEvent(newest) : void 0
						});
					}
				}).catch(() => {
					if (active) setLatestAttention({ key: attentionKey });
				});
				return () => {
					active = false;
				};
			}, [
				api,
				attentionKey,
				runId,
				source,
				taskId
			]);
			function move(next) {
				setTrail({
					headVersion: pageVersion,
					cursors: next
				});
				pageHeading.current?.focus();
			}
			function reset() {
				head.current = void 0;
				setTrail({ cursors: [] });
				setExpanded(/* @__PURE__ */ new Set());
				setRefresh((value) => value + 1);
			}
			function show(nextSource, nextFilter) {
				if (nextSource === source && nextFilter === filter) return;
				setSource(nextSource);
				setFilter(nextFilter);
				reset();
			}
			function toggle(id, open) {
				setExpanded((previous) => {
					if (previous.has(id) === open) return previous;
					const next = new Set(previous);
					if (open) next.add(id);
					else next.delete(id);
					return next;
				});
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "ops-surface",
				"aria-labelledby": heading,
				style: surface,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						style: header,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
							id: heading,
							style: title,
							children: taskId ? "任务事件" : "运行事件"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							className: "ops-meta",
							style: caption,
							children: [
								"最新记录在前，每页最多 ",
								pageSize,
								" 条。"
							]
						})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							className: "ops-button",
							onClick: reset,
							children: "刷新到最新"
						})]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: toolbar,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							style: toolbar,
							role: "group",
							"aria-label": "事件来源",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								className: source === "activity" ? "ops-button ops-button--primary" : "ops-button",
								"aria-pressed": source === "activity",
								onClick: () => show("activity", "all"),
								children: "近期进展"
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								className: source === "execution" ? "ops-button ops-button--primary" : "ops-button",
								"aria-pressed": source === "execution",
								onClick: () => show("execution", "all"),
								children: "执行明细"
							})]
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
							style: toolbar,
							children: ["筛选", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("select", {
								className: "ops-field",
								value: filter,
								onChange: (event) => show(source, event.target.value),
								children: (source === "activity" ? attentionOptions : executionOptions).map(([id, label]) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
									value: id,
									children: label
								}, id))
							})]
						})]
					}),
					source === "activity" && attention && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("aside", {
						style: notice,
						"aria-label": "最近记录的执行异常",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "最近记录的执行异常" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: caption,
							children: [
								attention.title,
								" · ",
								eventTime(attention.time)
							]
						})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							className: "ops-button",
							onClick: () => show("execution", "attention"),
							children: "查看执行异常"
						})]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: "status",
						ref: pageHeading,
						tabIndex: -1,
						className: "ops-meta",
						style: caption,
						children: [
							"第 ",
							cursors.length + 1,
							" 页",
							loading ? " · 正在读取…" : current ? ` · ${current.events.length} 条` : ""
						]
					}),
					loadFailed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: "alert",
						style: caption,
						children: ["事件读取失败。 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							className: "ops-button",
							onClick: () => {
								setFailed(void 0);
								setRetry((value) => value + 1);
							},
							children: "重试本页"
						})]
					}),
					current?.events.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						className: "ops-meta",
						style: caption,
						children: filter === "all" ? "这个范围暂无已记录事件。" : "这个范围没有符合筛选的记录。"
					}),
					!!current?.events.length && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ol", {
						role: "list",
						style: list,
						children: current.events.map((event) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", {
							style: row,
							children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
								open: expanded.has(event.id),
								onToggle: (change) => toggle(event.id, change.currentTarget.open),
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", {
									style: summary,
									children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
										style: eventHeader,
										children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
											style: eventTitle,
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: event.title }), event.attention && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
												style: badge,
												children: "需留意"
											})]
										}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", {
											className: "ops-meta",
											dateTime: event.time,
											style: timestamp,
											children: eventTime(event.time)
										})]
									})
								}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(EventDetails, { entries: [
									{
										label: "事件类型",
										value: event.type
									},
									{
										label: "账本位置",
										value: String(event.id)
									},
									...event.details
								] })]
							})
						}, event.id))
					}),
					(cursors.length > 0 || current?.next != null) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("nav", {
						"aria-label": "事件分页",
						style: toolbar,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							className: "ops-button",
							disabled: cursors.length === 0,
							onClick: () => move(cursors.slice(0, -1)),
							children: "较新一页"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							className: "ops-button",
							disabled: current?.next == null,
							onClick: () => {
								if (current?.next != null) move([...cursors, current.next]);
							},
							children: "较早一页"
						})]
					})
				]
			});
		}
		const surface = {
			display: "grid",
			gap: 10,
			padding: 14
		};
		const header = {
			display: "flex",
			gap: 10,
			alignItems: "center",
			justifyContent: "space-between",
			flexWrap: "wrap"
		};
		const title = {
			margin: 0,
			fontSize: FONT_PX.section
		};
		const caption = { margin: "3px 0" };
		const toolbar = {
			display: "flex",
			gap: 8,
			alignItems: "center",
			flexWrap: "wrap"
		};
		const notice = {
			...header,
			background: COLOR.sidebar,
			border: `1px solid ${COLOR.line}`,
			borderInlineStart: `3px solid ${COLOR.warning}`,
			borderRadius: 6,
			padding: 8
		};
		const list = {
			display: "grid",
			listStyle: "none",
			padding: 0,
			margin: 0,
			border: `1px solid ${COLOR.line}`,
			borderRadius: 8
		};
		const row = {
			display: "grid",
			minWidth: 0,
			gap: 4,
			padding: "8px 11px",
			borderBottom: `1px solid ${COLOR.line}`,
			overflowWrap: "anywhere"
		};
		const summary = {
			cursor: "pointer",
			paddingBlock: 3
		};
		const eventHeader = {
			display: "inline-flex",
			width: "calc(100% - 20px)",
			verticalAlign: "middle",
			justifyContent: "space-between",
			alignItems: "baseline",
			flexWrap: "wrap",
			gap: "3px 8px"
		};
		const eventTitle = {
			display: "flex",
			alignItems: "baseline",
			flexWrap: "wrap",
			gap: "3px 7px"
		};
		const timestamp = { fontVariantNumeric: "tabular-nums" };
		const badge = {
			fontSize: FONT_PX.helper,
			color: COLOR.warning,
			padding: "1px 5px",
			border: `1px solid ${COLOR.warning}`,
			borderRadius: 4
		};
		// src/client/panel/run-notes.tsx
		function RunNotes({ client, runId, reloadVersion }) {
			const [items, setItems] = (0, react.useState)();
			const [body, setBody] = (0, react.useState)("");
			const [revision, setRevision] = (0, react.useState)(0);
			const [loadError, setLoadError] = (0, react.useState)(false);
			const [saveError, setSaveError] = (0, react.useState)("");
			const [saving, setSaving] = (0, react.useState)(false);
			const pending = (0, react.useRef)();
			const digits = [...body].length;
			(0, react.useEffect)(() => {
				let active = true;
				setLoadError(false);
				client.get("/v1/run-notes", { run_id: runId }).then(({ data }) => {
					if (active) setItems(data.items);
				}).catch(() => {
					if (active) setLoadError(true);
				});
				return () => {
					active = false;
				};
			}, [
				client,
				runId,
				reloadVersion,
				revision
			]);
			async function submit(event) {
				event.preventDefault();
				const text = body.trim();
				if (saving || !text || digits > 500) return;
				const id = pending.current?.text === text ? pending.current.id : newOperationId();
				pending.current = {
					text,
					id
				};
				setSaving(true);
				setSaveError("");
				try {
					await client.post("/v1/run-notes", {
						run_id: runId,
						body: text,
						operation_id: id
					});
					pending.current = void 0;
					setBody("");
					setRevision((value) => value + 1);
				} catch (error) {
					setSaveError(error instanceof Error ? error.message : "暂未确认便条是否已添加。");
				} finally {
					setSaving(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "task-drawer-card",
				"aria-label": "本次运行便条",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: "本次运行便条" }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "所附内容仅在本次运行内共享，供参考。验收仍需独立证据。" }),
					loadError ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: "alert",
						children: ["暂时无法读取本次运行便条。", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							className: "task-quiet-button",
							onClick: () => setRevision((value) => value + 1),
							children: "重试读取"
						})]
					}) : items ? items.length ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: items.map((note) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
						note.source === "human" ? "操作人" : "步骤",
						" ",
						note.author,
						note.attempt !== null && ` · 第 ${note.attempt} 次尝试`,
						" · ",
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", {
							dateTime: note.created_at,
							children: new Date(note.created_at).toLocaleString()
						})
					] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: {
							whiteSpace: "pre-wrap",
							overflowWrap: "anywhere"
						},
						children: note.body
					})] }, note.note_id)) }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "本次运行暂无共同可见的便条。" }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取运行便条…"
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
						onSubmit: (event) => {
							submit(event);
						},
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["本运行可见的便条", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
								value: body,
								maxLength: 1e3,
								rows: 3,
								disabled: saving,
								onChange: (event) => setBody(event.target.value)
							})] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
								"aria-live": "polite",
								children: [digits, " / 500 字"]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
								type: "submit",
								disabled: saving || !body.trim() || digits > 500,
								children: "添加便条"
							}),
							saveError && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								role: "alert",
								children: saveError
							})
						]
					})
				]
			});
		}
		// src/client/panel/task-detail-screen.tsx
		const TASK_TABS = [
			"概览",
			"运行",
			"时间线",
			"管理",
			"编排",
			"事件"
		];
		const RUN_TABS = [
			"步骤",
			"结果",
			"证据",
			"事件",
			"便条"
		];
		function TaskDetailScreen({ api, taskId, initialRunId, onBack, reloadVersion = 0, onSnapshot, rework, modelEscalation, reworkPlannerEnabled, taskCommands, triggerCommands, onArchiveChanged, onOpenMemory, onOpenDecision, notesClient, standalone = false }) {
			const [loaded, setLoaded] = (0, react.useState)();
			const data = loaded?.api === api && loaded.taskId === taskId ? loaded.data : void 0;
			const [loading, setLoading] = (0, react.useState)(true);
			const [failed, setFailed] = (0, react.useState)(false);
			const [reload, setReload] = (0, react.useState)(0);
			const [taskTab, setTaskTab] = (0, react.useState)("概览");
			const [runTab, setRunTab] = (0, react.useState)("步骤");
			const [reviewRunId, setReviewRunId] = (0, react.useState)();
			const [runSelection, setRunSelection] = (0, react.useState)();
			const [runRead, setRunRead] = (0, react.useState)();
			const [showRun, setShowRun] = (0, react.useState)(Boolean(initialRunId));
			const close = (0, react.useRef)(null);
			const dialog = (0, react.useRef)(null);
			const body = (0, react.useRef)(null);
			(0, react.useEffect)(() => {
				if (standalone) return;
				const element = dialog.current;
				if (!element) return;
				if (typeof element.showModal === "function") element.showModal();
				else element.setAttribute("open", "");
				close.current?.focus({ preventScroll: true });
				return () => {
					if (element.open && typeof element.close === "function") element.close();
				};
			}, [standalone]);
			(0, react.useEffect)(() => {
				setShowRun(Boolean(initialRunId));
				setRunSelection(void 0);
				setRunTab("步骤");
			}, [initialRunId, taskId]);
			(0, react.useEffect)(() => {
				let current = true;
				setLoading(true);
				setFailed(false);
				Promise.all([
					api.getTask(taskId),
					api.listRuns({
						task_id: taskId,
						limit: 50
					}),
					api.listTaskAttempts ? api.listTaskAttempts(taskId, 100).catch(() => ({ data: { items: [] }, as_of_seq: 0 })) : Promise.resolve({ data: { items: [] }, as_of_seq: 0 }),
					api.getTaskTimeline ? api.getTaskTimeline(taskId, 200).catch(() => ({ data: { events: [] }, as_of_seq: 0 })) : Promise.resolve({ data: { events: [] }, as_of_seq: 0 }),
					api.getTaskAcceptance ? api.getTaskAcceptance(taskId).catch(() => ({ data: null, as_of_seq: 0 })) : Promise.resolve({ data: null, as_of_seq: 0 })
				]).then(([taskResult, runsResult, attemptsResult, timelineResult, acceptanceResult]) => {
					if (!current) return;
					setLoaded({
						api,
						taskId,
						data: {
							task: taskResult.data,
							runs: runsResult.data.items,
							next: runsResult.data.next_cursor ?? null,
							attempts: attemptsResult.data.items ?? [],
							timeline: timelineResult.data ?? { events: [] },
							acceptance: acceptanceResult.data ?? null
						}
					});
					onSnapshot?.(Math.max(taskResult.as_of_seq, runsResult.as_of_seq, attemptsResult.as_of_seq ?? 0, timelineResult.as_of_seq ?? 0, acceptanceResult.as_of_seq ?? 0));
				}).catch(() => {
					if (current) setFailed(true);
				}).finally(() => {
					if (current) setLoading(false);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				taskId,
				initialRunId,
				reload,
				reloadVersion,
				onSnapshot
			]);
			const task = data?.task;
			const taskState = task ? taskCardStatus(task) : null;
			const latest = data?.runs.find((item) => item.run_id === task?.current_run_id) ?? data?.runs[0];
			const selection = runSelection?.api === api && runSelection.taskId === taskId ? runSelection.run : void 0;
			const selectedRunId = selection?.run_id ?? initialRunId;
			const detail = runRead?.api === api && runRead.taskId === taskId && runRead.runId === selectedRunId ? runRead : void 0;
			const detailFailed = detail?.failed && detail.revision === reloadVersion && detail.retry === reload;
			const summary = data?.runs.find((item) => item.run_id === selectedRunId) ?? selection;
			const run = detail?.run ?? summary;
			(0, react.useEffect)(() => {
				if (!showRun || !selectedRunId) return;
				let active = true;
				const key = {
					api,
					taskId,
					runId: selectedRunId,
					revision: reloadVersion,
					retry: reload
				};
				api.getRun(selectedRunId).then(({ data }) => {
					if (data.run_id !== selectedRunId || data.task_id !== taskId) throw new Error("运行归属已变化");
					if (active) setRunRead({
						...key,
						run: data
					});
				}).catch(() => {
					if (active) setRunRead((last) => ({
						...key,
						failed: true,
						run: last?.api === api && last.taskId === taskId && last.runId === selectedRunId ? last.run : void 0
					}));
				});
				return () => {
					active = false;
				};
			}, [
				api,
				taskId,
				selectedRunId,
				showRun,
				reload,
				reloadVersion
			]);
			const resetScroll = () => {
				if (body.current) body.current.scrollTop = 0;
			};
			const closeDetail = () => {
				dialog.current?.close?.();
				onBack();
			};
			const returnToTask = () => {
				setShowRun(false);
				resetScroll();
			};
			const openRun = (selected) => {
				setRunSelection({
					api,
					taskId,
					run: selected
				});
				setRunTab("步骤");
				setShowRun(true);
				resetScroll();
			};
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dialog", {
				ref: dialog,
				id: "task-inspector",
				className: standalone ? "task-inspector task-inspector--page task-detail-page" : "task-inspector",
				open: standalone || void 0,
				"aria-labelledby": "task-detail-title",
				onCancel: (event) => {
					event.preventDefault();
					closeDetail();
				},
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("style", { children: TASK_INSPECTOR_CSS }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("style", { children: OPS_MONITOR_CSS }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "task-inspector-top",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "task-inspector-heading",
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									className: "task-inspector-kicker",
									children: showRun ? "这次运行" : "任务"
								}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
									id: "task-detail-title",
									style: titleStyle,
									children: task?.title || "任务详情"
								})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									ref: close,
									type: "button",
									onClick: closeDetail,
									style: quietButtonStyle,
									"aria-label": standalone ? "返回任务列表" : "关闭任务详情",
									children: standalone ? "返回" : "关闭"
								})]
							}),
							task && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "task-inspector-badges",
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
										className: "task-type-badge",
										children: taskOriginLabel(showRun && run ? run : task)
									}),
									showRun && run ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(RunStatusLabel, {
										status: run.status,
										displayStatus: run.display_status
									}) : taskState?.label ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
										style: { color: taskState.tone },
										children: taskState.label
									}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)(UnknownState, { value: task.display_status || task.status }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: showRun && run ? formatTaskDate(run.started_at ?? run.created_at) : taskAutonomyLabel(task) }),
									task.archived_at && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
										className: "task-type-badge",
										children: "已归档"
									})
								]
							}),
							showRun ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
								className: "task-inspector-tabs",
								role: "group",
								"aria-label": "运行详情视图",
								children: RUN_TABS.filter((label) => label !== "便条" || notesClient).map((label) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									"aria-pressed": runTab === label,
									onClick: () => {
										if (label === "结果") setReviewRunId(selectedRunId);
										setRunTab(label);
										resetScroll();
									},
									children: label
								}, label))
							}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
								className: "task-inspector-tabs",
								role: "group",
								"aria-label": "任务详情视图",
								children: TASK_TABS.map((label) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("button", {
									type: "button",
									"aria-pressed": taskTab === label,
									onClick: () => {
										setTaskTab(label);
										resetScroll();
									},
									children: [label, label === "运行" && task && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
										"aria-hidden": "true",
										children: [" · ", task.run_count]
									})]
								}, label))
							})
						]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-inspector-content",
						ref: body,
						children: [
							loading && !data && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								role: "status",
								style: mutedStyle$7,
								children: "正在读取任务详情…"
							}),
							failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReadError, { onRetry: () => setReload((n) => n + 1) }),
							showRun && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
								className: "task-inspector-back",
								children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									className: "task-quiet-button",
									type: "button",
									onClick: returnToTask,
									children: "返回任务"
								})
							}),
							showRun && !failed && detailFailed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReadError, { onRetry: () => setReload((n) => n + 1) }),
							showRun && task && run && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
								runTab === "便条" && notesClient ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(RunNotes, {
									client: notesClient,
									runId: run.run_id,
									reloadVersion
								}, run.run_id) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)(RunDetails, {
									run,
									task,
									api,
									tab: runTab === "便条" ? "步骤" : runTab,
									reworkEnabled: Boolean(rework),
									modelEscalation,
									reloadVersion,
									onSnapshot,
									onOpenDecision,
									onOpenResult: () => {
										setReviewRunId(run.run_id);
										setRunTab("结果");
										resetScroll();
									}
								}, run.run_id),
								rework && reviewRunId === run.run_id && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
									hidden: runTab !== "结果",
									children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReworkPanel, {
										api,
										commands: rework,
										runId: run.run_id,
										plannerEnabled: reworkPlannerEnabled,
										reloadVersion,
										onSnapshot
									}, run.run_id)
								}),
								runTab === "结果" && run.status === "succeeded" && run.run_id === task.current_run_id && taskCommands && onOpenMemory && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(StartMemory, {
									runId: run.run_id,
									commands: taskCommands,
									onOpenMemory
								}, run.run_id)
							] }),
							!showRun && data && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
								taskTab === "概览" && latest && rework && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReworkPanel, {
									api,
									commands: rework,
									runId: latest.run_id,
									plannerEnabled: reworkPlannerEnabled,
									reloadVersion,
									onSnapshot
								}),
								taskTab === "概览" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskOverview, {
									data,
									latest,
									api,
									reloadVersion,
									commands: taskCommands,
									triggerCommands,
									onArchiveChanged,
									onOpenRun: openRun
								}),
								taskTab === "时间线" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskTimelinePanel, {
									task: data.task,
									events: data.timeline?.events ?? [],
									total: data.timeline?.total ?? 0,
									loading,
									onRefresh: () => setReload((value) => value + 1)
								}),
								taskTab === "管理" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskManagementPanel, {
									task: data.task,
									runs: data.runs,
									latest,
									attempts: data.attempts ?? [],
									timeline: data.timeline,
									acceptance: data.acceptance,
									api,
									commands: taskCommands,
									reloadVersion,
									onChanged: () => {
										setReload((value) => value + 1);
										onArchiveChanged?.();
									}
								}),
								taskTab === "编排" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskOrchestration, {
									reworkEnabled: Boolean(rework),
									api,
									task: data.task,
									run: latest,
									reloadVersion,
									onOpenDecision
								}),
								taskTab === "事件" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskEventTimeline, {
									api,
									scope: { taskId },
									reloadVersion
								})
							] }),
							data && taskTab === "运行" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
								hidden: showRun,
								children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)(RunHistory, {
									data,
									api,
									selectedRunId: run?.run_id,
									onOpenRun: openRun
								})
							})
						]
					})
				]
			});
		}
		function TaskFactGrid({ task, latest, attempts, acceptance }) {
			const attempt = attempts?.[0] ?? null;
			const model = attempt?.model_name ? `${attempt.model_provider ? `${attempt.model_provider} / ` : ""}${attempt.model_name}` : "未记录";
			const error = attempts?.find((item) => item.error_message)?.error_message ?? latest?.error_message ?? latest?.failure_reason ?? null;
			const facts = [
				["任务 ID", task.task_id],
				["生命周期", task.lifecycle_state ?? "未记录"],
				["执行 Agent", attempt?.session_id ?? task.session_id ?? "未记录"],
				["模型", model],
				["推理强度", attempt?.reasoning_effort ?? "未记录"],
				["工作目录", task.workspace_source ?? "未隔离"],
				["工作区模式", task.workspace_mode ?? "none"],
				["Git 分支", task.workspace_branch ?? "—"],
				["重试", `${task.retry_count ?? 0} / ${task.retry_limit ?? 0}`],
				["最近尝试", attempt ? `第 ${attempt.attempt_no} 次 · ${attempt.status}` : "未记录"],
				["验收模式", acceptance?.mode ?? task.acceptance_mode ?? "—"],
				["验收状态", acceptance?.status ?? task.acceptance_status ?? "—"],
				["错误详情", error ?? "无"]
			];
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dl", {
				className: "ops-detail-grid",
				children: facts.map(([label, value]) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
					className: "ops-fact",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: label }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: value })]
				}, label))
			});
		}
		function TaskAcceptanceSummary({ acceptance }) {
			if (!acceptance) return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				className: "task-drawer-card",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "验收结果" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					style: mutedStyle$7,
					children: "尚未记录验收数据。"
				})]
			});
			const report = acceptance.reviewReport ?? acceptance.review ?? null;
			const remediation = Array.isArray(acceptance.remediation) ? acceptance.remediation : Array.isArray(acceptance.pending_remediation) ? acceptance.pending_remediation : [];
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				className: "task-drawer-card",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "验收结果" }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
						className: "ops-detail-grid",
						children: [
							["状态", acceptance.status],
							["模式", acceptance.mode],
							["标准版本", acceptance.criteria_version ?? "—"],
							["审批人", acceptance.approved_by ?? "—"],
							["审批时间", acceptance.approved_at ? formatTaskDate(acceptance.approved_at) : "—"],
							["审批理由", acceptance.reason ?? "—"]
						].map(([label, value]) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "ops-fact",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: label }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: value })]
						}, label))
					}),
					report && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						style: mutedStyle$7,
						children: ["独立审查：", report.verdict ?? report.status ?? "已记录", report.score !== void 0 ? ` · 评分 ${report.score}` : ""]
					}),
					remediation.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "整改意见" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
							children: remediation.map((item, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", { children: item.action ?? item.message ?? String(item) }, index))
						})]
					})
				]
			});
		}
		function TaskAttemptHistory({ attempts }) {
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ol", {
				className: "ops-task-timeline",
				children: attempts.map((attempt) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", {
					className: "ops-timeline-item",
					"data-kind": attempt.status === "passed" ? "completed" : attempt.status === "failed" || attempt.status === "timeout" ? "failed" : "executing",
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { className: "ops-timeline-dot" }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "ops-timeline-title",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("strong", { children: ["第 ", attempt.attempt_no, " 次尝试"] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: attempt.status }), attempt.model_name && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [attempt.model_provider ? `${attempt.model_provider} / ` : "", attempt.model_name] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", { dateTime: attempt.created_at, children: formatTaskDate(attempt.created_at) })]
						}),
						attempt.completed_at && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							className: "ops-timeline-detail",
							children: ["完成于 ", formatTaskDate(attempt.completed_at), attempt.outcome ? ` · ${attempt.outcome}` : ""]
						}),
						attempt.error_message && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							className: "ops-timeline-detail",
							children: attempt.error_message
						}),
						attempt.session_id && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							className: "ops-timeline-detail",
							children: ["子会话 ", attempt.session_id]
						})
					]
				}, attempt.attempt_id))
			});
		}
		function TaskToolSummary({ api, runId, reloadVersion }) {
			const [summary, setSummary] = (0, react.useState)(null);
			(0, react.useEffect)(() => {
				if (!runId) return;
				let active = true;
				api.listExecutionHistory({
					run_id: runId,
					limit: 200,
					filter: "all"
				}).then(({ data }) => {
					if (!active) return;
					const items = data.items ?? [];
					const tools = items.filter((item) => typeof item.type === "string" && item.type.includes("tool")).length;
					const tests = items.filter((item) => `${item.type ?? ""} ${item.title ?? ""} ${item.label ?? ""}`.toLowerCase().includes("test")).length;
					const attention = items.filter((item) => item.attention === true).length;
					setSummary({
						total: items.length,
						tools,
						tests,
						attention
					});
				}).catch(() => {
					if (active) setSummary({ failed: true });
				});
				return () => {
					active = false;
				};
			}, [
				api,
				runId,
				reloadVersion
			]);
			if (!summary) return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				style: mutedStyle$7,
				children: "正在汇总工具调用…"
			});
			if (summary.failed) return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				style: mutedStyle$7,
				children: "工具调用摘要暂不可用。"
			});
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				style: mutedStyle$7,
				children: `已记录 ${summary.total} 条执行事件；工具调用 ${summary.tools} 次，测试相关 ${summary.tests} 次，需留意 ${summary.attention} 次。`
			});
		}
		function TaskTimelinePanel({ task, events, total, loading, onRefresh }) {
			const labels = {
				created: "创建",
				dispatched: "派发",
				executing: "执行",
				retry: "重试",
				failed: "失败",
				review: "验收",
				completed: "完成",
				cancelled: "取消"
			};
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "task-drawer-section",
				"aria-label": "任务时间线",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: "任务时间线" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							className: "task-quiet-button",
							onClick: onRefresh,
							children: loading ? "正在刷新…" : "刷新"
						})]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						style: mutedStyle$7,
						children: ["任务 ", task.task_id, " · 共 ", total || events.length, " 条事件"]
					}),
					events.length === 0 ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$7,
						children: "暂无时间线事件。"
					}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ol", {
						className: "ops-task-timeline",
						children: events.map((event, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", {
							className: "ops-timeline-item",
							"data-kind": event.kind,
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { className: "ops-timeline-dot" }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
									className: "ops-timeline-title",
									children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: labels[event.kind] ?? event.kind }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: event.title }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", { dateTime: event.at, children: formatTaskDate(event.at) })]
								}),
								event.detail && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									className: "ops-timeline-detail",
									children: event.detail
								}),
								(event.attempt_id || event.run_id) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
									className: "ops-timeline-detail",
									children: [event.attempt_id ? `尝试 ${event.attempt_id}` : "", event.attempt_id && event.run_id ? " · " : "", event.run_id ? `运行 ${event.run_id}` : ""]
								})
							]
						}, `${event.kind}:${event.at}:${index}`))
					})
				]
			});
		}
		function downloadTextFile(fileName, text, type = "application/json") {
			const blob = new Blob([text], { type });
			const url = URL.createObjectURL(blob);
			const anchor = document.createElement("a");
			anchor.href = url;
			anchor.download = fileName;
			anchor.click();
			setTimeout(() => URL.revokeObjectURL(url), 1e3);
		}
		function TaskManagementPanel({ task, runs, latest, attempts, timeline, acceptance, api, commands, reloadVersion, onChanged }) {
			const [busy, setBusy] = (0, react.useState)(null);
			const [notice, setNotice] = (0, react.useState)("");
			const [diff, setDiff] = (0, react.useState)(null);
			const [diffError, setDiffError] = (0, react.useState)(false);
			const [showDiff, setShowDiff] = (0, react.useState)(false);
			const [showDependencies, setShowDependencies] = (0, react.useState)(false);
			const canRetry = !["running", "dispatching", "queued", "retrying", "review"].includes(task.lifecycle_state) && task.lifecycle_state !== "passed";
			async function retry() {
				if (!commands || busy) return;
				setBusy("retry");
				setNotice("");
				try {
					await commands.retryTask(task.task_id);
					setNotice("已创建新的执行批次。");
					onChanged?.();
				} catch (error) {
					setNotice(error instanceof Error ? error.message : "重试失败。");
				} finally {
					setBusy(null);
				}
			}
			async function cancel() {
				if (!commands || busy) return;
				if (!window.confirm(`确认取消任务「${task.title || task.task_id}」？`)) return;
				setBusy("cancel");
				setNotice("");
				try {
					await commands.cancelTask(task.task_id, "cancelled_by_user");
					setNotice("已发送取消请求。");
					onChanged?.();
				} catch (error) {
					setNotice(error instanceof Error ? error.message : "取消失败。");
				} finally {
					setBusy(null);
				}
			}
			async function loadDiff() {
				if (diff || busy) {
					setShowDiff(true);
					return;
				}
				setBusy("diff");
				setDiffError(false);
				try {
					const workspace = await api.getTaskWorkspace(task.task_id);
					const item = (workspace.data.items ?? []).find((entry) => entry.status !== "removed") ?? workspace.data.items?.[0];
					if (!item) {
						setNotice("该任务没有可用的隔离工作区。");
						return;
					}
					const result = await api.getWorkspaceDiff(item.workspace_id);
					setDiff(result.data);
					setShowDiff(true);
				} catch {
					setDiffError(true);
				} finally {
					setBusy(null);
				}
			}
			function exportReport() {
				const report = {
					exported_at: new Date().toISOString(),
					task,
					runs,
					attempts: attempts ?? [],
					timeline: timeline ?? { events: [] },
					acceptance: acceptance ?? null
				};
				downloadTextFile(`task-${task.task_id}.json`, JSON.stringify(report, null, 2));
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "task-drawer-section",
				"aria-label": "任务管理",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: "任务管理" }) }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "ops-detail-actions",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								disabled: !commands || busy !== null || !canRetry,
								onClick: retry,
								children: busy === "retry" ? "正在派发…" : "手动重试 / 派发"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"data-tone": "danger",
								disabled: !commands || busy !== null || ["passed", "rejected"].includes(task.lifecycle_state),
								onClick: cancel,
								children: busy === "cancel" ? "正在取消…" : "取消任务"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: () => setShowDependencies((value) => !value),
								children: showDependencies ? "收起依赖" : "查看任务依赖"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								disabled: busy === "diff",
								onClick: loadDiff,
								children: busy === "diff" ? "正在读取…" : diff ? "查看 Git Diff" : "读取 Git Diff"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: exportReport,
								children: "导出执行报告"
							})
						]
					}),
					notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: mutedStyle$7,
						children: notice
					}),
					showDependencies && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-drawer-card",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "任务依赖" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: ["前置任务：", (task.dependencies ?? []).join("、") || "无"]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: ["后续任务：", (task.dependents ?? []).join("、") || "无"]
							}),
							latest && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								style: mutedStyle$7,
								children: "步骤级依赖可在「编排」页查看。"
							})
						]
					}),
					showDiff && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-drawer-card",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "Git Diff" }),
							diffError && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								style: mutedStyle$7,
								children: "Git Diff 读取失败。"
							}),
							diff && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
									style: mutedStyle$7,
									children: [diff.kind === "git_worktree" ? `分支 ${diff.branch ?? "—"} · 基线 ${diff.baseCommit ?? "—"}` : "非 Git 工作区文件差异", diff.truncated ? " · 内容已截断" : ""]
								}),
								(diff.changedFiles?.length ?? 0) > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
									style: mutedStyle$7,
									children: ["变更文件：", diff.changedFiles.join("、")]
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("pre", {
									className: "task-diff",
									children: diff.stat || diff.patch || "没有差异。"
								})
							] })
						]
					})
				]
			});
		}
		function TaskOverview({ data, latest, api, reloadVersion, commands, triggerCommands, onArchiveChanged, onOpenRun }) {
			const { task, runs } = data;
			const workflow = usePinnedWorkflow(api, latest ? latest.workflow_id : task.workflow_id, latest ? latest.workflow_version : task.workflow_version);
			const nextVersion = latest ? latest.workflow_version : task.workflow_version;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
				/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
					className: "task-drawer-section",
					"aria-label": "任务概况",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: "目标与验收" }) }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-drawer-card",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: task.title || "未命名任务" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskPlanBrief, {
								api,
								taskId: task.task_id
							}),
							task.attention_reason && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								style: mutedStyle$7,
								children: operatorReason(task.attention_reason)
							}),
							workflow?.doc && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: [
									"按固定流程 ",
									workflow.doc.name,
									" v",
									nextVersion,
									" 执行。"
								]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PendingNotices, { task })
						]
					})]
				}),
				/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
					className: "task-drawer-section",
					"aria-label": "任务档案",
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: "任务档案" }) }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskFactGrid, {
							task,
							latest,
							attempts: data.attempts ?? [],
							acceptance: data.acceptance
						}),
						latest && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
							className: "task-drawer-card",
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "执行日志与工具调用" }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskToolSummary, {
									api,
									runId: latest.run_id,
									reloadVersion
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)(ExecutionHistory, {
									api,
									taskId: task.task_id,
									runId: latest.run_id,
									reloadVersion
								})
							]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskAcceptanceSummary, { acceptance: data.acceptance }),
						(data.attempts?.length ?? 0) > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
							className: "task-drawer-card",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", { children: ["执行历史 · ", data.attempts.length, " 次尝试"] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskAttemptHistory, { attempts: data.attempts })]
						})
					]
				}),
				(task.trigger_kind === "scheduled" || task.trigger_kind === "event") && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
					className: "task-drawer-section",
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: task.trigger_kind === "scheduled" ? "排期与历史运行" : "触发条件与历史运行" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
							className: "task-type-badge",
							children: "一条任务 · 多次运行"
						})] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskTriggerCard, {
							api,
							task,
							commands: triggerCommands,
							reloadVersion
						}),
						runs.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$7,
							children: [
								"最近 ",
								Math.min(runs.length, 4),
								" 次运行"
							]
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(RunRows, {
							runs: runs.slice(0, 4),
							onOpenRun
						})] })
					]
				}),
				/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
					className: "task-drawer-section",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: "最近一次运行" }), latest && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						className: "task-quiet-button",
						type: "button",
						onClick: () => onOpenRun(latest),
						children: "完整运行"
					})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						className: "task-drawer-card",
						children: latest ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(RunStatusLabel, {
								status: latest.status,
								displayStatus: latest.display_status
							}), latest.settling ? " · 正在收尾" : ""] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: [
									formatTaskDate(latest.started_at ?? latest.created_at),
									" · ",
									taskOriginLabel(latest),
									durationSuffix(latest)
								]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: [
									doneCount(latest),
									" / ",
									latest.total,
									" 个步骤已完成"
								]
							})
						] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							style: mutedStyle$7,
							children: "这项任务还没有运行记录。"
						})
					})]
				}),
				/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
					className: "task-drawer-section",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: "编排与资源" }) }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-drawer-card",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
							className: "task-run-facts",
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "工作流" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: workflow?.doc?.name || (latest?.workflow_id || task.workflow_id ? "已关联，名称暂不可用" : "未关联") })] }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "固定版本" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: nextVersion != null ? `v${nextVersion}` : "未记录" })] }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "自治档位" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: taskAutonomyLabel(task) })] })
							]
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看任务登记明细" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
								"任务编号 ",
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: task.task_id }),
								(latest?.workflow_id || task.workflow_id) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [" · 流程编号 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: latest?.workflow_id || task.workflow_id })] })
							] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
								"状态 ",
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: task.status }),
								task.attention_reason && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [" · 原因 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: task.attention_reason })] })
							] })
						] })]
					})]
				}),
				(commands || task.archived_at) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
					className: "task-drawer-section",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: "归档" }) }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "task-drawer-card",
						children: [task.archived_at && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$7,
							children: ["归档于 ", formatTaskDate(task.archived_at)]
						}), commands && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ArchiveAction, {
							task,
							commands,
							onChange: onArchiveChanged
						})]
					})]
				})
			] });
		}
		function TaskTriggerCard({ api, task, commands, reloadVersion }) {
			const [trigger, setTrigger] = (0, react.useState)();
			const [retry, setRetry] = (0, react.useState)(0);
			const [readError, setReadError] = (0, react.useState)(false);
			const [writing, setWriting] = (0, react.useState)(false);
			const [writeError, setWriteError] = (0, react.useState)("");
			const operation = (0, react.useRef)(void 0);
			(0, react.useEffect)(() => {
				let current = true;
				setReadError(false);
				api.listTaskTriggers({ task_id: task.task_id }).then(({ data }) => {
					if (current) setTrigger(data.items.find((item) => item.task_id === task.task_id) ?? null);
				}).catch(() => {
					if (current) setReadError(true);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				reloadVersion,
				retry,
				task.task_id
			]);
			async function toggle(item) {
				if (!commands || writing) return;
				if (operation.current?.revision !== item.revision) operation.current = {
					revision: item.revision,
					id: newOperationId()
				};
				setWriting(true);
				setWriteError("");
				try {
					await commands.setEnabled(item, !item.enabled, operation.current.id);
					operation.current = void 0;
				} catch (error) {
					if (!(error instanceof ApiError) || error.code !== "OPERATION_OUTCOME_UNKNOWN") operation.current = void 0;
					setWriteError(error instanceof Error ? error.message : "触发器状态更新失败。");
				} finally {
					setWriting(false);
					setRetry((count) => count + 1);
				}
			}
			if (readError) return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReadError, { onRetry: () => setRetry((count) => count + 1) });
			if (trigger === void 0) return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				role: "status",
				style: mutedStyle$7,
				children: "正在读取触发配置…"
			});
			if (!trigger) return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
				className: "task-drawer-card task-schedule-card",
				children: "此任务没有已登记的触发配置。"
			});
			const scheduled = trigger.kind === "scheduled";
			const zone = trigger.time_zone ?? void 0;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				className: "task-drawer-card task-schedule-card",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
						className: "task-run-facts",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: scheduled ? "排期" : "事件类型" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: scheduled ? `每天 ${trigger.daily_time}` : trigger.event_type })] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: scheduled ? "下次触发" : "触发器状态" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: !trigger.enabled ? "已暂停" : scheduled ? trigger.next_fire_at ? formatTaskDate(trigger.next_fire_at, zone) : "暂无排期" : "等待匹配事件" })] }),
							zone && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "时区" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: zone })] }),
							trigger.last_fired_at && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "上次触发" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: formatTaskDate(trigger.last_fired_at, zone) })] })
						]
					}),
					commands && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
						disabled: writing,
						style: {
							...quietButtonStyle,
							marginTop: 9
						},
						onClick: () => {
							toggle(trigger);
						},
						children: writing ? "正在更新…" : trigger.enabled ? "暂停触发" : "恢复触发"
					}),
					writeError && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "alert",
						style: mutedStyle$7,
						children: writeError
					})
				]
			});
		}
		function RunHistory({ data, api, selectedRunId, onOpenRun }) {
			const [more, setMore] = (0, react.useState)([]);
			const [next, setNext] = (0, react.useState)();
			const [loading, setLoading] = (0, react.useState)(false);
			const [error, setError] = (0, react.useState)();
			const cursor = next === void 0 ? data.next : next;
			const runs = [...data.runs, ...more.filter((run) => !data.runs.some((recent) => recent.run_id === run.run_id))];
			async function loadMore() {
				if (loading || !cursor) return;
				setError(void 0);
				setLoading(true);
				try {
					const { data: page } = await api.listRuns({
						task_id: data.task.task_id,
						limit: 50,
						cursor
					});
					setMore((previous) => [...previous, ...page.items]);
					setNext(page.next_cursor ?? null);
				} catch (cause) {
					setError(cause);
				} finally {
					setLoading(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "task-drawer-section",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: "全部运行" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
						className: "task-type-badge",
						children: [
							"同一任务 · ",
							data.task.run_count,
							" 次"
						]
					})] }),
					runs.length > 0 ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(RunRows, {
						runs,
						selectedRunId,
						onOpenRun
					}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$7,
						children: "这项任务还没有运行记录。"
					}),
					cursor && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						className: "task-quiet-button",
						disabled: loading,
						onClick: () => {
							loadMore();
						},
						children: loading ? "正在读取…" : "加载更早的运行"
					}),
					error instanceof ApiError && error.code === "CURSOR_EXPIRED" ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: "alert",
						children: ["翻页位置已过期。", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							className: "task-quiet-button",
							onClick: () => {
								setMore([]);
								setNext(void 0);
								setError(void 0);
							},
							children: "返回最近运行"
						})]
					}) : Boolean(error) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "alert",
						style: mutedStyle$7,
						children: "读取失败，请重试。"
					})
				]
			});
		}
		function RunRows({ runs, selectedRunId, onOpenRun }) {
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
				className: "task-run-history",
				children: runs.map((run) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("button", {
					type: "button",
					"aria-current": selectedRunId === run.run_id,
					onClick: () => onOpenRun(run),
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [formatTaskDate(run.started_at ?? run.created_at), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("small", { children: taskOriginLabel(run) })] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)(RunStatusLabel, {
							status: run.status,
							displayStatus: run.display_status
						}) }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: runDuration(run.started_at, run.ended_at) ?? "—" })
					]
				}, run.run_id))
			});
		}
		function TaskOrchestration({ api, task, run, reloadVersion, reworkEnabled, onOpenDecision }) {
			const workflow = usePinnedWorkflow(api, run ? run.workflow_id : task.workflow_id, run ? run.workflow_version : task.workflow_version);
			const [snapshot, setSnapshot] = (0, react.useState)();
			const [failed, setFailed] = (0, react.useState)(false);
			const [retry, setRetry] = (0, react.useState)(0);
			const [showGraph, setShowGraph] = (0, react.useState)(false);
			const nodes = snapshot?.runId === run?.run_id ? snapshot?.nodes : void 0;
			(0, react.useEffect)(() => {
				if (!run) return;
				let current = true;
				setFailed(false);
				api.listNodes(run.run_id, 200).then(({ data }) => {
					if (current) setSnapshot({
						runId: run.run_id,
						nodes: data.items
					});
				}).catch(() => {
					if (current) setFailed(true);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				run?.run_id,
				reloadVersion,
				retry
			]);
			const topology = useRunTopology(api, run?.run_id, Boolean(reworkEnabled), reloadVersion + retry);
			const runtimeDoc = runtimeCanvasDoc(workflow?.definition, topology.data);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "task-drawer-section",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: "运行拓扑与步骤" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
						className: "task-type-badge",
						children: taskAutonomyLabel(task)
					})] }),
					failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReadError, { onRetry: () => setRetry((n) => n + 1) }),
					!failed && run && !nodes && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: mutedStyle$7,
						children: "正在读取执行步骤…"
					}),
					workflow?.error && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: mutedStyle$7,
						children: "固定版本流程定义暂时不可读取。"
					}),
					(nodes || !run) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskRunSteps, {
						nodes: nodes ?? [],
						doc: runtimeDoc,
						run,
						onOpenDecision,
						renderEvidence: (node) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(ExecutionHistory, {
							api,
							nodeId: node.node_run_id,
							reloadVersion
						}, node.node_run_id), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h4", { children: "执行证据" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(NodeEvidence, {
							api,
							node,
							reloadVersion,
							onOpenDecision
						}, node.node_run_id)] })] })
					}, run?.run_id ?? "planned"),
					topology.loading && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取本次运行拓扑…"
					}),
					topology.failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "alert",
						children: "运行拓扑读取失败，请刷新后重试。"
					}),
					workflow?.doc && !topology.failed && !topology.loading && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
						className: "task-flow-graph",
						onToggle: (event) => setShowGraph(event.currentTarget.open),
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", { children: [
							reworkEnabled && run ? "查看本次运行流程图" : "查看固定流程图",
							" · ",
							workflow.doc.name
						] }), showGraph && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowCanvasView, {
							definition: workflow.definition,
							readOnly: true,
							runtimeCompact: true,
							inspectNodes: true,
							runtimeTopology: topology.data,
							canvasId: "task-drawer-flow",
							runtimeNodes: nodes
						})]
					})
				]
			});
		}
		const doneCount = (run) => run.counts?.find((count) => count.status === "done")?.count ?? 0;
		const durationSuffix = (run) => {
			const duration = runDuration(run.started_at, run.ended_at);
			return duration ? ` · 用时 ${duration}` : "";
		};
		function StartMemory({ runId, commands, onOpenMemory }) {
			const [reason, setReason] = (0, react.useState)("");
			const [operationId, setOperationId] = (0, react.useState)();
			const [state, setState] = (0, react.useState)("idle");
			const [errorText, setErrorText] = (0, react.useState)("");
			async function submit() {
				const id = operationId ?? newOperationId();
				setOperationId(id);
				setState("busy");
				try {
					await commands.startMemory(runId, reason.trim(), id);
					setState("done");
				} catch (error) {
					if (error instanceof ApiError && error.code === "OPERATION_OUTCOME_UNKNOWN") setState("unknown");
					else {
						setOperationId(void 0);
						setState("error");
						setErrorText(error instanceof Error ? error.message : "暂时无法发起记忆整理。");
					}
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
				style: { marginTop: 18 },
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "整理为记忆" }), state === "done" ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					role: "status",
					children: "已取得整理回执，可在记忆页查看批次状态。"
				}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
					type: "button",
					style: quietButtonStyle,
					onClick: onOpenMemory,
					children: "查看记忆批次"
				})] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
					onSubmit: (event) => {
						event.preventDefault();
						submit();
					},
					style: {
						display: "grid",
						gap: 8,
						marginTop: 8
					},
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["希望下次复用什么？ ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
							value: reason,
							required: true,
							maxLength: 500,
							placeholder: "例如：保留这次核对数据口径的步骤和验证依据",
							disabled: state === "busy" || state === "unknown",
							onChange: (event) => setReason(event.target.value)
						})] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							style: mutedStyle$7,
							children: "系统会从本次运行提议记忆条目。请在审批中心核对，获批条目的写入结果可在「记忆」中查看。"
						}),
						state === "error" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "alert",
							children: errorText
						}),
						state === "unknown" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "status",
							children: "结果暂不可确认。再次查询会保留原回执编号。"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
							type: "submit",
							disabled: state === "busy" || !reason.trim(),
							children: state === "unknown" ? "查询或重试" : "发起整理"
						})
					]
				})]
			});
		}
		function ArchiveAction({ task, commands, onChange }) {
			const [confirm, setConfirm] = (0, react.useState)(false);
			const [busy, setBusy] = (0, react.useState)(false);
			const [notice, setNotice] = (0, react.useState)("");
			const archived = Boolean(task.archived_at);
			async function submit() {
				setBusy(true);
				setNotice("");
				try {
					await commands.setArchived(task.task_id, !archived, task.archived_at ?? null);
					setConfirm(false);
					setNotice(archived ? "已取回任务。" : "已归档，运行记录仍可查看。");
					onChange?.();
				} catch (error) {
					setNotice(error instanceof Error ? error.message : "暂时无法确认归档操作。");
					onChange?.();
				} finally {
					setBusy(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				className: "task-archive-actions",
				children: [
					!archived && !confirm && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
						style: quietButtonStyle,
						disabled: busy,
						onClick: () => setConfirm(true),
						children: "归档任务"
					}),
					confirm && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "任务会移入归档箱。已有运行与历史记录会保留。" }),
					(archived || confirm) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
						style: quietButtonStyle,
						disabled: busy,
						onClick: () => {
							submit();
						},
						children: archived ? "取回任务" : "确认归档"
					}),
					confirm && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						disabled: busy,
						style: quietButtonStyle,
						onClick: () => setConfirm(false),
						children: "取消"
					}),
					notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: notice
					})
				]
			});
		}
		function RunDetails({ api, run, task, tab, reloadVersion, onSnapshot, onOpenDecision, onOpenResult, modelEscalation, reworkEnabled }) {
			const [nodes, setNodes] = (0, react.useState)(null);
			const [failed, setFailed] = (0, react.useState)(false);
			const [retry, setRetry] = (0, react.useState)(0);
			(0, react.useEffect)(() => {
				let current = true;
				setFailed(false);
				api.listNodes(run.run_id, 200).then(({ data, as_of_seq }) => {
					if (!current) return;
					setNodes(data.items);
					onSnapshot?.(as_of_seq);
				}).catch(() => {
					if (current) setFailed(true);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				run.run_id,
				reloadVersion,
				onSnapshot,
				retry
			]);
			const stepsTab = tab === "步骤";
			const verificationStatuses = ["verifying", "blocked_post"];
			const verifying = Boolean(run.counts?.some((item) => item.count > 0 && verificationStatuses.includes(item.status)) || nodes?.some((node) => verificationStatuses.includes(node.status)));
			const delivery = useRunDelivery(api, run, tab === "结果" || stepsTab && verifying, verifying, reloadVersion);
			const workflow = usePinnedWorkflow(api, stepsTab ? run.workflow_id : void 0, run.workflow_version);
			const topology = useRunTopology(api, stepsTab ? run.run_id : void 0, reworkEnabled, reloadVersion + retry);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
				run.run_id === task.current_run_id && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PendingNotices, { task }),
				tab === "步骤" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
					className: "task-drawer-card",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
						className: "task-run-facts",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "触发来源" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: taskOriginLabel(run) })] }),
							run.scheduled_for && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "计划时间" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: formatTaskDate(run.scheduled_for) })] }),
							run.started_at && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "开始时间" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: formatTaskDate(run.started_at) })] }),
							run.ended_at && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "结束时间" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: formatTaskDate(run.ended_at) })] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "运行用时" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: runDuration(run.started_at, run.ended_at) ?? "未记录" })] }),
							run.workflow_version != null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "流程版本" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: ["v", run.workflow_version] })] })
						]
					}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看运行登记明细" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
						"运行编号 ",
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: run.run_id }),
						" · 状态 ",
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: run.status })
					] })] })]
				}),
				tab === "步骤" && verifying && delivery.artifacts && !delivery.failed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
					className: "task-drawer-card",
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
							style: sectionTitleStyle$3,
							children: "交付进展"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)(DeliveryProgress, {
							artifacts: delivery.artifacts,
							requirements: delivery.requirements,
							nodes
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							className: "task-quiet-button",
							onClick: onOpenResult,
							children: "查看交付产物"
						})
					]
				}),
				tab === "结果" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(RunResult, {
					api,
					run,
					delivery,
					verifying,
					nodes
				}),
				tab === "事件" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskEventTimeline, {
					api,
					scope: { runId: run.run_id },
					reloadVersion
				}),
				(tab === "步骤" || tab === "证据") && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
					failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReadError, { onRetry: () => setRetry((value) => value + 1) }),
					!failed && !nodes && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: mutedStyle$7,
						children: "正在读取执行步骤…"
					}),
					!failed && nodes && tab === "步骤" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskRunSteps, {
						nodes,
						run,
						runDetails: true,
						onOpenDecision,
						onOpenResult,
						doc: !nodes.length || topology.failed || topology.loading ? null : runtimeCanvasDoc(workflow?.definition, topology.data),
						renderEvidence: (node) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
							(node.declared_effect || node.verified_effect || node.evidence_state_reason || node.status === "done") && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(NodeEffectFacts, {
								node,
								onOpenDecision
							}),
							modelEscalation && node.node_type === "agent" && node.model_assignment && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ModelEscalation, {
								api: modelEscalation,
								node,
								reloadVersion
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)(ExecutionHistory, {
								api,
								nodeId: node.node_run_id,
								reloadVersion
							})
						] })
					}),
					tab === "证据" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(DeliveryChecks, { checks: run.delivery_checks }),
					!failed && nodes && tab === "证据" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(EvidenceList, {
						api,
						nodes,
						reloadVersion,
						onOpenDecision
					}),
					run.project_context && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(RunProjectContext, { context: run.project_context }),
					!failed && nodes && run.total > nodes.length && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						style: mutedStyle$7,
						children: [
							"仅显示本次读取到的 ",
							nodes.length,
							" 条尝试记录"
						]
					})
				] })
			] });
		}
		const PROJECT_SOURCE_STATE = {
			available: "已读取",
			missing: "缺失",
			unavailable: "暂不可读取",
			excluded: "未纳入"
		};
		const PROJECT_HASH_SCOPE = {
			read_text: "读取的文本",
			excerpt: "读取的片段",
			directory_index: "目录索引"
		};
		function RunProjectContext({ context }) {
			const readCount = context.sources.filter((source) => source.status === "available").length;
			const optionalBriefMissing = context.sources.some((source) => source.path === "ORCHESTRA.md" && source.status === "missing");
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
				className: "task-drawer-card",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", { children: ["本次派单的项目资料", readCount ? ` · 已读取 ${readCount} 项` : optionalBriefMissing ? " · 未提供可选项目说明" : ""] }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$7,
						children: "这些资料提供项目说明。任务指定文件的读取结果可在「执行证据」中查看。"
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: context.sources.map((source) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", {
						style: { overflowWrap: "anywhere" },
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: source.path === "." ? "工作区目录索引" : source.path }),
							" · ",
							source.path === "ORCHESTRA.md" && source.status === "missing" ? "可选项目说明，未提供" : PROJECT_SOURCE_STATE[source.status] ?? "状态未记录",
							source.truncated && (source.status === "available" ? " · 已截断" : " · 超出读取范围")
						]
					}, source.path)) }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看项目资料记录明细" }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$7,
							children: [
								"资料版本 ",
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: context.version }),
								" · 账本位置 ",
								context.as_of_seq
							]
						}),
						context.sources.filter((source) => source.status === "available" && source.sha256).map((source) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$7,
							children: [
								source.path === "." ? "工作区目录索引" : source.path,
								" · SHA-256",
								source.sha256_scope && ` · ${PROJECT_HASH_SCOPE[source.sha256_scope] ?? "范围未记录"}`,
								" ",
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: source.sha256 })
							]
						}, source.path))
					] })
				]
			});
		}
		function useRunDelivery(api, run, enabled, verifying, reloadVersion) {
			const [artifacts, setArtifacts] = (0, react.useState)(null);
			const [requirements, setRequirements] = (0, react.useState)(null);
			const [failed, setFailed] = (0, react.useState)(false);
			const [retry, setRetry] = (0, react.useState)(0);
			(0, react.useEffect)(() => {
				if (!enabled) return;
				let current = true;
				setFailed(false);
				setArtifacts(null);
				api.listRunArtifacts(run.run_id).then(({ data }) => {
					if (!Array.isArray(data?.items) || data.items.some((artifact) => artifact.run_id !== run.run_id)) throw new Error("未读取到本次运行的产物记录");
					if (current) setArtifacts(data);
				}).catch(() => {
					if (current) setFailed(true);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				enabled,
				run.run_id,
				reloadVersion,
				retry
			]);
			(0, react.useEffect)(() => {
				if (!enabled || !verifying) return;
				let current = true;
				api.getTaskPlanForTask(run.task_id).then(({ data }) => {
					if (!current) return;
					const matching = data?.launch?.task_id === run.task_id && data.launch.run_id === run.run_id;
					setRequirements(matching && Array.isArray(data.proposal?.deliverables) ? data.proposal.deliverables.flatMap(({ acceptance, node_id }) => acceptance.kind === "artifact" ? [{
						fileName: acceptance.file_name,
						minimumBytes: acceptance.minimum_bytes,
						nodeId: node_id
					}] : []) : null);
				}).catch(() => {
					if (current) setRequirements(null);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				enabled,
				verifying,
				run.task_id,
				run.run_id,
				reloadVersion
			]);
			return {
				artifacts,
				requirements,
				failed,
				retry: () => setRetry((value) => value + 1)
			};
		}
		function DeliveryProgress({ artifacts, requirements, nodes }) {
			const files = [...new Set(artifacts.items.flatMap((item) => item.file_name ? [item.file_name] : []))];
			const canConfirmMissing = artifacts.has_more === false && artifacts.items.every((item) => item.file_name);
			const steps = currentAttempts(nodes ?? []);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
				files.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
					style: mutedStyle$7,
					children: [
						artifacts.has_more ? "最近登记的文件" : "已登记的文件",
						"：",
						files.join("、")
					]
				}),
				requirements?.map(({ fileName, minimumBytes, nodeId }, index) => {
					const step = steps.get(nodeId);
					const status = step && artifacts.items.some((file) => file.file_name === fileName && file.source_node_run_id === step.node_run_id) ? "指定步骤已登记此文件，验收仍待确认。" : files.includes(fileName) ? "已登记同名文件，提交步骤仍待确认。" : canConfirmMissing ? "尚未登记该文件。" : "本页未显示该文件。";
					return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: mutedStyle$7,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$7,
							children: [
								"计划要求「",
								fileName,
								"」",
								step?.title && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
									"由「",
									step.title,
									"」提交"
								] }),
								"。",
								status
							]
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看文件验收要求" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
							"至少 ",
							minimumBytes,
							" 字节，须由计划指定的步骤提交。"
						] })] })]
					}, index);
				}),
				!files.length && !requirements?.length && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					style: mutedStyle$7,
					children: "暂无已登记文件可查看。"
				})
			] });
		}
		function RunResult({ api, run, delivery, verifying, nodes }) {
			const { artifacts, requirements, failed, retry } = delivery;
			const issues = run.delivery_checks?.items.filter((check) => check.outcome !== "passed");
			const sources = new Map(nodes?.map((node) => [node.node_run_id, node.title]));
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				style: cardStyle$2,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
						style: sectionTitleStyle$3,
						children: "运行结果"
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(RunStatusLabel, {
						status: run.status,
						displayStatus: run.display_status
					}), run.settling ? " · 正在收尾" : ""] }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						style: mutedStyle$7,
						children: [
							doneCount(run),
							" / ",
							run.total,
							" 个步骤已完成"
						]
					}),
					!failed && artifacts && verifying && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(DeliveryProgress, {
						artifacts,
						requirements,
						nodes
					}),
					!!issues?.length && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: issues.map((check) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
						check.file_name ?? check.tool ?? "交付项",
						" · ",
						DELIVERY_OUTCOME[check.outcome],
						"。",
						check.reason && deliveryReason(check.reason)
					] }, check.result_id)) })
				]
			}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [
				/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
					style: sectionTitleStyle$3,
					children: "交付产物"
				}),
				failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReadError, { onRetry: retry }),
				!failed && !artifacts && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					role: "status",
					style: mutedStyle$7,
					children: "正在读取产物…"
				}),
				artifacts?.items.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					style: mutedStyle$7,
					children: "这次运行尚无产物记录。"
				}),
				artifacts?.items.map((artifact) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
					style: cardStyle$2,
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", { children: [artifact.file_name || "未命名产物", artifact.expired ? " · 已超过保留期限" : ""] }),
						artifact.source_node_run_id && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$7,
							children: ["提交步骤 · ", sources.get(artifact.source_node_run_id) || "步骤名称暂不可用"]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$7,
							children: ["记录于 ", formatDate$4(artifact.created_at)]
						}),
						artifact.retain_until && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$7,
							children: ["保留至 ", formatDate$4(artifact.retain_until)]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)(ArtifactContent, {
							api,
							artifact
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看产物登记明细" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: [
									"产物编号 ",
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: artifact.artifact_id }),
									" · SHA-256 ",
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: artifact.sha256 })
								]
							}),
							artifact.source_node_run_id && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: ["提交步骤编号 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: artifact.source_node_run_id })]
							})
						] })
					]
				}, artifact.artifact_id)),
				artifacts?.has_more && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					style: mutedStyle$7,
					children: "显示最近 50 份产物。"
				})
			] })] });
		}
		const DELIVERY_OUTCOME = {
			passed: "通过",
			failed: "未通过",
			unverifiable: "待核验"
		};
		const DELIVERY_REASON = {
			artifact_not_registered: "没有找到规则要求的产物。",
			artifact_source_node_unbound: "产物缺少可核验的步骤来源。",
			artifact_source_node_mismatch: "产物来源与规则指定的步骤不符。",
			artifact_below_minimum_bytes: "产物小于规则要求的最低字节数。",
			artifact_retain_until_expired: "产物在验收时已超过保留期限。",
			tool_evidence_missing: "没有找到指定步骤的工具执行记录。",
			tool_exit_code_mismatch: "工具实际退出码与验收规则不符。"
		};
		const deliveryReason = (reason) => Object.hasOwn(DELIVERY_REASON, reason) ? DELIVERY_REASON[reason] : operatorReason(reason);
		function DeliveryChecks({ checks }) {
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [
				/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
					style: sectionTitleStyle$3,
					children: "交付验收"
				}),
				!checks?.items.length && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					style: mutedStyle$7,
					children: checks ? "本次运行暂无已记录的交付验收。" : "未读取到本次运行的验收记录。"
				}),
				checks?.items.map((check) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("article", {
					style: cardStyle$2,
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h4", {
							style: evidenceHeadingStyle,
							children: [
								check.kind === "artifact" ? "产物" : "工具验收",
								(check.file_name ?? check.tool) && ` · ${check.file_name ?? check.tool}`,
								" · ",
								DELIVERY_OUTCOME[check.outcome] ?? "结果未记录"
							]
						}),
						check.kind === "artifact" ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(react_jsx_runtime.Fragment, { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							style: mutedStyle$7,
							children: "规则核对产物名称、来源步骤和文件大小。内容可在「结果」中查看。"
						}) }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)(react_jsx_runtime.Fragment, { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							style: mutedStyle$7,
							children: "规则核对指定步骤的工具执行结果。"
						}) }),
						check.reason && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: deliveryReason(check.reason) }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看验收记录明细" }),
							check.minimum_bytes !== void 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: [
									"要求至少 ",
									check.minimum_bytes,
									" 字节",
									check.bytes !== void 0 && ` · 记录大小 ${check.bytes} 字节`
								]
							}),
							check.expected_exit_code !== void 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: [
									"要求退出码 ",
									check.expected_exit_code,
									!!check.exit_codes?.length && ` · 记录退出码 ${check.exit_codes.join("、")}`
								]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: ["记录于 ", formatDate$4(check.evaluated_at)]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: ["规则版本 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: check.rule_version })]
							}),
							check.reason && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								style: mutedStyle$7,
								children: ["原因代码 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: check.reason })]
							})
						] })
					]
				}, check.result_id)),
				checks?.has_more && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					style: mutedStyle$7,
					children: "仅显示 50 条验收记录。"
				})
			] });
		}
		const EVIDENCE_ATTENTION_STATES = /* @__PURE__ */ new Set([
			"failed",
			"timeout",
			"abandoned_unverified",
			"failed_with_unverified",
			"orphaned",
			"startup_unknown",
			"indeterminate_side_effect",
			"blocked_pre",
			"blocked_post"
		]);
		function EvidenceList({ api, nodes, reloadVersion, onOpenDecision }) {
			const [otherOpen, setOtherOpen] = (0, react.useState)(false);
			const visible = nodes.filter((node) => node.execution_ids?.length || node.attestation_id || node.evidence_state === "unverified" || node.verified_effect || node.status === "done" && node.evidence_state !== "not_required" || EVIDENCE_ATTENTION_STATES.has(node.status));
			const visibleIds = new Set(visible.map((node) => node.node_run_id));
			const remaining = nodes.filter((node) => !visibleIds.has(node.node_run_id));
			const positions = new Map([...new Set(nodes.map((node) => node.node_id))].map((id, index) => [id, index + 1]));
			const attempt = (node) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				style: cardStyle$2,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h4", {
						style: evidenceHeadingStyle,
						children: [
							node.title || `未命名步骤 ${positions.get(node.node_id)}`,
							" · 第 ",
							node.attempt,
							" 次尝试"
						]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$7,
						children: nodeExecutionLabel(node)
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)(NodeEvidence, {
						api,
						node,
						reloadVersion,
						onOpenDecision
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看步骤登记明细" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
						"步骤编号 ",
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: node.node_id }),
						" · 尝试编号 ",
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: node.node_run_id })
					] })] })
				]
			}, node.node_run_id);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [
				/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
					style: sectionTitleStyle$3,
					children: "执行证据"
				}),
				nodes.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					style: mutedStyle$7,
					children: "这次运行尚无步骤记录。"
				}),
				visible.map(attempt),
				!!remaining.length && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
					style: cardStyle$2,
					open: otherOpen,
					onToggle: (event) => setOtherOpen(event.currentTarget.open),
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", { children: ["未关联核验记录的其他尝试 · ", remaining.length] }), otherOpen && remaining.map(attempt)]
				})
			] });
		}
		function ReadError({ onRetry }) {
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				role: "alert",
				style: errorStyle$4,
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: COPY.queryFailed }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
					type: "button",
					onClick: onRetry,
					style: quietButtonStyle,
					children: COPY.retry
				})]
			});
		}
		function RunStatusLabel({ status, displayStatus }) {
			const view = taskStatusView(status, displayStatus);
			return view.label ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
				style: { color: view.tone },
				children: view.label
			}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)(UnknownState, { value: displayStatus || status });
		}
		/** 待裁决与未结算动作同时说清：光说 blocked 让人以为没有要处理的事。 */
		function PendingNotices({ task }) {
			const parts = [];
			if (task.open_decision_count > 0) parts.push(`${task.open_decision_count} 项${COPY.awaitingDecision}`);
			if (task.unverified_count > 0) parts.push(`${COPY.pendingIrreversible}（${task.unverified_count} ${COPY.unverifiedBadge}）`);
			if (parts.length === 0) return null;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				style: attentionStyle,
				children: parts.join(" · ")
			});
		}
		function formatDate$4(value) {
			const date = new Date(value);
			return Number.isNaN(date.valueOf()) ? "时间未知" : date.toLocaleString();
		}
		const titleStyle = {
			margin: 0,
			fontSize: FONT_PX.title,
			overflowWrap: "anywhere"
		};
		const sectionTitleStyle$3 = {
			margin: "0 0 8px",
			fontSize: FONT_PX.body
		};
		const evidenceHeadingStyle = {
			margin: "0 0 6px",
			fontSize: FONT_PX.body
		};
		const mutedStyle$7 = {
			margin: "6px 0 0",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.body
		};
		const attentionStyle = {
			margin: 0,
			padding: 9,
			borderRadius: 8,
			background: "#fff8e9",
			color: COLOR.warning,
			fontSize: FONT_PX.body
		};
		const cardStyle$2 = {
			minWidth: 0,
			padding: 11,
			marginBottom: 6,
			border: `1px solid ${COLOR.line}`,
			borderRadius: 8,
			background: COLOR.surface
		};
		const quietButtonStyle = {
			minHeight: 36,
			justifySelf: "start",
			padding: "0 12px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 7,
			background: COLOR.surface,
			color: COLOR.ink,
			fontSize: FONT_PX.menu,
			cursor: "pointer"
		};
		const errorStyle$4 = {
			display: "flex",
			justifyContent: "space-between",
			alignItems: "center",
			gap: 12,
			padding: 14,
			borderRadius: 8,
			background: "#fff1f0",
			color: COLOR.danger,
			fontSize: FONT_PX.body
		};
		// src/core/decision-priority.ts
		const DECISION_PRIORITY_RANK = {
			critical: 50,
			urgent: 40,
			high: 30,
			normal: 20,
			low: 10
		};
		function priorityRank(priority) {
			if (typeof priority !== "string" || !Object.hasOwn(DECISION_PRIORITY_RANK, priority)) return 0;
			return DECISION_PRIORITY_RANK[priority];
		}
		// src/client/panel/decision-inbox.tsx
		const KIND_LABEL = {
			pre_execution: "执行前确认",
			action_overrun: "执行超时待处理",
			abandon_ack: "确认放弃跟踪",
			verify_human: "人工核验",
			audit_result: "结果复核",
			rule_ambiguous: "规则待澄清",
			startup_unknown: "启动状态待确认",
			report_contradicted: "结果不一致"
		};
		const DECISION_OPTIONS = /* @__PURE__ */ new Map([
			["approved", "批准执行"],
			["rejected", "拒绝执行"],
			["expired", "按到期处理"],
			["continue", "继续执行"],
			["abort", "停止执行"],
			["confirm_applied", "确认外部操作已发生"],
			["confirm_not_applied", "确认外部操作未发生"],
			["confirm_started", "确认已启动"],
			["confirm_not_started", "确认未启动"],
			["confirm_started_attested", "签认已启动"],
			["continue_tracking", "继续跟踪"],
			["continue_wait", "继续等待"],
			["abandon", "停止跟踪"],
			["uphold_report", "采纳回报"],
			["uphold_decision", "保留原处理决定"]
		]);
		const PRIORITY_LABEL = /* @__PURE__ */ new Map([
			["critical", "最高"],
			["urgent", "紧急"],
			["high", "高"],
			["normal", "普通"],
			["low", "低"]
		]);
		const FIELD_LABEL = /* @__PURE__ */ new Map([
			["allowed_resolutions", "处理选项"],
			["priority", "优先级"],
			["priority_reason", "等待原因"],
			["required_evidence", "核验材料"],
			["target_summary", "事项说明"],
			["payload", "事项内容"]
		]);
		const APPROVER_LABEL = {
			human: "人工",
			either: "人工或授权执行主体",
			agent: "授权执行主体"
		};
		const EXPIRY_LABEL = {
			auto_resolve: "按已记录的规则自动处理",
			escalate_only: "升级给人工",
			none: "无自动动作"
		};
		const NOT_PROVIDED$1 = "服务端未提供";
		function optionalReason(reason, fallback = NOT_PROVIDED$1) {
			return reason ? operatorReason(reason) : fallback;
		}
		function missingFields(fields) {
			return [...new Set(fields.map((field) => FIELD_LABEL.get(field) ?? "部分事项信息"))].join("、");
		}
		/** 同优先级先处理等待时间较长的决定，缺失优先级的决定仍按等待时间排序。 */
		function sortDecisions(items) {
			return [...items].sort((a, b) => {
				const byPriority = priorityRank(b.priority) - priorityRank(a.priority);
				if (byPriority !== 0) return byPriority;
				const createdA = Date.parse(a.created_at) || 0;
				const createdB = Date.parse(b.created_at) || 0;
				if (createdA !== createdB) return createdA - createdB;
				return a.decision_id === b.decision_id ? 0 : a.decision_id < b.decision_id ? -1 : 1;
			});
		}
		const DECISION_VIEWS = {
			inbox: "待处理",
			active: "全部进行中",
			history: "已处理"
		};
		function DecisionInbox({ api, commands, rework, reworkPlannerEnabled, reloadVersion = 0, onSnapshot, decisionId, onShowAll }) {
			const canWrite = usePanelCommandsEnabled(true);
			const [items, setItems] = (0, react.useState)([]);
			const [cursor, setCursor] = (0, react.useState)();
			const [nextCursor, setNextCursor] = (0, react.useState)(null);
			const [loading, setLoading] = (0, react.useState)(true);
			const [failed, setFailed] = (0, react.useState)(false);
			const [retry, setRetry] = (0, react.useState)(0);
			const [decisionView, setDecisionView] = (0, react.useState)("inbox");
			const history = decisionView === "history";
			const lastReload = (0, react.useRef)(reloadVersion);
			const [selected, setSelected] = (0, react.useState)([]);
			const [developerView, setDeveloperView] = (0, react.useState)(false);
			const [busy, setBusy] = (0, react.useState)(false);
			const [notice, setNotice] = (0, react.useState)(null);
			const [cardNotices, setCardNotices] = (0, react.useState)({});
			const [pending, setPending] = (0, react.useState)(null);
			(0, react.useEffect)(() => {
				if (lastReload.current !== reloadVersion) {
					lastReload.current = reloadVersion;
					setCursor(void 0);
				}
			}, [reloadVersion]);
			(0, react.useEffect)(() => {
				let current = true;
				setLoading(true);
				setFailed(false);
				(decisionId ? api.getDecision(decisionId).then((result) => ({
					...result,
					data: {
						items: [result.data],
						next_cursor: null
					}
				})) : api.listDecisions({
					limit: 50,
					cursor,
					inbox: decisionView === "inbox" ? "human" : void 0,
					status: decisionView === "history" ? "resolved" : void 0
				})).then((result) => {
					if (!current) return;
					setItems((previous) => cursor ? [...previous, ...result.data.items] : result.data.items);
					setNextCursor(result.data.next_cursor);
					onSnapshot?.(result.as_of_seq);
				}).catch(() => {
					if (current) setFailed(true);
				}).finally(() => {
					if (current) setLoading(false);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				cursor,
				retry,
				decisionView,
				reloadVersion,
				onSnapshot,
				decisionId
			]);
			function refresh() {
				setCursor(void 0);
				setRetry((value) => value + 1);
			}
			const sorted = (0, react.useMemo)(() => sortDecisions(items), [items]);
			function setCardNotice(decisionId, value) {
				setCardNotices((previous) => {
					const next = { ...previous };
					if (value) next[decisionId] = value;
					else delete next[decisionId];
					return next;
				});
			}
			function toggleSelected(decisionId) {
				setSelected((previous) => previous.includes(decisionId) ? previous.filter((id) => id !== decisionId) : [...previous, decisionId]);
			}
			async function acknowledgeDecision(decision) {
				if (!canWrite || !commands) return;
				setBusy(true);
				setCardNotice(decision.decision_id, null);
				try {
					await commands.acknowledgeDecision({
						decisionId: decision.decision_id,
						revision: decision.current_revision
					});
					setCardNotice(decision.decision_id, {
						text: "已接手。",
						tone: "ok"
					});
					refresh();
				} catch (error) {
					setCardNotice(decision.decision_id, {
						text: describeDecisionError(error),
						tone: "error"
					});
				} finally {
					setBusy(false);
				}
			}
			async function resolveDecision(input, label) {
				if (!canWrite || !commands) return;
				const { decisionId } = input;
				setBusy(true);
				setPending(null);
				setCardNotice(decisionId, null);
				try {
					await commands.resolveDecision(input);
					setCardNotice(decisionId, {
						text: `已处理：${label}。`,
						tone: "ok"
					});
					refresh();
				} catch (error) {
					if (error instanceof DecisionConfirmationRequired) setPending({
						input: {
							...input,
							confirmation: error.confirmation
						},
						label
					});
					else setCardNotice(decisionId, {
						text: describeDecisionError(error),
						tone: "error"
					});
				} finally {
					setBusy(false);
				}
			}
			async function acknowledgeSelected() {
				if (!canWrite || !commands || selected.length === 0) return;
				setBusy(true);
				setNotice(null);
				const failures = [];
				let done = 0;
				for (const decisionId of selected) {
					const decision = items.find((item) => item.decision_id === decisionId);
					if (!decision) continue;
					try {
						await commands.acknowledgeDecision({
							decisionId,
							revision: decision.current_revision
						});
						done += 1;
					} catch (error) {
						failures.push(describeDecisionError(error));
					}
				}
				setSelected([]);
				setBusy(false);
				setNotice(failures.length > 0 ? {
					text: `批量接手完成 ${done} 条，${failures.length} 条没成功：${failures.join("；")}`,
					tone: "error"
				} : {
					text: `已批量接手 ${done} 条。`,
					tone: "ok"
				});
				refresh();
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "decision-inbox-title",
				className: "ops-page",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "ops-page-header",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", {
								id: "decision-inbox-title",
								children: "审批中心"
							}),
							decisionId && onShowAll && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								className: "ops-button",
								onClick: onShowAll,
								children: "查看全部决定"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: refresh,
								disabled: loading,
								className: "ops-button",
								children: loading ? "正在更新…" : "刷新"
							})
						]
					}),
					rework && !decisionId && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ReworkPanel, {
						api,
						commands: rework,
						plannerEnabled: reworkPlannerEnabled,
						reloadVersion,
						onSnapshot
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "ops-toolbar",
						children: [!decisionId && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
							role: "group",
							"aria-label": "决定视图",
							className: "ops-tabs",
							children: Object.entries(DECISION_VIEWS).map(([value, label]) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								"aria-pressed": value === decisionView,
								onClick: () => {
									setItems([]);
									setSelected([]);
									setPending(null);
									setNextCursor(null);
									setCursor(void 0);
									setDecisionView(value);
									setRetry((version) => version + 1);
								},
								children: label
							}, value))
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "ops-actions",
							children: [!history && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
								style: mutedStyle$6,
								children: [
									"已选 ",
									selected.length,
									" 条"
								]
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: () => {
									acknowledgeSelected();
								},
								disabled: !canWrite || failed || !commands || busy || selected.length === 0,
								className: "ops-button",
								children: "批量接手"
							})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
								style: toggleStyle$2,
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									type: "checkbox",
									"aria-label": "开发者视图",
									checked: developerView,
									onChange: (event) => setDeveloperView(event.target.checked)
								}), "开发者视图"]
							})]
						})]
					}),
					notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: notice.tone === "error" ? "alert" : "status",
						style: notice.tone === "error" ? errorTextStyle$3 : mutedStyle$6,
						children: notice.text
					}),
					failed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						role: "alert",
						style: errorStyle$3,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: COPY.queryFailed }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							onClick: refresh,
							style: buttonStyle$5,
							children: COPY.retry
						})]
					}),
					loading && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: mutedStyle$6,
						children: "正在读取决定事项…"
					}),
					!loading && !failed && items.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						className: "ops-surface ops-empty",
						children: history ? "还没有已处理的决定。" : COPY.emptyDecisions
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						className: "ops-card-grid",
						children: sorted.map((decision) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)(DecisionCard, {
							decision,
							commands: history || failed || !canWrite ? void 0 : commands,
							developerView,
							selected: selected.includes(decision.decision_id),
							busy,
							notice: cardNotices[decision.decision_id],
							onToggle: () => toggleSelected(decision.decision_id),
							onAcknowledge: () => {
								acknowledgeDecision(decision);
							},
							onResolve: (resolution, label, selectedNodeId, evidenceId) => {
								resolveDecision({
									decisionId: decision.decision_id,
									revision: decision.current_revision,
									resolution,
									...decision.content_hash ? { contentHash: decision.content_hash } : {},
									...selectedNodeId ? { selectedNodeId } : {},
									...evidenceId ? { evidenceId } : {},
									...resolution === "confirm_started_attested" ? { attestationId: `att_${newOperationId()}` } : {}
								}, label);
							},
							confirmation: !history && pending?.input.decisionId === decision.decision_id && pending.input.revision === decision.current_revision && pending.input.contentHash === (decision.content_hash ?? void 0) && decision.status !== "resolved" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								role: "group",
								"aria-label": "再次确认这条决定",
								style: actionStyle,
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("strong", { children: [
										"确认",
										pending.label,
										"？"
									] }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										disabled: !canWrite || failed || !commands || busy,
										style: buttonStyle$5,
										onClick: () => {
											resolveDecision(pending.input, pending.label);
										},
										children: "确认处理"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										style: buttonStyle$5,
										onClick: () => setPending(null),
										children: "取消"
									})
								]
							})
						}, decision.decision_id))
					}),
					nextCursor && !failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						disabled: loading,
						onClick: () => setCursor(nextCursor),
						style: buttonStyle$5,
						children: "加载更多"
					})
				]
			});
		}
		function isRecord(value) {
			return typeof value === "object" && value !== null && !Array.isArray(value);
		}
		/**
		* 认出服务端 buildApprovalCard 落到决定 payload 里的审批卡（规格 §9.6）。
		* 认不出就返回 null，这条决定仍按普通决定卡片渲染。
		*/
		function readMemoryApprovalCard(payload) {
			if (!isRecord(payload)) return null;
			if (payload.kind !== "audit_result") return null;
			if (typeof payload.title !== "string" || payload.title.length === 0) return null;
			if (typeof payload.summary_line !== "string") return null;
			const rawItems = Array.isArray(payload.items) ? payload.items.filter(isRecord) : [];
			const rawButtons = Array.isArray(payload.buttons) ? payload.buttons.filter(isRecord) : [];
			return {
				kind: "audit_result",
				title: payload.title,
				summary_line: payload.summary_line,
				items: rawItems.map((item) => ({
					layer: typeof item.layer === "string" ? item.layer : "",
					title: typeof item.title === "string" ? item.title : "",
					digest: typeof item.digest === "string" ? item.digest : "",
					judge_reason: typeof item.judge_reason === "string" ? item.judge_reason : null
				})),
				buttons: rawButtons.map((button) => ({
					label: typeof button.label === "string" ? button.label : "",
					resolution: typeof button.resolution === "string" ? button.resolution : ""
				})),
				allowed_resolutions: Array.isArray(payload.allowed_resolutions) ? payload.allowed_resolutions.filter((value) => typeof value === "string") : [],
				resolve_payload: isRecord(payload.resolve_payload) ? payload.resolve_payload : {},
				expand_label: typeof payload.expand_label === "string" && payload.expand_label.length > 0 ? payload.expand_label : "为什么记这条",
				batch_id: typeof payload.batch_id === "string" ? payload.batch_id : null,
				evaluation_id: typeof payload.evaluation_id === "string" ? payload.evaluation_id : null,
				proposal_hash: typeof payload.proposal_hash === "string" ? payload.proposal_hash : null
			};
		}
		function DecisionCard(props) {
			const { decision } = props;
			const [selectedBranch, setSelectedBranch] = (0, react.useState)("");
			const [attestedRevision, setAttestedRevision] = (0, react.useState)(null);
			const attest = attestedRevision === decision.current_revision;
			const kind = Object.hasOwn(KIND_LABEL, decision.kind) ? KIND_LABEL[decision.kind] : "待确认事项";
			const agentOnly = decision.approver?.startsWith("agent:") === true;
			const status = agentOnly && decision.status !== "resolved" ? "由授权核验主体处理" : operatorStatus(decision.status);
			const choices = (decision.allowed_resolutions ?? []).flatMap((value) => {
				const verdict = value === "confirm_applied" ? "applied" : value === "confirm_not_applied" ? "not_applied" : null;
				const evidenceId = verdict ? decision.available_effect_evidence?.[verdict] : void 0;
				if (verdict && !evidenceId) return [];
				const outcome = value === "confirm_not_started" || value === "confirm_not_applied" ? decision.available_recovery_outcomes?.[value] : void 0;
				const baseLabel = decision.kind === "rule_ambiguous" && value === "approved" ? "继续所选分支" : decision.kind === "abandon_ack" && value === "approved" ? "签字接受未核实的副作用" : decision.kind === "abandon_ack" && value === "rejected" ? "驳回放弃，恢复跟踪" : DECISION_OPTIONS.get(value);
				return baseLabel ? [{
					value,
					label: baseLabel + (outcome === "retry" ? "并发起新尝试" : outcome === "failed" ? "并将此步骤记为失败" : ""),
					evidenceId
				}] : [];
			});
			const branches = decision.kind === "rule_ambiguous" && isRecord(decision.payload) && Array.isArray(decision.payload.edges) ? decision.payload.edges.filter((value) => typeof value === "string") : [];
			const priority = decision.priority && PRIORITY_LABEL.get(decision.priority);
			const evidence = [...new Set((decision.required_evidence ?? []).map(operatorReason))];
			const missing = decision.projection_missing ?? [];
			const conflict = decision.kind === "report_contradicted" && isRecord(decision.payload) ? decision.payload : null;
			const ordinaryResolution = decision.closed_reason === "approved" && isRecord(decision.payload) && isRecord(decision.payload.ordinary_recovery) && typeof decision.payload.resolution === "string" ? DECISION_OPTIONS.get(decision.payload.resolution) : void 0;
			const restored = decision.kind === "abandon_ack" && decision.closed_reason === "rejected" && isRecord(decision.payload) && Array.isArray(decision.payload.affected);
			const memoryCard = (0, react.useMemo)(() => readMemoryApprovalCard(decision.payload), [decision.payload]);
			if (memoryCard && decision.status !== "resolved") return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(MemoryApprovalCardView, {
				decision,
				card: memoryCard,
				commands: props.commands,
				developerView: props.developerView,
				busy: props.busy,
				notice: props.notice,
				onResolve: props.onResolve,
				confirmation: props.confirmation
			});
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("article", {
				style: cardStyle$1,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: {
							display: "flex",
							flexWrap: "wrap",
							justifyContent: "space-between",
							gap: 8
						},
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
							style: cardTitleStyle$1,
							children: kind
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", {
							style: {
								color: !agentOnly && decision.status === "blocked_for_human" ? COLOR.warning : COLOR.inkMuted,
								fontSize: FONT_PX.helper
							},
							children: status
						})]
					}),
					priority && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						style: mutedStyle$6,
						children: ["优先级：", priority]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
						style: detailGridStyle$3,
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "说明" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: optionalReason(decision.target_summary, "说明暂不可用") }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "为什么卡" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: optionalReason(decision.priority_reason) }),
							conflict && typeof conflict.report_id === "string" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "回报 ID" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: conflict.report_id })] }),
							conflict && typeof conflict.report_status === "string" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "回报结论" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: operatorStatus(conflict.report_status) })] }),
							conflict && typeof conflict.report_attempt === "number" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "执行尝试" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: [
								"第 ",
								conflict.report_attempt,
								" 次"
							] })] }),
							decision.approver && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "谁能处理" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: agentOnly ? APPROVER_LABEL.agent : Object.hasOwn(APPROVER_LABEL, decision.approver) ? APPROVER_LABEL[decision.approver] : "处理人待确认" })] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "你能选什么" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: choices.length ? choices.map(({ label }) => label).join(" / ") : NOT_PROVIDED$1 }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "需要什么证据" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: evidence.length ? evidence.join("、") : NOT_PROVIDED$1 }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "过期会怎样" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: decision.kind === "report_contradicted" ? "仅由人工处理，系统不会自动收口。" : memoryCard && decision.expires_at ? formatDate$3(decision.expires_at) + " · 到期未批准，批次关闭。" : decision.expires_at ? formatDate$3(decision.expires_at) + " · " + (decision.expiry_effect && Object.hasOwn(EXPIRY_LABEL, decision.expiry_effect) ? EXPIRY_LABEL[decision.expiry_effect] : "截止后的处理方式尚未提供") : NOT_PROVIDED$1 }),
							decision.status === "resolved" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "处理结果" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: decision.kind === "report_contradicted" && (decision.closed_reason === "approved" || decision.closed_reason === "rejected") ? DECISION_OPTIONS.get(decision.closed_reason === "approved" ? "uphold_report" : "uphold_decision") : restored ? "已驳回放弃并恢复跟踪" : ordinaryResolution ?? DECISION_OPTIONS.get(decision.closed_reason ?? "") ?? optionalReason(decision.closed_reason) })] }),
							decision.status === "resolved" && decision.resolved_by && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "处理人" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: agentOnly ? "授权核验主体" : "已记录，见开发者视图" })] }),
							decision.status === "resolved" && decision.decided_at && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "处理时间" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: formatDate$3(decision.decided_at) })] })
						]
					}),
					props.developerView && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
						style: detailGridStyle$3,
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "决定 ID" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: decision.decision_id }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "运行 ID" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: decision.run_id }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "节点运行 ID" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: decision.node_run_id }),
							decision.resolved_by && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "处理人标识" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: decision.resolved_by })] }),
							missing.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "缺少的字段" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: missing.join("、") })] })
						]
					}),
					decision.confirm_required && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: warningStyle,
						children: "处理前需要再次确认。"
					}),
					choices.some(({ value }) => value === "continue_tracking") && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$6,
						children: "继续跟踪会再观察这次执行 120 秒。"
					}),
					choices.some(({ value }) => value === "continue_wait") && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$6,
						children: "继续等待会延长这次执行的观察时间，最多可延长三次。"
					}),
					decision.kind === "abandon_ack" && choices.some(({ value }) => value === "rejected") && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$6,
						children: "驳回后继续观察原来的执行。"
					}),
					missing.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						style: mutedStyle$6,
						children: [
							"当前记录缺少：",
							missingFields(missing),
							"。"
						]
					}),
					!agentOnly && decision.status !== "resolved" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: actionStyle,
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
								style: toggleStyle$2,
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									type: "checkbox",
									"aria-label": "选择这条决定",
									checked: props.selected,
									onChange: props.onToggle,
									disabled: props.busy
								}), "选择这条决定"]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: props.onAcknowledge,
								disabled: !props.commands || props.busy,
								style: buttonStyle$5,
								children: "接手"
							}),
							branches.length > 0 && choices.some(({ value }) => value === "approved") && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
								style: toggleStyle$2,
								children: ["后续分支", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
									"aria-label": "后续分支",
									value: selectedBranch,
									onChange: (event) => setSelectedBranch(event.target.value),
									children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "",
										children: "请选择分支"
									}), branches.map((id) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: id,
										children: id
									}, id))]
								})]
							}),
							choices.some(({ value }) => value === "confirm_started_attested") && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
								style: toggleStyle$2,
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									type: "checkbox",
									checked: attest,
									disabled: !props.commands || props.busy,
									onChange: (event) => setAttestedRevision(event.target.checked ? decision.current_revision : null)
								}), "我亲眼看到这次执行已启动。签认只记录启动事实。"]
							}),
							choices.length > 0 ? choices.map(({ value, label, evidenceId }) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: () => props.onResolve(value, label, value === "approved" && branches.length ? selectedBranch : void 0, evidenceId),
								disabled: !props.commands || props.busy || value === "approved" && branches.length > 0 && !branches.includes(selectedBranch) || value === "confirm_started_attested" && !attest,
								style: buttonStyle$5,
								children: label
							}, value)) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								disabled: true,
								style: buttonStyle$5,
								children: "处理"
							})
						]
					}),
					props.confirmation,
					decision.status !== "resolved" && choices.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$6,
						children: optionalReason(decision.resolution_unavailable_reason, "当前没有可从这里执行的处理选项。")
					}),
					props.notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: props.notice.tone === "error" ? "alert" : "status",
						style: props.notice.tone === "error" ? errorTextStyle$3 : mutedStyle$6,
						children: props.notice.text
					})
				]
			});
		}
		/** 记忆审批卡：标题、首行、每条一行、两个按钮；技术编号只在开发者视图里出现。 */
		function MemoryApprovalCardView(props) {
			const { decision, card } = props;
			const [expanded, setExpanded] = (0, react.useState)(false);
			const missing = decision.projection_missing ?? [];
			const choices = decision.allowed_resolutions ?? [];
			const approve = card.buttons.find((button) => button.resolution === "approved" && choices.includes(button.resolution));
			const decline = card.buttons.find((button) => button.resolution === "rejected" && choices.includes(button.resolution));
			const incomplete = approve === void 0 || decline === void 0;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("article", {
				style: cardStyle$1,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: {
							display: "flex",
							flexWrap: "wrap",
							justifyContent: "space-between",
							gap: 8
						},
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
							style: cardTitleStyle$1,
							children: card.title
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", {
							style: {
								color: decision.status === "blocked_for_human" ? COLOR.warning : COLOR.inkMuted,
								fontSize: FONT_PX.helper
							},
							children: operatorStatus(decision.status)
						})]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$6,
						children: card.summary_line
					}),
					decision.expires_at && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						style: mutedStyle$6,
						children: [formatDate$3(decision.expires_at), " · 到期未批准，批次关闭。"]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
						style: listStyle$2,
						children: card.items.map((item, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", {
							style: itemStyle$1,
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
									style: itemTitleStyle$1,
									children: [
										"层：",
										item.layer,
										" · ",
										item.title
									]
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$6,
									children: item.digest
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
									style: mutedStyle$6,
									children: ["理由：", optionalReason(item.judge_reason)]
								})
							]
						}, `${item.layer}-${index}`))
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						onClick: () => setExpanded(!expanded),
						"aria-expanded": expanded,
						style: buttonStyle$5,
						children: card.expand_label
					}),
					expanded && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						style: detailStyle$1,
						children: card.items.map((item, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$6,
							children: [
								item.title,
								"：",
								optionalReason(item.judge_reason)
							]
						}, `why-${index}`))
					}),
					props.developerView && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
						style: detailGridStyle$3,
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "批次 ID" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: ["batch_id: ", card.batch_id ?? NOT_PROVIDED$1] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "评估 ID" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: ["evaluation_id: ", card.evaluation_id ?? NOT_PROVIDED$1] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "提议哈希" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: ["proposal_hash: ", card.proposal_hash ?? NOT_PROVIDED$1] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "决定 ID" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: ["decision_id: ", decision.decision_id] }),
							missing.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "缺少的字段" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: missing.join("、") })] })
						]
					}),
					incomplete && decision.resolution_unavailable_reason && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$6,
						children: operatorReason(decision.resolution_unavailable_reason)
					}),
					incomplete && missing.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						style: mutedStyle$6,
						children: [
							"当前记录缺少：",
							missingFields(missing),
							"。"
						]
					}),
					incomplete && missing.length === 0 && !decision.resolution_unavailable_reason && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$6,
						children: "当前没有完整的处理选项。"
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: actionStyle,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							onClick: () => {
								if (approve) props.onResolve(approve.resolution, approve.label);
							},
							disabled: !props.commands || props.busy || !approve,
							style: buttonStyle$5,
							children: approve?.label ?? "按这个方案写入"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							onClick: () => {
								if (decline) props.onResolve(decline.resolution, decline.label);
							},
							disabled: !props.commands || props.busy || !decline,
							style: buttonStyle$5,
							children: decline?.label ?? "这次不写"
						})]
					}),
					props.confirmation,
					props.notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: props.notice.tone === "error" ? "alert" : "status",
						style: props.notice.tone === "error" ? errorTextStyle$3 : mutedStyle$6,
						children: props.notice.text
					})
				]
			});
		}
		function formatDate$3(value) {
			const date = new Date(value);
			return Number.isNaN(date.valueOf()) ? "时间未知" : date.toLocaleString();
		}
		const cardTitleStyle$1 = {
			margin: 0,
			fontSize: 16
		};
		const mutedStyle$6 = {
			margin: "6px 0 0",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.body
		};
		const cardStyle$1 = {
			minWidth: 0,
			padding: "11px 13px",
			border: "1px solid #dde4ed",
			borderRadius: 9,
			background: COLOR.surface
		};
		const detailGridStyle$3 = {
			display: "grid",
			gridTemplateColumns: "max-content minmax(0, 1fr)",
			gap: "4px 10px",
			margin: "8px 0 0",
			fontSize: FONT_PX.body
		};
		const listStyle$2 = {
			display: "grid",
			gap: 10,
			margin: "12px 0 0",
			padding: 0,
			listStyle: "none"
		};
		const itemStyle$1 = {
			display: "grid",
			gap: 3,
			minWidth: 0
		};
		const itemTitleStyle$1 = {
			margin: 0,
			fontSize: FONT_PX.body
		};
		const detailStyle$1 = {
			margin: "8px 0 0",
			paddingLeft: 10,
			borderLeft: `2px solid ${COLOR.line}`
		};
		const buttonStyle$5 = {
			minHeight: 36,
			justifySelf: "start",
			padding: "0 12px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 7,
			background: COLOR.surface,
			color: COLOR.ink,
			fontSize: FONT_PX.menu,
			cursor: "pointer"
		};
		const errorStyle$3 = {
			display: "flex",
			justifyContent: "space-between",
			alignItems: "center",
			padding: 14,
			color: COLOR.danger,
			background: "#fff1f0",
			borderRadius: 8
		};
		const warningStyle = {
			padding: 11,
			borderRadius: 7,
			background: "#fff8e9",
			color: COLOR.warning,
			fontSize: FONT_PX.body
		};
		const toggleStyle$2 = {
			display: "inline-flex",
			alignItems: "center",
			gap: 6,
			fontSize: FONT_PX.body
		};
		const actionStyle = {
			display: "flex",
			flexWrap: "wrap",
			alignItems: "center",
			gap: 10,
			margin: "14px 0 0"
		};
		const errorTextStyle$3 = {
			margin: "8px 0 0",
			color: COLOR.danger,
			fontSize: FONT_PX.body
		};
		// src/client/panel/memory-panel.tsx
		/** 服务端没给的字段一律写明「服务端未提供」，面板不替它编。 */
		const NOT_PROVIDED = "服务端未提供";
		/** 批次状态 → 面板文案（规格 §9.9 的十二句）。 */
		function memoryCopy(batch) {
			if (batch.status === "applying" && batch.recovery_available) return {
				text: "写入结果待核对",
				tone: "wait"
			};
			switch (batch.status) {
				case "draft": return {
					text: "正在整理",
					tone: "neutral"
				};
				case "evaluating": return {
					text: "管家在看有没有重复",
					tone: "neutral"
				};
				case "awaiting_human": return {
					text: `有 ${batch.pending_count} 条等你确认`,
					tone: "wait"
				};
				case "approved": return {
					text: "开始写入",
					tone: "neutral"
				};
				case "applying": return {
					text: "正在写入",
					tone: "neutral"
				};
				case "applied": return {
					text: `写了 ${batch.applied_count} 条`,
					tone: "good"
				};
				case "empty": return {
					text: "这次没有需要写的",
					tone: "neutral"
				};
				case "blocked": return {
					text: "有 1 条拿不准，这次没写",
					tone: "warn"
				};
				case "declined": return {
					text: "你选择先不写",
					tone: "warn"
				};
				case "expired": return {
					text: "超过 24 小时没确认，这次没写",
					tone: "warn"
				};
				case "failed": return {
					text: "没写成：裁判没给出结果",
					tone: "warn"
				};
				case "indeterminate": return {
					text: "写到一半，等你确认",
					tone: "wait"
				};
				default: return {
					text: String(batch.status),
					tone: "neutral"
				};
			}
		}
		const NOT_WRITTEN_STATUSES = /* @__PURE__ */ new Set([
			"blocked",
			"declined",
			"expired",
			"failed",
			"indeterminate"
		]);
		const NEXT_STEP = {
			blocked: "可以再让管家判断一次这条要不要记。",
			declined: "想记的话，可以让管家改一版。",
			expired: "可以让管家重新整理一批，这次重新确认。",
			failed: "可以让管家改一版，或者再试一次。",
			indeterminate: "如果这次不想写了，可以放弃这次写入。"
		};
		const EMPTY_BATCHES = [];
		const RECOVERY_LABEL = {
			confirm_applied: "确认全部写入",
			confirm_not_applied: "确认未写入并重新执行",
			abandon: "放弃这次写入",
			acknowledge_abandon: "签认并结束"
		};
		function recoveryError(error) {
			if (error instanceof ApiError && ["RESOLUTION_NOT_AVAILABLE", "OPERATION_OUTCOME_UNKNOWN"].includes(error.code)) return error.message;
			return describeDecisionError(error);
		}
		function MemoryPanel(props) {
			const { api, recovery, memoryEnabled, proposalOnly } = props;
			const [source, setSource] = (0, react.useState)({
				api,
				recovery,
				memoryEnabled,
				proposalOnly,
				generation: 0
			});
			if (source.api !== api || source.recovery !== recovery || source.memoryEnabled !== memoryEnabled || source.proposalOnly !== proposalOnly) setSource({
				api,
				recovery,
				memoryEnabled,
				proposalOnly,
				generation: source.generation + 1
			});
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(MemoryPanelContent, { ...props }, source.generation);
		}
		function MemoryPanelContent({ api, recovery, memoryEnabled, proposalOnly, reloadVersion = 0, onSnapshot, onOpenDecisions, onOpenTasks }) {
			const [readResult, setReadResult] = (0, react.useState)(null);
			const [retry, setRetry] = (0, react.useState)(0);
			const [developerView, setDeveloperView] = (0, react.useState)(false);
			const [openedItem, setOpenedItem] = (0, react.useState)(null);
			const [notice, setNotice] = (0, react.useState)(null);
			const [busy, setBusy] = (0, react.useState)(false);
			const [proof, setProof] = (0, react.useState)(null);
			(0, react.useEffect)(() => {
				let current = true;
				const request = {
					retry,
					reloadVersion,
					onSnapshot
				};
				api.listMemoryBatches({}).then((result) => {
					if (!current) return;
					setReadResult({
						...request,
						status: "ready",
						batches: result.data.items
					});
					onSnapshot?.(result.as_of_seq);
				}).catch(() => {
					if (current) setReadResult({
						...request,
						status: "failed"
					});
				});
				return () => {
					current = false;
				};
			}, [
				api,
				retry,
				reloadVersion,
				onSnapshot
			]);
			const result = readResult?.retry === retry && readResult.reloadVersion === reloadVersion && readResult.onSnapshot === onSnapshot ? readResult : null;
			const batches = result?.status === "ready" ? result.batches : EMPTY_BATCHES;
			const readOnly = memoryEnabled === false;
			const latest = (0, react.useMemo)(() => {
				if (batches.length === 0) return void 0;
				return [...batches].sort((a, b) => (Date.parse(b.updated_at) || 0) - (Date.parse(a.updated_at) || 0) || b.created_at.localeCompare(a.created_at))[0];
			}, [batches]);
			const writtenItems = (0, react.useMemo)(() => batches.flatMap((batch) => batch.items.filter((item) => item.status === "applied").map((item) => ({
				item,
				key: JSON.stringify([batch.batch_id, item.client_key])
			}))), [batches]);
			const awaitingBatches = (0, react.useMemo)(() => batches.filter((batch) => batch.status === "awaiting_human"), [batches]);
			const pendingTotal = (0, react.useMemo)(() => awaitingBatches.reduce((sum, batch) => sum + batch.pending_count, 0), [awaitingBatches]);
			const notWrittenBatches = (0, react.useMemo)(() => batches.filter((batch) => NOT_WRITTEN_STATUSES.has(batch.status)), [batches]);
			async function inspectBatch(batch) {
				if (readOnly || !recovery) return;
				setBusy(true);
				setNotice(null);
				setProof(null);
				try {
					setProof(await recovery.inspect(batch.batch_id));
					setRetry((value) => value + 1);
				} catch (error) {
					setNotice({
						text: recoveryError(error),
						tone: "error"
					});
				} finally {
					setBusy(false);
				}
			}
			async function resolveRecovery(inspection, choice) {
				if (readOnly || !recovery) return;
				setBusy(true);
				setNotice(null);
				try {
					await recovery.resolve(inspection, choice);
					setProof(null);
					setNotice({
						text: choice === "abandon" ? "已放弃本次写入，请签认。" : "已记录恢复决定。",
						tone: "ok"
					});
					setRetry((value) => value + 1);
				} catch (error) {
					setProof(null);
					setNotice({
						text: recoveryError(error),
						tone: "error"
					});
				} finally {
					setBusy(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "memory-panel-title",
				className: "ops-page",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "ops-page-header",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", {
								id: "memory-panel-title",
								children: "记忆管理"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								className: "ops-meta",
								children: readOnly ? "当前宿主未启用记忆整理，页面为只读。连接正常时，仍可查看已有条目和历史整理结果。" : proposalOnly ? "当前工作区开放记忆提议和来源核对，写入尚未开放。" : memoryEnabled ? "把成功任务中值得复用的经验整理成条目，核对提议和来源后，再批准写入。" : "查看任务整理出的记忆条目、提议和来源。"
							}),
							latest && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								className: "ops-meta",
								children: ["最近批次 · ", batchStateText(latest, readOnly, proposalOnly)]
							})
						] }), result?.status === "ready" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
							style: toggleStyle$1,
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
								type: "checkbox",
								"aria-label": "开发者视图",
								checked: developerView,
								onChange: (event) => setDeveloperView(event.target.checked)
							}), "开发者视图"]
						})]
					}),
					result?.status === "ready" && notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: notice.tone === "error" ? "alert" : "status",
						style: notice.tone === "error" ? errorTextStyle$2 : mutedStyle$5,
						children: notice.text
					}),
					result?.status === "failed" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						role: "alert",
						style: errorStyle$2,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: ["记忆", COPY.queryFailed] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							onClick: () => setRetry((value) => value + 1),
							style: buttonStyle$4,
							children: COPY.retry
						})]
					}),
					!result && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: mutedStyle$5,
						children: "正在读取记忆…"
					}),
					result?.status === "ready" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "ops-context-grid",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
								"aria-labelledby": "memory-q1-title",
								style: blockStyle$1,
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
									id: "memory-q1-title",
									style: sectionTitleStyle$2,
									children: "已写入条目"
								}), writtenItems.length === 0 ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$5,
									children: "这次还没有记下东西。"
								}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
									style: listStyle$1,
									children: writtenItems.map(({ item, key }) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", {
										style: itemStyle,
										children: [
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
												style: itemTitleStyle,
												children: item.title
											}),
											/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
												style: mutedStyle$5,
												children: ["层：", item.layer]
											}),
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
												style: mutedStyle$5,
												children: item.digest
											}),
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
												type: "button",
												onClick: () => setOpenedItem(openedItem === key ? null : key),
												style: buttonStyle$4,
												children: openedItem === key ? "收起" : "展开看全文与来源"
											}),
											openedItem === key && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
												style: detailStyle,
												children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
													style: mutedStyle$5,
													children: ["全文：", item.content ?? NOT_PROVIDED]
												}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
													style: mutedStyle$5,
													children: ["来源：", item.source ?? NOT_PROVIDED]
												})]
											})
										]
									}, key))
								})]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
								"aria-labelledby": "memory-q2-title",
								style: blockStyle$1,
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
										id: "memory-q2-title",
										style: sectionTitleStyle$2,
										children: !readOnly && proposalOnly ? "提议与来源" : "审批与对账"
									}),
									awaitingBatches.length === 0 ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										style: mutedStyle$5,
										children: !readOnly && proposalOnly ? "没有可查看的提议" : "没有待确认"
									}) : /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										style: statusStyle,
										children: readOnly ? `有 ${pendingTotal} 条提议待确认，当前仅可查看。` : proposalOnly ? `有 ${pendingTotal} 条记忆提议可查看` : `有 ${pendingTotal} 条等你确认`
									}), !readOnly && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										onClick: () => onOpenDecisions?.(),
										disabled: !onOpenDecisions,
										style: buttonStyle$4,
										children: proposalOnly ? "查看提议" : "查看审批"
									})] }),
									!readOnly && batches.filter((batch) => batch.recovery_available).map((batch) => {
										const inspection = proof?.batch_id === batch.batch_id ? proof : null;
										return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
											style: itemStyle,
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
												disabled: !recovery || busy,
												onClick: () => {
													inspectBatch(batch);
												},
												style: buttonStyle$4,
												children: "核对记忆写入"
											}), inspection && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
												style: mutedStyle$5,
												children: [
													"已写入 ",
													inspection.written_count,
													" 条，未写入 ",
													inspection.missing_count,
													" 条。"
												]
											}), inspection.allowed_resolutions.map((choice) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
												disabled: busy,
												style: buttonStyle$4,
												onClick: () => {
													resolveRecovery(inspection, choice);
												},
												children: RECOVERY_LABEL[choice]
											}, choice))] })]
										}, batch.batch_id);
									})
								]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
								"aria-labelledby": "memory-q3-title",
								style: blockStyle$1,
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
									id: "memory-q3-title",
									style: sectionTitleStyle$2,
									children: "未写入条目"
								}), notWrittenBatches.length === 0 ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$5,
									children: "这次没有没写成的条目。"
								}) : notWrittenBatches.map((batch) => {
									const hideItems = batch.status === "indeterminate";
									const notWritten = batch.items.filter((item) => item.status !== "applied");
									return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
										style: itemStyle,
										children: [
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
												style: statusStyle,
												children: batchStateText(batch, readOnly)
											}),
											!readOnly && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
												style: mutedStyle$5,
												children: NEXT_STEP[batch.status] ?? "可以再让管家处理一次。"
											}),
											!hideItems && notWritten.map((item) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
												style: mutedStyle$5,
												children: [
													item.title,
													"：",
													item.judge_reason ?? "这次没有写进去"
												]
											}, item.client_key))
										]
									}, batch.batch_id);
								})]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
								"aria-labelledby": "memory-q4-title",
								style: blockStyle$1,
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
									id: "memory-q4-title",
									style: sectionTitleStyle$2,
									children: "整理状态"
								}), readOnly ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$5,
									children: latest ? "以上是最近一次整理的记录。" : "暂无整理记录。"
								}) : !latest ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$5,
									children: "要记录这次任务的经验，打开成功运行的「结果」，选择「整理为记忆」，写下希望保留的内容。"
								}), memoryEnabled && onOpenTasks && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									onClick: onOpenTasks,
									style: buttonStyle$4,
									children: "去任务中选择一次成功运行"
								})] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)(react_jsx_runtime.Fragment, { children: latest.status === "failed" || latest.status === "declined" ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$5,
									children: "让管家改一版"
								}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$5,
									children: "再试一次"
								})] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$5,
									children: latest.status === "awaiting_human" ? proposalOnly ? "可以查看提议及其来源" : "先确认要写哪几条" : "这次不用再记了"
								}) })]
							})
						]
					}),
					result?.status === "ready" && developerView && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dl", {
						style: detailGridStyle$2,
						children: batches.map((batch) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react.Fragment, { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "批次 ID" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: ["batch_id: ", batch.batch_id] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "评估 ID" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: ["evaluation_id: ", batch.evaluation_id ?? NOT_PROVIDED] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "提议哈希" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: ["proposal_hash: ", batch.proposal_hash ?? NOT_PROVIDED] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "决定 ID" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: ["decision_id: ", batch.decision_id ?? NOT_PROVIDED] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "任务 ID" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dd", { children: ["task_id: ", batch.task_id] })
						] }, batch.batch_id))
					})
				]
			});
		}
		function batchStateText(batch, readOnly, proposalOnly) {
			if (batch.status === "awaiting_human" && (readOnly || proposalOnly)) return `有 ${batch.pending_count} 条${readOnly ? "提议待确认" : "提议可查看"}`;
			if (readOnly) {
				if (batch.status === "approved") return "已批准，尚未写入";
				if (batch.status === "applying" || batch.status === "indeterminate") return "写入结果待核对";
			}
			return memoryCopy(batch).text;
		}
		const sectionTitleStyle$2 = {
			margin: 0,
			fontSize: 16
		};
		const mutedStyle$5 = {
			margin: "6px 0 0",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.body
		};
		const statusStyle = {
			margin: "8px 0 0",
			fontSize: FONT_PX.body
		};
		const blockStyle$1 = {
			display: "grid",
			gap: 5,
			padding: "11px 13px",
			border: "1px solid #dde4ed",
			borderRadius: 9,
			background: COLOR.surface
		};
		const listStyle$1 = {
			display: "grid",
			gap: 12,
			margin: 0,
			padding: 0,
			listStyle: "none"
		};
		const itemStyle = {
			display: "grid",
			gap: 4,
			minWidth: 0
		};
		const itemTitleStyle = {
			margin: 0,
			fontSize: FONT_PX.body
		};
		const detailStyle = {
			margin: "6px 0 0",
			paddingLeft: 10,
			borderLeft: `2px solid ${COLOR.line}`
		};
		const detailGridStyle$2 = {
			display: "grid",
			gridTemplateColumns: "150px minmax(0, 1fr)",
			gap: "8px 12px",
			margin: "0",
			fontSize: FONT_PX.body
		};
		const buttonStyle$4 = {
			minHeight: 36,
			justifySelf: "start",
			padding: "0 12px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 7,
			background: COLOR.surface,
			color: COLOR.ink,
			fontSize: FONT_PX.menu,
			cursor: "pointer"
		};
		const errorStyle$2 = {
			display: "flex",
			justifyContent: "space-between",
			alignItems: "center",
			padding: 14,
			color: COLOR.danger,
			background: "#fff1f0",
			borderRadius: 8
		};
		const errorTextStyle$2 = {
			margin: "8px 0 0",
			color: COLOR.danger,
			fontSize: FONT_PX.body
		};
		const toggleStyle$1 = {
			display: "inline-flex",
			alignItems: "center",
			gap: 6,
			fontSize: FONT_PX.body
		};
		// src/client/panel/effect-evidence-review.tsx
		function EffectEvidenceReview({ api, client, nodeId, onResolved }) {
			const [review, setReview] = (0, react.useState)(null);
			const [evidenceId, setEvidenceId] = (0, react.useState)("");
			const [notice, setNotice] = (0, react.useState)("");
			const [busy, setBusy] = (0, react.useState)(false);
			async function inspect() {
				setBusy(true);
				setNotice("");
				setReview(null);
				setEvidenceId("");
				try {
					setReview((await api.getEffectReview(nodeId)).data);
				} catch (error) {
					setNotice(error instanceof Error ? operatorReason(error.message) : "证据查询没有完成。");
				} finally {
					setBusy(false);
				}
			}
			async function confirm() {
				const evidence = review?.evidence.find((item) => item.evidence_id === evidenceId);
				if (!review || !evidence) return;
				const basis = evidence.verdict === "not_applied" ? {
					basis: "verified_none",
					stop_evidence: evidenceId
				} : {
					basis: "evidence",
					evidence_id: evidenceId
				};
				setBusy(true);
				try {
					await postWithOperationReceipt(client, "/v1/effects/resolve", "effect.resolve", {
						node_run_id: review.node_run_id,
						run_id: review.run_id,
						...basis,
						evidence_source: "run_tool",
						expected: { revision: review.revision },
						operation_id: newOperationId()
					});
					setReview(null);
					setNotice("核实结果已记录。");
					onResolved();
				} catch (error) {
					setReview(null);
					setNotice(error instanceof Error ? operatorReason(error.message) : "暂时无法确认核实结果。");
				} finally {
					setBusy(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-label": "核对系统证据",
				style: {
					display: "grid",
					gap: 7,
					marginTop: 10
				},
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						onClick: () => {
							inspect();
						},
						disabled: busy,
						style: control,
						children: "核对系统证据"
					}),
					review?.evidence.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: hint$1,
						children: "目前没有能用于这次核实的系统证据。"
					}),
					review?.evidence.map((item, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: hint$1,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
								type: "radio",
								name: `verify-${nodeId}`,
								value: item.evidence_id,
								checked: evidenceId === item.evidence_id,
								onChange: () => setEvidenceId(item.evidence_id),
								disabled: busy
							}),
							"证据 ",
							index + 1,
							" · ",
							item.verdict === "not_applied" ? "外部操作未发生" : "外部操作已发生",
							" · ",
							evidenceToolLabel(item.tool, "工具执行记录"),
							" · ",
							new Date(item.finished_at).toLocaleString()
						] }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看证据明细" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
							"工具 ",
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: item.tool }),
							" · 证据编号 ",
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: item.evidence_id })
						] })] })]
					}, item.evidence_id)),
					evidenceId && review && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
						style: control,
						disabled: busy,
						onClick: () => {
							confirm();
						},
						children: "用所选证据记录核实"
					}),
					notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: hint$1,
						role: "status",
						children: notice
					})
				]
			});
		}
		const control = {
			justifySelf: "start",
			minHeight: 33,
			padding: "0 9px",
			background: COLOR.surface,
			border: `1px solid ${COLOR.line}`,
			borderRadius: 6,
			color: COLOR.ink,
			cursor: "pointer",
			fontSize: FONT_PX.menu
		};
		const hint$1 = {
			margin: 0,
			color: COLOR.inkMuted,
			fontSize: FONT_PX.menu,
			overflowWrap: "anywhere"
		};
		// src/client/panel/unverified-effects.tsx
		const VERIFY_BANNER = "人工签认记录责任接管。外部操作是否发生仍以核验证据为准。";
		const EFFECT_LABEL = {
			pure: "只读",
			compensable: "可补偿",
			irreversible: "不可逆"
		};
		const STATUS_LABEL = {
			running: "正在做",
			completed: "已完成",
			failed: "失败",
			blocked: "卡住了",
			waiting: "等待中",
			cancelled: "已取消",
			indeterminate_side_effect: "外部操作结果待核验",
			abandoned_unverified: "已停止跟踪，外部操作待核验",
			failed_with_unverified: "执行失败，外部操作待核验"
		};
		const UNKNOWN$1 = "未知";
		function UnverifiedEffects({ api, reloadVersion = 0, onSnapshot, client, onOpenDecision }) {
			const [items, setItems] = (0, react.useState)([]);
			const [cursor, setCursor] = (0, react.useState)();
			const [nextCursor, setNextCursor] = (0, react.useState)(null);
			const [loading, setLoading] = (0, react.useState)(true);
			const [failed, setFailed] = (0, react.useState)(false);
			const [retry, setRetry] = (0, react.useState)(0);
			const lastReload = (0, react.useRef)(reloadVersion);
			const [developerView, setDeveloperView] = (0, react.useState)(false);
			(0, react.useEffect)(() => {
				if (lastReload.current !== reloadVersion) {
					lastReload.current = reloadVersion;
					setCursor(void 0);
				}
			}, [reloadVersion]);
			(0, react.useEffect)(() => {
				let current = true;
				setLoading(true);
				setFailed(false);
				api.listUnverifiedEffects({
					limit: 50,
					cursor
				}).then((result) => {
					if (!current) return;
					setItems((previous) => cursor ? [...previous, ...result.data.items] : result.data.items);
					setNextCursor(result.data.next_cursor);
					onSnapshot?.(result.as_of_seq);
				}).catch(() => {
					if (current) setFailed(true);
				}).finally(() => {
					if (current) setLoading(false);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				cursor,
				retry,
				reloadVersion,
				onSnapshot
			]);
			function refresh() {
				setCursor(void 0);
				setRetry((value) => value + 1);
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "unverified-title",
				className: "ops-page",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "ops-page-header",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", {
							id: "unverified-title",
							children: "操作核验"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							className: "ops-meta",
							children: VERIFY_BANNER
						})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "ops-actions",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
								style: toggleStyle,
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									type: "checkbox",
									"aria-label": "开发者视图",
									checked: developerView,
									onChange: (event) => setDeveloperView(event.target.checked)
								}), "开发者视图"]
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: refresh,
								disabled: loading,
								className: "ops-button",
								children: loading ? "正在更新…" : "刷新"
							})]
						})]
					}),
					failed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						role: "alert",
						style: errorStyle$1,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: COPY.queryFailed }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							onClick: refresh,
							style: buttonStyle$3,
							children: COPY.retry
						})]
					}),
					loading && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: mutedStyle$4,
						children: "正在读取待核实事项…"
					}),
					!loading && !failed && items.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						className: "ops-surface ops-empty",
						children: COPY.emptyEffects
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						className: "ops-card-grid",
						children: items.map((effect) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)(EffectCard, {
							effect,
							developerView,
							onOpenDecision,
							children: client && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(EffectEvidenceReview, {
								api,
								client,
								nodeId: effect.node_run_id,
								onResolved: refresh
							})
						}, effect.node_run_id))
					}),
					nextCursor && !failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						disabled: loading,
						onClick: () => setCursor(nextCursor),
						style: buttonStyle$3,
						children: "加载更多"
					})
				]
			});
		}
		function EffectCard({ effect, developerView, children, onOpenDecision }) {
			const level = EFFECT_LABEL[effect.side_effect_policy];
			const age = effect.age_seconds === null ? "时长未知" : `已等待 ${effect.age_seconds} 秒`;
			const decisionId = effect.decision_id;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("article", {
				style: cardStyle,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: {
							display: "flex",
							flexWrap: "wrap",
							justifyContent: "space-between",
							gap: 8
						},
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
							style: cardTitleStyle,
							children: effect.node_id || "未命名步骤"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", {
							style: {
								color: effect.side_effect_policy === "irreversible" ? COLOR.danger : COLOR.warning,
								fontSize: FONT_PX.helper
							},
							children: level ?? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(UnknownState, { value: effect.side_effect_policy })
						})]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
						style: detailGridStyle$1,
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "当前状态" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: STATUS_LABEL[effect.status] ?? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(UnknownState, { value: effect.status }) }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "核实状态" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: "未验证" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "已等待" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: effect.age_seconds === null ? "时长未知" : `${effect.age_seconds} 秒` }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "开始时间" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: effect.started_at ? formatDate$2(effect.started_at) : "没有记录开始时间" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "已确认停止时间" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: effect.stopped_at ? formatDate$2(effect.stopped_at) : UNKNOWN$1 }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "停止记录" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: effect.stop_evidence ? "已有停止证据" : "没有记录停止证据" }),
							developerView && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "节点运行 ID" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: effect.node_run_id })] })
						]
					}),
					decisionId && onOpenDecision && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						className: "ops-button",
						onClick: () => onOpenDecision(decisionId),
						children: "查看相关决定"
					}),
					children,
					developerView && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
							style: mutedStyle$4,
							children: [
								"状态代码 ",
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: effect.status }),
								" · 风险级别代码 ",
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: effect.side_effect_policy }),
								effect.stop_evidence && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [" · 停止证据 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: effect.stop_evidence })] })
							]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
							className: "ops-table-scroll",
							children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("table", {
								className: "ops-table",
								style: tableStyle,
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("thead", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", { children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
										scope: "col",
										children: "运行"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
										scope: "col",
										children: "节点"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
										scope: "col",
										children: "动作摘要"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
										scope: "col",
										children: "外部操作风险"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
										scope: "col",
										children: "停止时刻 stopped_at"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
										scope: "col",
										children: "最后已知阶段 last_known_phase"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
										scope: "col",
										children: "验证状态"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
										scope: "col",
										children: "证据"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
										scope: "col",
										children: "年龄"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
										scope: "col",
										children: "是否已解决 / 解决依据"
									})
								] }) }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("tbody", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", { children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: effect.run_id }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: effect.node_id || "未命名步骤" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: UNKNOWN$1 }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: level ?? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(UnknownState, { value: effect.side_effect_policy }) }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: effect.stopped_at ? formatDate$2(effect.stopped_at) : UNKNOWN$1 }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: effect.last_known_phase ?? UNKNOWN$1 }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: "未验证" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: effect.evidence ?? UNKNOWN$1 }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: age }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("td", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", { children: effect.resolved ? "是（已有处理记录）" : "否（尚无处理记录）" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: ["解决依据：", effect.resolution_basis ?? UNKNOWN$1] })] })
								] }) })]
							})
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							style: mutedStyle$4,
							children: "读模型未提供的字段显示「未知」。"
						})
					] })
				]
			});
		}
		function formatDate$2(value) {
			const date = new Date(value);
			return Number.isNaN(date.valueOf()) ? "时间未知" : date.toLocaleString();
		}
		const cardTitleStyle = {
			margin: 0,
			fontSize: 16
		};
		const mutedStyle$4 = {
			margin: "6px 0 0",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.body
		};
		const cardStyle = {
			minWidth: 0,
			padding: "11px 13px",
			border: "1px solid #dde4ed",
			borderRadius: 9,
			background: COLOR.surface
		};
		const detailGridStyle$1 = {
			display: "grid",
			gridTemplateColumns: "max-content minmax(0, 1fr)",
			gap: "4px 10px",
			margin: "8px 0 0",
			fontSize: FONT_PX.body
		};
		const buttonStyle$3 = {
			minHeight: 36,
			justifySelf: "start",
			padding: "0 12px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 7,
			background: COLOR.surface,
			color: COLOR.ink,
			fontSize: FONT_PX.menu,
			cursor: "pointer"
		};
		const errorStyle$1 = {
			display: "flex",
			justifyContent: "space-between",
			alignItems: "center",
			padding: 14,
			color: COLOR.danger,
			background: "#fff1f0",
			borderRadius: 8
		};
		const toggleStyle = {
			display: "inline-flex",
			alignItems: "center",
			gap: 6,
			fontSize: FONT_PX.body
		};
		const tableStyle = {
			width: "100%",
			margin: "16px 0 0",
			borderCollapse: "collapse",
			fontSize: FONT_PX.helper,
			textAlign: "left"
		};
		// src/client/panel/workflow-editor.tsx
		const DEFINITION_NOTE = "画布和智能体改的是同一份定义，保存都走 workflow.update";
		const CONFLICT_TEXT = "草稿已被别的修订改动，这次改动没有生效。你的草稿还在，请重新加载后再保存。";
		function WorkflowEditor({ commands, workflow, onRefresh, snapshotStale = false }) {
			const canWrite = usePanelCommandsEnabled(true) && !snapshotStale;
			const draft = workflow.draft;
			const draftRevision = draft?.revision ?? null;
			const definitionRef = (0, react.useRef)(null);
			definitionRef.current = draft?.definition ?? null;
			const syncedRef = (0, react.useRef)(null);
			const [text, setText] = (0, react.useState)("");
			const [dirty, setDirty] = (0, react.useState)(false);
			const dirtyRef = (0, react.useRef)(false);
			const [busy, setBusy] = (0, react.useState)(null);
			const [receipt, setReceipt] = (0, react.useState)(null);
			const [conflict, setConflict] = (0, react.useState)(null);
			const [commandError, setCommandError] = (0, react.useState)(null);
			const [structural, setStructural] = (0, react.useState)([]);
			const [quality, setQuality] = (0, react.useState)([]);
			const [parseError, setParseError] = (0, react.useState)(null);
			const [note, setNote] = (0, react.useState)("");
			const [tested, setTested] = (0, react.useState)(null);
			const workflowId = workflow.workflow.workflow_id;
			(0, react.useEffect)(() => {
				let active = true;
				setTested(null);
				if (draftRevision !== null) commands.getWorkflowTest?.(workflowId).then((result) => {
					if (active) setTested((previous) => previous ?? result);
				}).catch(() => {});
				return () => {
					active = false;
				};
			}, [
				commands,
				workflowId,
				draftRevision
			]);
			const parsed = (0, react.useMemo)(() => parseForCanvas(text), [text]);
			(0, react.useEffect)(() => {
				if (draftRevision === null) {
					dirtyRef.current = false;
					setDirty(false);
					syncedRef.current = null;
					setText("");
					return;
				}
				if (draftRevision === syncedRef.current) return;
				if (dirtyRef.current) {
					setConflict(CONFLICT_TEXT);
					return;
				}
				dirtyRef.current = false;
				setDirty(false);
				syncedRef.current = draftRevision;
				setText(formatDefinition(definitionRef.current));
			}, [draftRevision]);
			const structuralIssues = (0, react.useMemo)(() => mergeIssues(workflow.validation.errors, structural), [workflow.validation.errors, structural]);
			const qualityIssues = (0, react.useMemo)(() => mergeIssues(workflow.validation.warnings, quality), [workflow.validation.warnings, quality]);
			const changes = (0, react.useMemo)(() => describeChanges(workflow.current?.definition ?? null, draft?.definition ?? null), [workflow.current, draft]);
			const targetVersion = (workflow.workflow.current_version ?? 0) + 1;
			const validated = (tested?.revision === draftRevision && tested?.passed) === true;
			const criticalApprovals = workflow.critical_approval_count ?? 0;
			const publishBlocked = structuralIssues.length > 0 || commands.testWorkflow !== void 0 && (!validated || criticalApprovals > 0);
			async function quickTest() {
				if (!canWrite || !commands.testWorkflow || !draft || dirty || busy !== null) return;
				setBusy("testing");
				setCommandError(null);
				try {
					setTested(await commands.testWorkflow(workflowId, draft.revision));
					onRefresh();
				} catch (error) {
					setTested(null);
					reportError(error);
				} finally {
					setBusy(null);
				}
			}
			function markDirty(value) {
				dirtyRef.current = true;
				setDirty(true);
				setText(value);
				setParseError(null);
			}
			function parseDefinition() {
				const trimmed = text.trim();
				if (trimmed === "") return {
					ok: false,
					message: "流程定义不能为空"
				};
				let parsed;
				try {
					parsed = JSON.parse(trimmed);
				} catch (error) {
					return {
						ok: false,
						message: error instanceof Error ? error.message : "JSON 解析失败"
					};
				}
				if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {
					ok: false,
					message: "流程定义必须是一个 JSON 对象"
				};
				return {
					ok: true,
					value: parsed
				};
			}
			function reportError(error) {
				if (!(error instanceof ApiError)) {
					setCommandError("命令失败：未知的错误");
					return;
				}
				const details = error.details;
				const issues = Array.isArray(details?.errors) ? details.errors : [];
				if (error.code === "WORKFLOW_REVISION_CONFLICT" || error.code === "WORKFLOW_VERSION_CONFLICT" || error.code === "WORKFLOW_DRAFT_NOT_OPEN") {
					setConflict(`${CONFLICT_TEXT}（${error.code}）`);
					return;
				}
				if (issues.length > 0) setStructural(issues);
				setCommandError(`${error.code}：${error.message}`);
			}
			async function save() {
				if (!canWrite || !draft || busy !== null) return;
				const parsed = parseDefinition();
				if (!parsed.ok) {
					setParseError(parsed.message);
					return;
				}
				setBusy("saving");
				setConflict(null);
				setCommandError(null);
				setStructural([]);
				try {
					const outcome = await commands.updateWorkflowDraft({
						workflow_id: workflow.workflow.workflow_id,
						revision: draft.revision,
						definition: parsed.value
					});
					dirtyRef.current = false;
					setDirty(false);
					syncedRef.current = outcome.result.revision;
					setQuality(outcome.result.validation?.errors ?? []);
					setReceipt({
						title: "保存",
						summary: `新修订 ${outcome.result.revision}；${outcome.result.changed ? "定义已更新" : "定义没有变化"}`,
						outcome
					});
					onRefresh();
				} catch (error) {
					reportError(error);
				} finally {
					setBusy(null);
				}
			}
			async function publish() {
				if (!canWrite || !draft || busy !== null || dirty || publishBlocked) return;
				const trimmedNote = note.trim();
				if (trimmedNote.length < 1 || trimmedNote.length > 500) {
					setCommandError("发布说明要写 1–500 字");
					return;
				}
				setBusy("publishing");
				setConflict(null);
				setCommandError(null);
				try {
					const outcome = await commands.publishWorkflow({
						workflow_id: workflow.workflow.workflow_id,
						revision: draft.revision,
						base_version: workflow.workflow.current_version ?? 0,
						publish_note: trimmedNote
					});
					setNote("");
					setReceipt({
						title: "发布",
						summary: `已发布 v${outcome.result.version}`,
						outcome
					});
					onRefresh();
				} catch (error) {
					reportError(error);
				} finally {
					setBusy(null);
				}
			}
			async function openDraft() {
				if (!canWrite || busy !== null) return;
				setBusy("opening");
				setConflict(null);
				setCommandError(null);
				try {
					const outcome = await commands.openWorkflowDraft({
						workflow_id: workflow.workflow.workflow_id,
						base_version: workflow.workflow.current_version ?? 0
					});
					setReceipt({
						title: "打开草稿",
						summary: `草稿修订 ${outcome.result.revision}`,
						outcome
					});
					onRefresh();
				} catch (error) {
					reportError(error);
				} finally {
					setBusy(null);
				}
			}
			function reloadDraft() {
				dirtyRef.current = false;
				setDirty(false);
				syncedRef.current = null;
				setConflict(null);
				onRefresh();
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "workflow-editor-title",
				className: "ops-surface",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
					className: "ops-surface-head",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
						id: "workflow-editor-title",
						children: draft ? "草稿画布" : "版本编辑"
					}), draft && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						className: "ops-meta",
						children: [
							"修订 ",
							draft.revision,
							" · 基线 ",
							draft.based_on_version === null ? "尚未发布" : `v${draft.based_on_version}`,
							dirty && " · 有未保存的改动"
						]
					})] }), draft ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						className: "ops-button ops-button--primary",
						disabled: !canWrite || busy !== null,
						onClick: () => {
							save();
						},
						children: "保存草稿"
					}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						className: "ops-button ops-button--primary",
						disabled: !canWrite || busy !== null,
						onClick: () => {
							openDraft();
						},
						children: "打开草稿"
					})]
				}), (draft || receipt || conflict || commandError) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
					className: "ops-inset ops-stack",
					children: [
						draft && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowCanvasView, {
								definition: parsed.ok ? parsed.value : null,
								readOnly: false,
								onChange: (next) => markDirty(JSON.stringify(next, null, 2)),
								canvasId: "draft-canvas",
								inspectNodes: true,
								editRecoveryRetry: canWrite && busy === null,
								workspace: true
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
								"data-definition-drawer": "true",
								style: drawerStyle,
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", {
										style: sectionTitleStyle$1,
										children: "流程定义 JSON 抽屉"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
										htmlFor: "workflow-definition-editor",
										style: labelStyle$1,
										children: "流程定义 JSON"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
										id: "workflow-definition-editor",
										style: editorStyle,
										rows: 16,
										value: text,
										onChange: (event) => markDirty(event.target.value)
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										style: mutedStyle$3,
										children: DEFINITION_NOTE
									})
								]
							}),
							parseError && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								role: "alert",
								style: errorTextStyle$1,
								children: ["定义 JSON 解析失败：", parseError]
							})
						] }),
						(structuralIssues.length > 0 || qualityIssues.length > 0) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(ValidationGroups, {
							title: "结构校验",
							issues: structuralIssues,
							tone: "error",
							emptyText: "结构校验没有发现问题。"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ValidationGroups, {
							title: "计划质量意见",
							issues: qualityIssues,
							tone: "warning",
							emptyText: "计划质量意见：没有项目。"
						})] }),
						draft !== null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "ops-workflow-controls",
							children: [commands.testWorkflow && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
									style: sectionTitleStyle$1,
									children: "结构与模拟流转测试"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$3,
									children: "检查流程结构和工具声明，按节点依赖演练顺序。实际工具调用、Agent 执行和交付验收需要在正式运行中验证。"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									className: "ops-button",
									style: { marginTop: 7 },
									disabled: !canWrite || dirty || busy !== null,
									onClick: () => {
										quickTest();
									},
									children: busy === "testing" ? "正在测试…" : "运行结构与模拟流转测试"
								}),
								dirty ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "请先保存修改再测试。" }) : tested?.revision === draft.revision && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
									role: "status",
									children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
										tested.passed ? "结构与模拟流转测试通过" : "结构与模拟流转测试未通过",
										" · 修订 ",
										tested.revision,
										" · ",
										new Date(tested.tested_at).toLocaleString()
									] }), tested.passed ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ol", { children: tested.order.map((step) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", { children: step.title }, step.node_id)) }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: tested.issues.errors.map((item, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
										item.message,
										" ",
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: item.path })
									] }, index)) })]
								})
							] }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
									style: sectionTitleStyle$1,
									children: "发布"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PublishConfirm, {
									targetVersion,
									changes: changes.lines,
									addsIrreversible: changes.addsIrreversible,
									affectsOnlyNewRuns: true
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
									htmlFor: "workflow-publish-note",
									style: labelStyle$1,
									children: "发布说明"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									id: "workflow-publish-note",
									style: inputStyle,
									value: note,
									placeholder: "这次发布改了什么",
									onChange: (event) => setNote(event.target.value)
								}),
								structuralIssues.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$3,
									children: "发布前须修复结构错误。"
								}),
								commands.testWorkflow && !validated && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$3,
									children: "当前草稿须通过结构与模拟流转测试后才能发布。"
								}),
								commands.testWorkflow && criticalApprovals > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
									style: mutedStyle$3,
									children: [
										"仍有 ",
										criticalApprovals,
										" 项最高优先级审批待处理。"
									]
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									className: "ops-button ops-button--primary",
									style: { marginTop: 7 },
									disabled: !canWrite || busy !== null || dirty || publishBlocked,
									onClick: () => {
										publish();
									},
									children: "确认发布"
								})
							] })]
						}),
						conflict && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							role: "alert",
							style: conflictStyle,
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: conflict }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								style: buttonStyle$2,
								onClick: reloadDraft,
								children: "重新加载"
							})]
						}),
						commandError && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "alert",
							style: errorTextStyle$1,
							children: commandError
						}),
						receipt && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
							style: receiptStyle,
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", { children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("strong", { children: [receipt.title, "回执"] }),
								" · ",
								receipt.summary
							] }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
								style: detailGridStyle,
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "operation_id" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: receipt.outcome.operation_id }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "kind" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: receipt.outcome.kind }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "回执状态" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: receipt.outcome.receipt?.status ?? "未取到回执" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "重放" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: receipt.outcome.replayed ? "是（同一 operation_id 的原回执）" : "否" })
								]
							})]
						})
					]
				})]
			});
		}
		const GROUP_LABEL = {
			node: "节点",
			edge: "连线",
			workflow: "整体"
		};
		/** 检查面板按节点与连线分组（规格 8.5 行 657）。 */
		function groupIssues(issues) {
			const node = {
				kind: "node",
				items: []
			};
			const edge = {
				kind: "edge",
				items: []
			};
			const workflow = {
				kind: "workflow",
				items: []
			};
			for (const issue of issues) {
				const item = {
					id: issue.path || "流程",
					message: `${issue.code}：${issue.message}`
				};
				if (issue.path.startsWith("/nodes")) node.items.push(item);
				else if (issue.path.startsWith("/edges")) edge.items.push(item);
				else workflow.items.push(item);
			}
			return [
				node,
				edge,
				workflow
			].filter((group) => group.items.length > 0);
		}
		function ValidationGroups({ title, issues, tone, emptyText }) {
			const groups = groupIssues(issues);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				"data-validation-group": title,
				style: { marginTop: 16 },
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h3", {
						style: {
							...sectionTitleStyle$1,
							color: tone === "error" ? COLOR.danger : COLOR.warning
						},
						children: [
							title,
							"（",
							issues.length,
							"）"
						]
					}),
					issues.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: mutedStyle$3,
						children: emptyText
					}),
					groups.map((group) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h4", {
						style: groupTitleStyle,
						children: [
							GROUP_LABEL[group.kind],
							"（",
							group.items.length,
							"）"
						]
					}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: group.items.map((item, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: item.id }),
						" · ",
						item.message
					] }, `${item.id}-${index}`)) })] }, group.kind))
				]
			});
		}
		/** 发布确认写四件事：目标版本、改动、是否新增不可逆节点、只影响之后新建的 run。 */
		function PublishConfirm({ targetVersion, changes, addsIrreversible, affectsOnlyNewRuns }) {
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				style: confirmStyle,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "ops-actions",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("strong", { children: ["发布到 v", targetVersion] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
							className: "ops-meta",
							children: affectsOnlyNewRuns ? "只影响之后新建的运行" : "会影响进行中的运行"
						})]
					}),
					addsIrreversible && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						style: {
							color: COLOR.warning,
							margin: "4px 0"
						},
						children: "这次新增了不可逆节点"
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "版本变更" }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: changes.map((change) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", { children: change }, change)) }),
						!addsIrreversible && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "这次没有新增不可逆节点" })
					] })
				]
			});
		}
		function formatDefinition(definition) {
			if (definition === null || definition === void 0) return "";
			return JSON.stringify(definition, null, 2);
		}
		/** 画布要的是一份对象：抽屉里的文本还没成对象时，画布先不显示。 */
		function parseForCanvas(text) {
			const trimmed = text.trim();
			if (trimmed === "") return { ok: false };
			try {
				const parsed = JSON.parse(trimmed);
				if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { ok: false };
				return {
					ok: true,
					value: parsed
				};
			} catch {
				return { ok: false };
			}
		}
		function mergeIssues(base, extra) {
			const seen = /* @__PURE__ */ new Set();
			const merged = [];
			for (const issue of [...base, ...extra]) {
				const key = `${issue.path}|${issue.code}|${issue.message}`;
				if (seen.has(key)) continue;
				seen.add(key);
				merged.push(issue);
			}
			return merged;
		}
		function readNodes(definition) {
			if (!definition || typeof definition !== "object") return [];
			const nodes = definition.nodes;
			if (!Array.isArray(nodes)) return [];
			const out = [];
			for (const raw of nodes) {
				if (!raw || typeof raw !== "object") continue;
				const node = raw;
				out.push({
					node_id: typeof node.node_id === "string" ? node.node_id : "",
					irreversible: declaresIrreversible(node),
					fingerprint: JSON.stringify(node)
				});
			}
			return out;
		}
		/** 不可逆按节点契约里的声明算：contract/config 的 side_effect_policy = irreversible。 */
		function declaresIrreversible(node) {
			const contract = node.contract && typeof node.contract === "object" ? node.contract : {};
			const config = node.config && typeof node.config === "object" ? node.config : {};
			return (contract.side_effect_policy ?? config.side_effect_policy) === "irreversible";
		}
		function describeChanges(published, draft) {
			const before = readNodes(published);
			const after = readNodes(draft);
			const beforeById = new Map(before.map((node) => [node.node_id, node]));
			const afterById = new Map(after.map((node) => [node.node_id, node]));
			const lines = [];
			let addsIrreversible = false;
			for (const node of after) {
				const previous = beforeById.get(node.node_id);
				if (!previous) {
					lines.push(`新增节点 ${node.node_id}`);
					if (node.irreversible) addsIrreversible = true;
				} else if (previous.fingerprint !== node.fingerprint) {
					lines.push(`修改节点 ${node.node_id}`);
					if (node.irreversible && !previous.irreversible) addsIrreversible = true;
				}
			}
			for (const node of before) if (!afterById.has(node.node_id)) lines.push(`删除节点 ${node.node_id}`);
			if (lines.length === 0) lines.push("定义没有变化");
			return {
				lines,
				addsIrreversible
			};
		}
		const sectionTitleStyle$1 = {
			margin: 0,
			fontSize: 16
		};
		const drawerStyle = {
			padding: "7px 9px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 7,
			background: COLOR.surface
		};
		const groupTitleStyle = {
			margin: "10px 0 4px",
			fontSize: 14
		};
		const mutedStyle$3 = {
			margin: "6px 0 0",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.body
		};
		const labelStyle$1 = {
			display: "block",
			margin: "12px 0 4px",
			fontSize: FONT_PX.body
		};
		const editorStyle = {
			width: "100%",
			boxSizing: "border-box",
			minHeight: 220,
			padding: 12,
			borderRadius: 8,
			border: `1px solid ${COLOR.line}`,
			background: COLOR.canvas,
			color: COLOR.ink,
			fontSize: FONT_PX.helper,
			fontFamily: "monospace"
		};
		const inputStyle = {
			width: "100%",
			boxSizing: "border-box",
			minHeight: 34,
			padding: "0 8px",
			borderRadius: 7,
			border: `1px solid ${COLOR.line}`,
			background: COLOR.surface,
			color: COLOR.ink,
			fontSize: FONT_PX.body
		};
		const buttonStyle$2 = {
			minHeight: 36,
			justifySelf: "start",
			padding: "0 12px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 7,
			background: COLOR.surface,
			color: COLOR.ink,
			fontSize: FONT_PX.menu,
			cursor: "pointer"
		};
		const conflictStyle = {
			display: "flex",
			justifyContent: "space-between",
			alignItems: "center",
			gap: 12,
			padding: 14,
			color: COLOR.danger,
			background: "#fff1f0",
			borderRadius: 8
		};
		const errorTextStyle$1 = {
			margin: "10px 0 0",
			padding: 12,
			color: COLOR.danger,
			background: "#fff1f0",
			borderRadius: 8,
			fontSize: FONT_PX.body
		};
		const receiptStyle = {
			padding: "7px 9px",
			border: `1px solid ${COLOR.line}`,
			borderRadius: 7,
			background: COLOR.canvas
		};
		const confirmStyle = {
			margin: "5px 0 0",
			fontSize: FONT_PX.body
		};
		const detailGridStyle = {
			display: "grid",
			gridTemplateColumns: "150px minmax(0, 1fr)",
			gap: "8px 12px",
			margin: "12px 0 0",
			fontSize: FONT_PX.body
		};
		// src/client/panel/workflow-list.tsx
		function WorkflowStatusBadge({ status }) {
			if (status === "active") return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
				className: "ops-status",
				"data-tone": "on",
				children: "已启用"
			});
			if (status === "disabled" || status === "archived") return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
				className: "ops-status",
				"data-tone": "off",
				children: "已停用"
			});
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(UnknownState, { value: status });
		}
		function WorkflowListScreen({ api, onSelectWorkflow, reloadVersion = 0, onSnapshot, commands = null }) {
			const [items, setItems] = (0, react.useState)([]);
			const [cursor, setCursor] = (0, react.useState)();
			const [nextCursor, setNextCursor] = (0, react.useState)(null);
			const [loading, setLoading] = (0, react.useState)(true);
			const [failed, setFailed] = (0, react.useState)(false);
			const [retry, setRetry] = (0, react.useState)(0);
			const lastReloadVersion = (0, react.useRef)(reloadVersion);
			const [newName, setNewName] = (0, react.useState)("");
			const [creating, setCreating] = (0, react.useState)(false);
			const [createError, setCreateError] = (0, react.useState)(null);
			const [creationMode, setCreationMode] = (0, react.useState)(null);
			const [templates, setTemplates] = (0, react.useState)([]);
			const [templateError, setTemplateError] = (0, react.useState)(null);
			const [templateBusy, setTemplateBusy] = (0, react.useState)(false);
			const [uploadedTemplate, setUploadedTemplate] = (0, react.useState)(null);
			const [uploadedName, setUploadedName] = (0, react.useState)("");
			const [uploadResult, setUploadResult] = (0, react.useState)(null);
			const [uploadBusy, setUploadBusy] = (0, react.useState)(false);
			const [uploadError, setUploadError] = (0, react.useState)(null);
			const [search, setSearch] = (0, react.useState)("");
			const [query, setQuery] = (0, react.useState)("");
			const [status, setStatus] = (0, react.useState)("");
			(0, react.useEffect)(() => {
				if (lastReloadVersion.current === reloadVersion) return;
				lastReloadVersion.current = reloadVersion;
				setItems([]);
				setCursor(void 0);
				setNextCursor(null);
				setRetry((value) => value + 1);
			}, [reloadVersion]);
			(0, react.useEffect)(() => {
				let current = true;
				setLoading(true);
				setFailed(false);
				api.listWorkflows({
					limit: 50,
					cursor,
					query: query || void 0,
					status: status || void 0
				}).then((result) => {
					if (!current) return;
					setItems((previous) => cursor ? [...previous, ...result.data.items] : result.data.items);
					setNextCursor(result.data.next_cursor);
					onSnapshot?.(result.as_of_seq);
				}).catch(() => {
					if (current) setFailed(true);
				}).finally(() => {
					if (current) setLoading(false);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				cursor,
				retry,
				onSnapshot,
				query,
				status
			]);
			function refresh() {
				setCursor(void 0);
				setRetry((value) => value + 1);
			}
			(0, react.useEffect)(() => {
				if (creationMode !== "template" || !api.listWorkflowTemplates) return;
				let current = true;
				setTemplateError(null);
				api.listWorkflowTemplates().then((result) => {
					if (current) setTemplates(result.data.items ?? []);
				}).catch((error) => {
					if (current) setTemplateError(error instanceof ApiError ? `${error.code}：${error.message}` : "模板读取失败");
				});
				return () => {
					current = false;
				};
			}, [api, creationMode]);
			/** 模板先编译成标准 workflow 文档，再走既有的创建命令，避免两套数据格式。 */
			async function createFromTemplate(source) {
				if (!commands || templateBusy) return;
				setTemplateBusy(true);
				setTemplateError(null);
				try {
					const compiled = await commands.compileWorkflowTemplate(source);
					const outcome = await commands.createWorkflow({
						workflow_id: newWorkflowId(),
						name: compiled.definition.name,
						definition: compiled.definition
					});
					setNewName("");
					openNewDraft(outcome.result.workflow_id);
				} catch (error) {
					setTemplateError(error instanceof ApiError ? `${error.code}：${error.message}` : "模板创建失败");
				} finally {
					setTemplateBusy(false);
				}
			}
			/** 上传先做本地结构与文件检查，再交给服务端做完整校验；确认前不创建。 */
			async function validateTemplateUpload(file) {
				if (!file || !commands) return;
				setUploadBusy(true);
				setUploadError(null);
				setTemplateError(null);
				setUploadResult(null);
				setUploadedTemplate(null);
				setUploadedName(file.name || "自定义模板");
				try {
					const localErrors = [];
					if (!/\.json$/i.test(file.name) && file.type !== "application/json") localErrors.push("请选择 .json 文件");
					if (file.size === 0) localErrors.push("模板文件为空");
					if (file.size > TEMPLATE_UPLOAD_MAX_BYTES) localErrors.push(`模板文件不能超过 ${TEMPLATE_UPLOAD_MAX_BYTES / 1024} KB`);
					let parsed = null;
					if (localErrors.length === 0) {
						const raw = await file.text();
						try {
							parsed = JSON.parse(raw);
						} catch (error) {
							localErrors.push(formatJsonParseError(error, raw));
						}
					}
					if (localErrors.length === 0) localErrors.push(...checkTemplateShape(parsed));
					if (localErrors.length > 0) {
						setUploadResult({
							ok: false,
							errors: localErrors.map((message) => ({
								path: "",
								code: "FRONTEND_VALIDATION",
								message
							})),
							warnings: []
						});
						return;
					}
					const result = await commands.validateWorkflowTemplate({
						template: parsed,
						...newName.trim() ? { name: newName.trim() } : {}
					});
					setUploadedTemplate(parsed);
					setUploadResult(result);
				} catch (error) {
					setUploadError(error instanceof ApiError ? `${error.code}：${error.message}` : "模板校验失败，请稍后重试");
				} finally {
					setUploadBusy(false);
				}
			}
			async function createUploadedTemplate() {
				if (!uploadedTemplate) return;
				await createFromTemplate({
					template: uploadedTemplate,
					...newName.trim() ? { name: newName.trim() } : {}
				});
				setUploadedTemplate(null);
				setUploadResult(null);
				setUploadedName("");
			}
			async function exportTemplate(templateId) {
				setTemplateError(null);
				try {
					const result = await api.getWorkflowTemplate(templateId);
					const blob = new Blob([JSON.stringify(result.data, null, 2)], { type: "application/json" });
					const url = URL.createObjectURL(blob);
					const anchor = document.createElement("a");
					anchor.href = url;
					anchor.download = templateId + ".json";
					anchor.click();
					URL.revokeObjectURL(url);
				} catch (error) {
					setTemplateError(error instanceof ApiError ? `${error.code}：${error.message}` : "模板导出失败");
				}
			}
			function openNewDraft(id) {
				setCreationMode(null);
				refresh();
				onSelectWorkflow(id);
			}
			/** 新建流程走控制面命令：先建工作流与 revision=1 的草稿，编辑与发布在详情页做。 */
			async function createFlow() {
				if (!commands || creating) return;
				const name = newName.trim();
				if (name.length < 1 || name.length > 80) {
					setCreateError("流程名必须是 1–80 字");
					return;
				}
				setCreating(true);
				setCreateError(null);
				try {
					const outcome = await commands.createWorkflow({
						workflow_id: newWorkflowId(),
						name,
						definition: starterDefinition(name)
					});
					setNewName("");
					openNewDraft(outcome.result.workflow_id);
				} catch (error) {
					setCreateError(error instanceof ApiError ? `${error.code}：${error.message}` : "新建流程失败");
				} finally {
					setCreating(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "workflow-list-title",
				className: "ops-page",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "ops-page-header",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", {
							id: "workflow-list-title",
							children: "Workflow 管理"
						}), !commands && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							className: "ops-meta",
							children: "只读访问"
						})] }), commands && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "ops-actions",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
								className: "ops-button",
								"aria-expanded": creationMode === "template",
								onClick: () => setCreationMode((current) => current === "template" ? null : "template"),
								children: "从模板新建"
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
								className: "ops-button",
								"aria-expanded": creationMode === "generate",
								onClick: () => setCreationMode((current) => current === "generate" ? null : "generate"),
								children: "自然语言生成"
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
								className: "ops-button ops-button--primary",
								"aria-expanded": creationMode === "manual",
								onClick: () => setCreationMode((current) => current === "manual" ? null : "manual"),
								children: "新建 Workflow"
							})]
						})]
					}),
					commands && creationMode === "generate" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(GenerateWorkflow, {
						commands,
						onCreated: openNewDraft
					}),
					commands && creationMode === "manual" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
						className: "ops-surface ops-entry-form",
						"aria-label": "新建 Workflow",
						onSubmit: (event) => {
							event.preventDefault();
							createFlow();
						},
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
								htmlFor: "workflow-new-name",
								children: "流程名称"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "ops-actions",
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									id: "workflow-new-name",
									className: "ops-field",
									value: newName,
									maxLength: 80,
									onChange: (event) => setNewName(event.target.value)
								}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
									type: "submit",
									className: "ops-button ops-button--primary",
									disabled: creating || !newName.trim(),
									children: creating ? "正在新建…" : "创建草稿"
								})]
							}),
							createError && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								role: "alert",
								style: errorTextStyle,
								children: createError
							})
						]
					}),
					commands && creationMode === "template" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
						className: "ops-surface ops-entry-form",
						"aria-label": "从模板新建 Workflow",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
								htmlFor: "workflow-template-name",
								children: "流程名称（可选，默认使用模板名）"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
								id: "workflow-template-name",
								className: "ops-field",
								value: newName,
								maxLength: 80,
								onChange: (event) => setNewName(event.target.value)
							}),
							templateError && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								role: "alert",
								style: errorTextStyle,
								children: templateError
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
								className: "ops-card-grid",
								children: templates.map((template) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("article", {
									className: "ops-surface ops-inset",
									children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
											className: "ops-actions",
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
												style: {
													margin: 0,
													fontSize: FONT_PX.section
												},
												children: template.name
											}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
												className: "ops-status",
												"data-tone": template.advisory_only ? "off" : "info",
												children: template.advisory_only ? "只分析建议" : "标准执行"
											})]
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
											className: "ops-meta",
											children: template.description
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
											className: "ops-meta",
											children: `${template.category} · ${template.node_count} 个任务 · ${template.agent_count} 个角色`
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
											className: "ops-actions",
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
												className: "ops-button ops-button--primary",
												disabled: templateBusy,
												onClick: () => createFromTemplate({
													template_id: template.template_id,
													...newName.trim() ? { name: newName.trim() } : {}
												}),
												children: templateBusy ? "正在创建…" : "用此模板创建"
											}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
												className: "ops-button",
												disabled: templateBusy,
												onClick: () => exportTemplate(template.template_id),
												children: "导出 JSON"
											})]
										})
									]
								}, template.template_id))
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "ops-surface ops-inset",
								style: {
									display: "grid",
									gap: 7
								},
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
										style: {
											margin: 0,
											fontSize: FONT_PX.section
										},
										children: "上传自定义模板"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										id: "workflow-template-upload-hint",
										className: "ops-meta",
										style: { margin: 0 },
										children: "支持 .json 文件，最大 256 KB。上传后先校验，通过后才会创建草稿。"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
										className: "ops-actions",
										children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
											htmlFor: "workflow-template-upload",
											style: { fontWeight: 600 },
											children: "选择模板文件"
										}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
											id: "workflow-template-upload",
											type: "file",
											accept: "application/json,.json",
											disabled: uploadBusy || templateBusy,
											"aria-describedby": "workflow-template-upload-hint",
											"aria-invalid": uploadResult && !uploadResult.ok ? "true" : void 0,
											onChange: (event) => {
												validateTemplateUpload(event.target.files?.[0]);
												event.target.value = "";
											}
										})]
									}),
									uploadedName && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										className: "ops-meta",
										style: { margin: 0 },
										children: `已选择：${uploadedName}`
									}),
									uploadBusy && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										className: "ops-meta",
										style: { margin: 0 },
										role: "status",
										"aria-live": "polite",
										children: "正在校验模板…"
									}),
									uploadError && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										role: "alert",
										style: errorTextStyle,
										children: uploadError
									}),
									uploadResult && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
										style: uploadResultStyle,
										children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
											id: "workflow-template-upload-status",
											role: "status",
											"aria-live": "polite",
											style: {
												margin: 0,
												display: "flex",
												gap: 7,
												alignItems: "center",
												flexWrap: "wrap"
											},
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
												className: "ops-status",
												"data-tone": uploadResult.ok ? "on" : "off",
												children: uploadResult.ok ? "校验通过" : `校验未通过（${uploadResult.errors.length} 项）`
											}), uploadResult.ok && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
												className: "ops-meta",
												style: { margin: 0 },
												children: `${uploadResult.template?.name ?? "自定义模板"} · ${uploadResult.definition?.nodes?.length ?? 0} 个节点`
											})]
										}),
										uploadResult.ok ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
											className: "ops-actions",
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
												className: "ops-button ops-button--primary",
												disabled: templateBusy || uploadBusy,
												onClick: createUploadedTemplate,
												children: templateBusy ? "正在创建…" : "用此模板创建"
											}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
												className: "ops-meta",
												style: { margin: 0 },
												children: "创建后仍可在编辑器里继续调整。"
											})]
										}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
											style: uploadErrorListStyle,
											children: uploadResult.errors.slice(0, 20).map((error, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", {
												children: `${error.path ? error.path + "：" : ""}${error.message}`
											}, `${error.path || "local"}-${index}`))
										}),
										!uploadResult.ok && uploadResult.errors.length > 20 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
											className: "ops-meta",
											style: { margin: 0 },
											children: `仅显示前 20 项，还有 ${uploadResult.errors.length - 20} 项未显示。`
										}),
										uploadResult.warnings?.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", {
												className: "ops-meta",
												children: ["校验提示（", uploadResult.warnings.length, "）"]
											}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
												style: uploadWarningListStyle,
												children: uploadResult.warnings.slice(0, 20).map((warning, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", {
													children: `${warning.path ? warning.path + "：" : ""}${warning.message}`
												}, index))
											})]
										})]
									})
								]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								className: "ops-meta",
								children: "模板支持自定义 Agent 角色、系统提示词、验收标准、知识库、任务依赖和节点模型档位；导入后仍可在编辑器里继续调整。安全事件响应模板默认只做分析和建议，不会执行封禁、删除或隔离。"
							})
						]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "ops-surface",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "ops-surface-head ops-toolbar",
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
									role: "search",
									className: "ops-actions",
									onSubmit: (event) => {
										event.preventDefault();
										setQuery(search.trim());
										setItems([]);
										setNextCursor(null);
										refresh();
									},
									children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
											"aria-label": "搜索 Workflow",
											className: "ops-field",
											value: search,
											maxLength: 200,
											placeholder: "搜索名称或 ID",
											onChange: (event) => setSearch(event.target.value)
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
											type: "submit",
											className: "ops-button",
											children: "搜索"
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
											"aria-label": "启用状态",
											className: "ops-field",
											value: status,
											onChange: (event) => {
												setStatus(event.target.value);
												setItems([]);
												setNextCursor(null);
												refresh();
											},
											children: [
												/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
													value: "",
													children: "全部状态"
												}),
												/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
													value: "active",
													children: "已启用"
												}),
												/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
													value: "disabled",
													children: "已停用"
												})
											]
										})
									]
								}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									onClick: refresh,
									disabled: loading,
									className: "ops-button",
									children: loading ? "正在更新…" : "刷新"
								})]
							}),
							failed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								role: "alert",
								className: "ops-empty",
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [COPY.queryFailed, " "] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									onClick: refresh,
									className: "ops-button",
									children: COPY.retry
								})]
							}),
							loading && items.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								role: "status",
								className: "ops-empty",
								children: "正在读取流程…"
							}),
							!loading && !failed && items.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								className: "ops-empty",
								children: query || status ? "没有符合条件的 Workflow。" : "当前没有已保存的流程"
							}),
							items.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
								className: "ops-table-scroll",
								children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("table", {
									className: "ops-table ops-table--wide",
									"aria-label": "Workflow 列表",
									children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("thead", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", { children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
											scope: "col",
											children: "名称"
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
											scope: "col",
											children: "启用状态"
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
											scope: "col",
											children: "版本"
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
											scope: "col",
											children: "步骤"
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
											scope: "col",
											children: "活跃任务"
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
											scope: "col",
											children: "最近发布"
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", {
											scope: "col",
											children: "操作"
										})
									] }) }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("tbody", { children: items.map((workflow) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", { children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("td", { children: [
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: workflow.name }),
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
												className: "ops-meta",
												children: workflow.scope === "task_local" ? "任务专属" : workflow.scope === "reusable" ? "可复用" : /* @__PURE__ */ (0, react_jsx_runtime.jsx)(UnknownState, { value: workflow.scope })
											}),
											/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看登记明细" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
												"流程编号 ",
												/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: workflow.workflow_id }),
												" · 范围 ",
												/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: workflow.scope }),
												" · 状态 ",
												/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: workflow.status })
											] })] })
										] }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowStatusBadge, { status: workflow.status }) }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("td", { children: [workflow.current_version === null ? "未发布" : `v${workflow.current_version}`, workflow.has_draft && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
											className: "ops-meta",
											children: ["草稿 · 修订 ", workflow.draft_revision ?? "未知"]
										})] }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", {
											className: "ops-number",
											children: workflow.node_count ?? "未知"
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", {
											className: "ops-number",
											children: workflow.active_task_count
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: workflow.published_at ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", {
											dateTime: workflow.published_at,
											children: formatDate$1(workflow.published_at)
										}) : "—" }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
											type: "button",
											onClick: () => onSelectWorkflow(workflow.workflow_id),
											className: "ops-button ops-button--link",
											children: "查看流程"
										}) })
									] }, workflow.workflow_id)) })]
								})
							}),
							nextCursor && !failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
								className: "ops-inset",
								children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									disabled: loading,
									onClick: () => setCursor(nextCursor),
									className: "ops-button",
									children: "加载更多"
								})
							})
						]
					})
				]
			});
		}
		function GenerateWorkflow({ commands, onCreated }) {
			const [enabled, setEnabled] = (0, react.useState)(false);
			const [goal, setGoal] = (0, react.useState)("");
			const [draft, setDraft] = (0, react.useState)();
			const [busy, setBusy] = (0, react.useState)();
			const [error, setError] = (0, react.useState)("");
			(0, react.useEffect)(() => {
				let active = true;
				commands.canGenerateWorkflow?.().then((value) => {
					if (active) setEnabled(value);
				}).catch(() => {});
				return () => {
					active = false;
				};
			}, [commands]);
			async function generate() {
				if (!commands.generateWorkflow || !goal.trim() || busy) return;
				setBusy("generating");
				setError("");
				setDraft(void 0);
				try {
					setDraft(await commands.generateWorkflow(goal.trim()));
				} catch (cause) {
					setError(cause instanceof Error ? cause.message : "生成草稿失败");
				} finally {
					setBusy(void 0);
				}
			}
			async function create() {
				if (!draft || busy) return;
				setBusy("saving");
				setError("");
				try {
					onCreated((await commands.createWorkflow({
						workflow_id: newWorkflowId(),
						name: draft.name,
						definition: draft
					})).result.workflow_id);
				} catch (cause) {
					setError(cause instanceof Error ? cause.message : "保存草稿失败");
				} finally {
					setBusy(void 0);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "ops-surface ops-inset",
				"aria-label": "从自然语言生成 Workflow",
				children: [enabled ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
					className: "ops-entry-form",
					style: { padding: 0 },
					onSubmit: (event) => {
						event.preventDefault();
						generate();
					},
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
							htmlFor: "workflow-generate-goal",
							children: "目标、步骤和验收要求"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
							id: "workflow-generate-goal",
							value: goal,
							rows: 3,
							maxLength: 2e3,
							className: "ops-field",
							onChange: (event) => {
								setGoal(event.target.value);
								setDraft(void 0);
							},
							disabled: !!busy
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
							type: "submit",
							className: "ops-button ops-button--primary",
							disabled: !!busy || !goal.trim(),
							children: busy === "generating" ? "正在生成…" : "生成草稿"
						})
					]
				}), draft && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", { children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", { children: draft.name }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
						draft.nodes.length,
						" 个节点 · ",
						draft.edges.length,
						" 条依赖"
					] }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowCanvasView, {
						definition: draft,
						readOnly: true,
						runtimeCompact: true,
						inspectNodes: true,
						canvasId: "generated-workflow-canvas"
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
						type: "button",
						disabled: !!busy,
						onClick: () => {
							create();
						},
						children: "确认保存草稿"
					})
				] })] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					style: mutedStyle$2,
					children: "启用宿主的 Workflow 生成能力后，可以从这里生成草稿。"
				}), error && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					role: "alert",
					style: errorTextStyle,
					children: error
				})]
			});
		}
		/** 上传前的本地检查：只拦明显无效的文件，完整语义仍由服务端校验。 */
		const TEMPLATE_UPLOAD_MAX_BYTES = 256 * 1024;
		function formatJsonParseError(error, raw) {
			const message = error instanceof Error ? error.message : String(error);
			const match = /position (\d+)/.exec(message);
			if (!match || typeof raw !== "string") return `JSON 格式错误：${message}`;
			const position = Math.min(Number(match[1]), raw.length);
			const before = raw.slice(0, position);
			const line = before.split("\n").length;
			const column = position - before.lastIndexOf("\n");
			return `JSON 格式错误：第 ${line} 行第 ${column} 列附近`;
		}
		function checkTemplateShape(value) {
			const errors = [];
			if (value === null || typeof value !== "object" || Array.isArray(value)) return ["模板必须是 JSON 对象"];
			if (value.schema_version !== "1") errors.push("schema_version 必须是 \"1\"");
			if (typeof value.template_id !== "string" || value.template_id.trim() === "") errors.push("缺少 template_id");
			if (typeof value.name !== "string" || value.name.trim() === "") errors.push("缺少 name");
			if (!Array.isArray(value.nodes) || value.nodes.length === 0) errors.push("nodes 必须是非空数组");
			if (value.agents !== void 0 && !Array.isArray(value.agents)) errors.push("agents 必须是数组");
			if (value.knowledge_base !== void 0 && !Array.isArray(value.knowledge_base)) errors.push("knowledge_base 必须是数组");
			if (value.execution_policy !== void 0 && value.execution_policy !== "standard" && value.execution_policy !== "advisory_only") errors.push("execution_policy 只能是 standard 或 advisory_only");
			return errors;
		}
		/** 新建流程的初始定义：一个 trigger 节点；名字必须与工作流名一致，否则保存层会拒。 */
		function starterDefinition(name) {
			return {
				name,
				nodes: [{
					node_id: "trigger",
					type: "trigger",
					title: "开始",
					position: {
						x: 0,
						y: 0
					}
				}],
				edges: []
			};
		}
		function newWorkflowId() {
			return `wf-${Date.now().toString(36)}-${Math.floor(Math.random() * 1048576).toString(36).padStart(4, "0")}`;
		}
		function formatDate$1(value) {
			const date = new Date(value);
			return Number.isNaN(date.valueOf()) ? "时间未知" : date.toLocaleString();
		}
		const mutedStyle$2 = {
			margin: "6px 0 0",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.body
		};
		const errorTextStyle = {
			margin: "10px 0 0",
			padding: 12,
			color: COLOR.danger,
			background: "#fff1f0",
			borderRadius: 8,
			fontSize: FONT_PX.body
		};
		const uploadResultStyle = {
			display: "grid",
			gap: 6,
			marginTop: 2,
			padding: "10px 12px",
			border: "1px solid " + COLOR.line,
			borderRadius: 8,
			background: COLOR.sidebar
		};
		const uploadErrorListStyle = {
			margin: "4px 0 0",
			paddingLeft: 20,
			color: COLOR.danger,
			fontSize: FONT_PX.body,
			maxHeight: 220,
			overflow: "auto"
		};
		const uploadWarningListStyle = {
			margin: "4px 0 0",
			paddingLeft: 20,
			color: COLOR.warning,
			fontSize: FONT_PX.helper
		};
		// src/client/panel/workflow-canvas.tsx
		function WorkflowCanvas({ api, workflowId, onBack, onOpenRun, reloadVersion = 0, onSnapshot, commands = null }) {
			const [workflow, setWorkflow] = (0, react.useState)(null);
			const [loading, setLoading] = (0, react.useState)(true);
			const [failed, setFailed] = (0, react.useState)(false);
			const [retry, setRetry] = (0, react.useState)(0);
			const [view, setView] = (0, react.useState)("design");
			const [confirmStatus, setConfirmStatus] = (0, react.useState)(false);
			const [statusBusy, setStatusBusy] = (0, react.useState)(false);
			const [statusProblem, setStatusProblem] = (0, react.useState)("");
			(0, react.useEffect)(() => {
				let current = true;
				setLoading(true);
				setFailed(false);
				api.getWorkflow(workflowId).then((result) => {
					if (!current) return;
					setWorkflow(result.data);
					onSnapshot?.(result.as_of_seq);
				}).catch(() => {
					if (current) setFailed(true);
				}).finally(() => {
					if (current) setLoading(false);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				workflowId,
				retry,
				reloadVersion,
				onSnapshot
			]);
			const status = workflow?.workflow.status;
			const changeable = status === "active" || status === "disabled";
			const action = status === "active" ? "停用" : "启用";
			const reload = () => setRetry((value) => value + 1);
			async function changeStatus() {
				if (!commands?.setWorkflowStatus || !changeable || statusBusy || failed) return;
				setStatusBusy(true);
				setStatusProblem("");
				try {
					await commands.setWorkflowStatus({
						workflow_id: workflowId,
						status: status === "active" ? "disabled" : "active",
						expected_status: status
					});
					setConfirmStatus(false);
				} catch (cause) {
					setStatusProblem(cause instanceof Error ? cause.message : "无法确认 Workflow 状态");
				} finally {
					setStatusBusy(false);
					reload();
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "workflow-canvas-title",
				className: "ops-page",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "ops-page-header",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: onBack,
								className: "ops-button ops-button--link",
								style: { paddingInline: 0 },
								children: "← 返回流程列表"
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", {
								id: "workflow-canvas-title",
								children: workflow?.workflow.name ?? "Workflow 详情"
							}),
							workflow && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "ops-actions ops-meta",
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowStatusBadge, { status: workflow.workflow.status }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: workflow.current ? `已发布 v${workflow.current.version}` : "未发布" }),
									workflow.draft && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
										className: "ops-status",
										"data-tone": "info",
										children: ["草稿 · 修订 ", workflow.draft.revision]
									}),
									workflow.workflow.node_count !== null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [workflow.workflow.node_count, " 步"] }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [workflow.workflow.active_task_count, " 个活跃任务"] })
								]
							})
						] }), commands?.setWorkflowStatus && changeable && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(PanelCommandButton, {
							className: "ops-button",
							"aria-expanded": confirmStatus,
							disabled: statusBusy || loading || failed,
							onClick: () => setConfirmStatus((open) => !open),
							children: [action, " Workflow"]
						})]
					}),
					confirmStatus && changeable && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "ops-surface ops-inset ops-toolbar",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: status === "active" ? "停用会阻止新任务和后续触发；已创建的运行继续执行。" : "启用后可以启动新任务，已启用的触发器按原配置运行。" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "ops-actions",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								className: "ops-button",
								type: "button",
								onClick: () => setConfirmStatus(false),
								disabled: statusBusy,
								children: "取消"
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(PanelCommandButton, {
								className: "ops-button ops-button--primary",
								disabled: statusBusy || loading || failed,
								onClick: () => {
									changeStatus();
								},
								children: ["确认", action]
							})]
						})]
					}),
					statusProblem && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "alert",
						className: "ops-meta",
						children: statusProblem
					}),
					failed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						role: "alert",
						style: errorStyle,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: COPY.queryFailed }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							onClick: reload,
							className: "ops-button",
							children: COPY.retry
						})]
					}),
					loading && !workflow && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						className: "ops-meta",
						children: "正在读取流程定义…"
					}),
					workflow && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							role: "group",
							"aria-label": "流程视图",
							className: "ops-tabs",
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									"aria-pressed": view === "design",
									onClick: () => setView("design"),
									children: "设计画布"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									"aria-pressed": view === "versions",
									onClick: () => setView("versions"),
									children: "版本记录"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									"aria-pressed": view === "runs",
									onClick: () => setView("runs"),
									children: "运行记录"
								})
							]
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
							hidden: view !== "design",
							children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "ops-stack",
								children: [
									(!commands || !workflow.draft) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(DefinitionPanel, { definition: workflow.draft?.definition ?? workflow.current?.definition ?? null }),
									commands && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowEditor, {
										commands,
										workflow,
										onRefresh: reload,
										snapshotStale: failed
									}),
									!commands && (workflow.validation.errors.length > 0 || workflow.validation.warnings.length > 0) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
										className: "ops-surface ops-inset",
										"aria-label": "检查结果",
										children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(IssueGroup, {
											title: "需要修正",
											issues: workflow.validation.errors,
											tone: "error"
										}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(IssueGroup, {
											title: "提醒",
											issues: workflow.validation.warnings,
											tone: "warning"
										})]
									})
								]
							})
						}),
						view === "runs" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowRuns, {
							api,
							workflowId,
							reloadVersion,
							onSnapshot,
							onOpenRun
						}), workflow.recent_tests && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
							"aria-labelledby": "workflow-tests-title",
							className: "ops-surface",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", {
								className: "ops-surface-head",
								children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
									id: "workflow-tests-title",
									children: "结构与模拟流转测试记录"
								})
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "ops-inset",
								children: [workflow.recent_tests.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle$1,
									children: "此流程尚无结构与模拟流转测试记录。"
								}), workflow.recent_tests.map((test) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("summary", { children: [
									"修订 ",
									test.revision,
									" · ",
									test.passed ? "结构与模拟流转测试通过" : "结构与模拟流转测试未通过",
									" · ",
									formatDate(test.tested_at)
								] }), test.passed ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ol", { children: test.order.map((step) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", { children: step.title }, step.node_id)) }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: test.issues.errors.map((issue, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", { children: issue.message }, index)) })] }, test.test_id))]
							})]
						})] }),
						view === "versions" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
							"aria-labelledby": "workflow-versions-title",
							className: "ops-surface",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", {
								className: "ops-surface-head",
								children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
									id: "workflow-versions-title",
									children: "已发布版本"
								})
							}), workflow.versions.length === 0 ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								className: "ops-empty",
								children: "还没有已发布版本。"
							}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
								className: "ops-table-scroll",
								children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("table", {
									className: "ops-table",
									children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("thead", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", { children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", { children: "版本" }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", { children: "发布时间" }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", { children: "发布说明" })
									] }) }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("tbody", { children: workflow.versions.map((version) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", { children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("td", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("strong", { children: ["v", version.version] }), workflow.current?.version === version.version && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
											className: "ops-status",
											"data-tone": "info",
											style: { marginLeft: 6 },
											children: "当前"
										})] }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", {
											dateTime: version.published_at,
											children: formatDate(version.published_at)
										}) }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: version.publish_note || "—" })
									] }, version.version)) })]
								})
							})]
						})
					] })
				]
			});
		}
		function DefinitionPanel({ definition }) {
			const doc = readCanvasDoc(definition);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "workflow-definition-title",
				className: "ops-surface",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
					className: "ops-surface-head",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
						id: "workflow-definition-title",
						children: "流程定义"
					}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
						className: "ops-status",
						children: "只读"
					})]
				}), definition == null ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					className: "ops-empty",
					children: "流程定义暂不可用。"
				}) : /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
					className: "ops-inset",
					children: [
						doc && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowDependencyView, { doc }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowCanvasView, {
							definition,
							readOnly: true,
							runtimeCompact: true,
							inspectNodes: true,
							workspace: true,
							canvasId: "published-canvas"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看定义 JSON" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("pre", {
							style: jsonStyle,
							children: JSON.stringify(definition, null, 2)
						})] })
					]
				})]
			});
		}
		function WorkflowRuns({ api, workflowId, reloadVersion, onSnapshot, onOpenRun }) {
			const [runs, setRuns] = (0, react.useState)(null);
			const [failed, setFailed] = (0, react.useState)(false);
			const [retry, refresh] = (0, react.useState)(0);
			const [cursor, setCursor] = (0, react.useState)();
			const [next, setNext] = (0, react.useState)(null);
			const [loading, setLoading] = (0, react.useState)(false);
			const lastGeneration = (0, react.useRef)(reloadVersion);
			(0, react.useEffect)(() => {
				if (lastGeneration.current === reloadVersion) return;
				lastGeneration.current = reloadVersion;
				setCursor(void 0);
				refresh((value) => value + 1);
			}, [reloadVersion]);
			(0, react.useEffect)(() => {
				let current = true;
				setFailed(false);
				setLoading(true);
				api.listRuns({
					workflow_id: workflowId,
					limit: 30,
					cursor
				}).then(({ data, as_of_seq }) => {
					if (!current) return;
					setRuns((prior) => cursor ? [...prior ?? [], ...data.items.filter((run) => !prior?.some((item) => item.run_id === run.run_id))] : data.items);
					setNext(data.next_cursor ?? null);
					onSnapshot?.(as_of_seq);
				}).catch(() => {
					if (current) setFailed(true);
				}).finally(() => {
					if (current) setLoading(false);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				workflowId,
				onSnapshot,
				retry,
				cursor
			]);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-label": "历史运行",
				className: "ops-surface",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", {
						className: "ops-surface-head",
						children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", { children: "历史运行" })
					}),
					failed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						role: "alert",
						className: "ops-empty",
						children: [
							COPY.queryFailed,
							" ",
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								className: "ops-button",
								onClick: () => {
									setCursor(void 0);
									refresh((value) => value + 1);
								},
								children: COPY.retry
							})
						]
					}),
					!failed && !runs && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						className: "ops-empty",
						children: "正在读取运行记录…"
					}),
					!failed && runs?.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						className: "ops-empty",
						children: "此流程还没有运行记录。"
					}),
					runs && runs.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						className: "ops-table-scroll",
						children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("table", {
							className: "ops-table ops-table--wide",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("thead", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", { children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", { children: "任务" }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", { children: "结果" }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", { children: "版本" }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", { children: "触发方式" }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", { children: "开始时间" }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("th", { children: "耗时" })
							] }) }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("tbody", { children: runs.map((run) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("tr", { children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("td", { children: [onOpenRun && run.task_id ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									className: "ops-button ops-button--link",
									style: {
										whiteSpace: "normal",
										textAlign: "left"
									},
									onClick: () => onOpenRun(run.task_id, run.run_id),
									children: run.task_title || "查看任务运行"
								}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: run.task_title || "未命名任务" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看运行登记明细" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
									"运行编号 ",
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: run.run_id }),
									" · 状态 ",
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: run.status })
								] })] })] }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
									className: "ops-status",
									children: taskStatusView(run.status, run.display_status).label ?? COPY.oldPanel
								}) }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: run.workflow_version === null ? "—" : `v${run.workflow_version}` }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: taskOriginLabel(run) }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("time", {
									dateTime: run.started_at ?? run.created_at,
									children: formatTaskDate(run.started_at ?? run.created_at)
								}) }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("td", { children: runDuration(run.started_at, run.ended_at) ?? "—" })
							] }, run.run_id)) })]
						})
					}),
					next && !failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						className: "ops-inset",
						children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							disabled: loading,
							className: "ops-button",
							onClick: () => setCursor(next),
							children: "加载更早的运行"
						})
					})
				]
			});
		}
		function IssueGroup({ title, issues, tone }) {
			if (issues.length === 0) return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
				style: mutedStyle$1,
				children: [title, "：没有项目"]
			});
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				style: { marginTop: 12 },
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("h3", {
					style: {
						...sectionTitleStyle,
						color: tone === "error" ? COLOR.danger : COLOR.warning
					},
					children: [
						title,
						"（",
						issues.length,
						"）"
					]
				}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: issues.map((issue, index) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: issue.path || "流程" }),
					" · ",
					issue.message
				] }, `${issue.path}-${issue.code}-${index}`)) })]
			});
		}
		function formatDate(value) {
			const date = new Date(value);
			return Number.isNaN(date.valueOf()) ? "时间未知" : date.toLocaleString();
		}
		const sectionTitleStyle = {
			margin: 0,
			fontSize: 16
		};
		const mutedStyle$1 = {
			margin: "6px 0 0",
			color: COLOR.inkMuted,
			fontSize: FONT_PX.body
		};
		const jsonStyle = {
			maxHeight: 560,
			overflow: "auto",
			padding: 16,
			borderRadius: 8,
			background: COLOR.canvas,
			color: COLOR.ink,
			fontSize: FONT_PX.helper,
			whiteSpace: "pre-wrap",
			overflowWrap: "anywhere"
		};
		const errorStyle = {
			display: "flex",
			justifyContent: "space-between",
			alignItems: "center",
			padding: 14,
			color: COLOR.danger,
			background: "#fff1f0",
			borderRadius: 8
		};
		// src/client/api/rework.ts
		/** Ambiguous writes recover the original receipt; they never resend the command. */
		function createReworkApi(client) {
			async function command(path, body) {
				const operation_id = newOperationId();
				try {
					return (await client.post(path, {
						...body,
						operation_id
					})).data;
				} catch (error) {
					if (!(error instanceof ApiError) || ![
						"OPERATION_TIMEOUT",
						"OPERATION_OUTCOME_UNKNOWN",
						"SERVICE_UNAVAILABLE"
					].includes(error.code)) throw error;
					try {
						const { data } = await client.get("/v1/operations/get", { operation_id });
						if (data.status === "applied" && data.receipt_json) {
							const receipt = JSON.parse(data.receipt_json);
							return receipt.result ?? receipt;
						}
					} catch {}
					throw new ApiError("OPERATION_OUTCOME_UNKNOWN", "提交结果待确认，请查回执后再操作", true, { operation_id });
				}
			}
			return {
				request: (input) => command("/v1/rework/request", input),
				inspect: (input) => command("/v1/rework/inspect", input),
				listInspections: (run_id) => client.get("/v1/rework/inspections", { run_id }),
				review: (input) => command("/v1/rework/review", input),
				plan: (input) => command("/v1/rework/plan", input),
				getPlan: (request_id) => client.get("/v1/rework/plan", { request_id }),
				approvePlan: (input) => command("/v1/rework/plan/approve", input),
				listHolds: (run_id) => client.get("/v1/rework/holds", { run_id }),
				hold: (input) => command("/v1/rework/hold", input),
				getOperation: (operation_id) => client.get("/v1/operations/get", { operation_id })
			};
		}
		// src/client/api/model-escalation.ts
		const path$2 = "/v1/model-escalation";
		function createModelEscalationApi(client) {
			return {
				get: (run_id, node_run_id) => client.get(path$2, {
					run_id,
					node_run_id
				}),
				assess: (precondition, operation_id = newOperationId()) => client.post(path$2, {
					...precondition,
					operation_id
				})
			};
		}
		// src/client/api/task-commands.ts
		function createTaskCommands(client) {
			return {
				async proposeTaskPlan(goal, requirements, signal) {
					return (await client.post("/v1/task-plans", {
						goal,
						requirements
					}, signal)).data;
				},
				async confirmTaskPlan(planId, contentHash) {
					try {
						return (await client.post("/v1/task-plans/confirm", {
							plan_id: planId,
							content_hash: contentHash,
							confirmed: true
						})).data;
					} catch (error) {
						if (!(error instanceof ApiError) || ![
							"OPERATION_TIMEOUT",
							"OPERATION_OUTCOME_UNKNOWN",
							"SERVICE_UNAVAILABLE"
						].includes(error.code)) throw error;
						try {
							const { data } = await client.get("/v1/task-plans/get", { plan_id: planId });
							if (data.content_hash === contentHash && data.launch) return data.launch;
						} catch {}
						throw new ApiError("OPERATION_OUTCOME_UNKNOWN", "这份计划的启动回执暂未确认，可用同一计划重试", true);
					}
				},
				startMemory(runId, reason, operationId) {
					return postWithOperationReceipt(client, "/v1/memory/runs/start", "memory.start_run", {
						source_run_id: runId,
						reason,
						request_key: operationId,
						operation_id: operationId
					});
				},
				async setArchived(taskId, archived, expectedArchivedAt) {
					await postWithOperationReceipt(client, "/v1/tasks/archive", archived ? "task.archive" : "task.restore", {
						task_id: taskId,
						archived,
						expected_archived_at: expectedArchivedAt,
						operation_id: newOperationId()
					});
				},
				async cancelTask(taskId, reason) {
					return (await client.post("/v1/tasks/cancel", {
						taskId,
						...reason ? { reason } : {}
					})).data;
				},
				async retryTask(taskId, options = {}) {
					return (await client.post("/v1/tasks/retry", {
						taskId,
						mode: "manual",
						...options
					})).data;
				},
				async batchTasks(action, taskIds, reason) {
					return (await client.post("/v1/tasks/batch", {
						action,
						taskIds,
						...reason ? { reason } : {}
					})).data;
				},
				async start(input) {
					try {
						return (await client.post("/v1/runs/start", {
							taskTitle: input.title,
							workflowId: input.workflowId,
							workflowVersion: input.version,
							autonomy: input.autonomy,
							triggerKind: "manual",
							operationId: input.operationId
						})).data;
					} catch (error) {
						if (error instanceof ApiError && [
							"OPERATION_TIMEOUT",
							"OPERATION_OUTCOME_UNKNOWN",
							"SERVICE_UNAVAILABLE"
						].includes(error.code)) throw new ApiError("OPERATION_OUTCOME_UNKNOWN", "建任务结果暂不可确认。使用「查询或重试」保留原请求编号。", true, { operation_id: input.operationId });
						throw error;
					}
				}
			};
		}
		// src/client/api/task-trigger-commands.ts
		function createTaskTriggerCommands(client) {
			return {
				create: (body) => postWithOperationReceipt(client, "/v1/task-triggers", "task.trigger_create", body),
				setEnabled: (item, enabled, operationId = newOperationId()) => postWithOperationReceipt(client, "/v1/task-triggers/enabled", "task.trigger_set_enabled", {
					trigger_id: item.trigger_id,
					operation_id: operationId,
					expected_revision: item.revision,
					enabled
				}),
				emit: async (eventType, eventId) => (await client.post("/v1/task-triggers/event", {
					event_type: eventType,
					event_id: eventId
				})).data
			};
		}
		// src/client/panel/collaboration-members.tsx
		function collaborationMemberNames(members) {
			const names = members.map((member) => member.title?.trim() || `步骤 ${member.node_id || member.node_run_id}`);
			return new Map(members.map((member, index) => {
				const name = names[index];
				const duplicate = names.indexOf(name) !== names.lastIndexOf(name);
				return [member.node_run_id, duplicate ? `${name} · ${member.node_run_id}` : name];
			}));
		}
		function CollaborationMembers({ client, selected, onChange, disabled }) {
			const [runs, setRuns] = (0, react.useState)([]);
			const [cursor, setCursor] = (0, react.useState)();
			const [next, setNext] = (0, react.useState)(null);
			const [run, setRun] = (0, react.useState)("");
			const [nodes, setNodes] = (0, react.useState)();
			const [error, setError] = (0, react.useState)("");
			const [loading, setLoading] = (0, react.useState)(false);
			const [retry, setRetry] = (0, react.useState)(0);
			const [manual, setManual] = (0, react.useState)("");
			(0, react.useEffect)(() => {
				let current = true;
				setError("");
				setLoading(true);
				client.get("/v1/runs", {
					limit: 50,
					cursor
				}).then(({ data }) => {
					if (!current) return;
					setRuns((previous) => {
						const byId = new Map((cursor ? previous : []).map((item) => [item.run_id, item]));
						for (const item of data.items) byId.set(item.run_id, item);
						return [...byId.values()];
					});
					setNext(data.next_cursor ?? null);
				}).catch(() => {
					if (current) setError("运行列表读取失败。");
				}).finally(() => {
					if (current) setLoading(false);
				});
				return () => {
					current = false;
				};
			}, [
				client,
				cursor,
				retry
			]);
			(0, react.useEffect)(() => {
				if (!run) return;
				let current = true;
				setNodes(void 0);
				client.get("/v1/node-runs", {
					run_id: run,
					limit: 200
				}).then(({ data }) => {
					if (current) setNodes(data.items.filter((item) => item.run_id === run));
				}).catch(() => {
					if (current) setError("步骤读取失败。");
				});
				return () => {
					current = false;
				};
			}, [
				client,
				run,
				retry
			]);
			const remove = (id) => onChange(selected.filter((member) => member.id !== id));
			const eligible = nodes?.filter((item) => item.node_type === "agent") ?? [];
			const names = collaborationMemberNames(eligible);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("fieldset", {
				disabled,
				className: "ops-stack",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("legend", { children: "哪些执行步骤可以相互核验？" }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["任务运行 ", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
						className: "ops-field",
						value: run,
						onChange: (event) => {
							setRun(event.target.value);
							setNodes(void 0);
							setError("");
						},
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
							value: "",
							children: loading ? "读取运行中…" : "选择任务的一次运行"
						}), runs.map((item) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
							value: item.run_id,
							children: [
								item.task_title || item.workflow_id || "任务",
								" · ",
								new Date(item.created_at).toLocaleString(),
								" · ",
								item.run_id
							]
						}, item.run_id))]
					})] }),
					next && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						disabled: loading,
						onClick: () => setCursor(next),
						children: "更多运行"
					}),
					error && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: "alert",
						children: [
							error,
							" ",
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: () => setRetry((value) => value + 1),
								children: "重新读取"
							})
						]
					}),
					run && !nodes && !error && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取本次运行的步骤…"
					}),
					run && nodes && eligible.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "本页没有可选的智能体步骤。" }),
					run && nodes?.length === 200 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "当前列出前 200 条中的智能体步骤。后续步骤可通过下方的节点运行 ID 添加。" }),
					run && eligible.map((item) => {
						const checked = selected.some((member) => member.id === item.node_run_id);
						const name = names.get(item.node_run_id);
						return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
								type: "checkbox",
								checked,
								disabled: !checked && selected.length >= 16,
								onChange: () => checked ? remove(item.node_run_id) : onChange([...selected, {
									id: item.node_run_id,
									label: (runs.find((r) => r.run_id === run)?.task_title || run) + " / " + name + " · 第 " + item.attempt + " 次"
								}])
							}),
							" ",
							name,
							" · 第 ",
							item.attempt,
							" 次 · ",
							item.status_label
						] }, item.node_run_id);
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "通过节点运行 ID 添加其他步骤" }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["节点运行 ID ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
							value: manual,
							onChange: (event) => setManual(event.target.value)
						})] }),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							disabled: !manual.trim(),
							onClick: () => {
								const ids = [...new Set(manual.trim().split(/[\s,，]+/u))].filter((id) => !selected.some((member) => member.id === id));
								if (selected.length + ids.length > 16) {
									setError("每个范围最多包含 16 个步骤。");
									return;
								}
								onChange([...selected, ...ids.map((id) => ({
									id,
									label: id
								}))]);
								setManual("");
								setError("");
							},
							children: "添加"
						})
					] }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [
						"已选 ",
						selected.length,
						" 个步骤，至少选择两个。"
					] }),
					selected.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", { children: selected.map((member) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
						member.label,
						" ",
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							"aria-label": "移除 " + member.label,
							onClick: () => remove(member.id),
							children: "移除"
						})
					] }, member.id)) })
				]
			});
		}
		// src/client/panel/collaboration-screen.tsx
		const LABELS = {
			proposed: "待授权",
			authorized: "已授权，等接收方读取",
			delivered: "已送达",
			accepted: "已接手",
			completed: "核验完成",
			rejected: "接收方拒绝",
			denied: "主理人拒绝",
			expired: "已到期",
			withdrawn: "已撤回",
			confirmed: "已证实",
			refuted: "已反证",
			inconclusive: "证据未能定论",
			adopted: "已采纳",
			dismissed: "未采纳"
		};
		const ACTIONS = {
			authorize: "授权核验",
			deny: "拒绝请求",
			adopt: "采纳结论",
			dismiss: "不采纳结论",
			reassign: "重新指派"
		};
		function CollaborationScreen({ client, reloadVersion, onSnapshot, onOpenRun }) {
			const [scopes, setScopes] = (0, react.useState)();
			const [selected, setSelected] = (0, react.useState)("");
			const [detail, setDetail] = (0, react.useState)();
			const [retry, refresh] = (0, react.useState)(0);
			const [busy, setBusy] = (0, react.useState)(false);
			const [notice, setNotice] = (0, react.useState)("");
			const [failed, setFailed] = (0, react.useState)(false);
			const [title, setTitle] = (0, react.useState)("");
			const [members, setMembers] = (0, react.useState)([]);
			const [creating, setCreating] = (0, react.useState)(false);
			const pending = (0, react.useRef)(/* @__PURE__ */ new Map());
			const [minutes, setMinutes] = (0, react.useState)(60);
			const [review, setReview] = (0, react.useState)();
			const [note, setNote] = (0, react.useState)("");
			const [replacement, setReplacement] = (0, react.useState)("");
			const scopeId = selected || scopes?.[0]?.scope_id;
			(0, react.useEffect)(() => {
				let current = true;
				client.get("/v1/collaboration/scopes").then(({ data, as_of_seq }) => {
					if (current) {
						setScopes(data.items);
						onSnapshot(as_of_seq);
					}
				}).catch(() => {
					if (current) {
						setFailed(true);
						setNotice("协作范围读取失败。");
					}
				});
				return () => {
					current = false;
				};
			}, [
				client,
				onSnapshot,
				reloadVersion,
				retry
			]);
			(0, react.useEffect)(() => {
				if (!scopeId) return;
				let current = true;
				client.get("/v1/collaboration/scopes/get", { scope_id: scopeId }).then(({ data, as_of_seq }) => {
					if (current) {
						setDetail(data);
						onSnapshot(as_of_seq);
					}
				}).catch(() => {
					if (current) {
						setFailed(true);
						setNotice("核验详情读取失败。");
					}
				});
				return () => {
					current = false;
				};
			}, [
				client,
				scopeId,
				onSnapshot,
				reloadVersion,
				retry
			]);
			async function command(path, kind, body, done) {
				if (busy) return;
				const key = JSON.stringify([kind, body]);
				const id = pending.current.get(key) ?? newOperationId();
				pending.current.set(key, id);
				setNotice("");
				setFailed(false);
				setBusy(true);
				try {
					await postWithOperationReceipt(client, path, kind, {
						...body,
						operation_id: id
					});
					pending.current.delete(key);
					done();
					refresh((value) => value + 1);
					setNotice("已记录。");
				} catch (error) {
					if (!(error instanceof ApiError && error.code === "OPERATION_OUTCOME_UNKNOWN")) pending.current.delete(key);
					setFailed(true);
					setNotice(error instanceof Error ? operatorReason(error.message) : "操作结果暂不可确认。");
				} finally {
					setBusy(false);
				}
			}
			const memberIds = members.map((member) => member.id).sort();
			const shown = detail?.scope.scope_id === scopeId ? detail : void 0;
			const participantNames = collaborationMemberNames(shown?.members ?? []);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "ops-page",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
						className: "ops-page-header",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", { children: "协作核验" }), scopes && scopes.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
							className: "ops-actions",
							children: ["协作范围 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("select", {
								className: "ops-field",
								value: scopeId,
								onChange: (event) => {
									setSelected(event.target.value);
									setReview(void 0);
								},
								children: scopes.map((scope) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
									value: scope.scope_id,
									children: scope.title
								}, scope.scope_id))
							})]
						})]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						className: "ops-meta",
						children: "让一个执行步骤独立复核另一个步骤的发现。例如，分析步骤发现汇总金额有误，可请复核步骤重算，再由你决定是否采纳。"
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", {
						className: "ops-surface ops-inset",
						open: creating,
						onToggle: (event) => setCreating(event.currentTarget.open),
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "选择可相互核验的步骤" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
							style: form$1,
							onSubmit: (event) => {
								event.preventDefault();
								command("/v1/collaboration/scopes", "collaboration.scope_create", {
									title: title.trim(),
									node_run_ids: memberIds,
									ttl_minutes: minutes
								}, () => {
									setTitle("");
									setMembers([]);
									setSelected("");
									setCreating(false);
								});
							},
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["本次核验名称 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									required: true,
									maxLength: 200,
									placeholder: "例如：复核月报汇总金额",
									value: title,
									onChange: (event) => setTitle(event.target.value)
								})] }),
								creating && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(CollaborationMembers, {
									client,
									selected: members,
									onChange: setMembers,
									disabled: busy
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["允许提出核验的时长（分钟） ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									type: "number",
									required: true,
									min: 1,
									max: 10080,
									value: minutes,
									onChange: (event) => setMinutes(Number(event.target.value))
								})] }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									className: "ops-meta",
									children: "保存后，选中的步骤可在范围内提出带证据的请求。出现请求时，在这里选择「授权核验」。"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
									type: "submit",
									disabled: busy || !title.trim() || memberIds.length < 2 || memberIds.length > 16,
									children: "保存范围"
								})
							]
						})]
					}),
					notice && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: failed ? "alert" : "status",
						children: [
							notice,
							" ",
							failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								onClick: () => refresh((value) => value + 1),
								children: "刷新"
							})
						]
					}),
					!scopes && !failed && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取协作范围…"
					}),
					scopes?.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						className: "ops-surface ops-empty",
						children: "还没有协作范围。"
					}),
					shown && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
							style: card,
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "ops-toolbar",
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: "参与步骤" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
									className: "ops-meta",
									children: ["有效至 ", new Date(shown.scope.expires_at).toLocaleString()]
								})]
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
								className: "ops-actions",
								children: shown.members.map((node) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									className: "ops-button ops-button--link",
									type: "button",
									style: {
										whiteSpace: "normal",
										overflowWrap: "anywhere"
									},
									onClick: () => onOpenRun(node.task_id, node.run_id),
									children: participantNames.get(node.node_run_id)
								}, node.node_run_id))
							})]
						}),
						shown.findings.map((finding) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("article", {
							style: card,
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
									style: {
										margin: 0,
										fontSize: FONT_PX.section
									},
									children: finding.title
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: finding.summary }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看原始证据编号" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: finding.evidence_id }) })] }),
								shown.requests.filter((request) => request.finding_id === finding.finding_id).map((request) => {
									const candidates = request.reassignment_candidates;
									const actions = request.status === "proposed" ? candidates === null ? ["authorize", "deny"] : ["deny"] : request.status === "completed" && request.adoption === null ? ["adopt", "dismiss"] : [];
									if (candidates?.length) actions.push("reassign");
									return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
										id: request.request_id,
										style: {
											borderTop: `1px solid ${COLOR.line}`,
											marginTop: 10,
											paddingTop: 10
										},
										children: [
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: LABELS[request.status] ?? operatorStatus(request.status) }),
											request.withdrawn_from_status && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", { children: [" · 撤回前", LABELS[request.withdrawn_from_status]] }),
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: request.question }),
											/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["接收步骤 ", participantNames.get(request.target_node_run_id) ?? "步骤名称暂不可用"] }),
											request.target_attempt !== null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
												"第 ",
												request.target_attempt,
												" 次尝试"
											] }),
											request.replacement_request_id && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("a", {
												href: "#" + request.replacement_request_id,
												children: "查看重新指派的请求"
											}) }),
											request.replaces_request_id && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("a", {
												href: "#" + request.replaces_request_id,
												children: "查看已撤回的请求"
											}) }),
											candidates?.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "当前协作范围内没有可用的替代接收方。" }),
											request.authorization_note && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["派发记录 · ", request.authorization_note] }),
											request.result && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
												LABELS[request.verdict ?? ""],
												" · ",
												request.result
											] }),
											request.adoption && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
												LABELS[request.adoption],
												" · ",
												request.adoption_note
											] }),
											/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("details", { children: [
												/* @__PURE__ */ (0, react_jsx_runtime.jsx)("summary", { children: "查看核验登记明细" }),
												/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
													"请求编号 ",
													/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: request.request_id }),
													" · 接收步骤编号 ",
													/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: request.target_node_run_id })
												] }),
												request.target_generation !== null && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["派发代次 ", request.target_generation] }),
												request.evidence_id && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: ["证据编号 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: request.evidence_id })] })
											] }),
											actions.map((action) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
												disabled: busy,
												onClick: () => {
													setReview({
														id: request.request_id,
														revision: request.revision,
														action
													});
													setNote("");
													setReplacement("");
												},
												children: ACTIONS[action]
											}, action)),
											review?.id === request.request_id && review.revision === request.revision && actions.includes(review.action) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
												style: form$1,
												onSubmit: (event) => {
													event.preventDefault();
													const base = {
														request_id: review.id,
														expected_revision: review.revision,
														note: note.trim()
													};
													if (review.action === "reassign") command("/v1/collaboration/reassign", "collaboration.reassign", {
														...base,
														target_node_run_id: replacement
													}, () => setReview(void 0));
													else command("/v1/collaboration/review", "collaboration.review", {
														...base,
														action: review.action
													}, () => setReview(void 0));
												},
												children: [
													review.action === "reassign" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["新接收步骤 ", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
														required: true,
														value: replacement,
														onChange: (event) => setReplacement(event.target.value),
														children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
															value: "",
															children: "选择接收步骤"
														}), candidates?.map((id) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
															value: id,
															children: participantNames.get(id) ?? id
														}, id))]
													})] }),
													/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["处理理由 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
														required: true,
														maxLength: 1e3,
														value: note,
														onChange: (event) => setNote(event.target.value)
													})] }),
													/* @__PURE__ */ (0, react_jsx_runtime.jsxs)(PanelCommandButton, {
														type: "submit",
														disabled: busy || !note.trim() || review.action === "reassign" && !candidates?.includes(replacement),
														children: ["确认", ACTIONS[review.action]]
													})
												]
											})
										]
									}, request.request_id);
								})
							]
						}, finding.finding_id)),
						shown.findings.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "参与步骤已选好。执行者提交发现后，这里会显示原始证据和待授权的核验请求。" })
					] })
				]
			});
		}
		const card = {
			border: "1px solid #dde4ed",
			background: COLOR.surface,
			borderRadius: 8,
			padding: "9px 12px",
			overflowWrap: "anywhere"
		};
		const form$1 = {
			display: "grid",
			gap: 7,
			marginTop: 8,
			maxWidth: 540
		};
		// src/client/panel/gate-card.tsx
		const content = {
			idle: [
				"等待冻结证据",
				"发布负责人",
				"从受信工作区启动核验"
			],
			checking: [
				"检查组正在核验",
				"发布执行器",
				"等待六组检查完成"
			],
			approval: [
				"判卷通过，等待人工确认",
				"获授权的审批人",
				"在受信工作区完成二次确认"
			],
			blocked: [
				"检查组证据尚未收齐",
				"发布负责人",
				"核对缺失的检查组"
			],
			failed: [
				"本次核验未通过",
				"发布负责人",
				"查看未通过的必过项"
			],
			confirmed: [
				"运行已确认，等待独立联检",
				"独立核验人",
				"核对独立判卷与发布判卷"
			]
		};
		function GateCard({ state }) {
			const [conclusion, waitingOn, nextAction] = content[state];
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "release-gate-title",
				className: "ops-surface",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", {
						className: "ops-surface-head",
						children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
							id: "release-gate-title",
							children: "S6 · G6 发布门"
						})
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: conclusion }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "当前负责人" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: waitingOn })] }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "下一步" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: nextAction })] })] })
				]
			});
		}
		// src/client/panel/overview-screen.tsx
		function OverviewScreen({ api, reloadVersion, onSnapshot, onNavigate }) {
			const [data, setData] = (0, react.useState)(null);
			const [failed, setFailed] = (0, react.useState)(false);
			const [retry, refresh] = (0, react.useState)(0);
			(0, react.useEffect)(() => {
				let current = true;
				setFailed(false);
				Promise.all([api.getWorkspaceOverview(), api.listWorkflows({ limit: 4 })]).then(([overview, workflows]) => {
					if (!current) return;
					setData({
						summary: overview.data,
						workflows: workflows.data.items
					});
					onSnapshot(Math.max(overview.as_of_seq, workflows.as_of_seq));
				}).catch(() => {
					if (current) setFailed(true);
				});
				return () => {
					current = false;
				};
			}, [
				api,
				onSnapshot,
				reloadVersion,
				retry
			]);
			const lanes = data?.summary.lanes;
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "overview-title",
				className: "ops-page",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", { children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", {
						id: "overview-title",
						children: "总览"
					}) }),
					failed && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						role: "alert",
						children: ["总览读取失败。 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							style: buttonStyle$1,
							onClick: () => refresh((value) => value + 1),
							children: "重试"
						})]
					}),
					!failed && !data && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取总览…"
					}),
					data?.summary.release_gate && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(GateCard, { state: data.summary.release_gate }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("style", { children: OPS_MONITOR_CSS }),
					data?.summary.metrics && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
						className: "ops-kpi-grid",
						"aria-label": "编排指标",
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
								className: "ops-kpi-head",
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", { children: "运行指标" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: "数据来自本地事件账本与宿主真实费用上报" })]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
								className: "ops-kpi-grid-inner",
								children: overviewKpis(data.summary.metrics).map((item) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("button", {
									type: "button",
									className: "ops-kpi-card",
									"data-tone": item.tone ?? "plain",
									onClick: () => onNavigate(item.route),
									children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { className: "ops-kpi-label", children: item.label }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { className: "ops-kpi-value", children: item.value }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("small", { children: item.hint })
									]
								}, item.label))
							})
						]
					}),
					data && lanes && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: statsStyle,
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("button", {
								type: "button",
								style: statStyle,
								onClick: () => onNavigate("tasks"),
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: lanes.upcoming + lanes.running + lanes.attention + lanes.finished }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: "未归档任务" })]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("button", {
								type: "button",
								style: statStyle,
								onClick: () => onNavigate("decisions"),
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: data.summary.open_decisions }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: "待审批" })]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("button", {
								type: "button",
								style: statStyle,
								onClick: () => onNavigate("effects"),
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: data.summary.unverified_effects }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: "待核实" })]
							})
						]
					}),
					lanes && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
						style: blockStyle,
						"aria-label": "任务状态",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
							style: headingStyle,
							children: "任务状态"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dl", {
							style: {
								...statsStyle,
								margin: 0
							},
							children: [
								["待执行", lanes.upcoming],
								["执行中", lanes.running],
								["待处理", lanes.attention],
								["已结束", lanes.finished]
							].map(([label, count]) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: label }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", {
								style: {
									margin: 0,
									fontSize: FONT_PX.title,
									fontWeight: 650
								},
								children: count
							})] }, label))
						})]
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						className: "ops-context-grid",
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
							style: blockStyle,
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
								style: sectionHeaderStyle,
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
									style: headingStyle,
									children: "近期事件"
								}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									style: buttonStyle$1,
									onClick: () => onNavigate("activity"),
									children: "全部记录"
								})]
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ActivityList, {
								api,
								reloadVersion,
								onSnapshot,
								compact: true
							})]
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
							style: blockStyle,
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
									style: sectionHeaderStyle,
									children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
										style: headingStyle,
										children: "Workflow"
									}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										style: buttonStyle$1,
										onClick: () => onNavigate("workflows"),
										children: "查看全部"
									})]
								}),
								data?.workflows.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
									style: mutedStyle,
									children: "当前没有流程。"
								}),
								data?.workflows.map((workflow) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
									style: {
										paddingBlock: 9,
										borderBottom: `1px solid ${COLOR.line}`
									},
									children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: workflow.name }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
										style: mutedStyle,
										children: [
											workflow.current_version === null ? "待发布" : `v${workflow.current_version}`,
											" · ",
											workflow.active_task_count,
											" 个活跃任务"
										]
									})]
								}, workflow.workflow_id))
							]
						})]
					})
				]
			});
		}
		function formatOverviewDuration(ms) {
			if (typeof ms !== "number" || !Number.isFinite(ms)) return "—";
			if (ms < 1e3) return `${Math.round(ms)} ms`;
			const seconds = Math.round(ms / 1e3);
			if (seconds < 60) return `${seconds} 秒`;
			const minutes = Math.floor(seconds / 60);
			if (minutes < 60) return `${minutes} 分 ${seconds % 60} 秒`;
			const hours = Math.floor(minutes / 60);
			return `${hours} 小时 ${minutes % 60} 分`;
		}
		function formatTokenCount(value) {
			if (typeof value !== "number" || !Number.isFinite(value)) return "—";
			if (value < 1e4) return String(Math.round(value));
			return `${(value / 1e4).toFixed(value < 1e6 ? 1 : 2)} 万`;
		}
		function formatActualCost(cost) {
			if (!cost || cost.available !== true || typeof cost.amount_micros !== "number") return "数据不可用";
			const amount = cost.amount_micros / 1e6;
			const symbol = cost.currency === "USD" ? "$" : cost.currency === "CNY" ? "¥" : "";
			const digits = Math.abs(amount) < 1 ? 4 : 2;
			return symbol ? `${symbol}${amount.toFixed(digits)}` : `${amount.toFixed(digits)} ${cost.currency ?? ""}`.trim();
		}
		function overviewKpis(metrics) {
			const tokenTotal = metrics.tokens?.total ?? 0;
			return [
				{ label: "编排板", value: metrics.boards, hint: "已登记流程", route: "workflows" },
				{ label: "任务总数", value: metrics.tasks_total, hint: "未归档任务", route: "tasks" },
				{ label: "正在执行", value: metrics.tasks_running, hint: "派发 / 执行 / 重试中", route: "tasks", tone: "running" },
				{ label: "待验收", value: metrics.tasks_pending_acceptance, hint: "等待人工确认", route: "tasks", tone: "warning" },
				{ label: "已通过", value: metrics.tasks_passed, hint: "验收通过", route: "tasks", tone: "success" },
				{ label: "失败", value: metrics.tasks_failed, hint: "失败 / 超时 / 驳回", route: "tasks", tone: "danger" },
				{ label: "平均执行时间", value: formatOverviewDuration(metrics.average_duration_ms), hint: "已完成运行的平均用时", route: "tasks" },
				{ label: "重试次数", value: metrics.retries, hint: "任务级累计", route: "tasks" },
				{ label: "Token 消耗", value: formatTokenCount(tokenTotal), hint: `输入 ${formatTokenCount(metrics.tokens?.uncached_input)} · 输出 ${formatTokenCount(metrics.tokens?.output)}`, route: "tasks" },
				{ label: "实际费用", value: formatActualCost(metrics.actual_cost), hint: metrics.actual_cost?.available ? "宿主真实统计" : "宿主尚未上报", route: "settings" }
			];
		}
		const OPS_MONITOR_CSS = `
.ops-kpi-grid, .task-inspector, .task-detail-page, .ops-canvas-shell, .task-list {
  --ops-surface: var(--dsw-alias-bg-elevated, #ffffff);
  --ops-canvas: var(--dsw-alias-bg-base, #ffffff);
  --ops-line: var(--dsw-alias-divider, #e6e7ea);
  --ops-ink: var(--dsw-alias-text-primary, #111316);
  --ops-muted: var(--dsw-alias-text-secondary, #5a616b);
  --ops-accent: var(--dsw-alias-brand-primary, #1f5fbf);
  --ops-success: var(--dsw-alias-text-success, #1c7c54);
  --ops-warning: var(--dsw-alias-text-warning, #8a5c17);
  --ops-danger: var(--dsw-alias-text-danger, #a83239);
  --ops-soft: var(--dsw-alias-bg-subtle, #f5f7fa);
}
@media (prefers-color-scheme: dark) {
  .ops-kpi-grid, .task-inspector, .task-detail-page, .ops-canvas-shell, .task-list {
    --ops-surface: #1b2027; --ops-canvas: #12161b; --ops-line: #2c3540;
    --ops-ink: #e8edf3; --ops-muted: #9aa7b6; --ops-accent: #6cb0ff;
    --ops-success: #67d39a; --ops-warning: #e0b45c; --ops-danger: #ff8a90; --ops-soft: #171c22;
  }
}
[data-dsw-theme="dark"] .ops-kpi-grid, [data-dsw-theme="dark"] .task-inspector,
[data-dsw-theme="dark"] .task-detail-page, [data-dsw-theme="dark"] .ops-canvas-shell,
[data-dsw-theme="dark"] .task-list {
  --ops-surface: #1b2027; --ops-canvas: #12161b; --ops-line: #2c3540;
  --ops-ink: #e8edf3; --ops-muted: #9aa7b6; --ops-accent: #6cb0ff;
  --ops-success: #67d39a; --ops-warning: #e0b45c; --ops-danger: #ff8a90; --ops-soft: #171c22;
}
.ops-kpi-grid { display: grid; gap: 9px; }
.task-inspector--page {
  position: static; inset: auto; display: block; width: 100%; max-width: none;
  max-height: none; height: auto; min-height: 0; margin: 0; padding: 0;
  border: 0; border-radius: 0; box-shadow: none; background: var(--ops-canvas); color: var(--ops-ink);
}
.task-inspector--page .task-inspector-content { max-height: none; }
.task-inspector--page .task-inspector-top { position: sticky; top: 0; z-index: 2; background: var(--ops-surface); }
.ops-kpi-head { display: flex; flex-wrap: wrap; align-items: baseline; justify-content: space-between; gap: 8px; }
.ops-kpi-head h2 { margin: 0; font-size: 16px; color: var(--ops-ink); }
.ops-kpi-head span { color: var(--ops-muted); font-size: 12px; }
.ops-kpi-grid-inner { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(168px, 100%), 1fr)); gap: 8px; }
.ops-kpi-card {
  display: grid; gap: 3px; min-width: 0; padding: 11px 13px; text-align: left;
  border: 1px solid var(--ops-line); border-radius: 10px; background: var(--ops-surface);
  color: var(--ops-ink); cursor: pointer; font: inherit; transition: border-color .15s ease, transform .15s ease;
}
.ops-kpi-card:hover { border-color: var(--ops-accent); transform: translateY(-1px); }
.ops-kpi-card:focus-visible { outline: 2px solid var(--ops-accent); outline-offset: 2px; }
.ops-kpi-label { font-size: 12px; color: var(--ops-muted); }
.ops-kpi-value { font-size: 24px; line-height: 1.12; font-weight: 650; overflow-wrap: anywhere; }
.ops-kpi-card small { color: var(--ops-muted); font-size: 11.5px; }
.ops-kpi-card[data-tone="running"] .ops-kpi-value { color: var(--ops-accent); }
.ops-kpi-card[data-tone="warning"] .ops-kpi-value { color: var(--ops-warning); }
.ops-kpi-card[data-tone="success"] .ops-kpi-value { color: var(--ops-success); }
.ops-kpi-card[data-tone="danger"] .ops-kpi-value { color: var(--ops-danger); }
.ops-task-timeline { display: grid; gap: 0; margin: 0; padding: 0; list-style: none; }
.ops-timeline-item { position: relative; margin-left: 6px; padding: 0 0 14px 20px; border-left: 1px solid var(--ops-line); }
.ops-timeline-item:last-child { border-left-color: transparent; padding-bottom: 0; }
.ops-timeline-dot { position: absolute; left: -5px; top: 3px; width: 9px; height: 9px; border-radius: 50%; background: var(--ops-muted); box-shadow: 0 0 0 3px var(--ops-surface); }
.ops-timeline-item[data-kind="completed"] .ops-timeline-dot { background: var(--ops-success); }
.ops-timeline-item[data-kind="failed"] .ops-timeline-dot { background: var(--ops-danger); }
.ops-timeline-item[data-kind="review"] .ops-timeline-dot { background: var(--ops-warning); }
.ops-timeline-item[data-kind="executing"] .ops-timeline-dot,
.ops-timeline-item[data-kind="dispatched"] .ops-timeline-dot { background: var(--ops-accent); }
.ops-timeline-title { display: flex; flex-wrap: wrap; align-items: baseline; gap: 8px; color: var(--ops-ink); font-size: 13px; }
.ops-timeline-title time { color: var(--ops-muted); font-size: 11.5px; }
.ops-timeline-detail { margin: 3px 0 0; color: var(--ops-muted); font-size: 12px; overflow-wrap: anywhere; white-space: pre-wrap; }
.ops-detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(190px, 100%), 1fr)); gap: 8px; }
.ops-fact { min-width: 0; padding: 8px 10px; border: 1px solid var(--ops-line); border-radius: 8px; background: var(--ops-surface); }
.ops-fact dt { color: var(--ops-muted); font-size: 11.5px; }
.ops-fact dd { margin: 3px 0 0; color: var(--ops-ink); font-size: 13px; overflow-wrap: anywhere; }
.ops-detail-actions { display: flex; flex-wrap: wrap; gap: 7px; }
.ops-detail-actions button { padding: 6px 10px; border: 1px solid var(--ops-line); border-radius: 7px; background: var(--ops-surface); color: var(--ops-ink); cursor: pointer; }
.ops-detail-actions button:hover { border-color: var(--ops-accent); }
.ops-detail-actions button[data-tone="danger"] { color: var(--ops-danger); }
.ops-detail-actions button:disabled { opacity: .55; cursor: not-allowed; }
.ops-canvas-shell { display: grid; gap: 8px; }
.ops-canvas-toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; }
.ops-canvas-toolbar button { min-height: 30px; padding: 0 10px; border: 1px solid var(--ops-line); border-radius: 7px; background: var(--ops-surface); color: var(--ops-ink); cursor: pointer; }
.ops-canvas-toolbar button[aria-pressed="true"] { border-color: var(--ops-accent); color: var(--ops-accent); }
.ops-canvas-stage { position: relative; overflow: auto; border: 1px solid var(--ops-line); border-radius: 8px; background: var(--ops-canvas); }
.ops-canvas-scale { transform-origin: 0 0; }
.ops-canvas-node { position: absolute; display: grid; align-content: center; gap: 2px; padding: 6px 10px; border: 1px solid var(--ops-line); border-radius: 8px; background: var(--ops-surface); color: var(--ops-ink); box-shadow: 0 1px 2px rgba(17,19,22,.08); cursor: move; }
.ops-canvas-node[data-runtime-status="running"] { border-color: var(--ops-accent); animation: opsNodePulse 1.8s ease-in-out infinite; }
.ops-canvas-node[data-runtime-status="failed"], .ops-canvas-node[data-runtime-status="timeout"], .ops-canvas-node[data-runtime-status="failed_with_unverified"] { border-color: var(--ops-danger); }
.ops-canvas-node[data-runtime-status="done"] { border-color: var(--ops-success); }
.ops-canvas-node[data-collapsed="true"] > span ~ span { display: none; }
.ops-canvas-node[data-failure-chain="true"] { box-shadow: 0 0 0 2px color-mix(in srgb, var(--ops-danger) 45%, transparent); }
.ops-canvas-node[data-dependency-highlight="true"] { box-shadow: 0 0 0 2px color-mix(in srgb, var(--ops-accent) 40%, transparent); }
.ops-canvas-edge[data-edge-active="true"] { stroke: var(--ops-accent); stroke-width: 2.5; }
.ops-canvas-edge[data-failure-chain="true"] { stroke: var(--ops-danger); stroke-width: 2.5; stroke-dasharray: 6 3; }
.ops-canvas-group { position: absolute; border: 1px dashed var(--ops-line); border-radius: 10px; background: color-mix(in srgb, var(--ops-accent) 5%, transparent); pointer-events: none; }
.ops-canvas-group-label { position: absolute; top: -11px; left: 10px; padding: 1px 7px; border: 1px solid var(--ops-line); border-radius: 6px; background: var(--ops-surface); color: var(--ops-muted); font-size: 11px; cursor: pointer; pointer-events: auto; }
.ops-canvas-minimap { position: absolute; right: 10px; top: 10px; width: 148px; height: 92px; border: 1px solid var(--ops-line); border-radius: 7px; background: color-mix(in srgb, var(--ops-canvas) 90%, transparent); cursor: crosshair; z-index: 3; }
.task-batchbar { align-items: center; gap: 10px; }
.task-batch-check { position: absolute; top: 8px; right: 8px; width: 15px; height: 15px; cursor: pointer; z-index: 1; }
@keyframes opsNodePulse { 0%, 100% { box-shadow: 0 0 0 0 color-mix(in srgb, var(--ops-accent) 30%, transparent); } 50% { box-shadow: 0 0 0 6px transparent; } }
@media (prefers-reduced-motion: reduce) { .ops-canvas-node[data-runtime-status="running"] { animation: none; } }
`;
		const statsStyle = {
			display: "grid",
			gridTemplateColumns: "repeat(auto-fit, minmax(min(135px, 100%), 1fr))",
			gap: 8
		};
		const blockStyle = {
			minWidth: 0,
			padding: "9px 12px",
			border: "1px solid #dde4ed",
			borderRadius: 9,
			background: COLOR.surface
		};
		const headingStyle = {
			margin: 0,
			fontSize: FONT_PX.section
		};
		const mutedStyle = {
			color: COLOR.inkMuted,
			margin: "5px 0",
			fontSize: FONT_PX.body
		};
		const sectionHeaderStyle = {
			display: "flex",
			gap: 8,
			justifyContent: "space-between",
			alignItems: "center",
			marginBottom: 7
		};
		const buttonStyle$1 = {
			padding: "6px 10px",
			border: `1px solid ${COLOR.line}`,
			background: COLOR.surface,
			color: COLOR.ink,
			borderRadius: 6,
			cursor: "pointer"
		};
		const statStyle = {
			...blockStyle,
			display: "flex",
			flexWrap: "wrap",
			alignItems: "baseline",
			gap: 8,
			color: COLOR.ink,
			fontSize: FONT_PX.body,
			cursor: "pointer"
		};
		// src/client/panel/settings-screen.tsx
		const UNKNOWN = "未知";
		/** 设置屏只显示真实状态：拿不到的项一律写「未知」，不填猜测值。 */
		function SettingsScreen(props) {
			const rows = buildRows(props);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "settings-title",
				className: "ops-surface",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
					className: "ops-surface-head",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
						id: "settings-title",
						className: "ops-settings-anchor",
						tabIndex: -1,
						children: "服务状态"
					}), props.onRefresh && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						onClick: props.onRefresh,
						className: "ops-button",
						children: "重新检测"
					})]
				}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dl", {
					style: listStyle,
					children: rows.map((row) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: rowStyle,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", {
							style: labelStyle,
							children: row.label
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", {
							style: {
								...valueStyle,
								color: toneColor(row.tone)
							},
							children: row.value
						})]
					}, row.key))
				})]
			});
		}
		function buildRows(props) {
			return [
				{
					key: "service",
					label: "服务连接",
					value: props.serviceConnected === void 0 ? UNKNOWN : props.serviceConnected ? "已连接" : "未连接",
					tone: props.serviceConnected === void 0 ? "plain" : props.serviceConnected ? "ok" : "bad"
				},
				{
					key: "protocol",
					label: "协议版本",
					value: props.protocolMismatch === void 0 ? UNKNOWN : props.protocolMismatch ? "与服务不一致" : "与服务一致",
					tone: props.protocolMismatch === void 0 ? "plain" : props.protocolMismatch ? "bad" : "ok"
				},
				{
					key: "snapshot",
					label: "数据快照",
					value: snapshotValue(props),
					tone: props.snapshotStale === true || props.streamStatus === "stale" ? "warn" : props.streamStatus === void 0 ? "plain" : "ok"
				},
				{
					key: "stream",
					label: "实时事件流",
					value: streamValue(props.streamStatus),
					tone: props.streamStatus === void 0 ? "plain" : props.streamStatus === "live" ? "ok" : "warn"
				},
				{
					key: "phase",
					label: "当前阶段",
					value: props.phase ?? UNKNOWN,
					tone: "plain"
				},
				{
					key: "version",
					label: "面板版本",
					value: props.panelVersion ?? UNKNOWN,
					tone: "plain"
				}
			];
		}
		function snapshotValue(props) {
			if (props.snapshotStale === true) return "可能已过期";
			if (props.streamStatus === void 0) return UNKNOWN;
			if (props.streamStatus === "stale") return "可能已过期";
			if (props.streamStatus === "polling") return "可能滞后";
			return "最新";
		}
		function streamValue(status) {
			if (status === void 0) return UNKNOWN;
			if (status === "live") return "实时更新";
			if (status === "polling") return "轮询更新";
			return "数据可能已过期";
		}
		function toneColor(tone) {
			if (tone === "ok") return COLOR.success;
			if (tone === "warn") return COLOR.warning;
			if (tone === "bad") return COLOR.danger;
			return COLOR.ink;
		}
		const listStyle = {
			margin: 0,
			display: "grid",
			minWidth: 0
		};
		const rowStyle = {
			display: "grid",
			gridTemplateColumns: "minmax(90px, 42%) minmax(0, 1fr)",
			alignItems: "baseline",
			gap: "4px 10px",
			minWidth: 0,
			borderBottom: `1px solid ${COLOR.line}`,
			padding: "7px 12px"
		};
		const labelStyle = {
			margin: 0,
			color: COLOR.inkMuted,
			fontSize: FONT_PX.menu
		};
		const valueStyle = {
			margin: 0,
			fontSize: FONT_PX.body,
			fontWeight: 600
		};
		// src/client/panel/model-routing-settings.tsx
		const PATH$2 = "/v1/settings/model-routing";
		function draftValue(draft, value, latest, equals = Object.is) {
			if (equals(value, latest)) return void 0;
			return {
				base: draft ? draft.base : latest,
				value
			};
		}
		function conflicting(draft, latest, equals = Object.is) {
			return !!draft && !equals(draft.base, latest) && !equals(draft.value, latest);
		}
		function sameRoute(left, right) {
			return left?.ref === right?.ref && left?.effort === right?.effort;
		}
		const sameNote = (left, right) => left.trim() === right.trim();
		function providerId(profile) {
			if (profile.provider) return profile.provider;
			const slash = profile.ref.indexOf("/");
			return slash > 0 ? profile.ref.slice(0, slash) : "";
		}
		function providerLabel(profile) {
			const id = providerId(profile);
			return profile.provider_name && profile.provider_name !== id ? `${profile.provider_name} (${id})` : id || "当前宿主";
		}
		function modelName({ ref, model, name }) {
			const id = model ?? ref.slice(ref.indexOf("/") + 1);
			return name && name !== id ? `${name} (${id})` : id;
		}
		function modelLabel(profile) {
			return providerId(profile) ? `${providerLabel(profile)} / ${modelName(profile)}` : modelName(profile);
		}
		function presetOnly(profile) {
			const reasoning = profile?.reasoning;
			if (reasoning?.efforts.length === 1 && reasoning.efforts[0]?.id === reasoning.defaultEffort) return reasoning.efforts[0];
		}
		function ModelPicker({ scope, profiles, refValue, locked, onPick }) {
			const selected = profiles.find(({ ref }) => ref === refValue);
			const provider = selected ? providerId(selected) : void 0;
			const providers = new Map(profiles.map((profile) => [providerId(profile), providerLabel(profile)]));
			const choices = profiles.filter((profile) => providerId(profile) === provider);
			const unavailable = "::unavailable::";
			const label = scope === "planner" ? "主脑" : "子智能体";
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				style: editorSelectors,
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
					style: fieldList,
					children: ["供应商", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
						id: `model-${scope}-provider`,
						"aria-label": `${label}供应商`,
						className: "ops-field",
						value: provider ?? unavailable,
						disabled: locked,
						onChange: (event) => {
							const profile = profiles.find((item) => providerId(item) === event.target.value);
							if (profile) onPick(profile.ref);
						},
						children: [provider === void 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
							value: unavailable,
							disabled: true,
							children: refValue ? "已保存供应商不可用" : "请选择供应商"
						}), [...providers].map(([id, name]) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
							value: id,
							children: name
						}, id))]
					})]
				}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
					style: fieldList,
					children: ["模型", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
						id: `model-${scope}-model`,
						"aria-label": `${label}模型`,
						className: "ops-field",
						value: refValue,
						disabled: locked || !choices.length,
						onChange: (event) => onPick(event.target.value),
						children: [!selected && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
							value: refValue,
							children: refValue ? `已不可用 · ${refValue}` : "请选择模型"
						}), choices.map((profile) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
							value: profile.ref,
							children: modelName(profile)
						}, profile.ref))]
					})]
				})]
			});
		}
		function ModelRoutingSettings({ client, reloadVersion = 0, onSnapshot, plannerHeadingRef, submodelHeadingRef }) {
			const [saved, setSaved] = (0, react.useState)();
			const [modelDrafts, setModelDrafts] = (0, react.useState)(/* @__PURE__ */ new Map());
			const [tierDrafts, setTierDrafts] = (0, react.useState)({});
			const [editorRef, setEditorRef] = (0, react.useState)();
			const [plannerDraft, setPlannerDraft] = (0, react.useState)();
			const [reload, setReload] = (0, react.useState)(0);
			const [loadedVersion, setLoadedVersion] = (0, react.useState)();
			const [busy, setBusy] = (0, react.useState)(false);
			const [message, setMessage] = (0, react.useState)("");
			const [error, setError] = (0, react.useState)(false);
			const version = reloadVersion + ":" + reload;
			const profiles = saved?.profiles ?? [];
			const editor = profiles.find(({ ref }) => ref === editorRef) ?? profiles[0];
			const plannerConfigurable = saved?.planner_model_configurable === true;
			const persistedPlanner = saved?.planner_model ?? null;
			const planner = plannerDraft === void 0 ? persistedPlanner : plannerDraft.value;
			const plannerChanged = plannerConfigurable && !sameRoute(planner, persistedPlanner);
			const plannerProfile = profiles.find(({ ref }) => ref === planner?.ref);
			const plannerReasoning = plannerProfile?.reasoning;
			const plannerPreset = presetOnly(plannerProfile);
			const plannerNativeDefault = plannerReasoning?.efforts.find(({ id }) => id === plannerReasoning.defaultEffort);
			const invalidPlannerEffort = !!planner?.effort && !plannerReasoning?.efforts.some(({ id }) => id === planner.effort);
			const invalidPlannerDraft = plannerChanged && planner !== null && (!plannerProfile || invalidPlannerEffort);
			const available = new Map(profiles.map((profile) => [profile.ref, profile]));
			const savedNotes = new Map(saved?.model_notes?.map(({ ref, note }) => [ref, note]));
			const savedCapacities = new Map(saved?.model_capacities?.map(({ ref, capacity }) => [ref, capacity]));
			const savedDefaults = new Map(saved?.model_reasoning_defaults?.map(({ ref, effort }) => [ref, effort]));
			function capacityOf(ref) {
				const capacity = modelDrafts.get(ref)?.capacity?.value;
				return capacity === void 0 ? savedCapacities.get(ref) ?? null : capacity;
			}
			function effortOf(ref) {
				const effort = modelDrafts.get(ref)?.effort?.value;
				return effort === void 0 ? savedDefaults.get(ref) ?? null : effort;
			}
			const noteOf = (ref) => modelDrafts.get(ref)?.note?.value ?? savedNotes.get(ref) ?? "";
			const changedModels = [...modelDrafts].filter(([ref]) => available.has(ref));
			const changedNotes = changedModels.flatMap(([ref, entry]) => {
				const note = entry.note?.value.trim();
				return note === void 0 || note === (savedNotes.get(ref) ?? "") ? [] : [{
					ref,
					note
				}];
			});
			const changedCapacities = changedModels.flatMap(([ref, entry]) => {
				const capacity = entry.capacity?.value;
				return capacity === void 0 || capacity === (savedCapacities.get(ref) ?? null) ? [] : [{
					ref,
					capacity
				}];
			});
			const changedDefaults = changedModels.flatMap(([ref, entry]) => {
				const effort = entry.effort?.value;
				return effort === void 0 || effort === (savedDefaults.get(ref) ?? null) ? [] : [{
					ref,
					effort
				}];
			});
			function savedTier(level) {
				return {
					ref: saved?.mapping[level] ?? null,
					effort: saved?.reasoning_efforts?.[level] ?? null
				};
			}
			const mapping = Object.fromEntries(MODEL_COMPLEXITIES.map((level) => [level, (tierDrafts[level]?.value ?? savedTier(level)).ref]));
			const changedEfforts = MODEL_COMPLEXITIES.flatMap((level) => {
				const edit = tierDrafts[level]?.value;
				return !edit || sameRoute(edit, savedTier(level)) ? [] : [[level, edit.effort]];
			});
			const liveCatalog = saved?.unavailable_providers !== void 0;
			const capacityFor = (profile) => capacityOf(profile.ref) ?? (profile.host_capacity === void 0 ? profile.capacity : profile.host_capacity);
			const dirty = plannerChanged || changedEfforts.length + changedNotes.length + changedCapacities.length + changedDefaults.length > 0;
			const unavailableDrafts = modelDrafts.size - changedModels.length;
			const locked = busy || loadedVersion !== version;
			const updateModel = (ref, update) => setModelDrafts((current) => {
				const next = new Map(current);
				const model = update(current.get(ref) ?? {});
				if (model.note || model.capacity || model.effort) next.set(ref, model);
				else next.delete(ref);
				return next;
			});
			function editModel(ref, edit) {
				updateModel(ref, (model) => ({
					note: edit.note === void 0 ? model.note : draftValue(model.note, edit.note, savedNotes.get(ref) ?? ""),
					capacity: edit.capacity === void 0 ? model.capacity : draftValue(model.capacity, edit.capacity, savedCapacities.get(ref) ?? null),
					effort: edit.effort === void 0 ? model.effort : draftValue(model.effort, edit.effort, savedDefaults.get(ref) ?? null)
				}));
			}
			const editTier = (level, edit) => setTierDrafts((current) => {
				return {
					...current,
					[level]: draftValue(current[level], edit, savedTier(level), sameRoute)
				};
			});
			const editPlanner = (edit) => setPlannerDraft((current) => draftValue(current, edit, persistedPlanner, sameRoute));
			const conflicts = [];
			const describeRoute = (choice, fallback) => {
				if (!choice?.ref) return fallback;
				const profile = available.get(choice.ref);
				const effort = profile?.reasoning?.efforts.find(({ id }) => id === choice.effort)?.name ?? choice.effort;
				return `${profile ? modelLabel(profile) : choice.ref} · ${effort ?? "默认思考深度"}`;
			};
			if (plannerConfigurable && plannerDraft && conflicting(plannerDraft, persistedPlanner, sameRoute)) conflicts.push({
				key: "planner",
				label: "主脑模型",
				latest: describeRoute(persistedPlanner, "跟随当前会话"),
				mine: describeRoute(plannerDraft.value, "跟随当前会话"),
				resolve: (keep) => setPlannerDraft(keep ? {
					base: persistedPlanner,
					value: plannerDraft.value
				} : void 0)
			});
			for (const level of MODEL_COMPLEXITIES) {
				const latest = savedTier(level);
				const edit = tierDrafts[level];
				if (edit && conflicting(edit, latest, sameRoute)) conflicts.push({
					key: level,
					label: MODEL_LEVELS[level].label + "档分配",
					latest: describeRoute(latest, "自动选择"),
					mine: describeRoute(edit.value, "自动选择"),
					resolve: (keep) => setTierDrafts((current) => ({
						...current,
						[level]: keep ? {
							base: latest,
							value: edit.value
						} : void 0
					}))
				});
			}
			for (const [ref, draft] of changedModels) {
				const profile = available.get(ref);
				const label = modelLabel(profile);
				function addField(key, title, edit, latest, format, equals = Object.is) {
					if (!edit || !conflicting(edit, latest, equals)) return;
					conflicts.push({
						key: `${ref}:${key}`,
						label: `${label} · ${title}`,
						latest: format(latest),
						mine: format(edit.value),
						resolve: (keep) => updateModel(ref, (model) => ({
							...model,
							[key]: keep ? {
								base: latest,
								value: edit.value
							} : void 0
						}))
					});
				}
				addField("note", "用途说明", draft.note, savedNotes.get(ref) ?? "", (value) => value.trim() || "空白", sameNote);
				addField("capacity", "最大能力", draft.capacity, savedCapacities.get(ref) ?? null, (value) => value ? MODEL_LEVELS[value].label : "宿主默认");
				addField("effort", "默认思考深度", draft.effort, savedDefaults.get(ref) ?? null, (value) => profile.reasoning?.efforts.find(({ id }) => id === value)?.name ?? value ?? "宿主默认");
			}
			const editorEffort = editor ? effortOf(editor.ref) : null;
			const editorReasoning = editor?.reasoning;
			const editorPreset = presetOnly(editor);
			const editorHostCapacity = editor?.host_capacity === void 0 ? editor?.capacity : editor.host_capacity;
			const nativeDefault = editorReasoning?.efforts.find(({ id }) => id === editorReasoning.defaultEffort);
			const invalidEditorEffort = !!editorEffort && !editorReasoning?.efforts.some(({ id }) => id === editorEffort);
			(0, react.useEffect)(() => {
				let active = true;
				client.get(PATH$2).then(({ data, as_of_seq }) => {
					if (active) {
						setSaved(data);
						const modelRefs = new Set(data.profiles.map(({ ref }) => ref));
						const notes = new Map(data.model_notes?.map(({ ref, note }) => [ref, note]));
						const capacities = new Map(data.model_capacities?.map(({ ref, capacity }) => [ref, capacity]));
						const defaults = new Map(data.model_reasoning_defaults?.map(({ ref, effort }) => [ref, effort]));
						setModelDrafts((current) => {
							const next = new Map(current);
							for (const [ref, model] of current) {
								if (!modelRefs.has(ref)) continue;
								const note = model.note && !sameNote(model.note.value, notes.get(ref) ?? "") ? model.note : void 0;
								const capacity = model.capacity?.value === (capacities.get(ref) ?? null) ? void 0 : model.capacity;
								const effort = model.effort?.value === (defaults.get(ref) ?? null) ? void 0 : model.effort;
								if (note || capacity || effort) next.set(ref, {
									note,
									capacity,
									effort
								});
								else next.delete(ref);
							}
							return next;
						});
						setTierDrafts((current) => {
							const next = { ...current };
							for (const level of MODEL_COMPLEXITIES) if (sameRoute(current[level]?.value ?? null, {
								ref: data.mapping[level],
								effort: data.reasoning_efforts?.[level] ?? null
							})) delete next[level];
							return next;
						});
						if (data.planner_model_configurable === true) setPlannerDraft((current) => current && sameRoute(current.value, data.planner_model ?? null) ? void 0 : current);
						setLoadedVersion(version);
						onSnapshot?.(as_of_seq);
					}
				}).catch((failure) => {
					if (active) {
						setError(true);
						setMessage(failure instanceof Error ? failure.message : "模型配置暂时无法读取。请重新读取。");
					}
				});
				return () => {
					active = false;
				};
			}, [
				client,
				onSnapshot,
				version
			]);
			async function save() {
				if (!saved || !dirty || locked || invalidPlannerDraft || conflicts.length) return;
				setBusy(true);
				setMessage("");
				setError(false);
				try {
					await postWithOperationReceipt(client, PATH$2, "model_routing.save", {
						expected_revision: saved.revision,
						mapping,
						operation_id: newOperationId(),
						...changedNotes.length > 0 ? { model_notes: changedNotes } : {},
						...changedCapacities.length > 0 ? { model_capacities: changedCapacities } : {},
						...changedDefaults.length > 0 ? { model_reasoning_defaults: changedDefaults } : {},
						...plannerChanged ? { planner_model: planner } : {},
						...changedEfforts.length > 0 && (changedEfforts.some(([, effort]) => effort !== null) || liveCatalog || !!saved.reasoning_efforts) ? { reasoning_efforts: Object.fromEntries(changedEfforts) } : {}
					});
					setTierDrafts({});
					if (plannerChanged) setPlannerDraft(void 0);
					setModelDrafts((current) => new Map([...current].filter(([ref]) => !available.has(ref))));
					setMessage("已保存，下一次派发采用新配置。用途说明供后续任务规划参考。");
					setReload((value) => value + 1);
				} catch (failure) {
					setError(true);
					setMessage(failure instanceof Error ? failure.message : "保存结果暂不可确认。");
				} finally {
					setBusy(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				"aria-labelledby": "model-routing-title",
				className: "ops-surface",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", {
					className: "ops-surface-head",
					children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
						id: "model-routing-title",
						children: "模型设置"
					})
				}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
					className: "ops-inset ops-stack",
					children: [saved ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
						onSubmit: (event) => {
							event.preventDefault();
							save();
						},
						style: form,
						children: [
							liveCatalog && saved.profiles.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								role: "status",
								children: "DSH 当前没有提供可用模型。请在 DSH 模型设置中检查供应商连接，再重新读取。"
							}),
							!!saved.unavailable_providers?.length && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								role: "status",
								children: [
									"部分供应商的目录读取失败（",
									saved.unavailable_providers.join("、"),
									"）。请检查 DSH 中的连接后重新读取。"
								]
							}),
							conflicts.length > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
								"aria-labelledby": "model-conflicts-title",
								className: "ops-inset",
								style: fieldList,
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
										id: "model-conflicts-title",
										children: "模型设置有冲突"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										role: "alert",
										children: "其他操作已修改以下设置。请逐项选择采用最新设置，或确认保存时覆盖为你的修改。"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
										style: fieldList,
										children: conflicts.map(({ key, label, latest, mine, resolve }) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("li", { children: [
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("strong", { children: label }),
											/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
												className: "ops-meta",
												style: { overflowWrap: "anywhere" },
												children: [
													/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "最新设置" }),
													/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: latest }),
													/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "我的修改" }),
													/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: mine })
												]
											}),
											/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
												style: conflictActions,
												children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
													type: "button",
													className: "ops-button",
													disabled: locked,
													onClick: () => resolve(false),
													children: "采用最新设置"
												}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
													type: "button",
													className: "ops-button",
													disabled: locked,
													onClick: () => resolve(true),
													children: "保留我的修改"
												})]
											})
										] }, key))
									})
								]
							}),
							plannerConfigurable && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
								"aria-labelledby": "planner-routing-title",
								className: "ops-inset",
								style: fieldList,
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
										id: "planner-routing-title",
										ref: plannerHeadingRef,
										className: "ops-settings-anchor",
										tabIndex: -1,
										children: "主脑模型"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										className: "ops-meta",
										children: "用于生成任务计划、流程草稿和返工建议。"
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
										style: field,
										children: ["模型来源", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
											id: "model-planner-mode",
											className: "ops-field",
											value: planner ? "fixed" : "session",
											disabled: locked,
											onChange: (event) => editPlanner(event.target.value === "fixed" ? persistedPlanner ?? {
												ref: profiles[0]?.ref ?? "",
												effort: null
											} : null),
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
												value: "session",
												children: "跟随当前会话（默认）"
											}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
												value: "fixed",
												children: "指定主脑模型"
											})]
										})]
									}),
									planner && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)(ModelPicker, {
											scope: "planner",
											profiles,
											refValue: planner.ref,
											locked,
											onPick: (ref) => editPlanner({
												ref,
												effort: null
											})
										}),
										!plannerProfile && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
											className: "ops-meta",
											children: "当前目录没有已选主脑模型。请重新选择，或恢复跟随当前会话。"
										}),
										plannerProfile && (!!planner.effort || !plannerPreset && !!plannerReasoning?.efforts.length ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
											style: field,
											children: [
												/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
													htmlFor: "model-planner-effort",
													children: "思考深度"
												}),
												/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
													id: "model-planner-effort",
													className: "ops-field",
													value: planner.effort ?? "",
													disabled: locked,
													onChange: (event) => editPlanner({
														ref: planner.ref,
														effort: event.target.value || null
													}),
													children: [
														/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
															value: "",
															children: ["宿主默认", plannerNativeDefault ? `（${plannerNativeDefault.name}）` : ""]
														}),
														invalidPlannerEffort && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
															value: planner.effort,
															children: ["已不可用 · ", planner.effort]
														}),
														plannerReasoning?.efforts.map(({ id, name }) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
															value: id,
															children: name
														}, id))
													]
												}),
												invalidPlannerEffort && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
													className: "ops-meta",
													style: fieldHint,
													children: "请改选宿主默认或当前支持的思考档。"
												})
											]
										}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
											className: "ops-meta",
											children: plannerPreset ? `该模型只提供 ${plannerPreset.name}，使用宿主默认。` : "该模型未提供可调思考档，使用宿主默认。"
										}))
									] })
								]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h3", {
								id: "submodel-routing-title",
								ref: submodelHeadingRef,
								className: "ops-settings-anchor",
								tabIndex: -1,
								children: "子智能体模型分配"
							}),
							unavailableDrafts > 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								role: "status",
								children: [
									"有 ",
									unavailableDrafts,
									" 个模型的草稿已保留，模型重新出现在目录后可继续编辑。"
								]
							}),
							editor && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("fieldset", {
								className: "ops-inset",
								style: fieldList,
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("legend", { children: "编辑子智能体模型" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)(ModelPicker, {
										scope: "profile",
										profiles,
										refValue: editor.ref,
										locked,
										onPick: setEditorRef
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
										style: field,
										children: ["最大能力", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
											id: "model-profile-capacity",
											className: "ops-field",
											"aria-label": `${modelLabel(editor)} 最大能力`,
											disabled: locked,
											value: capacityOf(editor.ref) ?? "",
											onChange: (event) => editModel(editor.ref, { capacity: event.target.value ? event.target.value : null }),
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
												value: "",
												children: editorHostCapacity ? `宿主默认（${MODEL_LEVELS[editorHostCapacity].label}）` : "未评估"
											}), MODEL_COMPLEXITIES.map((level) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
												value: level,
												children: MODEL_LEVELS[level].label
											}, level))]
										})]
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
										style: field,
										children: ["用途说明", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
											className: "ops-field",
											"aria-label": `${modelLabel(editor)} 用途说明`,
											"aria-describedby": "model-note-help",
											disabled: locked,
											maxLength: 160,
											value: noteOf(editor.ref),
											onChange: (event) => editModel(editor.ref, { note: event.target.value })
										})]
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
										id: "model-note-help",
										className: "ops-meta",
										children: [
											"用途可留空，每条最多 ",
											160,
											" 字，供主脑选择模型时参考。"
										]
									}),
									!!editorEffort || !editorPreset && !!editorReasoning?.efforts.length ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
										style: field,
										children: [
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
												htmlFor: "model-profile-effort",
												children: "默认思考深度"
											}),
											/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
												id: "model-profile-effort",
												className: "ops-field",
												disabled: locked,
												value: editorEffort ?? "",
												onChange: (event) => editModel(editor.ref, { effort: event.target.value || null }),
												children: [
													/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
														value: "",
														children: ["宿主默认", nativeDefault ? `（${nativeDefault.name}）` : ""]
													}),
													invalidEditorEffort && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
														value: editorEffort,
														children: ["已不可用 · ", editorEffort]
													}),
													editorReasoning?.efforts.map(({ id, name }) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
														value: id,
														children: name
													}, id))
												]
											}),
											invalidEditorEffort && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
												className: "ops-meta",
												style: fieldHint,
												children: "请改选宿主默认或模型当前支持的思考档。"
											})
										]
									}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										className: "ops-meta",
										children: editorPreset ? `该模型只提供 ${editorPreset.name}，使用宿主默认。` : "该模型未提供可调思考档，使用宿主默认。"
									})
								]
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								className: "ops-meta",
								children: "自动分配采用合格的主脑候选，其次选满足要求的最低档。常规至进阶在无合格候选时可沿用当前 DSH 会话已授权且未评估的模型。分配时优先使用当前复杂度的思考档，其次使用模型配置，最后沿用宿主默认。"
							}),
							MODEL_COMPLEXITIES.map((level, index) => {
								const { label, description } = MODEL_LEVELS[level];
								const eligible = profiles.filter((item) => {
									const capacity = capacityFor(item);
									return capacity !== null && MODEL_COMPLEXITIES.indexOf(capacity) >= index;
								});
								const session = profiles.find(({ ref }) => ref === saved?.session_execution_model_ref);
								const usesSession = eligible.length === 0 && session && capacityFor(session) === null && index <= MODEL_COMPLEXITIES.indexOf("advanced");
								const current = mapping[level];
								const selected = eligible.find(({ ref }) => current === ref);
								const reasoning = selected?.reasoning;
								const preset = presetOnly(selected);
								const effort = (tierDrafts[level]?.value ?? savedTier(level)).effort ?? "";
								const modelDefault = selected ? effortOf(selected.ref) : null;
								const defaultId = modelDefault ?? reasoning?.defaultEffort;
								const defaultLabel = reasoning?.efforts.find(({ id }) => id === defaultId)?.name ?? (modelDefault ? `${modelDefault} 已不可用` : void 0);
								const effortUnavailable = !!effort && !reasoning?.efforts.some(({ id }) => id === effort);
								return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
									style: field,
									children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
											htmlFor: `model-route-${level}`,
											children: label
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
											id: `model-route-${level}`,
											className: "ops-field",
											"aria-describedby": `model-capacity-${level}`,
											disabled: locked,
											value: current ?? "",
											onChange: (event) => editTier(level, {
												ref: event.target.value || null,
												effort: null
											}),
											children: [
												/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
													value: "",
													children: ["自动选择", usesSession ? "（沿用当前会话模型）" : eligible.length === 0 ? "（暂无候选）" : ""]
												}),
												current && !selected && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
													value: current,
													children: ["已不可用 · ", current]
												}),
												eligible.map((item) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
													value: item.ref,
													children: modelLabel(item)
												}, item.ref))
											]
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("span", {
											id: `model-capacity-${level}`,
											className: "ops-meta",
											style: fieldHint,
											children: [
												description,
												usesSession && `。当前会话模型为 ${modelLabel(session)}，能力未评估。`,
												eligible.length === 0 && !usesSession && "。可调整模型的最大能力，或在 DSH 中配置更多模型后重新读取。",
												current && !selected && "。已保存的模型已移除或能力不足，请改选模型或自动选择。"
											]
										}),
										selected && (!!effort || !preset && !!reasoning?.efforts.length) ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
											/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
												htmlFor: `model-effort-${level}`,
												className: "ops-meta",
												children: "思考深度"
											}),
											/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
												id: `model-effort-${level}`,
												"aria-label": `${label}思考深度`,
												className: "ops-field",
												value: effort,
												disabled: locked,
												onChange: (event) => editTier(level, {
													ref: current,
													effort: event.target.value || null
												}),
												children: [
													/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
														value: "",
														children: ["模型默认", defaultLabel ? `（${defaultLabel}）` : ""]
													}),
													effortUnavailable && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
														value: effort,
														children: ["已不可用 · ", effort]
													}),
													reasoning?.efforts.map(({ id, name }) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
														value: id,
														children: name
													}, id))
												]
											}),
											effortUnavailable && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
												className: "ops-meta",
												style: fieldHint,
												children: "该模型已移除所保存的思考档，请改选模型默认或当前支持的档位。"
											})
										] }) : liveCatalog && selected && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
											className: "ops-meta",
											style: fieldHint,
											children: preset ? `该模型只提供 ${preset.name}，使用模型默认。` : "该模型未提供可调思考档，使用模型默认。"
										})
									]
								}, level);
							}),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								style: {
									display: "flex",
									gap: 8
								},
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
									type: "submit",
									className: "ops-button ops-button--primary",
									disabled: locked || !dirty || invalidPlannerDraft || conflicts.length > 0,
									children: "保存模型分配"
								}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									className: "ops-button",
									disabled: busy,
									onClick: () => {
										setError(false);
										setMessage("");
										setReload((value) => value + 1);
									},
									children: "重新读取"
								})]
							})
						]
					}) : error ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						className: "ops-button",
						onClick: () => {
							setError(false);
							setMessage("");
							setReload((value) => value + 1);
						},
						children: "重新读取"
					}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取宿主模型…"
					}), message && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: error ? "alert" : "status",
						children: message
					})]
				})]
			});
		}
		const form = {
			display: "grid",
			gap: 7
		};
		const field = {
			display: "grid",
			gridTemplateColumns: "85px minmax(0, 1fr)",
			alignItems: "center",
			gap: 8
		};
		const fieldHint = { gridColumn: 2 };
		const fieldList = {
			display: "grid",
			minWidth: 0,
			gap: 6
		};
		const editorSelectors = {
			display: "grid",
			gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
			gap: 8
		};
		const conflictActions = {
			display: "flex",
			flexWrap: "wrap",
			gap: 8
		};
		// src/client/panel/planning-timeout-styles.ts
		const PLANNING_TIMEOUT_STYLES = `
.planning-timeouts { container: planning-timeouts / inline-size; }
.planning-timeouts-form { display: grid; gap: 16px; padding: 14px 16px; }
.planning-timeouts-intro, .planning-timeouts-status, .planning-timeouts-context p { margin: 0; }
.planning-timeouts-context {
  display: grid; gap: 8px; padding: 9px 12px;
  background: #f5f8fc; border: 1px solid #e3e9f1; border-radius: 7px;
  font-size: ${FONT_PX.menu}px;
}
.planning-timeouts-context dl { display: flex; flex-wrap: wrap; gap: 6px 20px; margin: 0; }
.planning-timeouts-context dl div { display: flex; gap: 6px; }
.planning-timeouts-context dt { color: ${COLOR.inkMuted}; }
.planning-timeouts-context dd { margin: 0; font-weight: 650; }
.planning-timeouts-group { min-inline-size: 0; margin: 0; padding: 0; border: 0; }
.planning-timeouts-group legend {
  padding: 0; margin-block-end: 3px; font-weight: 700; font-size: ${FONT_PX.section}px;
}
.planning-timeouts-row {
  display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: start;
  column-gap: 16px; row-gap: 3px; padding-block: 10px; border-top: 1px solid ${COLOR.line};
}
.planning-timeouts-row label { font-weight: 650; align-self: center; grid-column: 1; grid-row: 1; }
.planning-timeouts-value {
  grid-column: 2; grid-row: 1 / span 2; display: flex; align-items: center;
  gap: 7px; font-variant-numeric: tabular-nums;
}
.planning-timeouts-value input { inline-size: 7ch; min-inline-size: 66px; }
.planning-timeouts-value input[aria-invalid=true] { border-color: ${COLOR.danger}; }
.planning-timeouts-value span, .planning-timeouts-help { color: ${COLOR.inkMuted}; }
.planning-timeouts-help { grid-column: 1; font-size: ${FONT_PX.menu}px; }
.planning-timeouts-help p { margin: 0; }
.planning-timeouts-help p + p { margin-block-start: 2px; font-size: ${FONT_PX.helper}px; }
.planning-timeouts-footer { display: grid; gap: 8px; padding-top: 12px; border-top: 1px solid ${COLOR.line}; }
.planning-timeouts-status { color: ${COLOR.inkMuted}; font-size: ${FONT_PX.menu}px; }
.planning-timeouts-status[role=alert] { color: ${COLOR.danger}; }
@container planning-timeouts (max-width: 460px) {
  .planning-timeouts-form { padding-inline: 12px; }
  .planning-timeouts-row { row-gap: 6px; }
  .planning-timeouts-value { grid-row: 1; }
  .planning-timeouts-help { grid-column: 1 / -1; }
}
`;
		// src/client/panel/planning-timeout-settings.tsx
		const PATH$1 = "/v1/settings/planning-timeouts";
		const groups = [
			{
				title: "非流式阶段",
				fields: [[
					"task_seconds",
					"任务规划",
					"每次调用主脑的时限，也适用于任务内自动生成流程。"
				], [
					"workflow_seconds",
					"独立流程生成",
					"从流程页单独生成草稿时，每次调用主脑的时限。"
				]]
			},
			{
				title: "流式阶段",
				fields: [[
					"idle_seconds",
					"无新输出等待",
					"从阶段开始计时，收到新的推理或正文后重新计时。"
				], [
					"stage_seconds",
					"单阶段总时长",
					"从调用主脑起计时，持续输出也不会延长此上限。"
				]]
			},
			{
				title: "请求上限",
				fields: [[
					"request_seconds",
					"整次规划总时长",
					"从宿主开始处理请求起计时，涵盖选择流程、自动生成流程和规划。"
				]]
			}
		];
		const fields$1 = groups.flatMap((group) => [...group.fields]);
		const branchNames = {
			streaming: "流式",
			fixed: "非流式",
			unavailable: "未装配"
		};
		const toDraft$1 = (values) => Object.fromEntries(fields$1.map(([key]) => [key, String(values[key])]));
		function PlanningTimeoutSettings({ client, reloadVersion = 0, onSnapshot }) {
			const [saved, setSaved] = (0, react.useState)();
			const [draft, setDraft] = (0, react.useState)();
			const [reload, setReload] = (0, react.useState)(0);
			const [loaded, setLoaded] = (0, react.useState)("");
			const [busy, setBusy] = (0, react.useState)(false);
			const [pending, setPending] = (0, react.useState)();
			const [notice, setNotice] = (0, react.useState)();
			const version = reloadVersion + ":" + reload;
			(0, react.useEffect)(() => {
				let active = true;
				client.get(PATH$1).then(({ data, as_of_seq }) => {
					if (!active) return;
					setSaved(data);
					setDraft(toDraft$1(data.values));
					setLoaded(version);
					onSnapshot?.(as_of_seq);
				}).catch(() => {
					if (active) setNotice({
						message: "暂时无法读取规划时限。",
						error: true
					});
				});
				return () => {
					active = false;
				};
			}, [
				client,
				onSnapshot,
				version
			]);
			async function save(reset) {
				if (!saved || !draft || busy || !pending && loaded !== version) return;
				let values = null;
				if (!pending && !reset) {
					values = Object.fromEntries(fields$1.map(([key]) => [key, Number(draft[key])]));
					const invalid = fields$1.find(([key]) => !Number.isInteger(values[key]) || values[key] < 1 || values[key] > saved.limits[key]);
					if (invalid || values.idle_seconds > values.stage_seconds || values.stage_seconds > values.request_seconds || values.task_seconds > values.request_seconds || values.workflow_seconds > values.request_seconds) {
						setNotice({
							error: true,
							field: invalid?.[0],
							message: invalid ? `${invalid[1]}请输入 1–${saved.limits[invalid[0]]} 的整数秒。` : "无新输出等待不能长于流式单阶段总时长。任何单阶段时限都不能长于整次规划总时长。"
						});
						return;
					}
				}
				const command = pending ?? {
					operation_id: newOperationId(),
					expected_revision: saved.revision,
					values
				};
				setBusy(true);
				setNotice(void 0);
				try {
					await postWithOperationReceipt(client, PATH$1, "planning_timeouts.save", command);
					setPending(void 0);
					setNotice({ message: command.values ? "已保存。仅从新发起的规划请求开始生效。" : "已恢复默认。仅从新发起的规划请求开始生效。" });
					setReload((value) => value + 1);
				} catch (error) {
					setPending(error instanceof ApiError && error.code === "OPERATION_OUTCOME_UNKNOWN" ? command : void 0);
					setNotice({
						error: true,
						message: error instanceof Error ? error.message : "保存结果暂不可确认。"
					});
				} finally {
					setBusy(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "ops-surface planning-timeouts",
				"aria-labelledby": "planning-timeouts-title",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("style", { children: PLANNING_TIMEOUT_STYLES }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", {
						className: "ops-surface-head",
						children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
							id: "planning-timeouts-title",
							className: "ops-settings-anchor",
							tabIndex: -1,
							children: "规划时限"
						})
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
						className: "planning-timeouts-form",
						noValidate: true,
						onSubmit: (event) => {
							event.preventDefault();
							save(false);
						},
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								className: "planning-timeouts-intro",
								children: "仅对当前会话中新发起的任务规划和流程草稿生成生效。已开始的规划、任务执行、返工与记忆整理沿用各自的时限。"
							}),
							saved && draft && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "planning-timeouts-context",
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
									"aria-label": "当前宿主规划方式",
									children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "任务规划" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: branchNames[saved.branches.task] })] }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "自动生成流程" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: branchNames[saved.branches.automatic_workflow] })] }),
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "独立流程生成" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", { children: branchNames[saved.branches.standalone_workflow] })] })
									]
								}), saved.legacy_timeout_ms !== null && !saved.overridden && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
									"未保存自定义时限时，任务内的各阶段沿用宿主设定的 ",
									saved.legacy_timeout_ms / 1e3,
									" 秒。独立流程生成使用下列默认值。"
								] })]
							}), groups.map(({ title, fields: groupFields }) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("fieldset", {
								className: "planning-timeouts-group",
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("legend", { children: title }), groupFields.map(([key, label, description]) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
									className: "planning-timeouts-row",
									children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
											htmlFor: `planning-${key}`,
											children: label
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
											className: "planning-timeouts-value",
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
												id: `planning-${key}`,
												className: "ops-field",
												type: "number",
												min: 1,
												max: saved.limits[key],
												step: 1,
												"aria-describedby": `budget-${key} budget-limits-${key}`,
												"aria-invalid": notice?.field === key || void 0,
												disabled: busy || Boolean(pending) || loaded !== version,
												value: draft[key],
												onChange: (event) => setDraft({
													...draft,
													[key]: event.target.value
												})
											}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
												"aria-hidden": "true",
												children: "秒"
											})]
										}),
										/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
											className: "planning-timeouts-help",
											children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
												id: `budget-${key}`,
												children: description
											}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
												id: `budget-limits-${key}`,
												children: [
													"范围 1–",
													saved.limits[key],
													" 秒 · 默认 ",
													saved.defaults[key],
													" 秒"
												]
											})]
										})
									]
								}, key))]
							}, title))] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "planning-timeouts-footer",
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
										className: "ops-actions",
										children: [saved && draft && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
											className: "ops-button ops-button--primary",
											type: "submit",
											disabled: busy || !pending && loaded !== version,
											children: pending ? "查询或重试" : "保存规划时限"
										}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
											className: "ops-button",
											type: "button",
											disabled: busy || Boolean(pending) || loaded !== version || !saved.overridden,
											onClick: () => {
												save(true);
											},
											children: "恢复默认"
										})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
											className: "ops-button ops-button--link",
											type: "button",
											disabled: busy || Boolean(pending),
											onClick: () => {
												setNotice(void 0);
												setReload((value) => value + 1);
											},
											children: "重新读取规划时限"
										})]
									}),
									busy && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										className: "planning-timeouts-status",
										role: "status",
										children: "正在确认操作结果…"
									}),
									!busy && !notice && loaded !== version && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										className: "planning-timeouts-status",
										role: "status",
										children: "正在读取规划时限…"
									}),
									notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
										className: "planning-timeouts-status",
										role: notice.error ? "alert" : "status",
										children: notice.message
									})
								]
							})
						]
					})
				]
			});
		}
		// src/client/api/chat-routing.ts
		const path$1 = "/v1/settings/chat-routing";
		function validated(data) {
			if (typeof data?.enabled !== "boolean" || !Number.isSafeInteger(data.revision) || data.revision < 0) throw new ApiError("PROTOCOL_VERSION_MISMATCH", "无法确认自动接管设置", false);
			return data;
		}
		function createChatRoutingApi(client) {
			return {
				async read() {
					return validated((await client.get(path$1)).data);
				},
				async save(enabled, revision) {
					const saved = validated((await client.post(path$1, {
						enabled,
						expected_revision: revision
					})).data);
					if (saved.enabled !== enabled || saved.revision !== revision + 1) throw new ApiError("OPERATION_OUTCOME_UNKNOWN", "无法确认自动接管设置是否已保存", true);
					return saved;
				}
			};
		}
		// src/client/panel/chat-routing-settings.tsx
		function ChatRoutingSettings({ client, reloadVersion, writable }) {
			const api = (0, react.useMemo)(() => createChatRoutingApi(client), [client]);
			const [reload, setReload] = (0, react.useState)(0);
			const [saved, setSaved] = (0, react.useState)();
			const [failure, setFailure] = (0, react.useState)();
			const [busy, setBusy] = (0, react.useState)(false);
			const version = `${reloadVersion}:${reload}`;
			const currentVersion = (0, react.useRef)(version);
			currentVersion.current = version;
			const view = saved?.version === version ? saved.view : void 0;
			const error = failure?.version === version ? failure.message : void 0;
			(0, react.useEffect)(() => {
				let active = true;
				api.read().then((data) => {
					if (active) setSaved({
						version,
						view: data
					});
				}).catch(() => {
					if (active) setFailure({
						version,
						message: "暂时无法读取自动接管设置。"
					});
				});
				return () => {
					active = false;
				};
			}, [api, version]);
			async function change(enabled) {
				if (!view || busy || !writable || error) return;
				setBusy(true);
				try {
					const confirmed = await api.save(enabled, view.revision);
					if (version === currentVersion.current) setSaved({
						version,
						view: confirmed
					});
				} catch {
					setFailure({
						version,
						message: "暂时无法确认设置是否已保存。请重新读取。"
					});
				} finally {
					if (version !== currentVersion.current) setReload((value) => value + 1);
					setBusy(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "ops-surface",
				"aria-labelledby": "chat-routing-title",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", {
					className: "ops-surface-head",
					children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
						id: "chat-routing-title",
						className: "ops-settings-anchor",
						tabIndex: -1,
						children: "聊天任务自动接管"
					})
				}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
					className: "ops-inset ops-stack",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
						id: "chat-routing-scope",
						children: [
							"适用于此 Desktop 用户的会话，从下一条消息生效。关闭后仍可用 ",
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("code", { children: "/orchestra 任务内容" }),
							" 交给 Orchestra。"
						]
					}), busy ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在确认设置是否已保存…"
					}) : error ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "alert",
						children: error
					}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						className: "ops-button",
						type: "button",
						onClick: () => setReload((value) => value + 1),
						children: "重新读取自动接管设置"
					})] }) : view ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
						type: "checkbox",
						role: "switch",
						"aria-label": "聊天任务自动接管",
						"aria-describedby": "chat-routing-scope",
						checked: view.enabled,
						disabled: !writable,
						onChange: (event) => {
							change(event.currentTarget.checked);
						}
					}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
						role: "status",
						children: view.enabled ? "已开启" : "已关闭"
					})] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: "正在读取自动接管设置…"
					})]
				})]
			});
		}
		// src/client/panel/display-preference-settings.tsx
		function DisplayPreferenceSettings() {
			const [{ preferences, persistent }, setState] = (0, react.useState)(readDisplayPreferences);
			(0, react.useEffect)(() => {
				const refresh = () => setState(readDisplayPreferences());
				const unsubscribe = subscribeDisplayPreferences(refresh);
				refresh();
				return unsubscribe;
			}, []);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "ops-surface",
				"aria-label": "显示偏好",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("header", {
					className: "ops-surface-head",
					children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
						id: "display-preferences-title",
						tabIndex: -1,
						className: "ops-settings-anchor",
						children: "显示偏好"
					}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						className: "ops-button",
						onClick: resetDisplayPreferences,
						children: "恢复显示默认值"
					})]
				}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
					className: "ops-inset",
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "偏好保存在当前浏览器，同一地址下的面板共用。下次打开任务页或事件页时生效。" }),
						!persistent && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "status",
							children: "浏览器存储不可用，本次更改仅保留在当前页面。"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							style: fieldsStyle,
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
								style: fieldStyle,
								children: ["任务默认视图", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("select", {
									className: "ops-field",
									value: preferences.taskView,
									onChange: (event) => {
										const view = TASK_VIEWS.find(({ id }) => id === event.target.value);
										if (view) updateDisplayPreferences({ taskView: view.id });
									},
									children: TASK_VIEWS.map(({ id, label }) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: id,
										children: label
									}, id))
								})]
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
								style: fieldStyle,
								children: ["任务与运行事件每页条数", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("select", {
									className: "ops-field",
									value: preferences.eventPageSize,
									onChange: (event) => {
										const size = EVENT_PAGE_SIZES.find((size) => String(size) === event.target.value);
										if (size !== void 0) updateDisplayPreferences({ eventPageSize: size });
									},
									children: EVENT_PAGE_SIZES.map((size) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
										value: size,
										children: [size, " 条"]
									}, size))
								})]
							})]
						})
					]
				})]
			});
		}
		const fieldsStyle = {
			display: "flex",
			flexWrap: "wrap",
			gap: 16
		};
		const fieldStyle = {
			display: "grid",
			gap: 4
		};
		// src/client/panel/execution-capacity-settings.tsx
		const path = "/v1/settings/execution-capacity";
		const fields = [
			{
				key: "concurrency",
				label: "同时执行名额",
				unit: "个",
				scale: 1,
				help: "最多同时执行多少个工作流节点。提高后会立即从等待队列补位。"
			},
			{
				key: "max_waiting",
				label: "可排队上限",
				unit: "个",
				scale: 1,
				help: "最多有多少个节点可以等待执行。队列满时，无法直接开跑的新节点会被拒绝。"
			},
			{
				key: "action_timeout_ms",
				label: "单步执行时限",
				unit: "秒",
				scale: 1e3,
				help: "节点执行阶段可用的时间。排队、规划、启动与核验分别计时。"
			}
		];
		function toDraft(values) {
			return {
				concurrency: String(values.concurrency),
				max_waiting: String(values.max_waiting),
				action_timeout_ms: String(values.action_timeout_ms / 1e3)
			};
		}
		const styles = `
.execution-capacity-form { display: grid; gap: 12px; padding: 14px 16px; }
.execution-capacity-form p { margin: 0; }
.execution-capacity-row {
  display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center;
  gap: 4px 12px; padding-top: 10px; border-top: 1px solid ${COLOR.line};
}
.execution-capacity-row label { font-weight: 650; }
.execution-capacity-row input { inline-size: 10ch; }
.execution-capacity-help { grid-column: 1 / -1; color: ${COLOR.inkMuted}; font-size: ${FONT_PX.menu}px; }
.execution-capacity-help p + p { margin-top: 2px; font-size: ${FONT_PX.helper}px; }
.execution-capacity-form [role=alert] { color: ${COLOR.danger}; }
`;
		function ExecutionCapacitySettings({ client, reloadVersion = 0, onSnapshot }) {
			const [saved, setSaved] = (0, react.useState)();
			const [draft, setDraft] = (0, react.useState)();
			const [reload, setReload] = (0, react.useState)(0);
			const [loaded, setLoaded] = (0, react.useState)("");
			const [busy, setBusy] = (0, react.useState)(false);
			const [pending, setPending] = (0, react.useState)();
			const [notice, setNotice] = (0, react.useState)();
			const version = reloadVersion + ":" + reload;
			const ready = saved && draft && loaded === version;
			(0, react.useEffect)(() => {
				let active = true;
				client.get(path).then(({ data, as_of_seq }) => {
					if (!active) return;
					setSaved(data);
					setDraft(toDraft(data.values));
					setLoaded(version);
					onSnapshot?.(as_of_seq);
				}).catch(() => {
					if (active) setNotice({
						message: "暂时无法读取执行与排队设置。",
						error: true
					});
				});
				return () => {
					active = false;
				};
			}, [
				client,
				onSnapshot,
				version
			]);
			async function save(reset) {
				if (!saved || !draft || busy || !pending && !ready) return;
				let values = null;
				if (!pending && !reset) {
					values = {
						concurrency: 0,
						max_waiting: 0,
						action_timeout_ms: 0
					};
					for (const { key, label, scale, unit } of fields) {
						const format = scale === 1 ? /^\d+$/u : /^\d+(?:\.\d{1,3})?$/u;
						const quantity = Number(draft[key]);
						if (!format.test(draft[key]) || quantity < 1 || quantity > saved.limits[key] / scale && quantity !== saved.defaults[key] / scale) {
							const precision = scale === 1 ? "整数" : "最多三位小数";
							setNotice({
								error: true,
								field: key,
								message: `${label}请输入 1–${saved.limits[key] / scale} ${unit}（${precision}）。`
							});
							return;
						}
						values[key] = Math.round(quantity * scale);
					}
				}
				const command = pending ?? {
					operation_id: newOperationId(),
					expected_revision: saved.revision,
					values
				};
				setBusy(true);
				setNotice(void 0);
				try {
					await postWithOperationReceipt(client, path, "execution_capacity.save", command);
					setPending(void 0);
					setNotice({ message: command.values ? "已保存并生效。" : "已恢复宿主默认值并生效。" });
					setReload((value) => value + 1);
				} catch (error) {
					setPending(error instanceof ApiError && error.code === "OPERATION_OUTCOME_UNKNOWN" ? command : void 0);
					setNotice({
						error: true,
						message: error instanceof Error ? error.message : "保存结果暂不可确认。"
					});
				} finally {
					setBusy(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "ops-surface",
				"aria-labelledby": "execution-capacity-title",
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("style", { children: styles }),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", {
						className: "ops-surface-head",
						children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
							id: "execution-capacity-title",
							tabIndex: -1,
							className: "ops-settings-anchor",
							children: "执行与排队"
						})
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
						className: "execution-capacity-form",
						noValidate: true,
						onSubmit: (event) => {
							event.preventDefault();
							save(false);
						},
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "设置应用于当前会话。调低容量会保留已运行和已排队的节点。新时限从节点实际开始执行时采用。" }),
							saved && draft && fields.map(({ key, label, unit, scale, help }) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "execution-capacity-row",
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("label", {
										htmlFor: `execution-capacity-${key}`,
										children: label
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
										id: `execution-capacity-${key}`,
										className: "ops-field",
										type: "number",
										min: 1,
										max: Math.max(saved.limits[key], saved.defaults[key]) / scale,
										step: 1 / scale,
										value: draft[key],
										"aria-describedby": `execution-capacity-help-${key}`,
										"aria-invalid": notice?.field === key || void 0,
										disabled: busy || Boolean(pending) || !ready,
										onChange: (event) => setDraft({
											...draft,
											[key]: event.target.value
										})
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
										className: "execution-capacity-help",
										id: `execution-capacity-help-${key}`,
										children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: help }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", { children: [
											"范围 1–",
											saved.limits[key] / scale,
											" ",
											unit,
											" · 宿主默认 ",
											saved.defaults[key] / scale,
											" ",
											unit
										] })]
									})
								]
							}, key)),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "ops-actions",
								children: [saved && draft && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
									className: "ops-button ops-button--primary",
									type: "submit",
									disabled: busy || !pending && !ready,
									children: pending ? "查询或重试" : "保存执行设置"
								}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
									className: "ops-button",
									type: "button",
									disabled: busy || Boolean(pending) || !ready || !saved.overridden,
									onClick: () => {
										save(true);
									},
									children: "恢复宿主默认"
								})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									className: "ops-button ops-button--link",
									type: "button",
									disabled: busy || Boolean(pending),
									onClick: () => {
										setNotice(void 0);
										setReload((value) => value + 1);
									},
									children: "重新读取执行设置"
								})]
							}),
							busy && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								role: "status",
								children: "正在确认操作结果…"
							}),
							!busy && !notice && loaded !== version && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								role: "status",
								children: "正在读取执行与排队设置…"
							}),
							notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								role: notice.error ? "alert" : "status",
								children: notice.message
							})
						]
					})
				]
			});
		}
		function isReleaseWorkflow(workflowId) {
			return workflowId === "wf_release_v1" || workflowId === "wf_gate_check_v1";
		}
		// src/client/panel/workflow-concurrency-settings.tsx
		const PATH = "/v1/settings/workflow-concurrency";
		const FIELDS = [
			{
				key: "run_limit",
				label: "每次流程运行的并行步骤数",
				help: "设为 2：同一份报表最多同时跑两个独立步骤。",
				min: 1,
				max: 32
			},
			{
				key: "node_limit",
				label: "同一步骤跨任务的并发数",
				help: "设为 2：十份报表到达同一流程的复核步骤时，一次复核两份。",
				min: 1,
				max: 32
			},
			{
				key: "helper_limit",
				label: "每个步骤内的并行助手数",
				help: "设为 2：一次复核可以同时使用两个文本助手，其余依次等候。",
				min: 0,
				max: 8
			}
		];
		function valueLabel(value, field) {
			if (value === void 0) return "待确认";
			if (value === null) return "无独立上限";
			if (field === "helper_limit" && value === 0) return "0（关闭）";
			return String(value);
		}
		function selectValue(value) {
			if (value === void 0) return "";
			if (value === null) return "unlimited";
			return String(value);
		}
		function WorkflowConcurrencySettings({ client, reloadVersion = 0, onSnapshot }) {
			const api = (0, react.useMemo)(() => createResourcesApi(client), [client]);
			const [workflowId, setWorkflowId] = (0, react.useState)("");
			const [nodeId, setNodeId] = (0, react.useState)("");
			const [workflows, setWorkflows] = (0, react.useState)([]);
			const [workflowsLoading, setWorkflowsLoading] = (0, react.useState)(true);
			const [workflowError, setWorkflowError] = (0, react.useState)(false);
			const [choiceReload, setChoiceReload] = (0, react.useState)(0);
			const [nodeList, setNodeList] = (0, react.useState)();
			const [loaded, setLoaded] = (0, react.useState)();
			const [readError, setReadError] = (0, react.useState)();
			const [drafts, setDrafts] = (0, react.useState)(/* @__PURE__ */ new Map());
			const [reload, setReload] = (0, react.useState)(0);
			const [busy, setBusy] = (0, react.useState)(false);
			const [notice, setNotice] = (0, react.useState)();
			const [pending, setPending] = (0, react.useState)();
			const key = JSON.stringify([workflowId, nodeId]);
			const version = reloadVersion + ":" + reload;
			const draft = drafts.get(key);
			const saved = loaded?.key === key ? loaded.view : void 0;
			const ready = Boolean(saved && loaded?.version === version && (readError?.key !== key || readError.version !== version));
			const conflict = Boolean(saved && draft && saved.revision !== draft.revision);
			const editable = ready && !conflict && !busy && !pending;
			const helperBlocked = Boolean(saved && !saved.helper_available && (draft?.values.helper_limit ?? 0) > 0);
			const fields = nodeId ? FIELDS.filter(({ key }) => key !== "run_limit") : FIELDS;
			const nodes = nodeList?.workflowId === workflowId ? nodeList.nodes : void 0;
			(0, react.useEffect)(() => {
				let active = true;
				setWorkflowError(false);
				setWorkflowsLoading(true);
				async function loadWorkflows() {
					const published = /* @__PURE__ */ new Map();
					let cursor;
					do {
						const { data } = await api.listWorkflows({
							limit: 100,
							cursor
						});
						if (!active) return;
						for (const item of data.items) if (item.current_version !== null && !isReleaseWorkflow(item.workflow_id)) published.set(item.workflow_id, item);
						setWorkflows([...published.values()]);
						cursor = data.next_cursor ?? void 0;
					} while (cursor);
				}
				loadWorkflows().catch(() => {
					if (active) setWorkflowError(true);
				}).finally(() => {
					if (active) setWorkflowsLoading(false);
				});
				return () => {
					active = false;
				};
			}, [
				api,
				choiceReload,
				reloadVersion
			]);
			(0, react.useEffect)(() => {
				if (!workflowId) return;
				let active = true;
				setNodeList(void 0);
				api.getWorkflow(workflowId).then(({ data }) => {
					if (!active) return;
					const nodes = readCanvasDoc(data.current?.definition)?.nodes.filter(({ type }) => type === "agent") ?? [];
					setNodeList({
						workflowId,
						nodes
					});
				}).catch(() => {
					if (active) setNodeList({
						workflowId,
						nodes: null
					});
				});
				return () => {
					active = false;
				};
			}, [
				api,
				workflowId,
				choiceReload,
				reloadVersion
			]);
			(0, react.useEffect)(() => {
				let active = true;
				client.get(PATH, {
					workflow_id: workflowId || void 0,
					node_id: nodeId || void 0
				}).then(({ data, as_of_seq }) => {
					if (!active) return;
					setLoaded({
						key,
						version,
						view: data
					});
					setReadError(void 0);
					onSnapshot?.(as_of_seq);
				}).catch(() => {
					if (active) setReadError({
						key,
						version
					});
				});
				return () => {
					active = false;
				};
			}, [
				client,
				key,
				nodeId,
				onSnapshot,
				version,
				workflowId
			]);
			function removeDraft() {
				setDrafts((current) => {
					const next = new Map(current);
					next.delete(key);
					return next;
				});
			}
			function updateField(field, value) {
				if (!saved || !editable) return;
				const values = { ...draft?.values ?? saved.overrides };
				if (value === "") delete values[field];
				else if (field === "helper_limit") values[field] = Number(value);
				else values[field] = value === "unlimited" ? null : Number(value);
				if (fields.every(({ key }) => values[key] === saved.overrides[key])) removeDraft();
				else setDrafts((current) => new Map(current).set(key, {
					revision: saved.revision,
					values
				}));
				setNotice(void 0);
			}
			function selectTarget(workflow, node) {
				if (busy || pending) return;
				setWorkflowId(workflow);
				setNodeId(node);
				setLoaded(void 0);
				setReadError(void 0);
				setNotice(void 0);
			}
			async function save(reset) {
				if (!saved || busy || !pending && (!editable || !reset && (!draft || helperBlocked))) return;
				const values = !reset && draft && Object.keys(draft.values).length > 0 ? draft.values : null;
				const command = pending ?? {
					operation_id: newOperationId(),
					expected_revision: saved.revision,
					...workflowId ? { workflow_id: workflowId } : {},
					...nodeId ? { node_id: nodeId } : {},
					values
				};
				setBusy(true);
				setNotice(void 0);
				try {
					await postWithOperationReceipt(client, PATH, "workflow_concurrency.save", command);
					setPending(void 0);
					removeDraft();
					setNotice({ message: command.values ? "已保存 Workflow 并发设置。" : "已恢复继承或会话默认值。" });
					setReload((value) => value + 1);
				} catch (error) {
					const unresolved = error instanceof ApiError && error.code === "OPERATION_OUTCOME_UNKNOWN";
					setPending(unresolved ? command : void 0);
					setNotice({
						error: true,
						message: error instanceof Error ? error.message : "暂时无法保存 Workflow 并发设置。"
					});
					if (!unresolved) setReload((value) => value + 1);
				} finally {
					setBusy(false);
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "ops-surface",
				"aria-labelledby": "workflow-concurrency-title",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", {
					className: "ops-surface-head",
					children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
						id: "workflow-concurrency-title",
						className: "ops-settings-anchor",
						tabIndex: -1,
						children: "Workflow 并发"
					})
				}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
					className: "ops-inset ops-stack",
					onSubmit: (event) => {
						event.preventDefault();
						save(false);
					},
					children: [
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							className: "ops-meta",
							children: "在当前会话设默认值，也可单独调整某个流程或步骤。节点同时受总执行名额和以下两项步骤限制。助手名额在步骤开始时确定。"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "ops-context-grid",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
								className: "ops-stack",
								children: ["应用范围", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
									className: "ops-field",
									value: workflowId,
									disabled: busy || Boolean(pending),
									onChange: (event) => selectTarget(event.target.value, ""),
									children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
											value: "",
											children: "当前会话默认"
										}),
										workflowId && !workflows.some(({ workflow_id }) => workflow_id === workflowId) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
											value: workflowId,
											children: workflowId
										}),
										workflows.map(({ workflow_id, name }) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
											value: workflow_id,
											children: [
												name,
												" · ",
												workflow_id
											]
										}, workflow_id))
									]
								})]
							}), workflowId && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
								className: "ops-stack",
								children: ["已发布步骤", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
									className: "ops-field",
									value: nodeId,
									disabled: busy || Boolean(pending) || !nodes,
									onChange: (event) => selectTarget(workflowId, event.target.value),
									children: [
										/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
											value: "",
											children: "整个 Workflow"
										}),
										nodeId && !nodes?.some(({ node_id }) => node_id === nodeId) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
											value: nodeId,
											children: nodeId
										}),
										nodes?.map(({ node_id, title }) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
											value: node_id,
											children: [
												title,
												" · ",
												node_id
											]
										}, node_id))
									]
								})]
							})]
						}),
						(workflowError || workflowId && nodes === null) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							role: "alert",
							className: "ops-actions",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", { children: workflowError ? "暂时无法完整读取已发布的 Workflow。" : "暂时无法读取这个 Workflow 的已发布步骤。" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								className: "ops-button",
								onClick: () => setChoiceReload((value) => value + 1),
								children: "重读应用范围"
							})]
						}),
						workflowsLoading && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							className: "ops-meta",
							role: "status",
							children: "正在读取已发布的 Workflow…"
						}),
						workflowId && nodes === void 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							className: "ops-meta",
							role: "status",
							children: "正在读取已发布步骤…"
						}),
						workflowId && nodes?.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							className: "ops-meta",
							children: "这个 Workflow 暂无已发布的智能体步骤。"
						}),
						saved && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
							className: "ops-context-grid",
							children: fields.map(({ key, label, min, max, help }) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", {
								className: "ops-stack",
								children: [
									label,
									/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
										className: "ops-field",
										value: selectValue((draft?.values ?? saved.overrides)[key]),
										disabled: !editable,
										onChange: (event) => updateField(key, event.target.value),
										children: [
											/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
												value: "",
												children: [
													workflowId ? "继承" : "使用默认值",
													"（",
													valueLabel(saved.inherited?.[key], key),
													"）"
												]
											}),
											key !== "helper_limit" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
												value: "unlimited",
												children: "无独立上限"
											}),
											Array.from({ length: max - min + 1 }, (_, index) => index + min).map((value) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
												value,
												disabled: key === "helper_limit" && !saved.helper_available && value > 0,
												children: valueLabel(value, key)
											}, value))
										]
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
										className: "ops-meta",
										children: help
									}),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
										className: "ops-meta",
										children: key === "helper_limit" && !saved.helper_available ? "当前宿主未提供受限助手能力" : `当前生效 ${valueLabel(saved.values[key], key)}`
									})
								]
							}, key))
						}),
						helperBlocked && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "alert",
							children: "请先将助手数改为继承或关闭，再保存此范围的设置。"
						}),
						conflict && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "alert",
							children: "此目标的设置已被其他操作更新。请舍弃本地修改，再基于最新设置编辑。"
						}),
						readError?.key === key && readError.version === version && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "alert",
							children: "暂时无法读取此目标的 Workflow 并发设置。"
						}),
						!ready && (readError?.key !== key || readError.version !== version) && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "status",
							children: "正在读取 Workflow 并发设置…"
						}),
						notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: notice.error ? "alert" : "status",
							children: notice.message
						}),
						busy && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
							role: "status",
							children: "正在确认操作结果…"
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
							className: "ops-actions",
							children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
									type: "submit",
									className: "ops-button ops-button--primary",
									disabled: busy || !pending && (!editable || !draft || helperBlocked),
									children: pending ? "查询或重试 Workflow 并发保存" : "保存 Workflow 并发"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
									className: "ops-button",
									disabled: !editable || !Object.keys(saved?.overrides ?? {}).length,
									onClick: () => {
										save(true);
									},
									children: workflowId ? "恢复继承" : "恢复会话默认值"
								}),
								draft && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									className: "ops-button",
									disabled: busy || Boolean(pending),
									onClick: removeDraft,
									children: "舍弃本地修改"
								}),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									className: "ops-button ops-button--link",
									disabled: busy || Boolean(pending),
									onClick: () => {
										setNotice(void 0);
										setReload((value) => value + 1);
									},
									children: "重读 Workflow 并发设置"
								})
							]
						})
					]
				})]
			});
		}
		// src/client/panel/task-trigger-manager.tsx
		const REASON_LABEL = {
			task_archived: "任务已归档",
			previous_run_active: "上次运行还未结束",
			unresolved_effects: "历史外部操作待核验",
			workflow_inactive: "绑定的流程目前不可用"
		};
		function TaskTriggerManager({ api, commands, onChanged }) {
			const [view, setView] = (0, react.useState)(null);
			const [workflows, setWorkflows] = (0, react.useState)(null);
			const [readError, setReadError] = (0, react.useState)(false);
			const [retry, retryWorkflows] = (0, react.useState)(0);
			const [kind, setKind] = (0, react.useState)("scheduled");
			const [title, setTitle] = (0, react.useState)("");
			const [workflowId, setWorkflowId] = (0, react.useState)("");
			const [dailyTime, setDailyTime] = (0, react.useState)("09:00");
			const [timeZone, setTimeZone] = (0, react.useState)(() => Intl.DateTimeFormat().resolvedOptions().timeZone);
			const [eventType, setEventType] = (0, react.useState)("");
			const [eventId, setEventId] = (0, react.useState)("");
			const [notice, setNotice] = (0, react.useState)("");
			const [fault, setFault] = (0, react.useState)(false);
			const [busy, setBusy] = (0, react.useState)(false);
			const [receipt, setReceipt] = (0, react.useState)(null);
			const [emitted, setEmitted] = (0, react.useState)(null);
			(0, react.useEffect)(() => {
				if (view !== "rules") return;
				let active = true;
				setReadError(false);
				api.listWorkflows({
					status: "active",
					limit: 100
				}).then(({ data }) => {
					if (active) setWorkflows(data.items.filter((workflow) => workflow.scope === "reusable" && workflow.current_version !== null));
				}).catch(() => {
					if (active) setReadError(true);
				});
				return () => {
					active = false;
				};
			}, [
				api,
				view,
				retry
			]);
			async function submitRule() {
				const workflow = workflows?.find((item) => item.workflow_id === workflowId);
				if (busy || !receipt && (!workflow || workflow.current_version === null || !title.trim())) return;
				const base = {
					title: title.trim(),
					workflow_id: workflowId,
					workflow_version: String(workflow?.current_version),
					operation_id: newOperationId()
				};
				const input = receipt ?? (kind === "scheduled" ? {
					...base,
					kind,
					daily_time: dailyTime,
					time_zone: timeZone.trim()
				} : {
					...base,
					kind,
					event_type: eventType.trim()
				});
				setBusy(true);
				setNotice("");
				setFault(false);
				try {
					await commands.create(input);
					setReceipt(null);
					setTitle("");
					setNotice("已登记触发任务。可在任务详情查看下一次触发与运行历史。");
					onChanged();
				} catch (error) {
					setFault(true);
					if (error instanceof ApiError && error.code === "OPERATION_OUTCOME_UNKNOWN") setReceipt(input);
					else setReceipt(null);
					setNotice(error instanceof Error ? error.message : "无法确认触发规则是否已登记。");
				} finally {
					setBusy(false);
				}
			}
			async function submitEvent() {
				if (busy || !eventType.trim() || !eventId.trim()) return;
				setBusy(true);
				setNotice("");
				setEmitted(null);
				setFault(false);
				try {
					const result = await commands.emit(eventType.trim(), eventId.trim());
					setEmitted(result.items);
					setNotice(result.replayed ? "已查到历史事件的触发结果。" : "事件已登记。");
					onChanged();
				} catch (error) {
					setFault(true);
					setNotice(error instanceof ApiError && ["SERVICE_UNAVAILABLE", "OPERATION_TIMEOUT"].includes(error.code) ? "暂未确认收件结果。用同一事件编号可查询或重试。" : error instanceof Error ? error.message : "事件提交失败。");
				} finally {
					setBusy(false);
				}
			}
			const switchView = (next) => {
				if (busy || receipt) return;
				setNotice("");
				setEmitted(null);
				setView(view === next ? null : next);
			};
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
				style: wrapper,
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						role: "group",
						"aria-label": "任务触发操作",
						style: actions,
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							style: buttonStyle,
							disabled: busy || Boolean(receipt),
							"aria-expanded": view === "rules",
							onClick: () => switchView("rules"),
							children: "新建触发任务"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							style: buttonStyle,
							disabled: busy || Boolean(receipt),
							"aria-expanded": view === "event",
							onClick: () => switchView("event"),
							children: "提交触发事件"
						})]
					}),
					view === "rules" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
						"aria-label": "新建触发任务",
						style: formStyle,
						onSubmit: (event) => {
							event.preventDefault();
							submitRule();
						},
						children: [
							readError && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("p", {
								role: "alert",
								children: ["无法读取已发布流程。 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
									type: "button",
									onClick: () => retryWorkflows((value) => value + 1),
									children: "重试"
								})]
							}),
							!readError && !workflows && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
								role: "status",
								children: "正在读取已发布流程…"
							}),
							!readError && workflows?.length === 0 && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "发布一个可复用流程后即可登记触发任务。" }),
							Boolean(workflows?.length) && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["任务标题 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									required: true,
									maxLength: 200,
									value: title,
									disabled: busy || Boolean(receipt),
									onChange: (event) => setTitle(event.target.value)
								})] }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["已发布流程 ", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
									required: true,
									value: workflowId,
									disabled: busy || Boolean(receipt),
									onChange: (event) => setWorkflowId(event.target.value),
									children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "",
										children: "选择流程"
									}), workflows?.map((workflow) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("option", {
										value: workflow.workflow_id,
										children: [
											workflow.name,
											" · v",
											workflow.current_version
										]
									}, workflow.workflow_id))]
								})] }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["触发方式 ", /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("select", {
									value: kind,
									disabled: busy || Boolean(receipt),
									onChange: (event) => setKind(event.target.value === "event" ? "event" : "scheduled"),
									children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "scheduled",
										children: "每日定时"
									}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("option", {
										value: "event",
										children: "接收事件"
									})]
								})] }),
								kind === "scheduled" ? /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["每天 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									type: "time",
									required: true,
									value: dailyTime,
									disabled: busy || Boolean(receipt),
									onChange: (event) => setDailyTime(event.target.value)
								})] }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["时区 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									required: true,
									maxLength: 100,
									placeholder: "Asia/Shanghai",
									value: timeZone,
									disabled: busy || Boolean(receipt),
									onChange: (event) => setTimeZone(event.target.value)
								})] })] }) : /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["事件类型 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
									required: true,
									maxLength: 128,
									placeholder: "project.ready",
									value: eventType,
									disabled: busy || Boolean(receipt),
									onChange: (event) => setEventType(event.target.value)
								})] }),
								/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
									style: buttonStyle,
									type: "submit",
									disabled: busy,
									children: receipt ? "查询或重试" : "登记任务"
								})
							] })
						]
					}),
					view === "event" && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("form", {
						"aria-label": "提交触发事件",
						style: formStyle,
						onSubmit: (event) => {
							event.preventDefault();
							submitEvent();
						},
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["事件类型 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
								required: true,
								maxLength: 128,
								placeholder: "project.ready",
								value: eventType,
								disabled: busy,
								onChange: (event) => setEventType(event.target.value)
							})] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["事件编号 ", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("input", {
								required: true,
								maxLength: 128,
								placeholder: "project-ready-001",
								value: eventId,
								disabled: busy,
								onChange: (event) => setEventId(event.target.value)
							})] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelCommandButton, {
								style: buttonStyle,
								type: "submit",
								disabled: busy,
								children: "提交事件"
							})
						]
					}),
					notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: fault ? "alert" : "status",
						style: noticeStyle,
						children: notice
					}),
					emitted && (emitted.length === 0 ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						style: noticeStyle,
						children: "当时没有匹配的启用规则。"
					}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
						style: noticeStyle,
						children: emitted.map((occurrence) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", { children: occurrence.reason ? `已跳过 · ${REASON_LABEL[occurrence.reason]}` : `已创建运行 · ${occurrence.run_id}` }, occurrence.trigger_id))
					}))
				]
			});
		}
		const wrapper = {
			display: "grid",
			gap: 8,
			fontSize: FONT_PX.body
		};
		const actions = {
			display: "flex",
			gap: 8,
			flexWrap: "wrap"
		};
		const formStyle = {
			display: "grid",
			gap: 8,
			padding: 12,
			border: `1px solid ${COLOR.line}`,
			borderRadius: 8
		};
		const buttonStyle = {
			padding: "5px 10px",
			minHeight: 34,
			border: `1px solid ${COLOR.line}`,
			borderRadius: 6,
			color: COLOR.ink,
			background: COLOR.surface,
			alignSelf: "start"
		};
		const noticeStyle = {
			margin: 0,
			color: COLOR.ink,
			fontSize: FONT_PX.helper
		};
		// src/client/panel/app.tsx
		function PanelApp(props) {
			const [streamStatus, setStreamStatus] = (0, react.useState)();
			const [resyncRequest, setResyncRequest] = (0, react.useState)(null);
			const writesEnabled = (0, react.useRef)(false);
			const snapshotStale = props.snapshotStale || streamStatus === "stale" || resyncRequest !== null;
			writesEnabled.current = props.serviceConnected && !props.protocolMismatch && !snapshotStale;
			const client = (0, react.useMemo)(() => ({
				get: (...args) => props.client.get(...args),
				async post(path, payload, signal) {
					if (!writesEnabled.current) throw new ApiError("PANEL_WRITE_UNAVAILABLE", "当前为只读状态，刷新成功后再操作", false);
					return props.client.post(path, payload, signal);
				}
			}), [props.client]);
			const rework = (0, react.useMemo)(() => props.reworkEnabled ? createReworkApi(client) : void 0, [client, props.reworkEnabled]);
			const modelEscalation = (0, react.useMemo)(() => props.modelEscalationEnabled ? createModelEscalationApi(client) : void 0, [client, props.modelEscalationEnabled]);
			const api = (0, react.useMemo)(() => createResourcesApi(client), [client]);
			const commands = (0, react.useMemo)(() => createWorkflowCommandsApi(client), [client]);
			const decisionCommands = (0, react.useMemo)(() => createDecisionCommandsApi(client), [client]);
			const memoryRecovery = (0, react.useMemo)(() => createMemoryRecoveryApi(client), [client]);
			const taskCommands = (0, react.useMemo)(() => createTaskCommands(client), [client]);
			const triggerCommands = (0, react.useMemo)(() => createTaskTriggerCommands(client), [client]);
			const [route, setRoute] = (0, react.useState)(props.initialRoute ?? "tasks");
			const [taskId, setTaskId] = (0, react.useState)(null);
			const [taskRunId, setTaskRunId] = (0, react.useState)();
			const taskTrigger = (0, react.useRef)(null);
			const [workflowId, setWorkflowId] = (0, react.useState)(null);
			const [decisionId, setDecisionId] = (0, react.useState)();
			const [reloadVersion, setReloadVersion] = (0, react.useState)(0);
			const [settingsReloadVersion, setSettingsReloadVersion] = (0, react.useState)(0);
			const [plannerHeading, setPlannerHeading] = (0, react.useState)(null);
			const [submodelHeading, setSubmodelHeading] = (0, react.useState)(null);
			const streamStarted = (0, react.useRef)(false);
			const resyncGeneration = (0, react.useRef)(0);
			const resyncAnchor = (0, react.useRef)(null);
			const [refreshGeneration, setRefreshGeneration] = (0, react.useState)(0);
			const requestResync = (0, react.useCallback)(() => {
				writesEnabled.current = false;
				resyncAnchor.current = null;
				setResyncRequest(++resyncGeneration.current);
			}, []);
			const eventStream = (0, react.useMemo)(() => props.createEventStream?.((type) => {
				if (type === "resync_required") requestResync();
				else setReloadVersion((version) => version + 1);
			}, (status) => {
				if (status === "stale") writesEnabled.current = false;
				setStreamStatus(status);
			}) ?? null, [props.createEventStream, requestResync]);
			(0, react.useEffect)(() => {
				streamStarted.current = false;
				return () => {
					eventStream?.stop();
					streamStarted.current = false;
				};
			}, [eventStream]);
			const { refreshServiceStatus } = props;
			(0, react.useEffect)(() => {
				if (!eventStream || resyncRequest === null) return;
				const generation = resyncRequest;
				let active = true;
				let retry;
				const isCurrent = () => active && generation === resyncGeneration.current;
				async function recover() {
					try {
						const [{ as_of_seq }, health] = await Promise.all([api.listEvents({ limit: 1 }), refreshServiceStatus?.()]);
						if (!isCurrent()) return;
						if (health === false) throw new Error("Service health is unavailable");
						resyncAnchor.current = {
							generation,
							seq: as_of_seq
						};
						setRefreshGeneration(generation);
						setSettingsReloadVersion((version) => version + 1);
						setReloadVersion((version) => version + 1);
					} catch {
						if (isCurrent()) retry = setTimeout(() => {
							recover();
						}, 5e3);
					}
				}
				recover();
				return () => {
					active = false;
					clearTimeout(retry);
				};
			}, [
				api,
				eventStream,
				refreshServiceStatus,
				resyncRequest
			]);
			const finishResync = (0, react.useCallback)((asOfSeq) => {
				const anchor = resyncAnchor.current;
				if (!eventStream || !anchor || anchor.generation !== refreshGeneration || anchor.generation !== resyncGeneration.current || asOfSeq !== void 0 && asOfSeq < anchor.seq) return;
				resyncAnchor.current = null;
				setResyncRequest(null);
				if (streamStarted.current) eventStream.setSnapshot(anchor.seq);
				else {
					streamStarted.current = true;
					eventStream.start(anchor.seq);
				}
			}, [eventStream, refreshGeneration]);
			const onSnapshot = (0, react.useCallback)((asOfSeq) => {
				if (!eventStream || refreshGeneration !== resyncGeneration.current) return;
				if (resyncAnchor.current) {
					finishResync(asOfSeq);
					return;
				}
				if (!streamStarted.current) {
					eventStream.start(asOfSeq);
					streamStarted.current = true;
				} else eventStream.setSnapshot(asOfSeq);
			}, [
				eventStream,
				finishResync,
				refreshGeneration
			]);
			const canReadViewSnapshot = route === "settings" ? props.modelRoutingEnabled === true || props.planningTimeoutsEnabled === true || props.executionCapacityEnabled === true || props.workflowConcurrencyEnabled === true : route === "collaboration" ? props.collaborationEnabled === true : route !== "memory" || !props.additionalScreens?.memory;
			(0, react.useEffect)(() => {
				if (!canReadViewSnapshot) finishResync();
			}, [canReadViewSnapshot, finishResync]);
			function refreshSettings() {
				if (resyncRequest !== null) requestResync();
				else Promise.resolve(refreshServiceStatus?.()).then((healthy) => {
					if (healthy !== false) setSettingsReloadVersion((version) => version + 1);
				});
			}
			function openDecision(id) {
				setTaskId(null);
				setDecisionId(id);
				setRoute("decisions");
			}
			const showServiceStatus = props.additionalScreens?.settings == null;
			const settingsSections = [
				...props.nativeChatRoutingEnabled ? [{
					id: "chat-routing-title",
					label: "会话路由"
				}] : [],
				...props.modelRoutingEnabled && plannerHeading ? [{
					id: plannerHeading.id,
					label: "主脑模型"
				}] : [],
				...props.modelRoutingEnabled && submodelHeading ? [{
					id: submodelHeading.id,
					label: "子模型及能力"
				}] : [],
				...props.planningTimeoutsEnabled ? [{
					id: "planning-timeouts-title",
					label: "规划时限"
				}] : [],
				...props.executionCapacityEnabled ? [{
					id: "execution-capacity-title",
					label: "执行与排队"
				}] : [],
				...props.workflowConcurrencyEnabled ? [{
					id: "workflow-concurrency-title",
					label: "Workflow 并发"
				}] : [],
				{
					id: "display-preferences-title",
					label: "显示偏好"
				},
				...showServiceStatus ? [{
					id: "settings-title",
					label: "服务状态"
				}] : [],
				{
					id: "memory-capability-title",
					label: "任务记忆"
				}
			];
			let screen;
			if (route === "overview") screen = /* @__PURE__ */ (0, react_jsx_runtime.jsx)(OverviewScreen, {
				api,
				reloadVersion,
				onSnapshot,
				onNavigate: setRoute
			});
			else if (route === "tasks") screen = /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, {
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
						className: "task-workspace",
						hidden: Boolean(taskId),
						children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskListScreen, {
							reworkEnabled: props.reworkEnabled,
							collaborationEnabled: props.collaborationEnabled,
							workflowLaunchRequiresPlan: props.workflowLaunchRequiresPlan,
							api,
							commands: taskCommands,
							selectedTaskId: taskId,
							triggerManager: /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskTriggerManager, {
								api,
								commands: triggerCommands,
								onChanged: () => setReloadVersion((version) => version + 1)
							}),
							onSelectTask: (id, trigger, runId) => {
								taskTrigger.current = trigger;
								setTaskRunId(runId);
								setTaskId(id);
							},
							reloadVersion,
							onSnapshot
						})
					}),
					taskId && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskDetailScreen, {
						standalone: true,
						rework,
						modelEscalation,
						reworkPlannerEnabled: props.reworkPlannerEnabled,
						api,
						taskId,
						initialRunId: taskRunId,
						onBack: () => {
							setTaskId(null);
							setTaskRunId(void 0);
							taskTrigger.current?.focus({ preventScroll: true });
						},
						taskCommands,
						triggerCommands,
						onArchiveChanged: () => setReloadVersion((version) => version + 1),
						onOpenDecision: openDecision,
						notesClient: props.runNotesEnabled ? client : void 0,
						onOpenMemory: props.memoryEnabled ? () => {
							setTaskId(null);
							setRoute("memory");
						} : void 0,
						reloadVersion,
						onSnapshot
					}, `page:${taskId}:${taskRunId ?? ""}`)
				]
			});
			else if (route === "decisions") screen = /* @__PURE__ */ (0, react_jsx_runtime.jsx)(DecisionInbox, {
				decisionId,
				onShowAll: () => setDecisionId(void 0),
				rework,
				reworkPlannerEnabled: props.reworkPlannerEnabled,
				api,
				commands: decisionCommands,
				reloadVersion,
				onSnapshot
			}, decisionId ?? "all");
			else if (route === "effects") screen = /* @__PURE__ */ (0, react_jsx_runtime.jsx)(UnverifiedEffects, {
				api,
				client,
				reloadVersion,
				onSnapshot,
				onOpenDecision: openDecision
			});
			else if (route === "activity") screen = /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ActivityScreen, {
				api,
				reloadVersion,
				onSnapshot
			});
			else if (route === "collaboration" && props.collaborationEnabled) screen = /* @__PURE__ */ (0, react_jsx_runtime.jsx)(CollaborationScreen, {
				client,
				reloadVersion,
				onSnapshot,
				onOpenRun: (task, run) => {
					setTaskId(task);
					setTaskRunId(run);
				}
			});
			else if (route === "memory") screen = props.additionalScreens?.memory ?? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(MemoryPanel, {
				api,
				recovery: memoryRecovery,
				memoryEnabled: props.memoryEnabled,
				proposalOnly: props.memoryProposalOnly,
				reloadVersion,
				onSnapshot,
				onOpenDecisions: () => {
					setRoute("decisions");
				},
				onOpenTasks: () => setRoute("tasks")
			});
			else if (route === "settings") screen = /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
				className: "ops-page ops-settings-page",
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", { children: "设置" }), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
					className: "ops-settings-section",
					children: [
						props.nativeChatRoutingEnabled && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ChatRoutingSettings, {
							client,
							reloadVersion: settingsReloadVersion,
							writable: writesEnabled.current
						}),
						props.modelRoutingEnabled && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ModelRoutingSettings, {
							client,
							reloadVersion: settingsReloadVersion,
							onSnapshot,
							plannerHeadingRef: setPlannerHeading,
							submodelHeadingRef: setSubmodelHeading
						}),
						props.planningTimeoutsEnabled && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PlanningTimeoutSettings, {
							client,
							reloadVersion: settingsReloadVersion,
							onSnapshot
						}),
						props.executionCapacityEnabled && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(ExecutionCapacitySettings, {
							client,
							reloadVersion: settingsReloadVersion,
							onSnapshot
						}),
						props.workflowConcurrencyEnabled && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowConcurrencySettings, {
							client,
							reloadVersion: settingsReloadVersion,
							onSnapshot
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsx)(DisplayPreferenceSettings, {}),
						props.additionalScreens?.settings ?? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(SettingsScreen, {
							serviceConnected: props.serviceConnected,
							protocolMismatch: props.protocolMismatch,
							snapshotStale,
							streamStatus,
							phase: props.servicePhase,
							panelVersion: props.panelVersion,
							onRefresh: refreshSettings
						}),
						/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("section", {
							className: "ops-surface",
							"aria-labelledby": "memory-capability-title",
							children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("header", {
								className: "ops-surface-head",
								children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)("h2", {
									id: "memory-capability-title",
									className: "ops-settings-anchor",
									tabIndex: -1,
									children: "任务记忆"
								})
							}), /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								className: "ops-inset",
								children: [
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: !props.serviceConnected ? "服务未连接，记忆整理状态待确认。" : props.memoryEnabled === true ? "宿主已启用记忆整理。" : props.memoryEnabled === false ? "当前宿主未启用记忆整理。" : "宿主尚未报告记忆整理状态。" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: props.memoryProposalOnly ? "当前工作区开放记忆提议。你可以从成功任务的「结果」整理经验，在记忆页查看建议和来源。" : "在成功任务的「结果」中发起整理。你在审批中心确认条目后，可到记忆页查看实际写入的内容和来源。" }),
									/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
										type: "button",
										className: "ops-button",
										onClick: () => setRoute("memory"),
										children: "查看记忆与写入结果"
									})
								]
							})]
						})
					]
				})]
			});
			else if (route === "workflows") screen = /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(react_jsx_runtime.Fragment, { children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
				hidden: workflowId !== null,
				children: /* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowListScreen, {
					api,
					onSelectWorkflow: setWorkflowId,
					reloadVersion,
					onSnapshot,
					commands
				})
			}), workflowId && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(WorkflowCanvas, {
				api,
				workflowId,
				onBack: () => setWorkflowId(null),
				onOpenRun: (task, run) => {
					taskTrigger.current = null;
					setTaskId(task);
					setTaskRunId(run);
				},
				reloadVersion,
				onSnapshot,
				commands
			}, workflowId)] });
			else screen = props.additionalScreens?.[route] ?? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("div", {
				role: "status",
				style: pendingStyle,
				children: "此页面尚未接入真实数据。"
			});
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)(PanelShell, {
				activeRoute: route,
				onNavigate: (next) => {
					setDecisionId(void 0);
					setRoute(next);
					if (next !== "tasks") setTaskId(null);
					if (next !== "workflows") setWorkflowId(null);
				},
				serviceConnected: props.serviceConnected,
				protocolMismatch: props.protocolMismatch,
				snapshotStale,
				streamStatus,
				collaborationEnabled: props.collaborationEnabled,
				settingsSections,
				children: [screen, taskId && route !== "tasks" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)(TaskDetailScreen, {
					rework,
					modelEscalation,
					reworkPlannerEnabled: props.reworkPlannerEnabled,
					api,
					taskId,
					initialRunId: taskRunId,
					onBack: () => {
						setTaskId(null);
						taskTrigger.current?.focus({ preventScroll: true });
					},
					taskCommands,
					triggerCommands,
					onArchiveChanged: () => setReloadVersion((version) => version + 1),
					onOpenDecision: openDecision,
					notesClient: props.runNotesEnabled ? client : void 0,
					onOpenMemory: props.memoryEnabled ? () => {
						setTaskId(null);
						setRoute("memory");
					} : void 0,
					reloadVersion,
					onSnapshot
				}, taskId + ":" + (taskRunId ?? ""))]
			});
		}
		const pendingStyle = {
			padding: 24,
			border: `1px solid ${COLOR.line}`,
			borderRadius: 9,
			color: COLOR.inkMuted,
			fontSize: FONT_PX.body
		};
		// src/client/panel-host.tsx
		/** 宿主正式入口：把 API 客户端、事件流与面板组装起来，外部宿主一行接入。 */
		function createPanelHost(options) {
			function BoundPanelHost() {
				return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelHost, { ...options });
			}
			BoundPanelHost.displayName = "PanelHost";
			return BoundPanelHost;
		}
		function PanelHost(props) {
			const fetcher = (0, react.useMemo)(() => props.fetcher ?? globalThis.fetch.bind(globalThis), [props.fetcher]);
			const client = (0, react.useMemo)(() => props.client ?? createApiClient(props.baseUrl, fetcher), [
				props.client,
				props.baseUrl,
				fetcher
			]);
			const requestInitRef = (0, react.useRef)(props.requestInit);
			(0, react.useEffect)(() => {
				requestInitRef.current = props.requestInit;
			}, [props.requestInit]);
			const [status, setStatus] = (0, react.useState)({});
			const probeGeneration = (0, react.useRef)(0);
			const probe = (0, react.useCallback)(async () => {
				const generation = ++probeGeneration.current;
				try {
					const result = await client.get("/health");
					if (generation !== probeGeneration.current) return false;
					setStatus((previous) => ({
						...previous,
						serviceConnected: true,
						protocolMismatch: false,
						phase: typeof result.data.phase === "string" ? result.data.phase : previous.phase,
						memoryEnabled: typeof result.data.memory_enabled === "boolean" ? result.data.memory_enabled : void 0,
						memoryProposalOnly: result.data.memory_proposal_only === true,
						modelRoutingEnabled: result.data.model_routing_enabled === true,
						planningTimeoutsEnabled: result.data.planning_timeouts_enabled === true,
						executionCapacityEnabled: result.data.execution_capacity_enabled === true,
						workflowConcurrencyEnabled: result.data.workflow_concurrency_enabled === true,
						nativeChatRoutingEnabled: result.data.native_chat_routing_enabled === true,
						modelEscalationEnabled: result.data.model_escalation_enabled === true,
						collaborationEnabled: result.data.collaboration_enabled === true,
						runNotesEnabled: result.data.run_notes_enabled === true,
						reworkEnabled: result.data.rework_enabled === true,
						reworkPlannerEnabled: result.data.rework_planner_enabled === true
					}));
					return true;
				} catch (error) {
					if (generation !== probeGeneration.current) return false;
					const mismatch = error instanceof ApiError && error.code === "PROTOCOL_VERSION_MISMATCH";
					setStatus((previous) => ({
						...previous,
						serviceConnected: mismatch,
						protocolMismatch: mismatch
					}));
					return false;
				}
			}, [client]);
			(0, react.useEffect)(() => {
				probe();
				return () => {
					probeGeneration.current += 1;
				};
			}, [probe]);
			const createEventStream = (0, react.useCallback)((onInvalidate, onStatus) => createConnectedEventStream({
				client,
				fetcher,
				baseUrl: props.baseUrl,
				requestInit: requestInitRef.current,
				onInvalidate,
				onStatus
			}), [
				client,
				fetcher,
				props.baseUrl
			]);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelApp, {
				client,
				serviceConnected: status.serviceConnected ?? false,
				protocolMismatch: status.protocolMismatch ?? false,
				initialRoute: props.initialRoute,
				workflowLaunchRequiresPlan: props.workflowLaunchRequiresPlan,
				runNotesEnabled: status.runNotesEnabled,
				memoryEnabled: status.serviceConnected ? status.memoryEnabled : false,
				memoryProposalOnly: status.serviceConnected && status.memoryProposalOnly,
				modelRoutingEnabled: status.modelRoutingEnabled,
				planningTimeoutsEnabled: status.planningTimeoutsEnabled,
				executionCapacityEnabled: status.executionCapacityEnabled,
				workflowConcurrencyEnabled: status.workflowConcurrencyEnabled,
				nativeChatRoutingEnabled: status.nativeChatRoutingEnabled,
				collaborationEnabled: status.collaborationEnabled,
				reworkEnabled: status.reworkEnabled,
				modelEscalationEnabled: status.modelEscalationEnabled,
				reworkPlannerEnabled: status.reworkPlannerEnabled,
				createEventStream,
				servicePhase: status.phase,
				panelVersion: props.panelVersion,
				refreshServiceStatus: probe
			});
		}
		// src/client/dsh-local-panel.tsx
		const route = "/api/orchestra.request";
		const localPanelBase = "http://orchestra.invalid/";
		const loopbackNames = /* @__PURE__ */ new Set([
			"localhost",
			"127.0.0.1",
			"[::1]"
		]);
		function isDshLocalShell(href) {
			const url = new URL(href);
			if (url.username || url.password) return false;
			if (url.protocol === "dsh-app:") return url.host === "app";
			return (url.protocol === "http:" || url.protocol === "https:") && loopbackNames.has(url.hostname);
		}
		function workspaceForSession(sessionId, workspaces) {
			if (!sessionId || workspaces.archivedSessionIds.includes(sessionId)) return void 0;
			const matches = workspaces.items.filter((workspace) => workspace.sessionIds.includes(sessionId));
			return matches.length === 1 ? matches[0]?.workspaceId : void 0;
		}
		function selectedSession(sessions) {
			const selected = Object.values(sessions.byId).filter((row) => (row.retainedBy?.mainView ?? 0) > 0);
			return selected.length === 1 ? selected[0]?.id : void 0;
		}
		function createLocalFetcher(workspaceId, sessionId, isCurrent) {
			return async (input, init) => {
				const request = new Request(input, init);
				const url = new URL(request.url);
				const isRead = request.method === "GET";
				const isCommand = request.method === "POST" && url.pathname.startsWith("/v1/");
				const isApiPath = url.pathname === "/health" || url.pathname.startsWith("/v1/");
				if (url.origin !== new URL("http://orchestra.invalid/").origin || !isApiPath || !isRead && !isCommand) throw new TypeError("Invalid Orchestra panel request");
				const query = new URLSearchParams({
					workspaceId,
					sessionId,
					path: url.pathname + url.search
				});
				const headers = new Headers();
				const accept = request.headers.get("accept");
				if (accept) headers.set("accept", accept);
				if (isCommand) {
					headers.set("content-type", "application/json");
					headers.set("x-orchestra-request", "1");
				}
				const body = isCommand ? await request.text() : void 0;
				if (!isCurrent()) throw new DOMException("DSH workspace selection changed", "AbortError");
				return window.fetch(`${route}?${query}`, {
					method: request.method,
					body,
					headers,
					signal: request.signal,
					credentials: "same-origin",
					mode: "same-origin",
					cache: "no-store",
					redirect: "error"
				});
			};
		}
		function createLocalSessionFetcher(sources, workspaceId, sessionId) {
			return createLocalFetcher(workspaceId, sessionId, () => selectedSession(sources.sessions.list.getSnapshot()) === sessionId && workspaceForSession(sessionId, sources.workspaces.list.getSnapshot()) === workspaceId);
		}
		function DshLocalPanel({ sessions, workspaces }) {
			const sessionList = (0, react.useSyncExternalStore)((listener) => sessions.list.subscribe(listener), () => sessions.list.getSnapshot());
			const workspaceList = (0, react.useSyncExternalStore)((listener) => workspaces.list.subscribe(listener), () => workspaces.list.getSnapshot());
			const sessionId = selectedSession(sessionList);
			const workspaceId = workspaceForSession(sessionId, workspaceList);
			const fetcher = (0, react.useMemo)(() => {
				if (!workspaceId || !sessionId) return void 0;
				return createLocalSessionFetcher({
					sessions,
					workspaces
				}, workspaceId, sessionId);
			}, [
				sessionId,
				workspaceId,
				sessions,
				workspaces
			]);
			if (!fetcher) return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("main", {
				style: { padding: 24 },
				children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h1", { children: "Orchestra" }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
					role: "status",
					children: "请在 DSH 的工作区中打开一个会话，再返回 Orchestra。"
				})]
			});
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(PanelHost, {
				baseUrl: localPanelBase,
				fetcher,
				workflowLaunchRequiresPlan: true
			}, JSON.stringify([workspaceId, sessionId]));
		}
		// src/client/dsh-orchestra-approval-card.ts
		const headlineSelector = "div[class$=\"_headline\"]";
		const structuredAttribute = "data-orchestra-approval";
		function formatNativeOrchestraApprovalCards(document) {
			const style = document.createElement("style");
			style.textContent = `[${structuredAttribute}] { white-space: pre-line; overflow-wrap: anywhere; min-width: 0; }`;
			document.head.append(style);
			function mark(headline) {
				const title = headline.textContent ?? "";
				headline.toggleAttribute(structuredAttribute, title.startsWith("Orchestra 审批\n任务　") || title.startsWith("Orchestra 审批 · 二次确认\n任务　"));
			}
			for (const headline of document.querySelectorAll(headlineSelector)) mark(headline);
			const observer = new MutationObserver((records) => {
				for (const record of records) {
					const title = (record.target instanceof Element ? record.target : record.target.parentElement)?.closest(headlineSelector);
					if (title) mark(title);
					for (const node of record.addedNodes) {
						if (!(node instanceof Element)) continue;
						if (node.matches(headlineSelector)) mark(node);
						for (const headline of node.querySelectorAll(headlineSelector)) mark(headline);
					}
				}
			});
			observer.observe(document.body, {
				subtree: true,
				childList: true,
				characterData: true
			});
			return () => {
				observer.disconnect();
				style.remove();
				for (const headline of document.querySelectorAll(`[${structuredAttribute}]`)) headline.removeAttribute(structuredAttribute);
			};
		}
		// src/client/dsh-orchestra-slash.ts
		const name = "orchestra";
		const token = "/orchestra";
		const hint = "任务内容";
		const usage = "用法：/orchestra 任务内容";
		const candidate = {
			name,
			description: "在当前工作区交给 Orchestra 执行一次任务",
			hint
		};
		function registerNativeOrchestraSlash(ctx, fetcher) {
			const lifetime = new AbortController();
			const workspaceOf = (sessionId) => {
				if (lifetime.signal.aborted || !ctx.sessions.subagentAddress || ctx.sessions.subagentAddress(sessionId) !== void 0) return void 0;
				return workspaceForSession(sessionId, ctx.workspaces.list.getSnapshot());
			};
			async function available({ sessionId }, signal) {
				const workspaceId = workspaceOf(sessionId);
				if (!workspaceId || signal.aborted) return false;
				const query = new URLSearchParams({
					workspaceId,
					sessionId,
					path: "/v1/settings/chat-routing"
				});
				try {
					const response = await fetcher("/api/orchestra.request?" + query, {
						signal: AbortSignal.any([
							signal,
							lifetime.signal,
							AbortSignal.timeout(2e3)
						]),
						credentials: "same-origin",
						mode: "same-origin",
						cache: "no-store",
						redirect: "error"
					});
					if (!response.ok) return false;
					const body = await response.json();
					return !signal.aborted && workspaceOf(sessionId) === workspaceId && body.ok === true && typeof body.data?.enabled === "boolean";
				} catch {
					return false;
				}
			}
			const dispose = ctx.inputTriggers.registerSource({
				name: "dsh-orchestra",
				trigger: "/",
				showGroupTitle: false,
				async candidates(session, { query, position, signal }) {
					if (position !== "leading" || !name.startsWith(query.trim().toLowerCase())) return [];
					return await available(session, signal) ? [candidate] : [];
				},
				onPick({ session, candidate: picked, position }) {
					if (position === "leading" && picked.name === name && workspaceOf(session.sessionId)) return { text: token + " " };
				},
				async matchEnter(session, line, signal) {
					if (line.trim() !== token || !await available(session, signal)) return void 0;
					return { claim: {
						name,
						token: token + " ",
						hint,
						submit: () => ({
							kind: "success",
							text: usage
						})
					} };
				}
			});
			return () => {
				lifetime.abort();
				dispose();
			};
		}
		// src/client/dsh-delivery-review-card.tsx
		function DshDeliveryReviewCard({ scope, sources }) {
			const client = (0, react.useMemo)(() => createApiClient(localPanelBase, createLocalSessionFetcher(sources, scope.workspaceId, scope.sessionId)), [
				sources,
				scope.workspaceId,
				scope.sessionId
			]);
			const api = (0, react.useMemo)(() => createResourcesApi(client), [client]);
			const commands = (0, react.useMemo)(() => createReworkApi(client), [client]);
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("section", {
				"aria-label": "待审成果",
				children: scope.reviews.map((review) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)(Review, {
					review,
					api,
					commands
				}, review.artifact.artifact_id + ":" + (review.revised?.revision ?? review.nodeRevision)))
			});
		}
		function Review({ review, api, commands }) {
			const { artifact, revised } = review;
			const [editing, setEditing] = (0, react.useState)(false);
			const [reason, setReason] = (0, react.useState)("");
			const [expected, setExpected] = (0, react.useState)("");
			const [busy, setBusy] = (0, react.useState)(false);
			const [notice, setNotice] = (0, react.useState)("");
			const [error, setError] = (0, react.useState)("");
			const [unconfirmed, setUnconfirmed] = (0, react.useState)();
			const locked = (0, react.useRef)(false);
			const retention = useArtifactRetention(artifact);
			const enabled = retention === "available" && !review.otherArtifacts && !busy && !notice && !unconfirmed;
			async function submit(action) {
				if (!enabled || locked.current || action === "request_changes" && (!reason.trim() || !revised && !expected.trim())) return;
				locked.current = true;
				setBusy(true);
				setError("");
				try {
					if (revised) {
						if ((await commands.review({
							request_id: revised.requestId,
							expected_revision: revised.revision,
							action,
							note: action === "accept" ? "人工验收通过" : reason.trim(),
							artifact_id: artifact.artifact_id,
							artifact_hash: artifact.sha256
						})).status === "escalated") {
							setNotice("已登记修改意见。本轮返工预算已用完，请在 Orchestra 处理升级。");
							return;
						}
					} else {
						const { data } = await commands.listHolds(artifact.run_id);
						if (!data.items.some((item) => item.status === "held" && item.node_run_id === review.nodeRunId)) {
							setNotice("这条待办已处理或成果版本已更新。请刷新任务结果。");
							return;
						}
						if (action === "accept") await commands.hold({
							run_id: artifact.run_id,
							node_run_id: review.nodeRunId,
							action: "accept",
							artifact_id: artifact.artifact_id,
							sha256: artifact.sha256
						});
						else if ((await commands.request({
							run_id: artifact.run_id,
							target_node_run_id: review.nodeRunId,
							source_artifact_id: artifact.artifact_id,
							source_artifact_hash: artifact.sha256,
							expected_revision: review.nodeRevision,
							reason: reason.trim(),
							expected_result: expected.trim(),
							preserve_requirements: [],
							suggested_scope: "targeted_revision",
							evidence_ids: []
						})).status === "scheduled") {
							setNotice("已登记修改意见，原步骤已排队返工。");
							return;
						}
					}
					setNotice(action === "accept" ? "已经通过此版本的验收。" : "已登记修改意见。返工按任务的授权流程处理。");
				} catch (cause) {
					setError(cause instanceof Error ? operatorReason(cause.message) : "提交未完成。");
					if (cause instanceof ApiError && cause.code === "OPERATION_OUTCOME_UNKNOWN") {
						const details = cause.details;
						if (details?.operation_id) setUnconfirmed(details.operation_id);
					}
				} finally {
					locked.current = false;
					setBusy(false);
				}
			}
			async function checkReceipt(operation) {
				try {
					const { data } = await commands.getOperation(operation);
					if (data.status === "applied") setNotice("操作已生效。");
					else setError("原操作尚未确认。请在 Orchestra 的任务结果中核对后再处理。");
				} catch {
					setError("暂时无法查询原操作回执。");
				}
			}
			return /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("article", {
				style: {
					border: "1px solid var(--dsw-alias-divider, #d5dee8)",
					borderRadius: 8,
					padding: 12,
					marginTop: 8,
					overflowWrap: "anywhere"
				},
				children: [
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)("h4", {
						style: { margin: "0 0 8px" },
						children: "成果待审核"
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("dl", {
						style: {
							display: "grid",
							gridTemplateColumns: "max-content minmax(0, 1fr)",
							gap: "6px 12px",
							margin: 0
						},
						children: [
							[
								["任务", review.title],
								["步骤", review.step],
								["待审成果", artifact.file_name ?? "未命名成果"],
								["审核原因", revised ? `第 ${revised.round} 轮修订已完成` : "该步骤的交付要求人工验收"]
							].map(([label, value]) => /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
								style: { display: "contents" },
								children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: label }), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", {
									style: { margin: 0 },
									children: value
								})]
							}, label)),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dt", { children: "审核标准" }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("dd", {
								style: { margin: 0 },
								children: review.criteria.length ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("ul", {
									style: {
										margin: 0,
										paddingInlineStart: "1.25em"
									},
									children: review.criteria.map((criterion) => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("li", { children: criterion }, criterion))
								}) : "对照原始任务要求核对成果"
							})
						]
					}),
					review.preview !== void 0 && retention === "available" && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("pre", {
						"aria-label": "待审成果全文",
						style: {
							whiteSpace: "pre-wrap",
							overflowWrap: "anywhere",
							maxHeight: 250,
							overflow: "auto",
							padding: 10,
							border: "1px solid #d5dee8"
						},
						children: review.preview
					}),
					/* @__PURE__ */ (0, react_jsx_runtime.jsx)(ArtifactContent, {
						api,
						artifact
					}),
					review.otherArtifacts ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", { children: "这个步骤还有其他成果。请在 Orchestra 的任务结果中核对完整交付再提交结论。" }) : /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: {
							display: "flex",
							flexWrap: "wrap",
							gap: 8,
							marginTop: 10
						},
						children: [/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							disabled: !enabled || editing,
							onClick: () => void submit("accept"),
							children: "通过验收"
						}), /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
							type: "button",
							disabled: !enabled,
							"aria-expanded": editing,
							onClick: () => setEditing(!editing),
							children: "要求修改"
						})]
					}),
					editing && !notice && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("div", {
						style: {
							display: "grid",
							gap: 8,
							marginTop: 8
						},
						children: [
							/* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["具体修改意见", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
								value: reason,
								maxLength: 2500,
								rows: 3,
								onChange: (event) => setReason(event.currentTarget.value),
								style: {
									display: "block",
									width: "100%"
								}
							})] }),
							!revised && /* @__PURE__ */ (0, react_jsx_runtime.jsxs)("label", { children: ["期望结果", /* @__PURE__ */ (0, react_jsx_runtime.jsx)("textarea", {
								value: expected,
								maxLength: 2500,
								rows: 2,
								onChange: (event) => setExpected(event.currentTarget.value),
								style: {
									display: "block",
									width: "100%"
								}
							})] }),
							/* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
								type: "button",
								disabled: !enabled || !reason.trim() || !revised && !expected.trim(),
								onClick: () => void submit("request_changes"),
								children: "提交修改意见"
							})
						]
					}),
					notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "status",
						children: notice
					}),
					error && !notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
						role: "alert",
						children: error
					}),
					unconfirmed && !notice && /* @__PURE__ */ (0, react_jsx_runtime.jsx)("button", {
						type: "button",
						onClick: () => void checkReceipt(unconfirmed),
						children: "核对提交回执"
					})
				]
			});
		}
		// src/client/dsh-orchestra-toolview.tsx
		const slotName = "tool.call.toolview";
		const toolName = "orchestra_task";
		function summaryOf(meta) {
			if (!meta || typeof meta !== "object" || !Object.hasOwn(meta, "summary")) return void 0;
			const { summary } = meta;
			return typeof summary === "string" && summary.trim() ? summary.trim() : void 0;
		}
		function OrchestraToolRow(owner, sources) {
			const result = owner.phase === "result" ? owner.block : void 0;
			const isError = result?.isError === true;
			const review = result?.meta && typeof result.meta === "object" ? result.meta.deliveryReview : void 0;
			if (!isError && review && Array.isArray(review.reviews) && review.reviews.length && sources && typeof window !== "undefined" && isDshLocalShell(window.location.href)) return /* @__PURE__ */ (0, react_jsx_runtime.jsx)(DshDeliveryReviewCard, {
				scope: review,
				sources
			});
			const text = result ? summaryOf(result.meta) ?? (isError ? "Orchestra 未能完成这次操作。请在 Orchestra 面板确认任务状态后再试。" : "请在 Orchestra 面板查看这次操作的结果。") : "Orchestra 正在处理这次操作…";
			return /* @__PURE__ */ (0, react_jsx_runtime.jsx)("p", {
				role: isError ? "alert" : "status",
				style: {
					margin: 0,
					whiteSpace: "pre-wrap",
					overflowWrap: "anywhere"
				},
				children: text
			});
		}
		function registerNativeOrchestraToolView(ctx, reviewSources) {
			return ctx.effect(function* () {
				yield ctx.slots.inject(slotName, () => ctx.slots.register({
					name: slotName,
					key: toolName
				}, (owner) => OrchestraToolRow(owner, reviewSources?.())));
			}, "orchestra: native chat tool row");
		}
		// src/client/dsh-sidebar.tsx
		const panelId = "dsh-orchestra";
		const registeredRoots = /* @__PURE__ */ new WeakSet();
		const inject = ["slots"];
		/** Activate the host panel while a trusted transport is available in this scope. */
		function apply(ctx) {
			let reviewSources;
			registerNativeOrchestraToolView(ctx, () => reviewSources);
			let explicit;
			let local;
			let unmount;
			let unmounting;
			const listeners = /* @__PURE__ */ new Set();
			function ActivePanel() {
				const Panel = (0, react.useSyncExternalStore)((listener) => {
					listeners.add(listener);
					return () => {
						listeners.delete(listener);
					};
				}, () => explicit ?? local);
				return Panel ? /* @__PURE__ */ (0, react_jsx_runtime.jsx)(Panel, {}) : /* @__PURE__ */ (0, react_jsx_runtime.jsx)(react_jsx_runtime.Fragment, {});
			}
			function refresh() {
				for (const listener of listeners) listener();
				if (explicit || local) {
					if (!unmount && !unmounting) {
						const dispose = registerSlots(ctx, ActivePanel);
						if (typeof dispose === "function") unmount = dispose;
					}
				} else if (unmount) {
					const dispose = unmount;
					unmount = void 0;
					unmounting = Promise.resolve(dispose()).finally(() => {
						unmounting = void 0;
						refresh();
					});
					return unmounting;
				}
			}
			ctx.inject(["slots", "orchestraPanelTransport"], function bindHostAdapter(ready) {
				ready.effect(function* () {
					explicit = bindTransport(ready.orchestraPanelTransport);
					refresh();
					yield () => {
						explicit = void 0;
						return refresh();
					};
				}, "orchestra: provided transport");
			});
			if (typeof window === "undefined" || !window.location?.href || !isDshLocalShell(window.location.href)) return;
			ctx.effect(function* () {
				yield formatNativeOrchestraApprovalCards(window.document);
			}, "orchestra: native approval fields");
			ctx.inject([
				"sessions",
				"workspaces",
				"inputTriggers"
			], (ready) => {
				ready.effect(function* () {
					yield registerNativeOrchestraSlash(ready, (input, init) => window.fetch(input, init));
				}, "orchestra: native slash suggestion");
			});
			ctx.inject([
				"slots",
				"sessions",
				"workspaces"
			], function bindLocalShell(ready) {
				ready.effect(function* () {
					const { sessions, workspaces } = ready;
					reviewSources = {
						sessions,
						workspaces
					};
					local = () => /* @__PURE__ */ (0, react_jsx_runtime.jsx)(DshLocalPanel, {
						sessions,
						workspaces
					});
					refresh();
					yield () => {
						local = void 0;
						reviewSources = void 0;
						return refresh();
					};
				}, "orchestra: local DSH workspace transport");
			});
		}
		/** Called by a trusted DSH web adapter after it resolves the workspace's endpoint and authenticated transport. */
		function registerDshOrchestraPanel(ctx, options) {
			return registerSlots(ctx, bindTransport(options));
		}
		function bindTransport(options) {
			if (typeof options?.baseUrl !== "string" || !options.baseUrl.trim() || typeof options.fetcher !== "function") throw new TypeError("Orchestra requires an endpoint and an authenticated host fetcher");
			return createPanelHost(options);
		}
		function registerSlots(ctx, panel) {
			const icon = () => /* @__PURE__ */ (0, react_jsx_runtime.jsx)("span", {
				"aria-hidden": "true",
				children: "◫"
			});
			const root = ctx.root;
			return ctx.effect(function* () {
				if (root) {
					if (registeredRoots.has(root)) return;
					registeredRoots.add(root);
					yield () => {
						registeredRoots.delete(root);
					};
				}
				yield ctx.slots.inject("main", () => ctx.slots.register({
					name: "main",
					key: panelId
				}, panel));
				yield ctx.slots.inject("sidebar.panellist", () => ctx.slots.register({
					name: "sidebar.panellist",
					id: panelId,
					order: 20,
					label: () => "Orchestra"
				}, icon));
			}, "orchestra: authenticated workspace panel");
		}
		// src/client/panel/contrast.ts
		function channels(hex) {
			const value = hex.replace(/^#/, "");
			if (!/^[0-9a-f]{6}$/i.test(value)) throw new TypeError(`Invalid color: ${hex}`);
			return [
				0,
				2,
				4
			].map((i) => parseInt(value.slice(i, i + 2), 16) / 255);
		}
		function relativeLuminance(hex) {
			const linearize = (v) => v <= .04045 ? v / 12.92 : ((v + .055) / 1.055) ** 2.4;
			const [r0, g0, b0] = channels(hex);
			const r = linearize(r0);
			const g = linearize(g0);
			const b = linearize(b0);
			return .2126 * r + .7152 * g + .0722 * b;
		}
		function contrastRatio(a, b) {
			const la = relativeLuminance(a);
			const lb = relativeLuminance(b);
			return (Math.max(la, lb) + .05) / (Math.min(la, lb) + .05);
		}
		const TEXT_PAIRS = [
			{
				fg: COLOR.ink,
				bg: COLOR.canvas,
				floor: 4.5,
				label: "正文"
			},
			{
				fg: COLOR.inkMuted,
				bg: COLOR.canvas,
				floor: 4.5,
				label: "辅助文字"
			},
			{
				fg: COLOR.success,
				bg: COLOR.canvas,
				floor: 4.5,
				label: "成功状态文字"
			},
			{
				fg: COLOR.warning,
				bg: COLOR.canvas,
				floor: 4.5,
				label: "警告状态文字"
			},
			{
				fg: COLOR.danger,
				bg: COLOR.canvas,
				floor: 4.5,
				label: "危险状态文字"
			},
			{
				fg: COLOR.link,
				bg: COLOR.canvas,
				floor: 3,
				label: "链接"
			}
		];
		// src/client/index.tsx
		const clientId = "dsh-orchestra";
		exports.ApiError = ApiError;
		exports.COLOR = COLOR;
		exports.DECISION_COMMAND_KINDS = DECISION_COMMAND_KINDS;
		exports.DECISION_ERROR_TEXT = DECISION_ERROR_TEXT;
		exports.DecisionInbox = DecisionInbox;
		exports.EventStream = EventStream;
		exports.FONT_FLOOR_PX = FONT_FLOOR_PX;
		exports.FONT_PX = FONT_PX;
		exports.MemoryPanel = MemoryPanel;
		exports.PANEL_ROUTES = PANEL_ROUTES;
		exports.PanelApp = PanelApp;
		exports.PanelCommandButton = PanelCommandButton;
		exports.PanelHost = PanelHost;
		exports.PanelShell = PanelShell;
		exports.ReworkPanel = ReworkPanel;
		exports.TEXT_PAIRS = TEXT_PAIRS;
		exports.TaskDetailScreen = TaskDetailScreen;
		exports.TaskListScreen = TaskListScreen;
		exports.UnknownState = UnknownState;
		exports.UnverifiedEffects = UnverifiedEffects;
		exports.VERIFY_BANNER = VERIFY_BANNER;
		exports.WorkflowCanvas = WorkflowCanvas;
		exports.WorkflowCanvasView = WorkflowCanvasView;
		exports.WorkflowListScreen = WorkflowListScreen;
		exports.apply = apply;
		exports.clientId = clientId;
		exports.contrastRatio = contrastRatio;
		exports.createApiClient = createApiClient;
		exports.createConnectedEventStream = createConnectedEventStream;
		exports.createDecisionCommandsApi = createDecisionCommandsApi;
		exports.createMemoryRecoveryApi = createMemoryRecoveryApi;
		exports.createPanelHost = createPanelHost;
		exports.createResourcesApi = createResourcesApi;
		exports.createReworkApi = createReworkApi;
		exports.createSseSubscribe = createSseSubscribe;
		exports.describeDecisionError = describeDecisionError;
		exports.inject = inject;
		exports.memoryCopy = memoryCopy;
		exports.priorityRank = priorityRank;
		exports.readMemoryApprovalCard = readMemoryApprovalCard;
		exports.registerDshOrchestraPanel = registerDshOrchestraPanel;
		exports.relativeLuminance = relativeLuminance;
		exports.sortDecisions = sortDecisions;
		exports.submitThenReceipt = submitThenReceipt;
		exports.unwrap = unwrap;
		exports.usePanelCommandsEnabled = usePanelCommandsEnabled;
		return module.exports;
	}
});
