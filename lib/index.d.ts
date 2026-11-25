import { DatabaseSync } from "node:sqlite";
import { z } from "zod";
import { IncomingMessage } from "node:http";
import { Context } from "@deepseek-ai/cordis";
import { AgentHandle } from "@deepseek-ai/dsh-agent";
type Db = {
  raw: DatabaseSync;
  file: string;
  epoch: number;
  readonly: boolean;
};
type Tx = {
  raw: DatabaseSync;
  exec(sql: string): void;
  all<T = Record<string, unknown>>(sql: string, params?: unknown[]): T[];
  get<T = Record<string, unknown>>(sql: string, params?: unknown[]): T | undefined;
  run(sql: string, params?: unknown[]): {
    changes: number;
    lastInsertRowid: number;
  };
};
type Logger = {
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
};
declare const PHASES: readonly ["S1", "S2", "S3", "S4", "S5", "S6"];
type Phase = (typeof PHASES)[number];
type ReportEvidence = {
  source: 'run_tool' | 'tool_result';
  exitCode?: number;
  stdoutHash?: string;
  stderrHash?: string;
  hashes?: Record<string, string>;
  producedAt?: string;
  tool?: string;
  commandHash?: string;
};
type Report = {
  report_id: string;
  node_run_id: string;
  attempt: number;
  status: string;
  evidence?: ReportEvidence;
  output?: unknown;
  error_code?: string;
};
type ApplyResult = {
  accepted: boolean;
  reason?: string;
  next_status?: string;
  registry_state: 'rejected' | 'replayed' | 'applied' | 'stale';
};
type PassCondition = {
  path: string;
  op: 'eq' | 'ne' | 'exists' | 'hashContains';
  value?: unknown;
};
type PolicyOperator = 'eq' | 'ne' | 'in' | 'gt' | 'gte' | 'lt' | 'lte' | 'exists' | 'match';
type PolicyCondition = {
  all: readonly PolicyCondition[];
} | {
  any: readonly PolicyCondition[];
} | {
  not: PolicyCondition;
} | {
  path: string;
  op: PolicyOperator;
  value?: unknown;
};
type BranchEdge = {
  to: string;
  when?: PolicyCondition;
  priority?: number;
  default?: boolean;
};
type NodeType = 'agent' | 'parallel' | 'subworkflow' | 'decision' | 'wait' | 'policy' | 'approval' | 'branch' | 'tool';
type PolicyNodeConfig = {
  condition: PolicyCondition;
  rule_id?: string;
};
type ApprovalNodeConfig = {
  prompt: string;
  on_timeout: 'fail';
};
type ToolNodeConfig = {
  command: string;
  cwd: string;
  timeout_ms: number;
};
type ParallelNodeConfig = {
  join: 'all' | 'any' | 'quorum';
  quorum?: number;
  allow_irreversible_children?: boolean;
  timeout_ms?: number;
};
type WaitNodeConfig = {
  wait_kind: 'event' | 'timer';
  match: {
    type: string;
    path?: string;
    equals?: unknown;
  };
  max_wait_ms: number;
  from_seq?: number;
};
type SubworkflowNodeConfig = {
  workflow_id: string;
  pinned_version: number;
  resume_from?: string | null;
};
type WorkflowNodeDef = {
  node_id: string;
  type: NodeType;
  side_effect_policy?: 'pure' | 'compensable' | 'irreversible';
  recovery_retry_idempotent?: boolean;
  tools?: {
    allow?: string[];
    deny?: string[];
  };
  verifier?: {
    toolPolicy?: {
      allow?: string[];
      deny?: string[];
    };
  };
  pass_conditions?: PassCondition[];
  required_hashes?: string[];
  wait?: WaitNodeConfig;
  subworkflow?: SubworkflowNodeConfig;
  policy?: PolicyNodeConfig;
  approval?: ApprovalNodeConfig;
  tool?: ToolNodeConfig;
  parallel?: ParallelNodeConfig;
};
type WorkflowDefinition = {
  workflow_id?: string;
  workflow_version: string;
  pass_conditions?: PassCondition[];
  required_hashes?: string[];
  verifier?: WorkflowNodeDef['verifier'];
  nodes: WorkflowNodeDef[];
  edges?: WorkflowEdge[];
};
type WorkflowEdge = {
  from: string;
  to: string;
  route?: Omit<BranchEdge, 'to'>;
};
type ToolCapability = 'read_only' | 'mutating' | 'compensating';
type EffectVerdict = 'applied' | 'not_applied';
type EffectVerificationTarget = {
  workflow_definition_hash: string;
  node_id: string;
  node_run_id: string;
  run_id: string;
  attempt: number;
};
type EffectVerifier = {
  id: string;
  workflowDefinitionHash: string;
  nodeId: string;
  cwd?: string;
  /** Interpret the actual read-only tool output for this specific action instance. */
  verify: (target: Readonly<EffectVerificationTarget>, output: {
    stdout: string;
    stderr: string;
  }) => {
    verdict: EffectVerdict;
    reference: string;
  } | null;
};
type ToolDescriptor = {
  name: string;
  capability: ToolCapability;
  command: {
    bin: string;
    args: string[];
  };
  compensation?: {
    tool: string;
    args: string[];
    verify: {
      command: string;
      args: string[];
    };
  };
  effectVerifier?: EffectVerifier;
};
declare class ToolRegistry {
  private readonly tools;
  register(descriptor: ToolDescriptor): void;
  lookup(name: string): ToolDescriptor | undefined;
  require(name: string): ToolDescriptor;
  list(): ToolDescriptor[];
}
type ToolExecutionBoundary = 'windows_appcontainer_bfs' | 'dsh_workspace_rpc';
type HostRunner = {
  readonlyAvailable(bin?: string): boolean;
  run(input: {
    bin: string;
    args: string[];
    cwd: string;
    timeoutMs: number;
    signal?: AbortSignal;
  }): Promise<{
    exitCode: number;
    stdout: string;
    stderr: string;
    boundary?: ToolExecutionBoundary;
  }>;
};
type StrongEvidence = {
  source: 'run_tool';
  boundary?: ToolExecutionBoundary;
  exitCode: number;
  stdoutHash: string;
  stderrHash: string;
  hashes: Record<string, string>;
  producedAt: string;
  tool: string;
  commandHash: string;
};
type RunToolParams = {
  command: string;
  args: string[];
  cwd: string;
  timeoutMs: number;
  workspaceRoot: string;
  signal?: AbortSignal;
};
type RunToolResult = {
  evidence: StrongEvidence;
  stdout: string;
  stderr: string;
};
declare class ReadOnlyToolRunner {
  private readonly registry;
  private readonly host;
  private readonly workspaceRoot;
  constructor(registry: ToolRegistry, host: HostRunner, workspaceRoot: string);
  get root(): string;
  readonlyAvailable(command?: string): boolean;
  runTool(params: RunToolParams): Promise<RunToolResult>;
}
type LoadWorkflowResult = {
  workflow_id: string;
  workflow_version: string;
  nodes: {
    node_id: string;
    type: string;
    pass_conditions: PassCondition[];
  }[];
};
declare class WorkflowCatalog {
  private readonly defs;
  private readonly nodes;
  private readonly phase;
  private readonly db?;
  private readonly isReadOnlyTool?;
  private readonly registry?;
  constructor(phase: Phase, db?: Db, isReadOnlyTool?: (tool: string) => boolean, registry?: ToolRegistry);
  initialAgentEffect(node: WorkflowNodeDef): 'pure' | 'compensable' | 'irreversible';
  hydrate(): number;
  /**
   * 发布事务提交后把新版本装进内存目录：同一进程立刻可起 run，不必等重启 hydrate（审计 P1-1）。
   * 哈希对不上就拒：内存定义必须和库内版本一字不差。
   */
  adopt(def: WorkflowDefinition, expectedHash: string): void;
  /** 发布前沿用目录的阶段限制和运行配置校验，避免提交目录无法加载的版本。 */
  validateDefinition(def: WorkflowDefinition): void;
  requireWorkflowCapability(workflowId: string | undefined): void;
  load(def: WorkflowDefinition): LoadWorkflowResult;
  private checkRegisteredTools;
  /**
   * 只在“引用的版本都已经加载”时做环/深度校验：
   * 缺版本说明这个子流程还没发布，此时无法判断整图，强校验放在派发路径（assertSubworkflowGraphFor）。
   */
  private assertSubworkflowClosure;
  get(workflowIdOrVersion: string | number, maybeVersion?: string | number): WorkflowDefinition;
  /** Shares a catalog-owned, deeply frozen definition with internal readers. */
  getImmutable(workflowId: string, workflowVersion: string | number): WorkflowDefinition;
  getImmutableNode(workflowId: string, workflowVersion: string | number, nodeId: string): WorkflowNodeDef | undefined;
  hasPersistedStorage(): boolean;
  definitionHash(workflowIdOrVersion: string | number, maybeVersion?: string | number): string;
  toolPolicyFor(workflowIdOrVersion: string | number, versionOrNodeId: string | number, maybeNodeId?: string): {
    allow: string[];
    deny: string[];
  };
  policyFor(workflowIdOrVersion: string | number, versionOrNodeId: string | number, maybeNodeId?: string): {
    passConditions: PassCondition[];
    requiredHashes: string[];
  };
  private internal;
  private resolve;
  private nodeOf;
  private assertAliasFree;
  private remember;
}
type IntakeAction = 'pause' | 'resume' | 'page' | 'ack';
type State = {
  revision: number;
  paused: boolean;
  reason: string;
  actor: string | null;
  awaiting: {
    id: string;
    sent_at: string;
    deadline: string;
    inbox_id?: string;
    channel?: 'dsh-onebot-qq';
  } | null;
};
type IntakeOptions = {
  /** The installed host resolves an authenticated operator for this particular action. */
  authenticate: (proof: string, action: IntakeAction) => string | null;
  /** Resolve a recorded, real tool invocation for the configured on-call chat in this workspace. */
  lookupQqDelivery?: (inboxId: string, invocationId: string) => {
    plugin: string;
    tool: string;
    sentAt: string;
    sent: boolean;
    messageId: string | null;
  } | null;
  now?: () => string;
};
declare function createIntakeControl(db: Db, options: IntakeOptions): {
  get: () => {
    paused: boolean;
    revision: number;
    reason: string;
    actor: string | null;
    awaiting: {
      id: string;
      sent_at: string;
      deadline: string;
      inbox_id?: string;
      channel?: "dsh-onebot-qq";
    } | null;
  };
  pause: (proof: string, reason: string, revision?: number) => State;
  resume: (proof: string, reason: string, revision?: number) => State;
  page: (proof: string, pageId: string, reason: string) => State;
  recordQqDelivery(proof: string, inboxId: string, invocationId: string, reason: string): State;
  ack: (proof: string, pageId: string, reason: string) => State;
};
type Circle = 'R0' | 'R1' | 'R2';
type Workspace = {
  id: string;
  kind: 'synthetic' | 'repository' | 'sandbox';
};
type Flow = {
  id: string;
  hash: string;
};
type TrialScope = {
  workspaces: Workspace[];
  flows: Flow[];
  tools: string[];
};
type RecordState = {
  candidate: string;
  circle: Circle;
  revision: number;
  since: string;
  scope: TrialScope;
  halted: boolean;
};
type Transition = {
  candidate: string;
  from: Circle;
  to: Circle;
  revision: number;
  since: string;
  scope: TrialScope;
};
type TrialOptions = {
  candidate: string;
  workspace: Workspace & {
    root: string;
  };
  /** Installed host verifies bound operator, expiry, scope, G6 and actual observation records. */
  authorize: (proof: string, transition: Readonly<Transition>) => {
    actor: string;
    expiresAt: string;
    observationFrom: string;
    observationTo: string;
    evidenceId: string;
    gateG6: boolean;
  } | null;
  authenticateOperator: (proof: string, action: 'stop' | 'restart') => string | null;
  now?: () => string;
};
declare function bindTrialHost(db: Db, options: TrialOptions, runnerRoot?: string): {
  get: () => RecordState;
  advance(proof: string, to: "R1" | "R2", scope: TrialScope, revision: number): {
    circle: "R1" | "R2";
    revision: number;
    since: string;
    scope: TrialScope;
    candidate: string;
    halted: boolean;
  };
  stop(proof: string, reason: string): RecordState;
  restartSynthetic(proof: string, reason: string, revision: number): {
    halted: boolean;
    revision: number;
    since: string;
    candidate: string;
    circle: Circle;
    scope: TrialScope;
  };
};
declare const ERROR_STATUS: {
  readonly UNAUTHORIZED: 401;
  readonly FORBIDDEN: 403;
  readonly PROTOCOL_VERSION_MISMATCH: 409;
  readonly SCHEMA_VERSION_UNSUPPORTED: 422;
  readonly NOT_FOUND: 404;
  readonly CURSOR_EXPIRED: 410;
  readonly CURSOR_FILTER_MISMATCH: 409;
  readonly RANGE_TOO_LARGE: 413;
  readonly QUERY_TIMEOUT: 504;
  readonly SERVICE_UNAVAILABLE: 503;
  readonly OPERATION_ID_CONFLICT: 409;
  readonly OPERATION_EXPIRED: 410;
  readonly OPERATION_TIMEOUT: 504;
  readonly COMMAND_BUSY: 409;
  readonly CONFIRMATION_REQUIRED: 428;
  readonly CONFIRMATION_MISMATCH: 409;
  readonly CONFIRMATION_EXPIRED: 410;
  readonly DECISION_REVISION_CONFLICT: 409;
  readonly RESOLUTION_NOT_AVAILABLE: 422;
  readonly VERIFICATION_TOO_WEAK: 422;
  readonly VERDICT_UNSUPPORTED: 422;
  readonly VERDICT_POLICY_UNMET: 422;
  readonly STALE_GENERATION: 409;
  readonly STALE_HANG: 409;
  readonly BRIDGE_NOT_SUBSCRIBED: 409;
  readonly TOOL_NOT_REGISTERED: 422;
  readonly RUN_TOOL_NOT_READ_ONLY: 403;
  readonly SIDE_EFFECT_UNDECLARED: 422;
  readonly COMPENSATION_UNEXECUTABLE: 422;
  readonly COMPENSATION_TOOL_NOT_COMPENSATING: 422;
  readonly COMPENSATION_TOOL_IN_FORWARD_PATH: 422;
  readonly EVIDENCE_SOURCE_UNAVAILABLE: 422;
  readonly EFFECT_RESOLUTION_CONFLICT: 409;
  readonly VERIFIER_TOOL_POLICY_MUTATING: 422;
  readonly WORKFLOW_NOT_FOUND: 404;
  readonly WORKFLOW_VERSION_NOT_FOUND: 404;
  readonly WORKFLOW_REVISION_CONFLICT: 409;
  readonly WORKFLOW_VERSION_CONFLICT: 409;
  readonly WORKFLOW_VERSION_IMMUTABLE: 409;
  readonly WORKFLOW_DRAFT_NOT_OPEN: 409;
  readonly WORKFLOW_DRAFT_EXISTS: 409;
  readonly WORKFLOW_NOT_PUBLISHED: 409;
  readonly WORKFLOW_VALIDATION_FAILED: 422;
  readonly WORKFLOW_TITLE_INVALID: 422;
  readonly WORKFLOW_NAME_INVALID: 422;
  readonly WORKFLOW_DEFINITION_TOO_LARGE: 413;
  readonly WORKFLOW_EDGE_SELF_LOOP: 422;
  readonly WORKFLOW_EDGE_DUPLICATE: 422;
  readonly WORKFLOW_DEFINITION_INVALID: 422;
  readonly WORKFLOW_NODE_ID_INVALID: 422;
  readonly WORKFLOW_NODE_ID_DUPLICATE: 422;
  readonly WORKFLOW_EDGE_ENDPOINT_MISSING: 422;
  readonly WORKFLOW_LAYOUT_INVALID: 422;
  readonly WORKFLOW_NO_TRIGGER: 422;
  readonly WORKFLOW_MULTIPLE_TRIGGERS: 422;
  readonly WORKFLOW_TRIGGER_HAS_INBOUND: 422;
  readonly WORKFLOW_TRIGGER_FANOUT: 422;
  readonly WORKFLOW_NODE_FANOUT_INVALID: 422;
  readonly WORKFLOW_NODE_UNREACHABLE: 422;
  readonly WORKFLOW_GRAPH_CYCLE: 422;
  readonly WORKFLOW_BRANCH_ROUTE_INVALID: 422;
  readonly WORKFLOW_BRANCH_PRIORITY_DUPLICATE: 422;
  readonly WORKFLOW_BRANCH_DEFAULT_DUPLICATE: 422;
  readonly NODE_NOT_SUPPORTED: 422;
  readonly WORKFLOW_NODE_CONFIG_INVALID: 422;
  readonly TOOL_NOT_ALLOWED: 422;
  readonly PAYLOAD_SCHEMA_INVALID: 422;
  readonly WORKFLOW_SUBWORKFLOW_VERSION_MISSING: 422;
  readonly SUBWORKFLOW_DEPTH_EXCEEDED: 422;
  readonly BRANCH_AMBIGUOUS: 422;
  readonly SUBWORKFLOW_CYCLE: 422;
  readonly MEMORY_BATCH_TOO_LARGE: 413;
  readonly MEMORY_PROPOSAL_INVALID: 422;
  readonly MEMORY_BATCH_CONFLICT: 422;
  readonly MEMORY_BATCH_NOT_FOUND: 404;
  readonly MEMORY_EVAL_INVALID_OUTPUT: 502;
  readonly MEMORY_EVAL_TIMEOUT: 504;
  readonly MEMORY_EVAL_UNAVAILABLE: 503;
  readonly MEMORY_EVAL_DISABLED: 503;
  readonly MEMORY_BATCH_NOT_APPROVED: 409;
  readonly MEMORY_APPLY_BINDING_INVALID: 403;
  readonly MEMORY_APPLY_CONFLICT: 409;
  readonly MEMORY_APPLY_FAILED: 500;
  readonly MEMORY_RUN_ACTIVE: 409;
  readonly MEMORY_SOURCE_RUN_NOT_SETTLED: 409;
  readonly MEMORY_WRITE_TOOL_FORBIDDEN: 422;
};
type OrchErrorCode = keyof typeof ERROR_STATUS;
/** S2 提案码：尚未进规格 17.1，统一从 proposedError 出，details 带 proposed=true。 */
declare const PROPOSED_ERROR_CODES: {
  readonly DISPATCH_SETTLE_FAILED: 503;
  readonly LEASE_EXPIRED: 409;
  readonly BRIDGE_ACK_LAG: 409;
  readonly BRIDGE_SEQ_CONFLICT: 409;
  readonly DECISION_REQUIRED: 422;
};
type ProposedErrorCode = keyof typeof PROPOSED_ERROR_CODES;
type AnyOrchErrorCode = OrchErrorCode | ProposedErrorCode;
type BootId = string;
type ClockSample = {
  bootId: BootId;
  monotonicNs: number;
  wallClockMs: number;
};
type Deadline = {
  bootId: BootId;
  monotonicNs: number;
  wallClockMs: number;
};
type ClockDecision = {
  kind: 'pending';
  remainingMs: number;
} | {
  kind: 'expired';
} | {
  kind: 'uncertain';
  reason: string;
};
type ClockProvider = {
  readonly bootId: BootId;
  sample(): ClockSample;
  decide(deadline: Deadline): ClockDecision;
};
type NodeRunRow = {
  node_run_id: string;
  run_id: string;
  node_id: string;
  node_type: string;
  seq: number;
  attempt: number;
  status: string;
  revision: number;
  owner_epoch: number | null;
  lease_id: string | null;
  lease_expires_at: string | null;
  heartbeat_at: string | null;
  action_generation: number;
  rearm_generation: number;
  fence_state: string;
  fence_grace_until: string | null;
  parent_node_run_id: string | null;
  side_effect_policy: 'pure' | 'compensable' | 'irreversible';
  deadline_at: string | null;
  deadline_boot_id: string | null;
  deadline_monotonic_ns: number | null;
  hang_reason: string | null;
  contradiction_decision_id: string | null;
  startup_unknown_deadline_at: string | null;
  startup_unknown_remaining_ns: number | null;
  startup_unknown_boot_id: string | null;
  startup_unknown_monotonic_ns: number | null;
};
type CancelResult$1 = 'interrupted' | 'not_interruptible' | 'already_stopped' | 'unknown';
type CancelOutcome = {
  cancel_result: CancelResult$1;
  next_status: string;
  fence_state: 'armed' | 'revoked' | null;
  reason: string;
};
type SweepResult = {
  lease_expired: string[];
  fence_revoked: string[];
  verify_timed_out: string[];
  verify_uncertain: string[];
  replay_heads: {
    node_run_id: string;
    report_id: string;
    report_seq: number;
  }[];
  chain_broken: string[];
  startup_unknown_expired: string[];
  bridge_paused: string[];
  bridge_restored: string[];
  wait_timed_out: number;
  executor_orphaned: string[];
  action_overrun: string[];
  operations_timed_out: number;
  receipts_expired: number;
  subworkflow_finished: string[];
  decisions_blocked: number;
  decisions_expired: number;
  decisions_released_nodes: string[];
  deadline_uncertain: string[];
  runs_closed: string[];
  scanned_at: string;
};
/** 查询事务仅暴露读方法，避免读模型通过类型化接口发出写语句。 */
type ReadTx = {
  all<T = Record<string, unknown>>(sql: string, params?: unknown[]): T[];
  get<T = Record<string, unknown>>(sql: string, params?: unknown[]): T | undefined;
};
type GroupStop = {
  nodeRunId: string;
  attempt: number;
  groupId: string;
};
type RuntimeDeps = {
  db: Db;
  secret: string;
  phase: Phase;
  catalog: WorkflowCatalog;
  clock: ClockProvider;
  cancelParallelAttempt?: (intent: GroupStop) => void;
};
declare function renewNodeLease(deps: RuntimeDeps, p: {
  nodeRunId: string;
  leaseTtlMs: number;
  now: string;
}): {
  expires_at: string;
};
declare function submitReport(deps: RuntimeDeps, p: {
  nodeRunId: string;
  attempt: number;
  report: Report;
  expectedStatus: string;
  generation: number;
  decision?: {
    decisionId: string;
    revision: number;
    resolution: 'confirm_applied' | 'confirm_not_applied' | 'uphold_report' | 'uphold_decision';
  };
  now: string;
}, allowArtifactCompletion?: boolean): ApplyResult;
declare function sweepNow(deps: RuntimeDeps, now: string): SweepResult;
type HostAgentFactory = {
  create(input: {
    nodeRunId: string;
    attempt: number;
    input: unknown;
    outputSchema: unknown;
    toolPolicy: unknown;
    inheritHistory: false;
    modelRef?: string;
    sessionModelFallback?: true;
    reasoningEffort?: string | null;
    actionGeneration?: number;
  }): Promise<{
    agentId: string;
    sessionId: string;
    effectiveModelRef?: string;
    effectiveReasoningEffort?: string | null;
  }>;
};
type BridgeEventSource = {
  listen(params: {
    executorAgentId: string;
    attempt: number;
    fromSeq: number;
  }, signal: AbortSignal): AsyncIterable<BridgeEvent>;
};
type BridgeTransport = BridgeEventSource & {
  request<T>(method: string, params: unknown): Promise<T>;
  /** Atomically fences send and retires a live child whose instruction was never admitted. */
  retireUnsentAttempt?(binding: UnsentAttemptBinding): Promise<boolean>;
  /** Optional local host snapshot before disposal. Never comes from an agent report. */
  snapshotAttemptModelUse?(params: {
    executorAgentId: string;
    attempt: number;
  }): unknown;
  /** Available only on a local host that can inspect the same resident child. */
  observeAttempt?(binding: ResidentAttemptBinding): Pick<LiveAttemptObservation, 'hasToolCall' | 'observedRunning' | 'lastToolCallId'> | undefined;
  fenceAttemptTools?(binding: ResidentAttemptBinding, mayUseTools: () => boolean, signal: AbortSignal): boolean;
};
declare const BRIDGE_EVENT_TYPES: readonly ["step", "turn", "tool.call", "tool.start", "tool.result"];
type BridgeEventType = (typeof BRIDGE_EVENT_TYPES)[number];
type BridgeEvent = {
  seq: number;
  type: BridgeEventType;
  at: string;
  toolStartAt: string | null;
  payload: unknown;
  attempt?: number;
  executorAgentId?: string;
};
type SpawnParams = {
  nodeRunId: string;
  attempt: number;
  input: unknown;
  outputSchema: unknown;
  toolPolicy: unknown;
  inheritHistory: false;
  modelRef?: string;
  sessionModelFallback?: true;
  reasoningEffort?: string | null;
  actionGeneration?: number;
};
type SpawnResult = {
  sessionId: string;
  agentId: string;
  effectiveModelRef?: string;
  effectiveReasoningEffort?: string | null;
};
type SubscribeParams = {
  executorAgentId: string;
  attempt: number;
  fromSeq: number;
};
type SubscribeResult = {
  subscribed: boolean;
  fromSeq: number;
  lastSeq: number;
};
type SendParams = {
  executorAgentId: string;
  attempt: number;
  instruction: string;
  attemptId: string;
};
type CancelResult = 'interrupted' | 'not_interruptible' | 'already_stopped' | 'unknown';
type ExecutorState = 'loaded' | 'unloaded' | 'deleted' | 'running' | 'idle';
type StateResult = {
  state: ExecutorState;
  toolCallSeq: number;
  hasToolCall: boolean;
};
type UnsentAttemptBinding = {
  nodeRunId: string;
  attempt: number;
  actionGeneration: number;
  executorAgentId: string;
};
type ResidentAttemptBinding = UnsentAttemptBinding & {
  executorSessionId: string;
};
type LiveAttemptBinding = ResidentAttemptBinding & {
  rearmGeneration: number;
};
type LiveAttemptObservation = {
  hasToolCall: boolean;
  observedRunning: boolean;
  lastToolCallId?: string;
  observationBudgetMs: number;
  actionTimeoutMs: number;
};
type LiveAttemptMonitor = {
  binding: ResidentAttemptBinding;
  budget: (rearmGeneration: number) => number;
  actionTimeoutMs: number;
};
type WhenIdleParams = {
  executorAgentId: string;
  timeoutMs: number;
};
declare class BridgeClient {
  private readonly transport;
  private readonly subscribed;
  private readonly monitors;
  constructor(transport: BridgeTransport);
  spawn(params: SpawnParams): Promise<SpawnResult>;
  subscribe(params: SubscribeParams): Promise<SubscribeResult>;
  snapshotAttemptModelUse(params: {
    executorAgentId: string;
    attempt: number;
  }): unknown;
  installAttemptMonitor(monitor: LiveAttemptMonitor, mayUseTools: () => boolean, signal: AbortSignal): (() => void) | undefined;
  liveAttempt(binding: LiveAttemptBinding): LiveAttemptObservation | undefined;
  retireUnsentAttempt(binding: UnsentAttemptBinding): Promise<boolean>;
  send(params: SendParams): Promise<void>;
  cancel(params: {
    executorAgentId: string;
    attempt: number;
  }): Promise<CancelResult>;
  dispose(params: {
    executorAgentId: string;
    serviceAckSeq: number;
    bridgeLastSeq: number;
  }): Promise<void>;
  state(params: {
    executorAgentId: string;
  }): Promise<StateResult>;
  whenIdle(params: WhenIdleParams): Promise<{
    idle: boolean;
    timedOut: boolean;
  }>;
  fetchPage(params: {
    executorAgentId: string;
    attempt: number;
    fromSeq: number;
    pageSize: number;
  }): Promise<{
    events: BridgeEvent[];
  }>;
  listen(params: {
    executorAgentId: string;
    attempt: number;
    fromSeq: number;
  }, signal: AbortSignal): AsyncIterable<BridgeEvent>;
  toolResult(params: {
    executorAgentId: string;
    attempt: number;
    callId: string;
    result: {
      exitCode: number;
      stdout: string;
      stderr: string;
      evidenceId?: string;
    };
  }): Promise<void>;
  isSubscribed(executorAgentId: string): boolean;
}
declare const MODEL_COMPLEXITIES: readonly ["routine", "standard", "advanced", "complex", "expert"];
type ModelComplexity = (typeof MODEL_COMPLEXITIES)[number];
type ModelReasoning = {
  efforts: readonly {
    id: string;
    name: string;
    description?: string;
  }[];
  defaultEffort?: string;
};
type ModelProfile = {
  ref: string;
  capacity: ModelComplexity;
  provider?: string;
  model?: string;
  reasoning?: ModelReasoning;
};
type ModelCatalogProfile = Omit<ModelProfile, 'capacity'> & {
  capacity: ModelComplexity | null;
  host_capacity?: ModelComplexity | null;
  name?: string;
  provider_name?: string;
};
type ModelCatalogSnapshot = {
  profiles: readonly ModelCatalogProfile[];
  unavailable_providers?: readonly string[];
  session_execution_model_ref?: string;
};
type ModelPlanningProfile = ModelProfile & {
  user_note: string;
};
type ModelRecommendation = {
  complexity: ModelComplexity;
  reason: string;
  preferredModel?: string;
};
type ModelTokenUsage = {
  uncached_input_tokens: number | null;
  output_tokens: number | null;
  cache_read_tokens: number | null;
  cache_write_tokens: number | null;
};
type ModelAttemptMetering = {
  tokens: ModelTokenUsage;
  settlement_incomplete?: true;
  context_compactions: {
    summaries: number;
    tool_result_prunes: number;
  } | null;
};
type ModelUse = {
  provider: string;
  model: string;
  observed_request_headers: number;
  reasoning_effort?: string | null;
  metering?: ModelAttemptMetering;
};
type ModelAssignment = {
  model_ref: string;
  complexity: ModelComplexity;
  reason: string;
  usage?: ModelUse;
  settings_revision?: number;
  provider?: string;
  model?: string;
  session_model_fallback?: true;
  reasoning_effort?: string | null;
};
type ModelRoutingContext = {
  taskId: string;
  runId: string;
  nodeRunId: string;
  nodeId: string;
  attempt: number;
  actionGeneration: number;
  workflowId: string | null;
  workflowVersion: string | null;
};
type ModelRouting = {
  /** Models already approved for this execution host, in preference order within a capacity. */
  profiles: readonly ModelProfile[];
  /** Personal DSH hosts expose unclassified entries as well as approved execution profiles. */
  catalog?: () => ModelCatalogSnapshot;
  refresh?: (signal?: AbortSignal) => Promise<void>;
  /** Read the approved task plan synchronously. Dispatch calls this inside a write transaction. */
  recommend(context: ModelRoutingContext): ModelRecommendation;
};
type ReworkBinding = {
  node_run_id: string;
  attempt: number;
  action_generation: number;
};
type ReworkActor = string | ReworkBinding;
declare const reworkRequestSchema: z.ZodObject<{
  operation_id: z.ZodString;
  run_id: z.ZodString;
  target_node_run_id: z.ZodString;
  source_artifact_id: z.ZodString;
  source_artifact_hash: z.ZodString;
  expected_revision: z.ZodNumber;
  reason: z.ZodString;
  expected_result: z.ZodString;
  preserve_requirements: z.ZodDefault<z.ZodArray<z.ZodString>>;
  suggested_scope: z.ZodDefault<z.ZodEnum<{
    targeted_revision: "targeted_revision";
    rerun_node: "rerun_node";
    replan_subgraph: "replan_subgraph";
  }>>;
  evidence_ids: z.ZodDefault<z.ZodArray<z.ZodString>>;
  verification_request_id: z.ZodOptional<z.ZodString>;
}, z.core.$strict>;
declare const reworkReviewSchema: z.ZodObject<{
  operation_id: z.ZodString;
  request_id: z.ZodString;
  expected_revision: z.ZodNumber;
  action: z.ZodEnum<{
    authorize: "authorize";
    accept: "accept";
    reject: "reject";
    reject_final: "reject_final";
    request_changes: "request_changes";
    escalate: "escalate";
  }>;
  note: z.ZodString;
  artifact_id: z.ZodOptional<z.ZodString>;
  artifact_hash: z.ZodOptional<z.ZodString>;
  evidence_ids: z.ZodDefault<z.ZodArray<z.ZodString>>;
}, z.core.$strict>;
type ReworkRequest = {
  request_id: string;
  run_id: string;
  requester_node_run_id: string | null;
  requester_actor: string;
  owner_actor: string;
  target_node_run_id: string;
  target_attempt: number;
  source_artifact_id: string;
  source_artifact_hash: string;
  reason: string;
  expected_result: string;
  preserve_requirements_json: string;
  suggested_scope: string;
  approved_scope: string | null;
  status: string;
  revision: number;
  round_no: number;
  max_rounds: number;
  budget_remaining: number;
  new_node_run_id: string | null;
  evidence_ids_json: string;
  verification_request_id: string | null;
  expires_at: string;
  created_at: string;
  updated_at: string;
};
type ReworkAttempt = {
  attempt_id: string;
  request_id: string;
  round_no: number;
  old_node_run_id: string;
  new_node_run_id: string;
  source_artifact_id: string;
  source_artifact_hash: string;
  result_artifact_id: string | null;
  result_artifact_hash: string | null;
  status: string;
  feedback: string;
  created_at: string;
  reviewed_at: string | null;
};
type ReworkReview = {
  review_id: string;
  request_id: string;
  round_no: number;
  reviewer_actor: string;
  artifact_id: string | null;
  artifact_hash: string | null;
  action: string;
  note: string;
  evidence_ids_json: string;
  created_at: string;
};
declare function createReworkHost(db: Db, secret: string, options?: {
  clock?: ClockProvider;
  humanOnly?: boolean;
}): {
  request(input: z.input<typeof reworkRequestSchema>, actor: ReworkActor, now: string): ReworkRequest;
  requestFromPlanner(input: z.input<typeof reworkRequestSchema>, supervisorOwner: string, now: string): ReworkRequest;
  requestFromPlannerInTx(tx: Tx, input: z.input<typeof reworkRequestSchema>, supervisorOwner: string, now: string): ReworkRequest;
  review(input: z.input<typeof reworkReviewSchema>, actor: ReworkActor, now: string): ReworkRequest;
  list(actor: ReworkActor, runId?: string): {
    items: ReworkRequest[];
    as_of_seq: number;
  };
  get(requestId: string, actor: ReworkActor): {
    item: {
      request: ReworkRequest;
      dispatch_blocks: {
        node_id: string;
        node_run_id: string;
        reason: string;
        message: string;
        created_at: string;
        resolved_at: string | null;
      }[];
      attempts: ReworkAttempt[];
      reviews: ReworkReview[];
      impacts: {
        node_id: string;
        node_run_id: string;
        replacement_node_run_id: string | null;
        reason: string;
        released_at: string | null;
      }[];
    };
    as_of_seq: number;
  };
  context(nodeRunId: string): ReworkRequest | null;
  recover(now: string): void;
};
type ReworkHost = ReturnType<typeof createReworkHost>;
declare const localPlanInput: z.ZodObject<{
  operation_id: z.ZodUUID;
  request_id: z.ZodString;
  expected_revision: z.ZodNumber;
}, z.core.$strict>;
declare const localPlanApproval: z.ZodObject<{
  operation_id: z.ZodUUID;
  request_id: z.ZodString;
  expected_revision: z.ZodNumber;
  plan_hash: z.ZodString;
}, z.core.$strict>;
type ReworkPlanner = (input: unknown, signal: AbortSignal) => Promise<unknown>;
declare function createReworkPlanningHost(db: Db, secret: string, planner?: ReworkPlanner, humanOnly?: boolean): {
  enabled: boolean;
  topology(runId: string, actor: string): {
    as_of_seq: number;
    edges: Readonly<{
      from: string;
      to: string;
    }>[];
    steps: Record<string, unknown>[];
    requests: Record<string, unknown>[];
    held_node_ids: string[];
  };
  get(id: string, actor: string): {
    plan: {
      plan_hash: string;
      revision: number;
      proposal: {
        reason: string;
        changes: {
          node_id: string;
          instruction: string;
        }[];
        preserved_node_ids: string[];
        additional_edges: {
          from: string;
          to: string;
        }[];
        insert_steps: {
          node_id: string;
          before_node_id: string;
          instruction: string;
        }[];
      };
      approved_by: string | null;
    } | null;
    as_of_seq: number;
  };
  propose(input: z.infer<typeof localPlanInput>, actor: string, now: string): Promise<{
    plan_hash: string;
    revision: number;
    proposal: {
      reason: string;
      changes: {
        node_id: string;
        instruction: string;
      }[];
      preserved_node_ids: string[];
      additional_edges: {
        from: string;
        to: string;
      }[];
      insert_steps: {
        node_id: string;
        before_node_id: string;
        instruction: string;
      }[];
    };
    approved_by: string | null;
  }>;
  approve(input: z.infer<typeof localPlanApproval>, actor: string, now: string): ReworkRequest;
};
declare const opinionSchema: z.ZodObject<{
  classification: z.ZodEnum<{
    insufficient: "insufficient";
    model_capability: "model_capability";
    model_efficiency: "model_efficiency";
    waiting_external: "waiting_external";
    other: "other";
  }>;
  reason: z.ZodString;
  evidence_ids: z.ZodArray<z.ZodString>;
  model_ref: z.ZodOptional<z.ZodString>;
}, z.core.$strict>;
declare const modelEscalationInput: z.ZodObject<{
  operation_id: z.ZodUUID;
  run_id: z.ZodString;
  node_run_id: z.ZodString;
  expected_attempt: z.ZodNumber;
  expected_action_generation: z.ZodNumber;
}, z.core.$strict>;
type Input = z.infer<typeof modelEscalationInput>;
type Profile = ModelProfile & {
  provider?: string;
  model?: string;
};
type ProfileSource = readonly Profile[] | ((tx: ReadTx) => readonly Profile[]);
type Trigger = 'prolonged_runtime' | 'execution_blocked' | 'repeated_rework';
type ModelEscalationAssessment = z.infer<typeof opinionSchema> & {
  attempt: number;
  action_generation: number;
  previous_model_ref: string;
  decided_at: string;
  expires_at: string;
  fingerprint: string;
  outcome: 'recommended' | 'kept' | 'no_higher_model';
  previous_profile: Profile;
  recommended_profile?: Profile;
  trigger: Trigger;
  actor: string;
};
declare function createModelEscalationHost(db: Db, secret: string, profiles: ProfileSource, planner?: ReworkPlanner, onError?: (error: unknown) => void, nowAt?: () => string, humanOnly?: boolean): {
  enabled: boolean;
  assess: (input: Input, actor: string, now: string, successor?: {
    id: string;
    generation: number;
  }) => Promise<ModelEscalationAssessment>;
  select(tx: Tx, node: NodeRunRow, currentProfiles: readonly Profile[], base: ModelAssignment, executionNow: string): ModelAssignment;
  prepare(nodeRunId: string, generation: number, now: string): Promise<void>;
  get(runId: string, nodeRunId: string, actor: string): {
    item: ModelEscalationAssessment | null;
    dispatch: {
      model_ref: string;
      state: string;
      provider: string | null;
      model: string | null;
    } | null;
    successor_node_run_id: string | null;
    precondition: {
      run_id: string;
      node_run_id: string;
      expected_attempt: number;
      expected_action_generation: number;
    } | null;
    as_of_seq: number;
  };
  tick(now: string): void;
};
declare const PROJECT_CONTEXT_FILE = "ORCHESTRA.md";
type ProjectStatus = 'available' | 'missing' | 'unavailable' | 'excluded';
type SourceInfo = {
  path: string;
  status: ProjectStatus;
  sha256?: string;
  truncated?: boolean;
  sha256_scope?: 'read_text' | 'excerpt' | 'directory_index';
};
type ProjectContextMetadata = {
  version: string;
  as_of_seq: number;
  sources: SourceInfo[];
};
type ProjectContextSource = {
  workspaceRoot: string;
  isAuthorized(): boolean;
  approvedFallback?: 'README.md' | 'AGENTS.md';
  read(path: typeof PROJECT_CONTEXT_FILE | 'README.md' | 'AGENTS.md', signal: AbortSignal): Promise<{
    text: string;
    eof: boolean;
  }>;
  list?(signal: AbortSignal): Promise<{
    entries: readonly {
      name: string;
      type: string;
    }[];
    truncated: boolean;
  }>;
};
type ArtifactInput = {
  upstream_node_run_id: string;
  artifact_id: string;
  artifact_hash: string;
  file_name: string;
};
declare function createRunSharedContext(db: Db, source: ProjectContextSource): (nodeRunId: string, nodeId: string, upstream: readonly ArtifactInput[]) => Promise<string | undefined>;
type RunNodeParams = {
  /** Internal dispatcher entry. The standalone runner rechecks the persisted node kind and definition. */
  standaloneTool?: true;
  /** The scheduler derives this from the persisted workflow and node identity at admission. */
  workflowHelperLimit?: number;
  /** Controlled by the dedicated release entrypoint. */
  releaseCheckCwd?: string;
  nodeRunId: string;
  nodeId: string;
  attempt: number;
  input: unknown;
  outputSchema: unknown;
  tools: {
    command: string;
    args: string[];
    cwd: string;
    timeoutMs: number;
  }[];
  now: string;
  ownerEpoch: number;
  leaseTtlMs: number;
  leaseRenewIntervalMs?: number;
  verifyTimeoutMs?: number;
  actionTimeoutMs?: number;
  startupTimeoutMs?: number;
  unknownWatchdogMs?: number;
};
type RunNodeResult = {
  node_run_id: string;
  final_status: string;
  evidence: StrongEvidence | null;
  agent_id: string;
  reason?: string;
};
type RunNodeDeps = {
  runtime: RuntimeDeps;
  db: Db;
  secret: string;
  registry: ToolRegistry;
  agents: HostAgentFactory;
  runner: ReadOnlyToolRunner;
  bridge: BridgeClient;
  spillDir?: string;
  cancelSignal?: AbortSignal;
  /** Trusted host routing. Incoming execution requests cannot set a model. */
  modelRouting?: ModelRouting;
  modelEscalation?: Pick<ReturnType<typeof createModelEscalationHost>, 'prepare' | 'select'>;
  sharedContext?: ReturnType<typeof createRunSharedContext>;
};
declare function runNode(deps: RunNodeDeps, p: RunNodeParams): Promise<RunNodeResult>;
type NodeSpec = {
  tools: {
    command: string;
    args: string[];
    cwd: string;
    timeoutMs: number;
  }[];
  input?: unknown;
  outputSchema?: unknown;
};
/** Internal to the approved-plan pump. Each continuation re-reads its dispatch authority. */
type DispatchProgress = {
  signal: AbortSignal;
  ticks: EventTarget;
  now: () => string;
  resolve: () => Promise<Record<string, NodeSpec> | undefined>;
};
/** 派发结果错误码：DISPATCH_SETTLE_FAILED 是派发结果级（非顶层 OrchError），其余复用 OrchErrorCode */
type DispatchOutcomeErrorCode = 'DISPATCH_SETTLE_FAILED' | AnyOrchErrorCode | '';
type DispatchOutcome = {
  node_run_id: string;
  node_id: string;
  status: 'done' | 'failed' | 'skipped' | 'waiting';
  reason?: string;
  /** 派发结果级错误码：含派发专属 DISPATCH_SETTLE_FAILED 与顶层 OrchErrorCode（S3 r64 P2） */
  error_code?: DispatchOutcomeErrorCode;
  agent_id?: string;
  evidence_source?: string;
  /** 节点被并发流程接管时的当前状态：返回 skipped 而不是 failed（S1/S2 round60 P2） */
  current_status?: string;
  /** 收口结果类别：storage_error 时调用方必须查状态后再决定能否重派 */
  settlement?: 'applied' | 'already_moved' | 'storage_error';
  /** storage_error 时节点在库里的真实状态；查不到写 unknown（S2/S3 round61 P2） */
  persisted_status?: string;
};
type TriggerRunReady = {
  run_id: string;
  task_id: string;
  workflow_id: string;
  workflow_version: string;
};
/** Trusted host resolves the pinned run's per-node specs. A run may be observed again until it drains. */
type ResolveTriggerNodeSpecs = (run: TriggerRunReady, signal: AbortSignal) => Record<string, NodeSpec> | undefined | Promise<Record<string, NodeSpec> | undefined>;
type TaskTriggerView = {
  trigger_id: string;
  task_id: string;
  title: string;
  workflow_id: string;
  workflow_version: string;
  kind: 'scheduled' | 'event';
  enabled: boolean;
  revision: number;
  daily_time: string | null;
  time_zone: string | null;
  event_type: string | null;
  next_fire_at: string | null;
  last_fired_at: string | null;
  created_at: string;
  updated_at: string;
};
type TaskTriggerCreate = {
  operation_id: string;
  title: string;
  workflow_id: string;
  workflow_version: string;
  autonomy?: 'autonomous' | 'assisted' | 'supervised';
} & ({
  kind: 'scheduled';
  daily_time: string;
  time_zone: string;
} | {
  kind: 'event';
  event_type: string;
});
type TaskTriggerOccurrence = {
  trigger_id: string;
  task_id: string;
  due_at: string;
  run_id: string | null;
  status: 'started' | 'skipped';
  reason: 'task_archived' | 'previous_run_active' | 'unresolved_effects' | 'workflow_inactive' | null;
};
declare const taskTriggerEnabledSchema: z.ZodObject<{
  operation_id: z.ZodString;
  trigger_id: z.ZodString;
  expected_revision: z.ZodNumber;
  enabled: z.ZodBoolean;
}, z.core.$strict>;
declare const taskTriggerEventSchema: z.ZodObject<{
  event_id: z.ZodString;
  event_type: z.ZodString;
}, z.core.$strict>;
type TriggerEnabled = z.infer<typeof taskTriggerEnabledSchema>;
type TriggerEvent = z.infer<typeof taskTriggerEventSchema>;
/** 触发执行就绪态：规则已配置 / 触发已发生 / 执行器未就绪 / 运行已派发 */
type TriggerExecutionState = 'configured' | 'fired' | 'executor_not_ready' | 'dispatched';
type TaskTriggerReadiness = {
  state: TriggerExecutionState;
  reason: string | null;
  run_id: string | null;
  fired_at: string | null;
  updated_at: string | null;
};
type TaskTriggerExecutionRow = TaskTriggerReadiness & {
  trigger_id: string;
  occurrence_key: string;
};
type TriggerView = TaskTriggerView & {
  readiness: TaskTriggerReadiness;
};
type TaskTriggerHostOptions = {
  /** 宿主是否已装配可信的节点规格解析器：没装配就只能停在"触发已发生"，不能冒充运行成功 */
  executorReady?: () => boolean;
};
declare function createTaskTriggerHost(db: Db, catalog: WorkflowCatalog, options?: TaskTriggerHostOptions): {
  list(taskId?: string): {
    items: TriggerView[];
    as_of_seq: number;
  };
  create(input: TaskTriggerCreate, actor: string, now: string): TriggerView;
  setEnabled(input: TriggerEnabled, actor: string, now: string): TriggerView;
  emit(input: TriggerEvent, actor: string, now: string): {
    items: TaskTriggerOccurrence[];
    replayed: boolean;
  };
  tick(now: string, onError: (error: unknown) => void): void;
};
type TaskTriggerExecutionFilter = {
  trigger_id?: string;
  run_id?: string;
  state?: TriggerExecutionState;
};
/** 就绪态读取入口：规则当前四态与每次触发的历史都能查，事件与运行都能当筛选条件 */
declare function listTriggerExecutionStates(db: Db, filter?: TaskTriggerExecutionFilter): TaskTriggerExecutionRow[];
type TaskTriggerHost = ReturnType<typeof createTaskTriggerHost>;
type Actor = {
  kind: 'human' | 'agent';
  id: string;
  capabilities?: readonly string[];
};
declare function acknowledgeDecision(tx: Tx, req: {
  decisionId: string;
  revision: number;
  operationId: string;
  actor: Actor;
  now: string;
}): {
  decisionId: string;
  revision: number;
};
type Resolution$1 = 'approved' | 'rejected' | 'expired' | 'confirm_applied' | 'confirm_not_applied' | 'confirm_started' | 'confirm_started_attested' | 'confirm_not_started' | 'continue_tracking' | 'continue_wait' | 'abandon' | 'uphold_report' | 'uphold_decision';
type OrdinaryLiveRecovery = {
  secret: string;
  ownerEpoch: number;
  clock: ClockProvider;
  liveAttempt: (binding: LiveAttemptBinding) => LiveAttemptObservation | undefined;
};
type ChatModelRecoveryChecks = Map<string, {
  attempts: number;
  retryAt?: number;
}>;
/** Only temporary catalog failures are retried in the loaded conversation. */
declare function recoverChatModelAssignment(db: Db, secret: string, routing: ModelRouting, input: {
  actor: string;
  runId: string;
  signal: AbortSignal;
  checked: ChatModelRecoveryChecks;
  isCurrent(): boolean;
  clock?: () => number;
}): Promise<{
  resumed: number;
  retrying: string[];
}>;
declare const TASK_ORIGINS: readonly ["manual", "scheduled", "event", "continuous"];
type TaskOrigin = (typeof TASK_ORIGINS)[number];
declare const TASK_LANES: readonly [{
  readonly id: "upcoming";
  readonly label: "待执行";
}, {
  readonly id: "running";
  readonly label: "执行中";
}, {
  readonly id: "attention";
  readonly label: "待处理";
}, {
  readonly id: "finished";
  readonly label: "已结束";
}];
type TaskLane = (typeof TASK_LANES)[number]['id'];
type SetTaskArchive = {
  task_id: string;
  archived: boolean;
  expected_archived_at: string | null;
  operation_id: string;
};
declare class WriteQueue {
  private pending;
  private readonly max;
  private readonly log;
  private tail;
  constructor(max?: number, log?: Logger);
  get size(): number;
  get capacity(): number;
  run<T>(fn: () => Promise<T> | T): Promise<T>;
}
declare const planningSettingsSchema: z.ZodObject<{
  operation_id: z.ZodString;
  expected_revision: z.ZodNumber;
  values: z.ZodNullable<z.ZodObject<{
    task_seconds: z.ZodNumber;
    workflow_seconds: z.ZodNumber;
    idle_seconds: z.ZodNumber;
    stage_seconds: z.ZodNumber;
    request_seconds: z.ZodNumber;
  }, z.core.$strict>>;
}, z.core.$strict>;
type Branch = 'streaming' | 'fixed' | 'unavailable';
type PlanningBranches = {
  task: Branch;
  automatic_workflow: Branch;
  standalone_workflow: Branch;
};
type PlanningTimeoutSnapshot = {
  task: number;
  workflow: number;
  idle: number;
  stage: number;
  taskStage: number;
  request: number;
};
declare function createPlanningSettingsHost(db: Db, branches: PlanningBranches, legacyTimeoutMs?: number): {
  snapshot: () => PlanningTimeoutSnapshot;
  get(): {
    item: {
      revision: number;
      values: {
        task_seconds: number;
        workflow_seconds: number;
        idle_seconds: number;
        stage_seconds: number;
        request_seconds: number;
      };
      defaults: {
        task_seconds: number;
        workflow_seconds: number;
        idle_seconds: number;
        stage_seconds: number;
        request_seconds: number;
      };
      limits: {
        readonly task_seconds: 600;
        readonly workflow_seconds: 600;
        readonly idle_seconds: 300;
        readonly stage_seconds: 600;
        readonly request_seconds: 900;
      };
      overridden: boolean;
      branches: PlanningBranches;
      legacy_timeout_ms: number | null;
    };
    as_of_seq: number;
  };
  save(request: z.input<typeof planningSettingsSchema>, actor: string, now: string): {
    revision: number;
  };
};
type PlanningSettingsHost = ReturnType<typeof createPlanningSettingsHost>;
/** Apply a captured request budget to any frontend, including a non-HTTP chat caller. */
declare function withPlanningBudget<T>(maximumMs: number, caller: AbortSignal | undefined, run: (signal: AbortSignal) => Promise<T>): Promise<T>;
type ExecutionWorkflowScope = {
  runId: string;
  workflowId: string;
  nodeId: string;
};
type ResolveWorkflowLimits = (scope: ExecutionWorkflowScope) => {
  runLimit: number | null;
  nodeLimit: number | null;
};
type ExecutionTaskOptions = {
  human?: boolean;
  label?: string;
  nodeRunId?: string;
  attempt?: number;
  workflowScope?: ExecutionWorkflowScope;
};
type ExecutionStats = {
  inFlight: number;
  waiting: number;
  concurrency: number;
  humanSlots: number;
  humanInFlight: number;
  maxWaiting: number;
};
type ExecutionScheduler = {
  run<T>(fn: () => Promise<T> | T, opts?: ExecutionTaskOptions): Promise<T>;
  stats(): ExecutionStats;
  isWaiting?(nodeRunId: string): boolean;
  cancelWaiting?(nodeRunId: string, attempt: number): void;
  configureWorkflowLimits?(resolve: ResolveWorkflowLimits): void;
  readonly inFlight: number;
  readonly waiting: number;
  readonly concurrency: number;
  readonly humanSlots: number;
  close(opts?: {
    drain?: boolean;
  }): Promise<void>;
};
type ExecutionCapacity = Pick<ExecutionStats, 'concurrency' | 'maxWaiting'>;
type ConfigurableExecutionScheduler = ExecutionScheduler & {
  setCapacity(capacity: ExecutionCapacity): void;
  configureWorkflowLimits(resolve: ResolveWorkflowLimits): void;
};
declare const executionCapacitySettingsSchema: z.ZodObject<{
  operation_id: z.ZodString;
  expected_revision: z.ZodNumber;
  values: z.ZodNullable<z.ZodObject<{
    concurrency: z.ZodNumber;
    max_waiting: z.ZodNumber;
    action_timeout_ms: z.ZodNumber;
  }, z.core.$strict>>;
}, z.core.$strict>;
declare function createExecutionCapacitySettingsHost(db: Db, scheduler: ConfigurableExecutionScheduler, actionTimeoutMs: number): {
  actionTimeoutMs: () => number;
  get(): {
    item: {
      revision: number;
      values: {
        concurrency: number;
        max_waiting: number;
        action_timeout_ms: number;
      };
      defaults: {
        concurrency: number;
        max_waiting: number;
        action_timeout_ms: number;
      };
      limits: {
        readonly concurrency: 32;
        readonly max_waiting: 10000;
        readonly action_timeout_ms: 3600000;
      };
      overridden: boolean;
    };
    as_of_seq: number;
  };
  save(request: z.input<typeof executionCapacitySettingsSchema>, actor: string, now: string): {
    revision: number;
  };
};
type ExecutionCapacitySettingsHost = ReturnType<typeof createExecutionCapacitySettingsHost>;
declare const targetSchema: z.ZodObject<{
  workflow_id: z.ZodOptional<z.ZodString>;
  node_id: z.ZodOptional<z.ZodString>;
}, z.core.$strict>;
declare const workflowConcurrencySchema: z.ZodObject<{
  workflow_id: z.ZodOptional<z.ZodString>;
  node_id: z.ZodOptional<z.ZodString>;
  operation_id: z.ZodString;
  expected_revision: z.ZodNumber;
  values: z.ZodNullable<z.ZodObject<{
    run_limit: z.ZodOptional<z.ZodNullable<z.ZodNumber>>;
    node_limit: z.ZodOptional<z.ZodNullable<z.ZodNumber>>;
    helper_limit: z.ZodOptional<z.ZodNumber>;
  }, z.core.$strict>>;
}, z.core.$strict>;
type Target = z.infer<typeof targetSchema>;
declare function createWorkflowConcurrencyHost(db: Db, scheduler: ExecutionScheduler, helperAvailable: boolean): {
  helperLimit: (scope: ExecutionWorkflowScope) => number;
  get(target: Target): {
    as_of_seq: number;
    item: {
      revision: number;
      overrides: {
        run_limit?: number | null | undefined;
        node_limit?: number | null | undefined;
        helper_limit?: number | undefined;
      };
      inherited: {
        run_limit: number | null;
        node_limit: number | null;
        helper_limit: number;
      };
      values: {
        run_limit: number | null;
        node_limit: number | null;
        helper_limit: number;
      };
      helper_available: boolean;
    };
  };
  save(input: z.infer<typeof workflowConcurrencySchema>, actor: string, now: string): {
    revision: number;
  };
};
type WorkflowConcurrencyHost = ReturnType<typeof createWorkflowConcurrencyHost>;
declare const humanInput: z.ZodObject<{
  run_id: z.ZodString;
  body: z.ZodString;
  operation_id: z.ZodUUID;
}, z.core.$strict>;
type RunNoteBinding = {
  node_run_id: string;
  attempt: number;
  action_generation: number;
  executor_session_id: string;
};
declare function createRunNotes(db: Db, secret: string): {
  captureBrain(binding: RunNoteBinding, requestId: string, assertCurrent: () => void, capture: () => Promise<string | null>): Promise<{
    captured: true;
    note_id: string;
    seq: number;
  } | {
    captured: false;
  }>;
  reviewBrain(run: string, actor: string, assertCurrent: () => void, review: (bodies: readonly string[], assertSnapshot: () => void) => Promise<unknown>): Promise<{
    classification: "follow_up" | "informational" | "insufficient";
    evidence_strength: "none";
  }>;
  listHuman(run: string, actor: string): {
    as_of_seq: number;
    items: {
      note_id: string;
      seq: number;
      run_id: string;
      node_run_id: string | null;
      author: string;
      source: "human" | "node";
      attempt: number | null;
      scope: "run" | "downstream" | "brain";
      body: string;
      created_at: string;
      evidence_strength: "none";
    }[];
    next_before_seq: number | null;
  };
  addHuman(input: z.infer<typeof humanInput>, actor: string): {
    note_id: string;
    seq: number;
  };
  listChatShared(run: string, actor: string, before?: number): {
    items: {
      note_id: string;
      seq: number;
      run_id: string;
      node_run_id: string | null;
      author: string;
      source: "human" | "node";
      attempt: number | null;
      scope: "run" | "downstream" | "brain";
      body: string;
      created_at: string;
      evidence_strength: "none";
    }[];
    next_before_seq: number | null;
  };
  callNode(binding: RunNoteBinding, input: unknown, requestId: string, now: string): {
    note_id: string;
    seq: number;
  } | {
    items: {
      note_id: string;
      seq: number;
      run_id: string;
      node_run_id: string | null;
      author: string;
      source: "human" | "node";
      attempt: number | null;
      scope: "run" | "downstream" | "brain";
      body: string;
      created_at: string;
      evidence_strength: "none";
    }[];
    next_before_seq: number | null;
  };
};
type RunNotes = ReturnType<typeof createRunNotes>;
/** 必过项：结论 + 指向的证据 id。 */
type MustPassItem = {
  id: string;
  result: 'pass' | 'fail';
  evidence_ids: string[];
};
declare const RELEASE_GROUPS: {
  readonly migration_recovery: readonly ["G-01", "G-02", "G-07", "G-13", "G-14", "G-15", "upgrade", "load_rollback"];
  readonly single_node: readonly ["G-03", "G-04", "G-05", "G-06"];
  readonly state_concurrency: readonly ["G-08", "G-12", "G-16"];
  readonly version_publication: readonly ["G-10"];
  readonly panel_memory: readonly ["G-09", "G-11"];
  readonly performance_safety: readonly ["control_p95", "event_lag", "soak_24h", "oncall_drill"];
};
type ReleaseGroup = keyof typeof RELEASE_GROUPS;
declare const evidenceEntry: z.ZodObject<{
  evidence_id: z.ZodString;
  sha256: z.ZodString;
  strength: z.ZodEnum<{
    strong: "strong";
    weak: "weak";
  }>;
  source: z.ZodString;
  file: z.ZodString;
}, z.core.$strict>;
declare const manifest: z.ZodObject<{
  candidate: z.ZodObject<{
    git_sha: z.ZodString;
    schema_version: z.ZodLiteral<1>;
    phase: z.ZodLiteral<"S6">;
  }, z.core.$strict>;
  change_scope: z.ZodObject<{
    capability_ids: z.ZodArray<z.ZodString>;
    touched_paths: z.ZodArray<z.ZodString>;
  }, z.core.$strict>;
  evidence: z.ZodArray<z.ZodObject<{
    evidence_id: z.ZodString;
    sha256: z.ZodString;
    strength: z.ZodEnum<{
      strong: "strong";
      weak: "weak";
    }>;
    source: z.ZodString;
    file: z.ZodString;
  }, z.core.$strict>>;
  items: z.ZodArray<z.ZodObject<{
    id: z.ZodEnum<{
      "G-01": "G-01";
      "G-02": "G-02";
      "G-07": "G-07";
      "G-13": "G-13";
      "G-14": "G-14";
      "G-15": "G-15";
      upgrade: "upgrade";
      load_rollback: "load_rollback";
      "G-03": "G-03";
      "G-04": "G-04";
      "G-05": "G-05";
      "G-06": "G-06";
      "G-08": "G-08";
      "G-12": "G-12";
      "G-16": "G-16";
      "G-10": "G-10";
      "G-09": "G-09";
      "G-11": "G-11";
      control_p95: "control_p95";
      event_lag: "event_lag";
      soak_24h: "soak_24h";
      oncall_drill: "oncall_drill";
    }>;
    evidence_ids: z.ZodArray<z.ZodString>;
  }, z.core.$strict>>;
  invariants: z.ZodArray<z.ZodObject<{
    id: z.ZodEnum<{
      [x: string]: string;
    }>;
    method: z.ZodString;
    evidence_ids: z.ZodArray<z.ZodString>;
  }, z.core.$strict>>;
}, z.core.$strict>;
type FrozenReleaseManifest = z.infer<typeof manifest>;
type ReleaseEvidenceEntry = z.infer<typeof evidenceEntry>;
/** Host plumbing must resolve each evidence ID to the frozen bytes and re-derive the check from the original source. */
type ReleaseEvidenceReader = {
  read(entry: ReleaseEvidenceEntry): Uint8Array | undefined;
  observe(request: {
    id: string;
    candidate: FrozenReleaseManifest['candidate'];
    evidence: readonly {
      entry: ReleaseEvidenceEntry;
      bytes: Uint8Array;
    }[];
  }): unknown;
};
type ReleasePolicyResult = ReturnType<typeof judgeReleaseEvidence>;
declare function judgeReleaseEvidence(input: unknown, reader: ReleaseEvidenceReader): {
  candidate_hash: string;
  change_scope_hash: string;
  evidence_index_hash: string;
  invariants_hash: string;
  manifest_hash: string;
  must_pass_items: MustPassItem[];
  invariants: {
    id: string;
    method: string;
    result: string;
  }[];
  conclusion: "pass" | "fail";
};
type ReleaseGateCardState = 'idle' | 'checking' | 'approval' | 'blocked' | 'failed' | 'confirmed';
declare const toolReceipt: z.ZodObject<{
  execution_id: z.ZodString;
  command_hash: z.ZodString;
  stdout_hash: z.ZodString;
  exit_code: z.ZodLiteral<0>;
}, z.core.$strict>;
type ToolReceipt = z.infer<typeof toolReceipt>;
type BoundEvidence = Pick<ReleasePolicyResult, 'manifest_hash' | 'candidate_hash' | 'evidence_index_hash'>;
/** Only a server-owned isolated workspace can install these ports. No HTTP payload supplies a principal, command, reader, or receipt. */
type ReleaseWorkspace = {
  authorizeStart(credential: unknown): unknown;
  frozenEvidence(): {
    manifest: unknown;
    reader: ReleaseEvidenceReader;
  };
  runGroup(request: BoundEvidence & {
    group: ReleaseGroup;
    child_run_id: string;
    check_node_run_id: string;
  }): Promise<void>;
  verifyGroup(request: BoundEvidence & {
    group: ReleaseGroup;
    child_run_id: string;
    execution: ToolReceipt;
  }): boolean;
  verifyApproval(credential: unknown, challenge: BoundEvidence & {
    run_id: string;
    judgement_hash: string;
  }): unknown;
};
declare function createReleaseWorkflowHost(deps: {
  db: Db;
  secret: string;
  catalog: WorkflowCatalog;
  clock: ClockProvider;
  workspace: ReleaseWorkspace;
}): {
  start(input: {
    operation_id: string;
  }, credential: unknown, now: string): {
    manifest_hash: string;
    candidate_hash: string;
    evidence_index_hash: string;
    run_id: string;
  };
  advance(runId: string, now: string): Promise<{
    status: string;
    missing?: undefined;
    judgement_hash?: undefined;
  } | {
    status: string;
    missing: ("migration_recovery" | "single_node" | "state_concurrency" | "version_publication" | "panel_memory" | "performance_safety")[];
    judgement_hash?: undefined;
  } | {
    status: string;
    judgement_hash: string;
    missing?: undefined;
  }>;
  approve(runId: string, credential: unknown, operationId: string, now: string): {
    run_id: string;
    status: "succeeded";
  };
};
type Autonomy = 'autonomous' | 'assisted' | 'supervised';
type CreatedNodeRun = {
  node_run_id: string;
  node_id: string;
  seq: number;
  node_type: string;
  status: string;
};
type CreateRunResult = {
  task_id: string;
  run_id: string;
  workflow_id: string;
  workflow_version: string;
  autonomy: Autonomy;
  nodes: CreatedNodeRun[];
};
declare const ATTENTION_ORDER: readonly ["decision", "unverified", "failed", "orphaned", "running", "none"];
type Attention = (typeof ATTENTION_ORDER)[number];
declare const DELIVERY_ACCEPTANCE: readonly ["passed", "failed", "unverifiable", "not_applicable", "pending"];
type DeliveryAcceptance = (typeof DELIVERY_ACCEPTANCE)[number];
declare const SIDE_EFFECT_LEVEL: readonly ["pure", "compensable", "irreversible"];
type SideEffectLevel = (typeof SIDE_EFFECT_LEVEL)[number];
type TaskSummary = {
  task_id: string;
  title: string;
  workflow_id: string | null;
  workflow_version: number | null;
  status: string;
  display_status: string;
  current_run_id: string | null;
  session_id: string | null;
  run_count: number;
  open_decision_count: number;
  oldest_open_decision_age_seconds: number | null;
  unverified_count: number;
  max_side_effect_level: SideEffectLevel | null;
  max_heartbeat_age_ms: number | null;
  attention: Attention;
  attention_reason: string | null;
  updated_at: string;
  archived_at: string | null;
  trigger_kind: TaskOrigin | null;
  next_trigger_at?: string | null;
  trigger_enabled?: boolean | null;
  autonomy: Autonomy;
  lane?: TaskLane;
  current_run_status: string | null;
  delivery_acceptance?: DeliveryAcceptance;
  goal_met?: boolean;
};
type ListTaskParams = {
  status?: string;
  attention?: Attention;
  query?: string;
  lane?: TaskLane;
  trigger_kind?: TaskOrigin | 'unknown';
  archived?: boolean;
  updated_from?: string;
  updated_before?: string;
  limit?: number;
  cursor?: string;
  now?: number;
};
type WorkspaceOverview = {
  lanes: Record<TaskLane, number>;
  open_decisions: number;
  unverified_effects: number;
  release_gate?: ReleaseGateCardState;
};
declare function listEventHistory(db: Db, opts: {
  beforeSeq?: number;
  runId?: string;
  taskId?: string;
  limit?: number;
  filter?: string;
}): {
  items: {
    attention?: boolean | undefined;
    details: {
      label: string;
      value: string;
    }[];
    seq: number;
    event_id: string;
    type: string;
    occurred_at: string;
    run_id: string | null;
    node_run_id: string | null;
    decision_id: string | null;
  }[];
  next_before_seq: number | null;
  as_of_seq: number;
};
type Detail = {
  label: string;
  value: string;
};
declare function listExecutionHistory(db: Db, opts: {
  taskId?: string;
  runId?: string;
  nodeId?: string;
  before?: number;
  limit?: number;
  filter?: string;
}): {
  items: {
    attention?: boolean | undefined;
    title: string;
    details: Detail[];
    id: number;
    type: string;
    created_at: string;
    run_id: string;
    node_id: string;
    node_run_id: string;
  }[];
  next_before: number | null;
  as_of_seq: number;
};
type AcceptanceOutcome = 'passed' | 'failed' | 'unverifiable';
type AcceptanceKind = 'artifact' | 'tool_evidence';
type NodeRunCount = {
  status: string;
  count: number;
};
type NodeRunPreview = {
  node_run_id: string;
  node_id: string;
  node_type: string;
  attempt: number;
  status: string;
  revision: number;
  updated_at: string;
};
type RunDeliveryCheck = {
  result_id: string;
  kind: AcceptanceKind;
  outcome: AcceptanceOutcome;
  reason: string | null;
  evaluated_at: string;
  rule_version: string;
  artifact_id?: string;
  file_name?: string;
  minimum_bytes?: number;
  bytes?: number;
  tool?: string;
  expected_exit_code?: number;
  exit_codes?: number[];
};
type RunSummary = {
  run_id: string;
  task_id: string;
  task_title?: string | null;
  workflow_id: string | null;
  workflow_version: number | null;
  status: string;
  display_status: string;
  settling: boolean;
  revision: number;
  counts: NodeRunCount[];
  total: number;
  top_nodes: NodeRunPreview[];
  last_event_seq: number;
  as_of_seq: number;
  created_at: string;
  updated_at: string;
  started_at?: string | null;
  ended_at?: string | null;
  scheduled_for?: string | null;
  trigger_kind?: TaskOrigin | null;
  project_context?: ProjectContextMetadata;
  delivery_checks?: {
    items: RunDeliveryCheck[];
    has_more: boolean;
  };
};
type NodeRunView = {
  node_run_id: string;
  run_id: string;
  node_id: string;
  title?: string;
  node_type: string;
  attempt: number;
  status: string;
  display_status: string;
  status_label: string;
  execution_state?: string;
  /** Direct predecessors from the pinned workflow and approved runtime edges. Missing means unknown. */
  depends_on?: string[];
  waiting_on?: string[];
  revision: number;
  side_effect_policy: string;
  side_effect_verified_none: boolean;
  declared_effect: string | null;
  verified_effect: string | null;
  evidence_strength: null;
  evidence_state: 'none' | 'not_required' | 'unverified' | 'settled';
  evidence_state_reason: string | null;
  effect_resolution_basis?: 'verified_none' | 'evidence' | 'decision' | null;
  effect_decision_id?: string | null;
  execution_ids: string[];
  execution_count: number;
  projection_missing: string[];
  attestation_id: string | null;
  action_start_source: 'human_attested' | 'human_confirmed' | null;
  stop_evidence: string | null;
  hang_reason: string | null;
  recovery_uncertain: string | null;
  replacement_node_run_id: string | null;
  heartbeat_at: string | null;
  started_at: string | null;
  created_at: string;
  updated_at: string;
  active_decision_ids: string[];
  model_assignment?: ModelAssignment & {
    state: 'selected' | 'confirmed' | 'rejected';
  };
};
type ArtifactView = {
  artifact_id: string;
  run_id: string;
  sha256: string;
  evidence_index: unknown;
  retain_until: string | null;
  expired: boolean;
  file_name?: string;
  size_bytes?: number;
  source_node_run_id?: string | null;
};
type RunArtifact = Omit<ArtifactView, 'evidence_index'> & {
  created_at: string;
};
type ArtifactContentMetadata = {
  file_name: string;
  size_bytes: number;
  sha256: string;
};
type ArtifactContentMode = 'preview' | 'download';
type ArtifactContentView = ArtifactContentMetadata & ({
  kind: 'preview';
  text: string;
  truncated: boolean;
} | {
  kind: 'download';
  base64: string;
});
declare function getArtifactContent(db: Db, runId: string, artifactId: string, mode: ArtifactContentMode, now: number): {
  item: ArtifactContentView;
  as_of_seq: number;
};
type AttestationView = {
  attestation_id: string;
  run_id: string;
  node_run_id: string;
  attempt: number;
  claim: string;
  attested_by: string;
  attested_at: string;
  action_start_source: 'human_attested';
  summary: string | null;
  classification: '人证，不是系统证据';
};
type RecoveryOutcomes = Partial<Record<'confirm_not_started' | 'confirm_not_applied', 'retry' | 'failed'>>;
type MemoryDecisionAccess = boolean | 'proposal_only';
type DecisionView = {
  decision_id: string;
  run_id: string;
  node_run_id: string | null;
  kind: string;
  status: string;
  current_revision: number;
  closed_reason: string | null;
  payload: unknown;
  content_hash: string | null;
  expires_at: string | null;
  confirm_required: boolean;
  created_at: string;
  decided_at: string | null;
  resolved_by?: string | null;
  priority: string | null;
  priority_reason: string | null;
  allowed_resolutions: string[] | null;
  resolution_unavailable_reason?: string | null;
  required_evidence: string[] | null;
  available_effect_evidence?: {
    applied?: string;
    not_applied?: string;
  };
  available_recovery_outcomes?: RecoveryOutcomes;
  target_summary: string | null;
  approver?: string | null;
  expiry_effect?: string | null;
  projection_missing: string[];
};
type ListDecisionParams = {
  run_id?: string;
  status?: string;
  humanInbox?: boolean;
  limit?: number;
  cursor?: string;
  now?: number;
  memoryEnabled?: MemoryDecisionAccess;
  liveRecovery?: OrdinaryLiveRecovery;
};
type EvidenceView = {
  evidence_id: string;
  node_run_id: string;
  run_id: string;
  attempt: number;
  strength: 'strong';
  source: 'run_tool';
  execution_boundary?: ToolExecutionBoundary;
  tool: string;
  exit_code: number;
  stdout_hash: string | null;
  stderr_hash: string | null;
  command_hash: string | null;
  observed_at: string;
  summary: string;
  artifact_id: null;
};
type LedgerGap = {
  node_run_id: string;
  seq: number;
  reason: string;
};
type LedgerVerify = {
  valid: boolean;
  /** node_run_events 的链内 seq，与全局快照 as_of_seq 属于不同序列。 */
  first_bad_event_seq: number | null;
  broken_node_run_id: string | null;
  reason: string | null;
  ok: boolean;
  chains: number;
  entries: number;
  break: LedgerGap | null;
  gaps: LedgerGap[];
};
type UnverifiedEffectView = {
  run_id: string;
  node_run_id: string;
  node_id: string;
  status: string;
  side_effect_policy: string;
  side_effect_verified_none: false;
  stop_evidence: string | null;
  started_at: string | null;
  created_at: string;
  age_seconds: number | null;
  resolved: false;
  resolved_at: null;
  resolution_basis: null;
  stopped_at?: string | null;
  last_known_phase?: string | null;
  evidence?: string | null;
  decision_id?: string | null;
};
type ListEffectsParams = {
  run_id?: string;
  limit?: number;
  cursor?: string;
  now?: number;
};
/** 批次状态：规格 §9.10 的十二个值，与客户端 MemoryBatchStatus 逐字一致。 */
type MemoryBatchStatus = 'draft' | 'evaluating' | 'awaiting_human' | 'approved' | 'applying' | 'applied' | 'empty' | 'blocked' | 'declined' | 'expired' | 'failed' | 'indeterminate';
/** 条目状态：规格 §9.10 的五个值，与客户端 MemoryItemStatus 逐字一致。 */
type MemoryItemStatus = 'proposed' | 'approved' | 'skipped' | 'applied' | 'failed';
/** 一条记忆条目（面板只读）。字段与 src/client/api/types.ts:39-49 一一对应。 */
type MemoryItemView = {
  client_key: string;
  layer: string;
  title: string;
  digest: string;
  content: string | null;
  source: string | null;
  status: MemoryItemStatus;
  verdict: string | null;
  judge_reason: string | null;
};
/** 一次记忆整理批次（面板只读）。字段与 src/client/api/types.ts:52-71 一一对应。 */
type MemoryBatchView = {
  batch_id: string;
  task_id: string;
  status: MemoryBatchStatus;
  attempt: number;
  outcome: string | null;
  total_count: number;
  pending_count: number;
  applied_count: number;
  skipped_count: number;
  items: MemoryItemView[];
  evaluation_id: string | null;
  proposal_hash: string | null;
  decision_id: string | null;
  decision_revision: number | null;
  allowed_resolutions: string[] | null;
  recovery_available?: boolean;
  error_message: string | null;
  created_at: string;
  updated_at: string;
};
type ListMemoryBatchesParams = {
  task_id?: string;
  limit?: number;
  cursor?: string;
  now?: number;
  recoveryEnabled?: boolean;
};
type MemoryBatchPage = {
  items: MemoryBatchView[];
  next_cursor: string | null;
  as_of_seq: number;
};
type ValidationIssue = {
  path: string;
  code: string;
  message: string;
};
type ValidationReport = {
  errors: ValidationIssue[];
  truncated: boolean;
  total: number;
};
type WorkflowScope = 'reusable' | 'task_local';
type WorkflowStatus = 'active' | 'disabled';
type SetWorkflowStatusParams = {
  workflowId: string;
  status: WorkflowStatus;
  expectedStatus: WorkflowStatus;
  actor: string;
  now: string;
  operationId: string;
};
type CreateWorkflowParams = {
  workflowId: string;
  name: string;
  definition: unknown;
  scope?: WorkflowScope;
  ownerTaskId?: string | null;
  actor: string;
  now: string;
};
type OpenDraftParams = {
  workflowId: string;
  baseVersion: number;
  actor: string;
  now: string;
};
type UpdateDraftParams = {
  workflowId: string;
  revision: number;
  definition: unknown;
  actor: string;
  now: string;
};
type PublishParams = {
  workflowId: string;
  revision: number;
  baseVersion: number | null;
  /** 发布说明必填 1–500 字（规格 3.8 / Task 8） */
  note: string;
  actor: string;
  now: string;
};
type WorkflowDraftTest = {
  revision: number;
  passed: boolean;
  tested_at: string;
  issues: ValidationReport;
  order: {
    node_id: string;
    title: string;
    type: string;
  }[];
};
type WorkflowListItem = {
  workflow_id: string;
  name: string;
  status: string;
  scope: string;
  owner_task_id: string | null;
  current_version: number | null;
  has_draft: boolean;
  draft_revision: number | null;
  node_count: number | null;
  active_task_count: number;
  published_at: string | null;
  updated_at: string;
  projection_missing: string[];
};
type WorkflowVersionView = {
  workflow_id: string;
  version: number;
  based_on_version: number | null;
  definition: unknown;
  definition_hash: string;
  publish_note: string;
  published_at: string;
  published_by: string | null;
  projection_missing: string[];
};
type WorkflowView = {
  workflow: WorkflowListItem;
  draft: {
    revision: number;
    based_on_version: number | null;
    definition: unknown;
    definition_hash: string;
    updated_at: string;
    updated_by: string;
  } | null;
  current: WorkflowVersionView | null;
  validation: {
    errors: ValidationIssue[];
    warnings: ValidationIssue[];
  };
  versions: Array<{
    version: number;
    published_at: string;
    publish_note: string;
  }>;
  recent_tests?: Array<WorkflowDraftTest & {
    test_id: number;
  }>;
  critical_approval_count?: number;
};
type ListWorkflowParams = {
  owner_task_id?: string;
  status?: string;
  query?: string;
  limit?: number;
  cursor?: string;
  now?: number;
};
type OperationRow = {
  operation_id: string;
  kind: string;
  status: string;
  receipt_json: string | null;
  created_at: string;
  completed_at: string | null;
  receipt_expired_at: string | null;
};
type CompensationDescriptor = {
  tool: string;
  args: readonly string[];
  verify: {
    command: string;
    args: readonly string[];
  };
  descriptorVersion: string;
};
type RecoveryResolveResult = {
  node_run_id: string;
  from: string;
  to: 'indeterminate_side_effect';
  channel: string | null;
};
type RecoveryUncertainItem = {
  node_run_id: string;
  run_id: string;
  node_id: string;
  status: string;
  attempt: number;
  revision: number;
  /** 算不出可信剩余的通道；旧数据可能为空 */
  channel: string | null;
  /** 标记时间（recovery_uncertain_at） */
  since: string | null;
  deadline_at: string | null;
  deadline_boot_id: string | null;
  deadline_monotonic_ns: number | null;
  checkpoint_ns: number | null;
  /** 检查点换算出的剩余毫秒；不可信，仅供展示 */
  remaining_ms_estimate: number;
  /** 估算是否可信：这一列永远是 false，宿主不能拿它当到期依据 */
  remaining_trusted: false;
  /** 需要宿主做的事：确认节点状态后调用 /v1/recovery/resolve */
  action_required: 'confirm_node_state';
};
type StartRunResult = {
  task_id: string;
  run_id: string;
  workflow_id: string;
  workflow_version: string;
  autonomy: Autonomy;
  replayed: boolean;
};
type ResolutionBasis = 'verified_none' | 'evidence' | 'decision';
/** 证据来源声明：模型侧 tool_result 是弱证据，不能结算（附录 §2.1 E5） */
type EvidenceSource = 'run_tool' | 'tool_result';
type EffectResolutionResult = {
  node_run_id: string;
  resolution_basis: ResolutionBasis;
  resolved_at: string;
  node_revision: number;
  evidence_id: string | null;
  decision_id: string | null;
  stop_evidence: string | null;
  replayed: boolean;
};
declare function inspectEffectEvidence(db: Db, secret: string, nodeRunId: string): {
  item: {
    node_run_id: string;
    run_id: string;
    revision: number;
    evidence: {
      evidence_id: string;
      tool: string;
      exit_code: number;
      finished_at: string;
      verdict: EffectVerdict;
    }[];
  };
  as_of_seq: number;
};
type MemoryItemInput = {
  client_key: string;
  action: 'create' | 'update';
  target_id: string | null;
  layer: 'L2' | 'L4' | 'L5' | 'L6';
  title: string;
  content: string;
  tags: string[];
  questions: string[];
  key: string | null;
  importance: number | null;
  ttl_days: number | null;
  quote: string;
  reason: string;
};
type StartMemoryRunParams = {
  source_run_id: string;
  reason: string;
  request_key: string;
  retry?: boolean;
  /** 幂等键（UUIDv7）：回执走既有幂等表 */
  operation_id: string;
  /** 任务会话模板给发起方的 allow：出现直接写工具就拒 */
  caller_allow?: readonly string[];
  now: string;
};
type MemoryRunStartResult = {
  memory_run_id: string;
  attempt: number;
  batch_id: string;
  source_run_id: string;
  task_id: string;
  replayed: boolean;
};
type RunTerminal = 'succeeded' | 'succeeded_with_unverified' | 'failed' | 'failed_with_unverified' | 'orphaned' | 'cancelled';
type DecisionCommandResult = {
  decisionId: string;
  revision: number;
  replayed?: boolean;
};
/** 记忆层级白名单（规格 9.11 policy.allowed_layers） */
type MemoryLayer = 'L2' | 'L4' | 'L5' | 'L6';
type EvalVerdict = 'approve' | 'reject' | 'merge' | 'needs_user';
type EvalRecommendation = 'ready' | 'empty' | 'blocked';
type EvalItem = {
  client_key: string;
  verdict: EvalVerdict;
  reason: string;
  confidence: number;
  conflict_with?: string | null;
};
type EvalSimilar = {
  id: string;
  title: string;
  summary: string;
  score: number;
};
type EvalRequestSource = {
  run_id: string;
  task_title: string;
  goal: string;
  result_summary: string;
  evidence_refs: string[];
};
type EvalRequestPolicy = {
  allowed_layers: MemoryLayer[];
  max_items: number;
  max_content_bytes: number;
  allow_update: boolean;
  require_quote: boolean;
  judge_content_limit: number;
};
type EvalRequestItem = {
  client_key: string;
  action: 'create' | 'update';
  target_id: string | null;
  layer: MemoryLayer;
  title: string;
  content: string;
  content_hash: string;
  quote: string;
  similar: EvalSimilar[];
};
type EvalRequest = {
  batch_id: string;
  prepare_node_run_id: string;
  source: EvalRequestSource;
  policy: EvalRequestPolicy;
  items: EvalRequestItem[];
};
type ApprovalCardItem = {
  layer: string;
  title: string;
  digest: string;
  judge_reason: string;
};
type ApprovalCardButton = {
  label: string;
  resolution: 'approved' | 'rejected';
};
type ApprovalCard = {
  kind: 'audit_result';
  title: string;
  summary_line: string;
  items: ApprovalCardItem[];
  buttons: ApprovalCardButton[];
  /** allowed_resolutions 为空，只产生 approved / rejected（规格 9.6 行 728） */
  allowed_resolutions: string[];
  /** resolve 的 payload 是空对象（规格 9.6 行 728） */
  resolve_payload: Record<string, unknown>;
  /** 展开「为什么记这条」（规格 9.6 行 729） */
  expand_label: string;
};
/** 裁判回调：返回裁判原始文本，由 parseJudgeResponse 解析。 */
type MemoryJudge = (request: EvalRequest) => Promise<string>;
/** 受控写回调的入参：只给已批准的条目。 */
type GovernedWriteInput = {
  batch_id: string;
  run_id: string;
  node_run_id: string;
  source_run_id: string;
  decision_id: string;
  items: readonly MemoryItemInput[];
};
/** A fence is durable for this node_run_id. A fenced attempt can no longer commit. */
type MemoryWriteInspection = {
  batch_id: string;
  node_run_id: string;
  stopped_at: string;
  entries: readonly {
    client_key: string;
    content_hash: string;
    applied_item_id: string;
  }[];
};
/** The adapter fences this attempt and reads the same durable store used by writeMemory. */
type InspectMemoryWrite = (scope: Pick<GovernedWriteInput, 'batch_id' | 'run_id' | 'node_run_id'>) => Promise<MemoryWriteInspection>;
/** 受控写回调逐条回执。 */
type GovernedWriteApplied = {
  client_key: string;
  item_id: string;
};
/** 受控写回调的返回值：applied_count / skipped_count / applied_item_ids。 */
type GovernedWriteResult = {
  applied_count: number;
  skipped_count: number;
  applied_item_ids: string[];
  /** 可选：逐条对应关系；缺省按已批准条目的顺序对齐 applied_item_ids。 */
  applied?: GovernedWriteApplied[];
};
/** 受控写实现（真实写库由外部注入）。 */
type MemoryWriteGoverned = (input: GovernedWriteInput) => Promise<GovernedWriteResult | {
  outcome: 'not_written';
}>;
/** 端口依赖。 */
type MemoryPortDeps = {
  db: Db;
  /** 裁判：返回原始文本。 */
  judge: MemoryJudge;
  /** 受控写：真正写记忆正文。 */
  writeMemory: MemoryWriteGoverned;
  /** 评估开关：返回 false 时端口以 MEMORY_EVAL_DISABLED 收口。 */
  evalEnabled?: () => boolean;
  /** 裁判超时阈值（毫秒）；缺省不额外计时，由 judge 自己抛超时。 */
  judgeTimeoutMs?: number;
};
type EvaluatePortInput = {
  batch_id: string;
  prepare_node_run_id: string;
  source: EvalRequestSource;
  policy?: Partial<EvalRequestPolicy>;
  now?: string;
};
type EvaluatePortResult = {
  batch_id: string;
  evaluation_id: string;
  eval_attempt: number;
  recommendation: EvalRecommendation;
  items: EvalItem[];
  truncated: boolean;
  duration_ms: number;
  request_hash: string;
};
type ApplyGovernedPortInput = {
  decision_id: string;
  batch_id: string;
  run_id: string;
  node_run_id: string;
  now?: string;
};
type ApplyGovernedPortResult = {
  batch_id: string;
  applied_count: number;
  skipped_count: number;
  applied_item_ids: string[];
  already_applied: boolean;
};
type InspectBatchPortInput = {
  batch_id: string;
};
type InspectBatchItem = {
  item_id: string;
  client_key: string;
  status: string;
  applied_item_id: string | null;
};
type InspectBatchPortResult = {
  batch_id: string;
  batch_status: string;
  items: InspectBatchItem[];
  written_count: number;
  missing_count: number;
  partial: boolean;
  evidence_id: string;
};
/** 三个端口的集合类型。 */
type MemoryPorts = {
  evaluate: (input: EvaluatePortInput) => Promise<EvaluatePortResult>;
  applyGoverned: (input: ApplyGovernedPortInput, renewWriteLease: RenewWriteLease) => Promise<ApplyGovernedPortResult>;
  inspectBatch: (input: InspectBatchPortInput) => InspectBatchPortResult;
};
/** Renew the current attempt inside the transaction and return the current receipt time. */
type RenewWriteLease = (tx: Tx) => string;
type InspectedItem = {
  client_key: string;
  content_hash: string;
  status: 'applied' | 'missing' | 'skipped';
  applied_item_id: string | null;
};
type MemoryWriteEvidence = {
  batch_id: string;
  node_run_id: string;
  attempt: number;
  stopped_at: string;
  evidence_id: string;
  written_count: number;
  missing_count: number;
  partial: boolean;
  items: InspectedItem[];
};
type Resolution = 'confirm_applied' | 'confirm_not_applied' | 'abandon' | 'acknowledge_abandon';
type MemoryRecoveryRequest = {
  batch_id: string;
  decision_id: string;
  revision: number;
  resolution: Resolution;
  stopped_at: string;
  evidence_id: string;
  operation_id: string;
};
type Deps = {
  db: Db;
  secret: string;
  inspectWrite: InspectMemoryWrite;
  now?: () => string;
};
/** Bind authoritative writer facts to a decision. Partial writes are projected immediately. */
declare function inspectMemoryRecovery(deps: Deps, batchId: string): Promise<{
  decision_id: string;
  revision: number;
  allowed_resolutions: Resolution[];
  batch_id: string;
  node_run_id: string;
  attempt: number;
  stopped_at: string;
  evidence_id: string;
  written_count: number;
  missing_count: number;
  partial: boolean;
  items: InspectedItem[];
}>;
/** Complete one human recovery decision and its state transitions in the same transaction. */
declare function resolveMemoryRecovery(deps: Pick<Deps, 'db' | 'secret' | 'now'>, actor: Actor, req: MemoryRecoveryRequest): {
  batch_id: string;
  status: string;
  replacement_node_run_id: string | null;
  abandon_ack_id: string | null;
  terminal: RunTerminal | null;
};
/** mem_prepare 的产出由外部注入（真实实现由 lead 接线）。 */
type PrepareItemsInput = {
  run_id: string;
  batch_id: string;
  task_id: string;
  task_title: string;
  goal: string;
  result_summary: string;
  evidence_refs: string[];
};
type PrepareItems = (input: PrepareItemsInput) => Promise<{
  items: unknown[];
}>;
/**
 * 审批卡开卡由外部注入（lead 接线到 memory-approval.ts 的 openMemoryApprovalCard）。
 * 不注入时 runtime 直接用 openMemoryApprovalCard，保证开卡与批次绑定只有一份实现。
 */
type OpenApprovalCardInput = {
  run_id: string;
  node_run_id: string;
  batch_id: string;
  card: ApprovalCard;
  now: string;
};
type OpenApprovalCard = (input: OpenApprovalCardInput) => Promise<{
  decision_id: string;
}>;
/** 评估请求的来源：缺省从 tasks 表取标题。 */
type MemorySourceResolver = (input: {
  run_id: string;
  batch_id: string;
  task_id: string;
}) => Promise<EvalRequestSource> | EvalRequestSource;
type MemoryRuntimeDeps = {
  db: Db;
  secret: string;
  ports: MemoryPorts;
  prepareItems: PrepareItems;
  /** 不注入时用 memory-approval.ts 的 openMemoryApprovalCard。 */
  openApprovalCard?: OpenApprovalCard;
  resolveSource?: MemorySourceResolver;
  ownerEpoch?: number;
  leaseTtlMs?: number;
  /** 注入时钟：续租与状态落库都按它取真实时刻（墙钟 + 单调钟）。 */
  clock?: ClockProvider;
  /** 续租间隔（毫秒）：实际取值被压到 ≤ TTL/3。 */
  leaseRenewIntervalMs?: number;
  /** 单步执行超时（毫秒）：超时按真实流逝时间判定。 */
  stepTimeoutMs?: number;
  now?: () => string;
};
type MemoryRuntimeOptions = Omit<MemoryRuntimeDeps, 'ports'> & Pick<MemoryPortDeps, 'judge' | 'evalEnabled' | 'judgeTimeoutMs'> & {
  writer: {
    writeMemory: MemoryPortDeps['writeMemory'];
    fenceAndInspect: InspectMemoryWrite;
  };
};
/** Host entry point. Both operations are bound to the same trusted writer. */
declare function createMemoryRuntime(options: MemoryRuntimeOptions): {
  advance: (params: AdvanceMemoryRuntimeParams) => Promise<MemoryRuntimeResult>;
  inspectBatch: (batchId: string) => Promise<{
    decision_id: string;
    revision: number;
    allowed_resolutions: ("confirm_applied" | "confirm_not_applied" | "abandon" | "acknowledge_abandon")[];
    batch_id: string;
    node_run_id: string;
    attempt: number;
    stopped_at: string;
    evidence_id: string;
    written_count: number;
    missing_count: number;
    partial: boolean;
    items: {
      client_key: string;
      content_hash: string;
      status: "applied" | "missing" | "skipped";
      applied_item_id: string | null;
    }[];
  }>;
  resolveRecovery: (actor: Actor, request: MemoryRecoveryRequest) => {
    batch_id: string;
    status: string;
    replacement_node_run_id: string | null;
    abandon_ack_id: string | null;
    terminal: RunTerminal | null;
  };
};
type AdvanceMemoryRuntimeParams = {
  memory_run_id: string;
  now?: string;
  max_steps?: number;
};
type MemoryRuntimeStepStatus = 'done' | 'failed' | 'blocked_pre' | 'superseded' | 'indeterminate_side_effect';
type MemoryRuntimeStep = {
  node_id: string;
  node_run_id: string;
  status: MemoryRuntimeStepStatus;
  route?: string | null;
  detail?: Record<string, unknown>;
};
type MemoryRuntimeResult = {
  memory_run_id: string;
  batch_id: string;
  status: 'completed' | 'blocked' | 'failed';
  steps: MemoryRuntimeStep[];
  terminal: RunTerminal | null;
  batch_status: string;
  run_status: string;
  error_code?: string;
};
type PluginEntry = {
  client_key: string;
  content_hash: string;
  applied_item_id: string;
};
type MemoryPluginWriter = {
  protocol: 1;
  writeMemory(input: Omit<GovernedWriteInput, 'items'> & {
    items: (MemoryItemInput & {
      content_hash: string;
    })[];
  }): Promise<PluginEntry[]>;
  fenceAndInspect: InspectMemoryWrite;
};
type MemoryHostOptions = Pick<MemoryRuntimeOptions, 'prepareItems' | 'judge' | 'resolveSource' | 'judgeTimeoutMs'> & {
  /** 注入时钟：记忆运行时的续租与状态落库按它取真实时刻；不注入就用系统时钟。 */
  clock?: ClockProvider;
} & ({
  context: {
    get(name: 'orchestraMemoryWriter'): unknown;
  };
  plugin?: never;
} | {
  plugin: MemoryPluginWriter;
  context?: never;
});
declare function createMemoryHost(db: Db, secret: string, options: MemoryHostOptions, logError: (error: unknown) => void): {
  start(input: StartMemoryRunParams): MemoryRunStartResult;
  inspect: (batchId: string) => Promise<{
    decision_id: string;
    revision: number;
    allowed_resolutions: ("confirm_applied" | "confirm_not_applied" | "abandon" | "acknowledge_abandon")[];
    batch_id: string;
    node_run_id: string;
    attempt: number;
    stopped_at: string;
    evidence_id: string;
    written_count: number;
    missing_count: number;
    partial: boolean;
    items: {
      client_key: string;
      content_hash: string;
      status: "applied" | "missing" | "skipped";
      applied_item_id: string | null;
    }[];
  }>;
  resolve(actor: Actor, request: MemoryRecoveryRequest): {
    batch_id: string;
    status: string;
    replacement_node_run_id: string | null;
    abandon_ack_id: string | null;
    terminal: RunTerminal | null;
  };
  resolveApproval(input: Parameters<NodeCommands["resolveDecision"]>[0]): DecisionCommandResult | undefined;
  resume(now?: string): void;
  close(): Promise<void>;
};
type MemoryHost = ReturnType<typeof createMemoryHost>;
type ChatTaskOrigin = {
  messageKey: string;
  requestKey: string;
  inputHash: string;
  signal: AbortSignal;
  assertCurrent(): void;
};
declare const proposalSchema: z.ZodObject<{
  title: z.ZodString;
  workflow_id: z.ZodString;
  workflow_version: z.ZodString;
  steps: z.ZodArray<z.ZodObject<{
    node_id: z.ZodString;
    description: z.ZodString;
    goal_ids: z.ZodArray<z.ZodString>;
    depends_on: z.ZodArray<z.ZodString>;
    tools: z.ZodArray<z.ZodString>;
    complexity: z.ZodEnum<{
      routine: "routine";
      standard: "standard";
      advanced: "advanced";
      complex: "complex";
      expert: "expert";
    }>;
    model_reason: z.ZodString;
    preferred_model_ref: z.ZodOptional<z.ZodString>;
  }, z.core.$strict>>;
  deliverables: z.ZodArray<z.ZodObject<{
    title: z.ZodString;
    node_id: z.ZodString;
    goal_ids: z.ZodArray<z.ZodString>;
    acceptance: z.ZodDiscriminatedUnion<[z.ZodObject<{
      kind: z.ZodLiteral<"artifact">;
      file_name: z.ZodString;
      minimum_bytes: z.ZodNumber;
    }, z.core.$strict>, z.ZodObject<{
      kind: z.ZodLiteral<"tool_evidence">;
      tool: z.ZodString;
      exit_code: z.ZodLiteral<0>;
    }, z.core.$strict>], "kind">;
  }, z.core.$strict>>;
  risks: z.ZodArray<z.ZodObject<{
    severity: z.ZodEnum<{
      low: "low";
      medium: "medium";
      high: "high";
    }>;
    description: z.ZodString;
    mitigation: z.ZodString;
    node_id: z.ZodOptional<z.ZodString>;
  }, z.core.$strict>>;
}, z.core.$strict>;
declare const planInputSchema: z.ZodObject<{
  goal: z.ZodString;
  requirements: z.ZodDefault<z.ZodArray<z.ZodString>>;
}, z.core.$strict>;
declare const planConfirmSchema: z.ZodObject<{
  plan_id: z.ZodUUID;
  content_hash: z.ZodString;
  confirmed: z.ZodLiteral<true>;
}, z.core.$strict>;
type TaskPlanInput = z.infer<typeof planInputSchema>;
type TaskPlanProposal = z.infer<typeof proposalSchema>;
type PlanConfirm = z.infer<typeof planConfirmSchema>;
type Goal = {
  id: string;
  text: string;
};
type PlanInput = {
  goal: string;
  requirements: Goal[];
};
type WorkflowCandidate = {
  workflowId: string;
  version: string;
  title: string;
  nodes: Array<{
    id: string;
    type: string;
    title?: string;
    description?: string;
    approvalPrompt?: string;
    dependsOn: string[];
    toolAllow: string[];
    declaredTools: string[];
  }>;
};
type TaskPlanningOptions = {
  propose(input: PlanInput & {
    workflows: WorkflowCandidate[];
    model_profiles?: readonly ModelPlanningProfile[];
  }, signal: AbortSignal, onProgress?: () => void): Promise<unknown>;
  timeoutMs?: number;
  /** This planner reports nonempty provider output through onProgress. */
  streamProgress?: boolean;
  /**
   * 自动生成任务专属流程的能力：没有可复用的合适流程时，由服务端生成 task_local 流程。
   * 不装配它就退回原来的口径：提示先发布至少一个可复用流程。
   */
  workflowGenerator?: (input: {
    goal: string;
  }, signal: AbortSignal, onProgress?: () => void) => Promise<unknown>;
  workflowStreamProgress?: boolean;
  /** 运行时允许的节点类型（发布层校验用）。 */
  runtimeNodeTypes?: ReadonlySet<string>;
};
declare const checks: readonly ["workflow", "coverage", "deliverables", "tools", "dependencies", "risks"];
type PlanCheck = (typeof checks)[number];
type PlanQuality = {
  ready: boolean;
  checks: Array<{
    check: PlanCheck;
    passed: boolean;
    issues: string[];
  }>;
};
type TaskPlanView = {
  plan_id: string;
  content_hash: string;
  brief: PlanInput;
  proposal: TaskPlanProposal;
  quality: PlanQuality;
  created_at: string;
  launch: {
    task_id: string;
    run_id: string;
  } | null;
  /** 自动生成的流程归属的预留任务号（只有自动生成的计划才有） */
  owner_task_id?: string | null;
  /** 自动生成并发布的任务专属流程号（只有自动生成的计划才有） */
  auto_workflow_id?: string | null;
};
type PlanRow = {
  plan_id: string;
  input_json: string;
  proposal_json: string;
  quality_json: string;
  content_hash: string;
  workflow_hash: string | null;
  launch_operation_id: string;
  created_at: string;
  task_id: string | null;
  run_id: string | null;
  owner_task_id: string | null;
  auto_workflow_id: string | null;
};
declare function createTaskPlanningHost(db: Db, catalog: WorkflowCatalog, registry: ToolRegistry | undefined, options?: TaskPlanningOptions, planningProfiles?: () => readonly ModelPlanningProfile[] | Promise<readonly ModelPlanningProfile[]>, readTimeouts?: () => PlanningTimeoutSnapshot): {
  enabled: boolean;
  readonly requestTimeoutMs: number;
  get: (planId: string, actor: string) => {
    row: PlanRow;
    view: TaskPlanView;
    as_of_seq: number;
  };
  getForTask(taskId: string, actor: string): {
    item: TaskPlanView | null;
    as_of_seq: number;
  };
  propose: (input: TaskPlanInput, actor: string, now: string, signal?: AbortSignal) => Promise<{
    row: PlanRow;
    view: TaskPlanView;
    as_of_seq: number;
  }>;
  proposeFromChat: (input: TaskPlanInput, actor: string, now: string, origin: ChatTaskOrigin) => Promise<{
    row: PlanRow;
    view: TaskPlanView;
    as_of_seq: number;
  }>;
  confirm(input: PlanConfirm, actor: string, now: string): {
    replayed: boolean;
    task_id: string;
    run_id: string;
  };
  launchFromChat(planId: string, contentHash: string, actor: string, now: string, origin: ChatTaskOrigin, nativeApproval?: boolean): {
    replayed: boolean;
    task_id: string;
    run_id: string;
  };
};
type TaskPlanningHost = ReturnType<typeof createTaskPlanningHost>;
/** The executor calls this inside its dispatch transaction, after the plan was confirmed and started. */
declare function getTaskPlanRecommendation(db: Db, context: ModelRoutingContext): ModelRecommendation | null;
type CollaborationBinding = {
  node_run_id: string;
  attempt: number;
  action_generation: number;
};
type Scope = {
  scope_id: string;
  title: string;
  owner_actor: string;
  created_at: string;
  expires_at: string;
};
type Member = {
  node_run_id: string;
  run_id: string;
  task_id: string;
  node_id: string;
};
type Finding = {
  finding_id: string;
  scope_id: string;
  source_node_run_id: string;
  source_attempt: number;
  source_generation: number;
  title: string;
  summary: string;
  evidence_id: string;
  created_at: string;
};
type Status = 'proposed' | 'authorized' | 'delivered' | 'accepted' | 'completed' | 'rejected' | 'denied' | 'expired';
type VerificationVerdict = 'confirmed' | 'refuted' | 'inconclusive';
type VerificationRow = {
  request_id: string;
  scope_id: string;
  finding_id: string;
  target_node_run_id: string;
  status: Status;
  revision: number;
  target_attempt: number | null;
  target_generation: number | null;
  question: string;
  authorization_note: string | null;
  delivered_at: string | null;
  accepted_at: string | null;
  accepted_chain_seq: number | null;
  result: string | null;
  evidence_id: string | null;
  verdict: VerificationVerdict | null;
  adoption: 'adopted' | 'dismissed' | null;
  adoption_note: string | null;
  expires_at: string;
  created_at: string;
  updated_at: string;
  replacement_request_id: string | null;
  replaces_request_id: string | null;
};
type Verification = Omit<VerificationRow, 'status'> & {
  status: Status | 'withdrawn';
  withdrawn_from_status: Status | null;
};
declare const collaborationScopeSchema: z.ZodObject<{
  operation_id: z.ZodString;
  title: z.ZodString;
  node_run_ids: z.ZodArray<z.ZodString>;
  ttl_minutes: z.ZodNumber;
}, z.core.$strict>;
declare const collaborationReviewSchema: z.ZodObject<{
  operation_id: z.ZodString;
  request_id: z.ZodString;
  expected_revision: z.ZodNumber;
  action: z.ZodEnum<{
    authorize: "authorize";
    deny: "deny";
    adopt: "adopt";
    dismiss: "dismiss";
  }>;
  note: z.ZodString;
}, z.core.$strict>;
declare const collaborationReassignSchema: z.ZodObject<{
  operation_id: z.ZodString;
  request_id: z.ZodString;
  expected_revision: z.ZodNumber;
  target_node_run_id: z.ZodString;
  note: z.ZodString;
}, z.core.$strict>;
declare const collaborationFindingSchema: z.ZodObject<{
  scope_id: z.ZodString;
  title: z.ZodString;
  summary: z.ZodString;
  evidence_id: z.ZodString;
}, z.core.$strict>;
declare const collaborationRequestSchema: z.ZodObject<{
  finding_id: z.ZodString;
  target_node_run_id: z.ZodString;
  question: z.ZodString;
  ttl_minutes: z.ZodNumber;
}, z.core.$strict>;
declare const collaborationHandleSchema: z.ZodDiscriminatedUnion<[z.ZodObject<{
  action: z.ZodEnum<{
    delivered: "delivered";
    accept: "accept";
    reject: "reject";
  }>;
  operation_id: z.ZodString;
  request_id: z.ZodString;
  expected_revision: z.ZodNumber;
}, z.core.$strict>, z.ZodObject<{
  action: z.ZodLiteral<"complete">;
  result: z.ZodString;
  verdict: z.ZodEnum<{
    confirmed: "confirmed";
    refuted: "refuted";
    inconclusive: "inconclusive";
  }>;
  evidence_id: z.ZodString;
  operation_id: z.ZodString;
  request_id: z.ZodString;
  expected_revision: z.ZodNumber;
}, z.core.$strict>], "action">;
declare function createCollaborationHost(db: Db, secret: string): {
  createScope(input: z.input<typeof collaborationScopeSchema>, actor: string, now: string): Scope;
  listScopes(actor: string): {
    items: Scope[];
    as_of_seq: number;
  };
  getScope(scopeId: string, actor: string, now?: string): {
    item: {
      scope: Scope;
      members: {
        title?: string | undefined;
        node_run_id: string;
        run_id: string;
        task_id: string;
        node_id: string;
      }[];
      findings: Finding[];
      requests: ({
        reassignment_candidates: null;
        scope_id: string;
        evidence_id: string | null;
        finding_id: string;
        target_node_run_id: string;
        question: string;
        request_id: string;
        result: string | null;
        verdict: VerificationVerdict | null;
        revision: number;
        created_at: string;
        updated_at: string;
        target_attempt: number | null;
        target_generation: number | null;
        authorization_note: string | null;
        delivered_at: string | null;
        accepted_at: string | null;
        accepted_chain_seq: number | null;
        adoption: "adopted" | "dismissed" | null;
        adoption_note: string | null;
        expires_at: string;
        replacement_request_id: string | null;
        replaces_request_id: string | null;
        status: Status | "withdrawn";
        withdrawn_from_status: Status | null;
      } | {
        reassignment_candidates: string[];
        scope_id: string;
        evidence_id: string | null;
        finding_id: string;
        target_node_run_id: string;
        question: string;
        request_id: string;
        result: string | null;
        verdict: VerificationVerdict | null;
        revision: number;
        created_at: string;
        updated_at: string;
        target_attempt: number | null;
        target_generation: number | null;
        authorization_note: string | null;
        delivered_at: string | null;
        accepted_at: string | null;
        accepted_chain_seq: number | null;
        adoption: "adopted" | "dismissed" | null;
        adoption_note: string | null;
        expires_at: string;
        replacement_request_id: string | null;
        replaces_request_id: string | null;
        status: Status | "withdrawn";
        withdrawn_from_status: Status | null;
      })[];
    };
    as_of_seq: number;
  };
  reportFinding(binding: CollaborationBinding, input: z.input<typeof collaborationFindingSchema>, now: string): Finding;
  requestVerification(binding: CollaborationBinding, input: z.input<typeof collaborationRequestSchema>, now: string): Verification;
  review(input: z.input<typeof collaborationReviewSchema>, actor: string, now: string): Verification;
  reassignVerification(input: z.input<typeof collaborationReassignSchema>, actor: string, now: string): {
    withdrawn: Verification;
    replacement: Verification;
  };
  listMemberships(binding: CollaborationBinding, now: string): {
    items: {
      members: Member[];
      title: string;
      scope_id: string;
      expires_at: string;
    }[];
    as_of_seq: number;
  };
  listInbox(binding: CollaborationBinding, now: string): {
    items: {
      request: Verification;
      finding: Finding;
    }[];
    as_of_seq: number;
  };
  listOutgoing(binding: CollaborationBinding): {
    items: {
      finding: Finding;
      requests: Verification[];
    }[];
    as_of_seq: number;
  };
  handle(binding: CollaborationBinding, input: z.input<typeof collaborationHandleSchema>, now: string): Verification;
  sweep(now: string): number;
};
type CollaborationHost = ReturnType<typeof createCollaborationHost>;
declare const deliveryHoldSchema: z.ZodDiscriminatedUnion<[z.ZodObject<{
  action: z.ZodLiteral<"hold">;
  operation_id: z.ZodUUID;
  run_id: z.ZodString;
  node_run_id: z.ZodString;
}, z.core.$strict>, z.ZodObject<{
  action: z.ZodLiteral<"withdraw">;
  operation_id: z.ZodUUID;
  run_id: z.ZodString;
  node_run_id: z.ZodString;
}, z.core.$strict>, z.ZodObject<{
  action: z.ZodLiteral<"accept">;
  artifact_id: z.ZodString;
  sha256: z.ZodString;
  operation_id: z.ZodUUID;
  run_id: z.ZodString;
  node_run_id: z.ZodString;
}, z.core.$strict>], "action">;
declare function createDeliveryReviewHost(db: Db, secret: string, humanOnly?: boolean): {
  list(actor: string, runId?: string): {
    items: Record<string, unknown>[];
    as_of_seq: number;
  };
  change(input: z.infer<typeof deliveryHoldSchema>, actor: string, now: string): {
    run_id: string;
    node_run_id: string;
    action: "accept" | "hold" | "withdraw";
  };
};
declare const reworkInspectionInput: z.ZodObject<{
  operation_id: z.ZodUUID;
  run_id: z.ZodString;
  target_node_run_id: z.ZodString;
  source_artifact_id: z.ZodString;
  source_artifact_hash: z.ZodString;
  expected_revision: z.ZodNumber;
  criteria: z.ZodString;
}, z.core.$strict>;
type InspectionInput = z.infer<typeof reworkInspectionInput>;
type ReworkInspection = {
  inspection_id: string;
  run_id: string;
  node_run_id: string;
  artifact_id: string;
  artifact_hash: string;
  reviewer_actor: string;
  criteria: string;
  verdict: 'pass' | 'request_changes';
  summary: string;
  request_id: string | null;
  snapshot_hash: string;
  created_at: string;
};
type InspectionResult = ReworkInspection & {
  request: ReworkRequest | null;
};
declare function createReworkInspectionHost(db: Db, secret: string, rework: ReworkHost, planner?: ReworkPlanner, humanOnly?: boolean): {
  enabled: boolean;
  list(runId: string, actor: string): {
    items: ReworkInspection[];
    as_of_seq: number;
  };
  inspect(input: InspectionInput, actor: string, now: string): Promise<InspectionResult>;
};
declare const modelRoutingSettingsSchema: z.ZodObject<{
  operation_id: z.ZodString;
  expected_revision: z.ZodNumber;
  mapping: z.ZodObject<{
    routine: z.ZodNullable<z.ZodString>;
    standard: z.ZodNullable<z.ZodString>;
    complex: z.ZodNullable<z.ZodString>;
    advanced: z.ZodOptional<z.ZodNullable<z.ZodString>>;
    expert: z.ZodOptional<z.ZodNullable<z.ZodString>>;
  }, z.core.$strict>;
  model_notes: z.ZodOptional<z.ZodArray<z.ZodObject<{
    ref: z.ZodString;
    note: z.ZodString;
  }, z.core.$strict>>>;
  model_capacities: z.ZodOptional<z.ZodArray<z.ZodObject<{
    ref: z.ZodString;
    capacity: z.ZodNullable<z.ZodEnum<{
      routine: "routine";
      standard: "standard";
      advanced: "advanced";
      complex: "complex";
      expert: "expert";
    }>>;
  }, z.core.$strict>>>;
  reasoning_efforts: z.ZodOptional<z.ZodRecord<z.ZodEnum<{
    routine: "routine";
    standard: "standard";
    advanced: "advanced";
    complex: "complex";
    expert: "expert";
  }> & z.core.$partial, z.ZodNullable<z.ZodString & z.ZodType<string, string, z.core.$ZodTypeInternals<string, string>>>>>;
  model_reasoning_defaults: z.ZodOptional<z.ZodArray<z.ZodObject<{
    ref: z.ZodString;
    effort: z.ZodNullable<z.ZodString & z.ZodType<string, string, z.core.$ZodTypeInternals<string, string>>>;
  }, z.core.$strict>>>;
  planner_model: z.ZodOptional<z.ZodNullable<z.ZodObject<{
    ref: z.ZodString;
    effort: z.ZodNullable<z.ZodString & z.ZodType<string, string, z.core.$ZodTypeInternals<string, string>>>;
  }, z.core.$strict>>>;
}, z.core.$strict>;
declare function createModelRoutingSettingsHost(db: Db, source: readonly ModelCatalogProfile[] | (() => ModelCatalogSnapshot), refresh?: () => Promise<void>, plannerModelConfigurable?: boolean): {
  get: () => {
    item: {
      unavailable_providers?: readonly string[] | undefined;
      session_execution_model_ref?: string | undefined;
      planner_model_configurable: boolean;
      model_notes: {
        ref: string;
        note: string;
      }[];
      model_capacities: {
        ref: string;
        capacity: "routine" | "standard" | "advanced" | "complex" | "expert" | null;
      }[];
      model_reasoning_defaults: {
        ref: string;
        effort: string | null;
      }[];
      profiles: {
        host_capacity: "routine" | "standard" | "advanced" | "complex" | "expert" | null;
        model?: string | undefined;
        provider?: string | undefined;
        reasoning?: ModelReasoning | undefined;
        ref: string;
        capacity: ModelComplexity | null;
        name?: string;
        provider_name?: string;
      }[];
      updated_by: null;
      updated_at: null;
      reasoning_efforts: Partial<Record<"routine" | "standard" | "advanced" | "complex" | "expert", string | null>>;
      planner_model: {
        ref: string;
        effort: string | null;
      } | null;
      revision: number;
      mapping: {
        routine: string | null;
        standard: string | null;
        advanced: string | null;
        complex: string | null;
        expert: string | null;
      };
    } | {
      unavailable_providers?: readonly string[] | undefined;
      session_execution_model_ref?: string | undefined;
      planner_model_configurable: boolean;
      model_notes: {
        ref: string;
        note: string;
      }[];
      model_capacities: {
        ref: string;
        capacity: "routine" | "standard" | "advanced" | "complex" | "expert" | null;
      }[];
      model_reasoning_defaults: {
        ref: string;
        effort: string | null;
      }[];
      profiles: {
        host_capacity: "routine" | "standard" | "advanced" | "complex" | "expert" | null;
        model?: string | undefined;
        provider?: string | undefined;
        reasoning?: ModelReasoning | undefined;
        ref: string;
        capacity: ModelComplexity | null;
        name?: string;
        provider_name?: string;
      }[];
      updated_by: string;
      updated_at: string;
      reasoning_efforts: Partial<Record<"routine" | "standard" | "advanced" | "complex" | "expert", string | null>>;
      planner_model: {
        ref: string;
        effort: string | null;
      } | null;
      revision: number;
      mapping: {
        advanced: string | null;
        expert: string | null;
        routine: string | null;
        standard: string | null;
        complex: string | null;
      };
    };
    as_of_seq: number;
  };
  refresh: (() => Promise<void>) | undefined;
  planningProfiles(): {
    ref: string;
    capacity: "routine" | "standard" | "advanced" | "complex" | "expert";
    user_note: string;
  }[];
  save(request: z.input<typeof modelRoutingSettingsSchema>, actor: string, now: string): {
    revision: number;
  };
};
type ModelRoutingSettingsHost = ReturnType<typeof createModelRoutingSettingsHost>;
/** Host-owned verification of a human session on this request. Use authenticated host context. */
type ResolveHttpHuman = (request: IncomingMessage) => {
  id: string;
} | null | Promise<{
  id: string;
} | null>;
type ServerDeps = {
  secret: string;
  preferNamedPipe?: boolean;
  resolveHttpHuman?: ResolveHttpHuman;
  runNotes?: RunNotes;
  phase: string;
  epoch: number;
  asOfSeq: () => number;
  listTasks: (limit: number) => unknown[];
  listEvents: (fromSeq: number, limit: number) => unknown[];
  queries?: ServerQueries;
  appendLedgerEvent: (body: unknown, actor: string) => {
    seq: number;
    event_id: string;
  };
  log?: Logger;
  queue?: WriteQueue;
  now?: () => string;
  commands?: NodeCommands;
  memory?: MemoryHost;
  memoryProposalOnly?: () => boolean;
  planning?: TaskPlanningHost;
  collaboration?: CollaborationHost;
  rework?: ReworkHost;
  delivery?: ReturnType<typeof createDeliveryReviewHost>;
  localPlanning?: ReturnType<typeof createReworkPlanningHost>;
  inspection?: ReturnType<typeof createReworkInspectionHost>;
  modelEscalation?: ReturnType<typeof createModelEscalationHost>;
  modelSettings?: ModelRoutingSettingsHost;
  taskTriggers?: TaskTriggerHost;
  workflowGenerator?: (input: {
    goal: string;
  }, signal: AbortSignal, onProgress?: () => void) => Promise<unknown>;
  workflowGeneratorStreamProgress?: boolean;
  planningSettings?: PlanningSettingsHost;
  executionCapacitySettings?: ExecutionCapacitySettingsHost;
  workflowConcurrencySettings?: WorkflowConcurrencyHost;
};
type ServerQueries = {
  listEvents: (fromSeq: number, limit: number) => {
    rows: unknown[];
    as_of_seq: number;
  };
  listEventHistory: (params: Parameters<typeof listEventHistory>[1]) => ReturnType<typeof listEventHistory>;
  listExecutionHistory: (params: Parameters<typeof listExecutionHistory>[1]) => ReturnType<typeof listExecutionHistory>;
  listTaskSummaries: (params: ListTaskParams) => {
    items: TaskSummary[];
    next_cursor: string | null;
    as_of_seq: number;
  };
  getTaskSummary: (taskId: string, now: number) => {
    item: TaskSummary;
    as_of_seq: number;
  };
  getWorkspaceOverview: () => {
    item: WorkspaceOverview;
    as_of_seq: number;
  };
  listRuns: (opts: {
    task_id?: string;
    workflow_id?: string;
    limit?: number;
    cursor?: string;
  }) => {
    items: RunSummary[];
    next_cursor?: string | null;
    as_of_seq: number;
  };
  getRun: (runId: string) => RunSummary;
  listNodes: (runId: string, limit?: number) => {
    items: NodeRunView[];
    as_of_seq: number;
    has_more?: boolean;
  };
  getNode: (nodeRunId: string) => {
    item: NodeRunView;
    as_of_seq: number;
  };
  getArtifact: (artifactId: string, now: number) => {
    item: ArtifactView;
    as_of_seq: number;
  };
  getArtifactContent?: (runId: string, artifactId: string, mode: Parameters<typeof getArtifactContent>[3], now: number) => ReturnType<typeof getArtifactContent>;
  listRunArtifacts: (runId: string, now: number) => {
    items: RunArtifact[];
    has_more: boolean;
    as_of_seq: number;
  };
  getAttestation: (attestationId: string) => {
    item: AttestationView;
    as_of_seq: number;
  };
  listDecisions: (params: ListDecisionParams) => {
    items: DecisionView[];
    next_cursor: string | null;
    as_of_seq: number;
  };
  getDecision: (decisionId: string, now?: number) => {
    item: DecisionView;
    as_of_seq: number;
  };
  getEvidence: (evidenceId: string) => {
    item: EvidenceView;
    as_of_seq: number;
  };
  verifyLedger: () => {
    item: LedgerVerify;
    as_of_seq: number;
  };
  listUnverifiedEffects: (params: ListEffectsParams) => {
    items: UnverifiedEffectView[];
    next_cursor: string | null;
    as_of_seq: number;
  };
  inspectEffectEvidence: (nodeRunId: string) => ReturnType<typeof inspectEffectEvidence>;
  listMemoryBatches: (params: ListMemoryBatchesParams) => MemoryBatchPage;
  listWorkflows: (params: ListWorkflowParams) => {
    items: WorkflowListItem[];
    next_cursor: string | null;
    as_of_seq: number;
  };
  getWorkflow: (workflowId: string) => {
    item: WorkflowView;
    as_of_seq: number;
  };
  getWorkflowVersion: (workflowId: string, version: number) => {
    item: WorkflowVersionView;
    as_of_seq: number;
  };
  getOperation: (operationId: string, now: number) => {
    item: OperationRow;
    as_of_seq: number;
  };
};
type NodeCommands = {
  acquireLease: (body: {
    nodeRunId: string;
    ownerEpoch: number;
    leaseTtlMs: number;
    now: string;
  }) => {
    lease_id: string;
    expires_at: string;
    action_generation: number;
  };
  renewLease: (body: {
    nodeRunId: string;
    leaseTtlMs: number;
    now: string;
  }) => {
    expires_at: string;
  };
  report: (body: {
    nodeRunId: string;
    attempt: number;
    report: unknown;
    expectedStatus: string;
    generation: number;
    decision?: {
      decisionId: string;
      revision: number;
      resolution: 'confirm_applied' | 'confirm_not_applied' | 'uphold_report' | 'uphold_decision';
    };
    now: string;
  }) => ApplyResult;
  cancel: (body: {
    nodeRunId: string;
    attempt: number;
    revision: number;
    cancelResult: string;
    now: string;
    graceMs: number;
    extendDeadlineMs?: number;
  }) => CancelOutcome;
  loadWorkflow: (body: unknown) => LoadWorkflowResult;
  createWorkflow: (body: CreateWorkflowParams & {
    operationId: string;
  }) => unknown;
  openWorkflowDraft: (body: OpenDraftParams & {
    operationId: string;
  }) => unknown;
  updateWorkflowDraft: (body: UpdateDraftParams & {
    operationId: string;
  }) => unknown;
  testWorkflowDraft: (body: {
    workflowId: string;
    revision: number;
    now: string;
  }) => WorkflowDraftTest;
  getWorkflowDraftTest: (workflowId: string) => WorkflowDraftTest | null;
  setWorkflowStatus: (body: SetWorkflowStatusParams) => unknown;
  publishWorkflow: (body: PublishParams & {
    operationId: string;
  }) => unknown;
  reserveTask: (body: {
    title: string;
    actor: string;
    now: string;
    operationId: string;
  }) => unknown;
  getWorkflow: (body: {
    workflowId: string;
  }) => unknown;
  listWorkflows: (body: {
    ownerTaskId?: string | null;
  }) => unknown;
  createRun: (body: {
    taskTitle: string;
    workflowId?: string;
    workflowVersion: string;
    nodeIds?: string[];
    now: string;
  }) => CreateRunResult;
  startRun: (body: {
    taskId?: string;
    taskTitle: string;
    workflowId?: string;
    workflowVersion: string;
    nodeIds?: string[];
    autonomy?: 'autonomous' | 'assisted' | 'supervised';
    triggerKind?: TaskOrigin;
    escalations?: {
      kind: 'node_type' | 'action_category' | 'workflow_version_publish';
      value: string;
    }[];
    operationId: string;
    startedBy?: string;
    now: string;
  }) => StartRunResult;
  setTaskArchive?: (body: SetTaskArchive, actor: string, now: string) => {
    task_id: string;
    archived_at: string | null;
  };
  acknowledgeDecision: (body: Parameters<typeof acknowledgeDecision>[1]) => ReturnType<typeof acknowledgeDecision>;
  resolveDecision: (body: {
    decisionId: string;
    revision: number;
    resolution: Resolution$1;
    payload?: Record<string, unknown>;
    actor?: Actor;
    evidenceProducers?: string[];
    bindActor?: boolean;
    confirm?: boolean;
    confirmChallenge?: string;
    contentHash?: string;
    operationId?: string;
    now: string;
  }) => {
    decisionId: string;
    revision: number;
    replayed?: boolean;
  };
  resolveEffect: (body: {
    basis: ResolutionBasis;
    nodeRunId: string;
    runId?: string | null;
    evidenceId?: string | null;
    decisionId?: string | null;
    stopEvidence?: string | null;
    evidenceSource?: EvidenceSource | null;
    expected: {
      revision: number;
      decisionRevision?: number | null;
    };
    operationId: string;
    now: string;
    actor: Actor;
  }) => EffectResolutionResult;
  confirmStart: (body: {
    nodeRunId: string;
    attempt: number;
    attested?: boolean;
    now: string;
    actor?: string;
  }) => {
    trigger: 'confirm-started' | 'confirm-started-attested' | 'confirm-started-with-tool';
    attestation_id?: string;
  };
  startCompensation: (body: {
    nodeRunId: string;
    attempt: number;
    descriptor: CompensationDescriptor;
    now: string;
  }) => {
    compensationId: string;
    started: boolean;
  };
  settleCompensation: (body: {
    compensationId: string;
    state: 'applied' | 'failed' | 'unverified';
    evidence?: unknown;
    now: string;
    reason?: string;
  }) => {
    settled: boolean;
  };
  wakeOnEvent: (body: {
    runId: string;
    eventType: string;
    payload?: unknown;
    idempotencyKey?: string | null;
    now: string;
  }) => {
    eventSeq: number;
    woken: number;
  };
  cancelRun: (body: {
    runId: string;
    cancelResult: string;
    graceMs: number;
    extendDeadlineMs?: number;
    now: string;
  }) => {
    cancelledRuns: string[];
    cancelledNodes: string[];
    skippedIrreversible: string[];
    pendingHumanDecisions: string[];
    runStatus: string;
    terminalStatus: string | null;
    outcome: string | null;
  };
  dispatch: (body: {
    runId: string;
    nodes: Record<string, NodeSpec>;
    now: string;
    ownerEpoch?: number;
    leaseTtlMs?: number;
    leaseRenewIntervalMs?: number;
  }, progress?: DispatchProgress) => Promise<DispatchOutcome[]>;
  listRecoveryUncertain: () => RecoveryUncertainItem[];
  resolveRecoveryUncertain: (body: {
    nodeRunId: string;
    revision: number;
    resolution: 'mark_indeterminate_side_effect';
    now: string;
  }) => RecoveryResolveResult;
  runNode: (body: {
    nodeRunId: string;
    nodeId: string;
    attempt: number;
    input?: unknown;
    outputSchema?: unknown;
    tools: {
      command: string;
      args: string[];
      cwd: string;
      timeoutMs: number;
    }[];
    now: string;
    ownerEpoch?: number;
    leaseTtlMs?: number;
    leaseRenewIntervalMs?: number;
  }) => Promise<RunNodeResult>;
};
type Llm = Context['llm'];
type Agent = AgentHandle['agent'];
type PrivateNoteRoute = {
  provider: string;
  model: string;
  reasoningEffort?: string | null;
};
type RouteSource = PrivateNoteRoute | ((signal: AbortSignal) => PrivateNoteRoute | undefined | Promise<PrivateNoteRoute | undefined>);
declare function privateNoteCaptureMaterial(agent: Agent, instruction: string): {
  instruction: string;
  observations: string[];
};
/** Prepared direct model calls carry no session identity and never enter the agent loop. */
declare function createDshPrivateNotes(llm: Llm | undefined): {
  capture(route: PrivateNoteRoute, material: ReturnType<typeof privateNoteCaptureMaterial>, signal: AbortSignal, assertCurrent: () => void): Promise<string | null>;
  review(route: RouteSource, bodies: readonly string[], signal: AbortSignal, assertCurrent: () => void): Promise<"follow_up" | "informational" | "insufficient">;
} | undefined;
type DshExecutionModel = ModelProfile & {
  provider: string;
  model: string;
};
type DshAgent = AgentHandle['agent'];
type NamedModel = Pick<DshExecutionModel, 'provider' | 'model'>;
type PinnedModel = NamedModel & {
  reasoningEffort?: string | null;
};
type CatalogRoute = ModelCatalogProfile & NamedModel;
type DshCollaborationBinding = {
  node_run_id: string;
  attempt: number;
  action_generation: number;
};
/** A dedicated tool-free DSH child may propose a plan. Review and launch remain service commands. */
declare function createDshPlanningCallback(options: {
  context: Context;
  parent: () => DshAgent | undefined;
  model: PinnedModel | ((parent: DshAgent, signal: AbortSignal) => PinnedModel | undefined | Promise<PinnedModel | undefined>);
}, purpose?: 'plan' | 'workflow' | 'rework'): (input: unknown, signal?: AbortSignal, onProgress?: () => void) => Promise<unknown>;
/** Keep DSH's Agent handles private to the bridge. Callers authorize the parent from a trusted host session. */
declare function createDshExecutor(options: {
  context: Context;
  parentForNode(nodeRunId: string): DshAgent | undefined;
  models: readonly DshExecutionModel[];
  loadModelCatalog?: () => Promise<ModelCatalogSnapshot & {
    profiles: readonly CatalogRoute[];
  }>;
  sessionExecutionModelRef?: () => string | undefined;
  recommend: ModelRouting['recommend'];
  onModelUse?: (usage: {
    sessionId: string;
    nodeRunId: string;
    attempt: number;
    provider: string;
    model: string;
    reasoning_effort?: string | null;
  }) => void;
  collaboration?: (binding: DshCollaborationBinding, input: unknown) => unknown;
  runNote?: (binding: RunNoteBinding, input: unknown, requestId: string, now: string) => unknown;
  privateRunNotes?: {
    model: NonNullable<ReturnType<typeof createDshPrivateNotes>>;
    capture: RunNotes['captureBrain'];
  };
  rework?: (binding: DshCollaborationBinding, input: unknown) => unknown;
}): {
  bridge: BridgeClient;
  modelRouting: ModelRouting;
  resolvePlanningModel(ref: string, effort: string | null, signal: AbortSignal): Promise<PinnedModel>;
  stopModelRefresh: () => void;
  close(): Promise<void>;
};
declare class ExecutionCancels {
  private readonly active;
  private key;
  register(nodeRunId: string, attempt: number): AbortSignal;
  abort(nodeRunId: string, attempt: number, reason: string): boolean;
  release(nodeRunId: string, attempt: number): void;
  has(nodeRunId: string, attempt: number): boolean;
}
type RegisterRunArtifact = {
  artifactId: string;
  runId: string;
  fileName: string;
  bytes: Uint8Array;
  nodeRunId?: string;
  evidenceIndex?: unknown;
  retainUntil?: string;
};
/** Only trusted producers can register bytes. Metadata and content commit together. */
declare function registerRunArtifact(db: Db, input: RegisterRunArtifact, now?: string): {
  artifact_id: string;
  run_id: string;
  sha256: string;
  size_bytes: number;
};
declare const Config: import("zod").ZodPrefault<import("zod").ZodObject<{
  local: import("zod").ZodPrefault<import("zod").ZodObject<{
    enabled: import("zod").ZodDefault<import("zod").ZodUnion<readonly [import("zod").ZodBoolean, import("zod").ZodLiteral<"desktop">]>>;
    modelCapacity: import("zod").ZodOptional<import("zod").ZodEnum<{
      routine: "routine";
      standard: "standard";
      advanced: "advanced";
      complex: "complex";
      expert: "expert";
    }>>;
    dataDirectory: import("zod").ZodOptional<import("zod").ZodString>;
    allowWorkspaceListing: import("zod").ZodDefault<import("zod").ZodBoolean>;
    sampleFile: import("zod").ZodOptional<import("zod").ZodString>;
  }, import("zod/v4/core").$strict>>;
}, import("zod/v4/core").$strict>>;
/** Host half of the dual-face package. The Desktop bundle selects the personal adapter. */
declare function apply(ctx: Context, config: typeof Config._output): void;
/** Mount inside DSH's Cordis context with a host-authorized parent and model catalog. */
declare function serveDshOrchestra(options: Omit<Parameters<typeof serveOrchestra>[0], 'executor' | 'plannerModelConfigurable'> & {
  dsh: Parameters<typeof createDshExecutor>[0];
  dshPlanner?: Omit<Parameters<typeof createDshPlanningCallback>[0], 'context'> & {
    useModelRoutingSettings?: boolean;
  };
  tools: Pick<RunNodeDeps, 'registry' | 'runner'>;
}): Promise<{
  close: () => Promise<void>;
  reviewBrainNotes?: ((agent: NonNullable<ReturnType<() => import("@deepseek-ai/dsh-agent").Agent | undefined>>, run: string, actor: string, signal: AbortSignal, assertOrigin: () => void) => Promise<{
    classification: "follow_up" | "informational" | "insufficient";
    evidence_strength: "none";
  }>) | undefined;
  orchestra: Orchestra;
  endpoint: string;
  controlPipe?: string;
  collaboration?: CollaborationHost;
  rework?: ReworkHost;
  runNotes?: RunNotes;
  planning: TaskPlanningHost;
  planningSettings?: PlanningSettingsHost;
  recoverChatModelAssignment?: (input: Parameters<typeof recoverChatModelAssignment>[3]) => Promise<string[]>;
}>;
declare const name = "dsh-orchestra";
declare const version = "0.1.0";
type OrchestraOptions = {
  /** Set by the production DSH executor that implements bounded node assistants. */
  workflowHelpersAvailable?: boolean;
  dbFile: string;
  secret: string;
  phase: Phase;
  trial?: TrialOptions;
  planning?: TaskPlanningOptions;
  projectContext?: ProjectContextSource;
  reworkPlanner?: ReworkPlanner;
  release?: Omit<ReleaseWorkspace, 'runGroup'> & {
    checkDirectories: Record<ReleaseGroup, string>;
  };
  collaboration?: boolean;
  log?: Logger;
  executor?: Omit<RunNodeDeps, 'runtime' | 'db' | 'secret' | 'agents' | 'sharedContext'> & {
    agents?: HostAgentFactory;
  };
  scheduler?: ExecutionScheduler;
  configPath?: string;
  spillDir?: string;
};
type Orchestra = {
  db: Db;
  trial?: ReturnType<typeof bindTrialHost>;
  runtime: RuntimeDeps;
  catalog: WorkflowCatalog;
  commands: NodeCommands;
  modelEscalation?: ReturnType<typeof createModelEscalationHost>;
  executionCapacitySettings?: ExecutionCapacitySettingsHost;
  workflowConcurrencySettings?: WorkflowConcurrencyHost;
  ordinaryLiveRecovery?: OrdinaryLiveRecovery;
  release?: ReturnType<typeof createReleaseWorkflowHost>;
  cancels: ExecutionCancels;
  close: () => void;
};
declare function createOrchestra(opts: OrchestraOptions & {
  db?: Db;
}, proof?: symbol): Orchestra;
declare function openServiceOrchestra(opts: OrchestraOptions): Promise<{
  orchestra: Orchestra;
  release: () => Promise<void>;
}>;
declare function serveOrchestra(opts: OrchestraOptions & {
  resolveHttpHuman?: ServerDeps['resolveHttpHuman'];
  preferNamedPipe?: boolean;
  plannerModelConfigurable?: boolean;
  memory?: MemoryHostOptions;
  workflowGenerator?: ServerDeps['workflowGenerator'];
  workflowGeneratorStreamProgress?: boolean;
  resolveTriggerNodeSpecs?: ResolveTriggerNodeSpecs;
  asOfSeq: ServerDeps['asOfSeq'];
  listTasks: ServerDeps['listTasks'];
  listEvents: ServerDeps['listEvents'];
  appendLedgerEvent: ServerDeps['appendLedgerEvent'];
}): Promise<{
  orchestra: Orchestra;
  endpoint: string;
  controlPipe?: string;
  collaboration?: CollaborationHost;
  rework?: ReworkHost;
  runNotes?: RunNotes;
  planning: TaskPlanningHost;
  planningSettings?: PlanningSettingsHost;
  recoverChatModelAssignment?: (input: Parameters<typeof recoverChatModelAssignment>[3]) => Promise<string[]>;
  close: () => Promise<void>;
}>;
declare const SANDBOX_KINDS: readonly ["local_process", "read_only_workspace", "docker", "restricted_network"];
type SandboxKind = (typeof SANDBOX_KINDS)[number];
declare const TASK_RISKS: readonly ["low", "medium", "high"];
type TaskRisk = (typeof TASK_RISKS)[number];
type SandboxNetworkMode = "none" | "allowlist" | "host";
type SandboxPolicy = {
  kind: SandboxKind;
  task_risk: TaskRisk;
  isolation: "process" | "capability" | "container";
  container: boolean;
  filesystem: {
    workspace_root: string;
    roots: readonly string[];
    read_only: boolean;
  };
  network: {
    mode: SandboxNetworkMode;
    allow_hosts: readonly string[];
  };
  tools: {
    allow: readonly string[];
    deny: readonly string[];
    default: "deny";
  };
  allow_host_execution: boolean;
  fail_closed: true;
};
type SandboxToolCapability = "read_only" | "mutating" | "compensating";
type SandboxRunRequest = {
  bin: string;
  args?: string[];
  cwd: string;
  timeoutMs?: number;
  tool?: string;
  capability?: SandboxToolCapability;
  networkHost?: string;
  signal?: AbortSignal;
};
type SandboxRunResult = {
  exitCode: number;
  stdout: string;
  stderr: string;
  boundary: string;
  timedOut?: boolean;
};
type SandboxAdapter = {
  kind?: SandboxKind;
  available?: () => boolean;
  run: (request: SandboxRunRequest, policy: SandboxPolicy) => Promise<SandboxRunResult | {
    exitCode: number;
    stdout: string;
    stderr: string;
    boundary?: string;
    timedOut?: boolean;
  }>;
  dispose?: () => Promise<void> | void;
};
type ExecutionSandbox = {
  readonly kind: SandboxKind;
  readonly isolation: SandboxPolicy["isolation"];
  readonly policy: SandboxPolicy;
  describe(): Readonly<Record<string, unknown>>;
  assertTool(input: {
    name: string;
    capability?: SandboxToolCapability;
  }): true;
  assertPath(target: string): string;
  assertNetwork(input: {
    host: string;
  }): true;
  assertTask(sideEffectPolicy?: "pure" | "compensable" | "irreversible"): SandboxPolicy;
  run(request: SandboxRunRequest): Promise<SandboxRunResult>;
  dispose(): Promise<void>;
};
declare class SandboxError extends Error {
  code: string;
  details: Record<string, unknown>;
  constructor(code: string, message: string, details?: Record<string, unknown>);
}
declare function resolveSandboxPolicy(options?: {
  taskRisk?: TaskRisk;
  requestedKind?: SandboxKind;
  availableKinds?: SandboxKind[];
  workspaceRoot: string;
  allowedRoots?: string[];
  network?: SandboxNetworkMode;
  allowHosts?: string[];
  tools?: {
    allow?: string[];
    deny?: string[];
  };
  allowWrite?: boolean;
  allowHostExecution?: boolean;
}): SandboxPolicy;
declare function assertSandboxForTask(input: {
  sideEffectPolicy?: "pure" | "compensable" | "irreversible";
  policy: SandboxPolicy;
}): SandboxPolicy;
declare function createExecutionSandbox(input: {
  policy: SandboxPolicy;
  adapters?: Partial<Record<SandboxKind, SandboxAdapter>>;
}): ExecutionSandbox;
declare function createReadOnlyWorkspaceAdapter(options?: {
  run?: SandboxAdapter["run"];
}): SandboxAdapter;
declare function createLocalProcessAdapter(options?: {
  run?: SandboxAdapter["run"];
}): SandboxAdapter;
declare function createDockerAdapter(options?: {
  image?: string;
  dockerBin?: string;
  spawnImpl?: (...args: any[]) => any;
  user?: string;
  memory?: string;
  pidsLimit?: number;
  nofile?: number;
  cpus?: number | string;
  tmpSize?: string;
  maxOutputBytes?: number;
  networkName?: string;
}): SandboxAdapter & {
  buildArgs(request: SandboxRunRequest, policy: SandboxPolicy): string[];
};
declare function createRestrictedNetworkAdapter(options?: {
  image?: string;
  dockerBin?: string;
  spawnImpl?: (...args: any[]) => any;
  user?: string;
  memory?: string;
  pidsLimit?: number;
  nofile?: number;
  cpus?: number | string;
  tmpSize?: string;
  maxOutputBytes?: number;
  networkName?: string;
}): SandboxAdapter & {
  buildArgs(request: SandboxRunRequest, policy: SandboxPolicy): string[];
};
export { type CollaborationBinding, type CollaborationHost, Config, type DshExecutionModel, type EffectVerdict, type EffectVerificationTarget, type EffectVerifier, type ExecutionSandbox, type InspectMemoryWrite, type IntakeOptions, type MemoryHostOptions, type MemoryPluginWriter, type MemoryRecoveryRequest, type MemoryRuntimeOptions, type MemoryWriteEvidence, type MemoryWriteInspection, type ModelComplexity, type ModelProfile, type ModelRecommendation, type ModelRouting, type ModelRoutingContext, Orchestra, OrchestraOptions, type PlanningTimeoutSnapshot, ReadOnlyToolRunner, type RegisterRunArtifact, type ResolveTriggerNodeSpecs, SANDBOX_KINDS, SandboxError, type SandboxKind, type SandboxPolicy, type SandboxRunRequest, type SandboxRunResult, TASK_RISKS, type TaskPlanProposal, type TaskPlanningOptions, type TaskRisk, type TaskTriggerCreate, type TaskTriggerExecutionFilter, type TaskTriggerExecutionRow, type TaskTriggerOccurrence, type TaskTriggerReadiness, type TaskTriggerView, ToolRegistry, type TrialOptions, type TrialScope, type TriggerRunReady, apply, assertSandboxForTask, createCollaborationHost, createDockerAdapter, createDshExecutor, createDshPlanningCallback, createExecutionSandbox, createIntakeControl, createLocalProcessAdapter, createMemoryRuntime, createOrchestra, createReadOnlyWorkspaceAdapter, createRestrictedNetworkAdapter, getTaskPlanRecommendation, inspectMemoryRecovery, listTriggerExecutionStates, name, openServiceOrchestra, registerRunArtifact, renewNodeLease, resolveMemoryRecovery, resolveSandboxPolicy, runNode, serveDshOrchestra, serveOrchestra, submitReport, sweepNow, version, withPlanningBudget };