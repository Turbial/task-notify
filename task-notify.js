#!/usr/bin/env node
/**
 * task-notify.js — Supabase Realtime Notification Daemon
 * 
 * Runs on each agent's server. Subscribes to tasks + scopes changes
 * via Supabase Realtime (WebSocket), filters by agent/assignee, and
 * instantly inserts notification messages into the DB messaging system.
 *
 * Usage:
 *   node task-notify.js <agent-name>              # builder, operator, revenue, manager, turbo
 *   node task-notify.js operator --dry-run        # Log only, no DB writes
 *   AGENT=builder node task-notify.js             # Env-var override
 *
 * The daemon keeps a persistent WebSocket connection. On disconnect
 * it reconnects automatically (Supabase SDK handles this).
 *
 * ┌─────────────┐   INSERT/UPDATE    ┌──────────────┐
 * │  PM Board   │ ── tasks/scopes ──→  Supabase    │
 * │ pm.turbial  │   (agent=name)      │   Realtime   │
 * └─────────────┘                     └──────┬───────┘
 *                                            │ WebSocket
 *                                     ┌──────▼───────┐
 *                                     │ task-notify   │
 *                                     │   daemon      │
 *                                     └──────┬───────┘
 *                                            │ INSERT messages
 *                                     ┌──────▼───────┐
 *                                     │  Agent inbox  │
 *                                     │ <agent>-tasks │
 *                                     └──────────────┘
 */

import { createClient } from '@supabase/supabase-js';

// ── Config ────────────────────────────────────────────────────────────
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://afgmlkduuapquqkcqdsk.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFmZ21sa2R1dWFwcXVxa2NxZHNrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzgzNDA2NzYsImV4cCI6MjA5MzkxNjY3Nn0.-WQ34Jxy9CmI-SsQfcMNWPZi5AfZCzv9jZHDQ6ccEWc';

const AGENT_NAME = process.argv[2] || process.env.AGENT || (() => { throw new Error('Usage: node task-notify.js <agent-name>'); })();
const DRY_RUN = process.argv.includes('--dry-run');
const CONV_ID = `${AGENT_NAME}-tasks`;
const PM_URL = 'https://pm.turbial.com';

// ── State ─────────────────────────────────────────────────────────────
const stats = { started: 0, tasks: 0, scopes: 0, wrote: 0, errors: 0, lastEvent: null };
const seenTasks = new Map();   // dedup
const seenScopes = new Map();  // dedup

// ── Logging ────────────────────────────────────────────────────────────
function log(level, msg, data = null) {
  const ts = new Date().toISOString();
  const line = data
    ? `${ts} [${level}] ${msg} ${JSON.stringify(data)}`
    : `${ts} [${level}] ${msg}`;
  console[level === 'error' ? 'error' : 'log'](line);
}

function heartbeat() {
  const uptime = Math.round((Date.now() - stats.started) / 1000);
  log('info', `💓 Alive | uptime=${uptime}s tasks=${stats.tasks} scopes=${stats.scopes} wrote=${stats.wrote} errs=${stats.errors}`);
}

// ── Notification writers ───────────────────────────────────────────────

async function writeTaskNotification(sb, task, event) {
  const taskTitle = task.title || '(untitled)';
  const taskStatus = task.status || 'unknown';
  const taskId = task.task_id || task.id;
  const taskUrl = `${PM_URL}/#task/${taskId}`;

  const action = event === 'INSERT' ? 'assigned to you' : `updated — now "${taskStatus}"`;

  const content = [
    `📋 **${taskTitle}** ${action}`,
    '',
    `> Status: \`${taskStatus}\` | Priority: ${task.priority || '—'}`,
    task.description ? `> ${task.description.slice(0, 200)}` : '',
    '',
    `🔗 <${taskUrl}|Open on PM Board>`,
  ].join('\n');

  if (DRY_RUN) {
    log('info', `[DRY-RUN] Would write task to ${CONV_ID}: ${taskTitle} (${action})`);
    return;
  }

  const msgId = `task-${event.toLowerCase()}-${taskId}-${Date.now()}`;

  const { error } = await sb.from('messages').insert({
    conversation_id: CONV_ID,
    msg_id: msgId,
    from_agent: 'system',
    to_agent: AGENT_NAME,
    sender_type: 'system',
    message_type: 'message',
    content,
    captured_by: 'task-notify',
    metadata: {
      kind: 'task',
      task_id: taskId,
      event,
      task_status: taskStatus,
      task_title: taskTitle,
      task_assignee: task.agent || task.assignee,
      task_business: task.business,
      task_priority: task.priority,
      pm_url: taskUrl,
    },
  });

  if (error) throw new Error(`Failed to write task notification: ${error.message}`);
  stats.wrote++;
  stats.tasks++;
  log('info', `✉️  [TASK] "${taskTitle}" ${action}`);
}

async function writeScopeNotification(sb, scope, event) {
  const scopeTitle = scope.title || '(untitled)';
  const scopeId = scope.scope_id;
  const scopeUrl = `${PM_URL}/#scope/${scopeId}`;

  const action = event === 'INSERT'
    ? 'created — waiting for review'
    : scope.status === 'converted'
      ? 'converted to tasks'
      : `updated — now "${scope.status}"`;

  const content = [
    `📐 **New Scope: ${scopeTitle}**`,
    '',
    `> Status: \`${scope.status || 'new'}\` | By: ${scope.created_by || 'unknown'}`,
    scope.description ? `> ${scope.description.slice(0, 200)}` : '',
    '',
    `🔗 <${scopeUrl}|Open on PM Board>`,
  ].join('\n');

  if (DRY_RUN) {
    log('info', `[DRY-RUN] Would write scope to ${CONV_ID}: ${scopeTitle} (${action})`);
    return;
  }

  const msgId = `scope-${event.toLowerCase()}-${scopeId}-${Date.now()}`;

  const { error } = await sb.from('messages').insert({
    conversation_id: CONV_ID,
    msg_id: msgId,
    from_agent: 'system',
    to_agent: AGENT_NAME,
    sender_type: 'system',
    message_type: 'message',
    content,
    captured_by: 'task-notify',
    metadata: {
      kind: 'scope',
      scope_id: scopeId,
      event,
      scope_status: scope.status,
      scope_title: scopeTitle,
      scope_assignee: scope.assignee,
      scope_created_by: scope.created_by,
      pm_url: scopeUrl,
    },
  });

  if (error) throw new Error(`Failed to write scope notification: ${error.message}`);
  stats.wrote++;
  stats.scopes++;
  log('info', `✉️  [SCOPE] "${scopeTitle}" ${action}`);
}

// ── Dedup ──────────────────────────────────────────────────────────────

function shouldNotifyTask(task, event) {
  const taskId = task.task_id || task.id;
  const prev = seenTasks.get(taskId);
  const fp = `${task.status}|${task.agent || task.assignee}|${task.updated_at}`;
  if (event === 'INSERT' && !prev) return true;
  if (prev && prev === fp) return false;
  seenTasks.set(taskId, fp);
  return true;
}

function shouldNotifyScope(scope, event) {
  const scopeId = scope.scope_id;
  const prev = seenScopes.get(scopeId);
  const fp = `${scope.status}|${scope.assignee}|${scope.updated_at}`;
  if (event === 'INSERT' && !prev) return true;
  if (prev && prev === fp) return false;
  seenScopes.set(scopeId, fp);
  return true;
}

// ── Main ───────────────────────────────────────────────────────────────
async function main() {
  log('info', `🚀 task-notify starting for agent "${AGENT_NAME}"`);
  log('info', `📡 Supabase: ${SUPABASE_URL}`);
  log('info', `📨 Target conversation: ${CONV_ID}`);
  if (DRY_RUN) log('info', `⚠️  DRY RUN — no messages will be written`);

  const sb = createClient(SUPABASE_URL, SUPABASE_KEY, {
    realtime: { params: { eventsPerSecond: 10 } },
  });

  stats.started = Date.now();

  const channel = sb.channel('task-notify');

  // ═══════════════════════════════════════════════════════════════════
  // TASKS — INSERT: new task assigned
  // ═══════════════════════════════════════════════════════════════════
  channel.on('postgres_changes', {
    event: 'INSERT',
    schema: 'public',
    table: 'tasks',
    filter: `agent=eq.${AGENT_NAME}`,
  }, (payload) => {
    const task = payload.new;
    log('info', `📥 [TASK INSERT] "${task.title}" (status: ${task.status}) — ${task.task_id}`);
    if (shouldNotifyTask(task, 'INSERT')) {
      writeTaskNotification(sb, task, 'INSERT').catch(e => {
        stats.errors++;
        log('error', `Task notification failed: ${e.message}`);
      });
    }
  });

  // ═══════════════════════════════════════════════════════════════════
  // TASKS — UPDATE: status/priority change for our tasks
  // ═══════════════════════════════════════════════════════════════════
  channel.on('postgres_changes', {
    event: 'UPDATE',
    schema: 'public',
    table: 'tasks',
    filter: `agent=eq.${AGENT_NAME}`,
  }, (payload) => {
    const task = payload.new;
    const old = payload.old;

    const meaningful =
      task.status !== old.status ||
      task.priority !== old.priority ||
      task.agent !== old.agent ||
      task.title !== old.title;
    if (!meaningful) return;

    log('info', `📥 [TASK UPDATE] "${task.title}" ${old.status}→${task.status} — ${task.task_id}`);
    if (shouldNotifyTask(task, 'UPDATE')) {
      writeTaskNotification(sb, task, 'UPDATE').catch(e => {
        stats.errors++;
        log('error', `Task notification failed: ${e.message}`);
      });
    }
  });

  // ═══════════════════════════════════════════════════════════════════
  // TASKS — REASSIGN: agent changed TO us
  // ═══════════════════════════════════════════════════════════════════
  channel.on('postgres_changes', {
    event: 'UPDATE',
    schema: 'public',
    table: 'tasks',
  }, (payload) => {
    const task = payload.new;
    const old = payload.old;

    if (old.agent !== AGENT_NAME && task.agent === AGENT_NAME) {
      log('info', `📥 [REASSIGN] "${task.title}" → ${AGENT_NAME} — ${task.task_id}`);
      if (shouldNotifyTask(task, 'REASSIGN')) {
        writeTaskNotification(sb, task, 'INSERT').catch(e => {
          stats.errors++;
          log('error', `Reassign notification failed: ${e.message}`);
        });
      }
    }
  });

  // ═══════════════════════════════════════════════════════════════════
  // SCOPES — INSERT: new scope created
  // ═══════════════════════════════════════════════════════════════════
  channel.on('postgres_changes', {
    event: 'INSERT',
    schema: 'public',
    table: 'dashboard_scopes',
  }, (payload) => {
    const scope = payload.new;
    log('info', `📥 [SCOPE INSERT] "${scope.title}" by ${scope.created_by} — ${scope.scope_id}`);
    if (shouldNotifyScope(scope, 'INSERT')) {
      writeScopeNotification(sb, scope, 'INSERT').catch(e => {
        stats.errors++;
        log('error', `Scope notification failed: ${e.message}`);
      });
    }
  });

  // ═══════════════════════════════════════════════════════════════════
  // SCOPES — UPDATE: status change (assigned → converted, etc.)
  // ═══════════════════════════════════════════════════════════════════
  channel.on('postgres_changes', {
    event: 'UPDATE',
    schema: 'public',
    table: 'dashboard_scopes',
  }, (payload) => {
    const scope = payload.new;
    const old = payload.old;

    if (scope.status === old.status) return;

    log('info', `📥 [SCOPE UPDATE] "${scope.title}" ${old.status}→${scope.status} — ${scope.scope_id}`);
    if (shouldNotifyScope(scope, 'UPDATE')) {
      writeScopeNotification(sb, scope, 'UPDATE').catch(e => {
        stats.errors++;
        log('error', `Scope notification failed: ${e.message}`);
      });
    }
  });

  // ═══════════════════════════════════════════════════════════════════
  // Connection lifecycle
  // ═══════════════════════════════════════════════════════════════════
  channel.subscribe((status) => {
    if (status === 'SUBSCRIBED') {
      log('info', '✅ Connected to Supabase Realtime — listening for tasks + scopes');
    } else if (status === 'CHANNEL_ERROR') {
      stats.errors++;
      log('error', '❌ Channel error — will auto-reconnect');
    } else if (status === 'TIMED_OUT') {
      stats.errors++;
      log('error', '⏰ Connection timed out — will auto-reconnect');
    } else if (status === 'CLOSED') {
      log('info', '🔌 Connection closed');
    }
  });

  // Heartbeat every 5 minutes
  setInterval(heartbeat, 5 * 60 * 1000);

  // Graceful shutdown
  process.on('SIGINT', async () => {
    log('info', '🛑 Shutting down...');
    const uptime = Math.round((Date.now() - stats.started) / 1000);
    log('info', `📊 Final: uptime=${uptime}s tasks=${stats.tasks} scopes=${stats.scopes} wrote=${stats.wrote} errs=${stats.errors}`);
    await sb.removeChannel(channel);
    process.exit(0);
  });

  process.on('SIGTERM', () => {
    log('info', '🛑 SIGTERM received');
    process.exit(0);
  });
}

main().catch(e => {
  log('error', `Fatal: ${e.message}`);
  process.exit(1);
});
