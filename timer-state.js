function elapsed(state, now = Date.now()) {
  return (state.accumulatedMs || 0) +
    (state.status === 'WORKING' && state.startedAt != null ? Math.max(0, now - state.startedAt) : 0);
}

function pause(state, now = Date.now()) {
  if (state.status !== 'WORKING') return false;
  state.accumulatedMs = elapsed(state, now);
  state.startedAt = null;
  state.status = 'PAUSED';
  return true;
}

function resume(state, now = Date.now()) {
  if (state.status !== 'PAUSED' || !state.sessionId) return false;
  state.startedAt = now;
  state.status = 'WORKING';
  return true;
}

module.exports = { elapsed, pause, resume };
