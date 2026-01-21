let state = {
  currentPath: "",
  lastList: null,
  lastResultsKind: "tag",
  queue: [],
  sortable: null,
  exportPath: "",
  exportList: null,
  currentVideo: null,
  tagResults: [],
  tagIndex: { tags: [], building: false, error: null },
  currentFileTags: [],
  clipMarkers: [],
  clipBusy: false,
};

function $(id) {
  return document.getElementById(id);
}

function extractTagsFromFilename(filename) {
  const matches = String(filename || "").match(/\[(.*?)\]/g) || [];
  const out = [];
  const seen = new Set();
  for (const m of matches) {
    const inner = String(m).slice(1, -1);
    const parts = inner.split(/[\s,]+/).filter(Boolean);
    for (const p of parts) {
      const k = p.toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      out.push(p);
    }
  }
  return out;
}

function filenameFromRelpath(relpath) {
  const p = String(relpath || "");
  const parts = p.split("/");
  return parts[parts.length - 1] || "";
}

function setTagEditorEnabled(enabled) {
  const input = $("tagEditInput");
  const btn = $("tagAddBtn");
  if (input) input.disabled = !enabled;
  if (btn) btn.disabled = !enabled;
}

function setTagEditorHint(text) {
  const el = $("tagEditorHint");
  if (!el) return;
  el.textContent = text || "";
}

function setClipEditorEnabled(enabled) {
  const add = $("markerAddBtn");
  const clear = $("markerClearBtn");
  const all = $("createAllClipsBtn");
  if (add) add.disabled = !enabled;
  if (clear) clear.disabled = !enabled;
  if (all) all.disabled = !enabled;
}

function setClipEditorHint(text) {
  const el = $("clipEditorHint");
  if (!el) return;
  el.textContent = text || "";
}

function formatTime(sec) {
  const s = Math.max(0, Number(sec) || 0);
  const m = Math.floor(s / 60);
  const r = s - m * 60;
  const mm = String(m).padStart(2, "0");
  const rr = r.toFixed(1).padStart(4, "0");
  return `${mm}:${rr}`;
}

function _sortedUniqueMarkers(markers) {
  const arr = (markers || [])
    .map((x) => Number(x))
    .filter((x) => Number.isFinite(x) && x >= 0)
    .sort((a, b) => a - b);

  const out = [];
  for (const t of arr) {
    if (out.length === 0) {
      out.push(t);
      continue;
    }
    if (Math.abs(out[out.length - 1] - t) < 0.05) continue;
    out.push(t);
  }
  return out;
}

function computeSegmentsFromMarkers(markers) {
  const ms = _sortedUniqueMarkers(markers);
  const out = [];
  if (ms.length === 0) return out;

  const first = ms[0];
  if (first > 0.05) {
    out.push({ start: 0, end: first });
  }
  for (let i = 0; i < ms.length - 1; i++) {
    const a = ms[i];
    const b = ms[i + 1];
    if (b - a > 0.05) {
      out.push({ start: a, end: b });
    }
  }
  return out;
}

function renderMarkerList() {
  const wrap = $("markerList");
  if (!wrap) return;
  wrap.innerHTML = "";

  const markers = _sortedUniqueMarkers(state.clipMarkers || []);
  if (markers.length === 0) {
    renderInlineMessage(wrap, "MARKER", "Keine Marker gesetzt.");
    return;
  }

  for (let i = 0; i < markers.length; i++) {
    const t = markers[i];
    const row = document.createElement("div");
    row.className = "clip-row";
    row.innerHTML = `
      <div class="clip-row-left">
        <span class="badge">M</span>
        <div class="name">${escapeHtml(formatTime(t))}</div>
      </div>
      <div class="clip-actions">
        <button class="btn btn-sm" type="button" data-action="jump">↦</button>
        <button class="btn btn-sm" type="button" data-action="del">✕</button>
      </div>
    `;
    row.querySelector('[data-action="jump"]').addEventListener("click", () => {
      const player = $("videoPlayer");
      if (!player) return;
      player.currentTime = t;
      player.play().catch(() => {});
    });
    row.querySelector('[data-action="del"]').addEventListener("click", () => {
      state.clipMarkers = (state.clipMarkers || []).filter((x) => Math.abs(Number(x) - t) >= 0.05);
      refreshClipEditorForCurrentVideo();
    });
    wrap.appendChild(row);
  }
}

function renderSegmentList() {
  const wrap = $("segmentList");
  if (!wrap) return;
  wrap.innerHTML = "";

  const segs = computeSegmentsFromMarkers(state.clipMarkers || []);
  if (segs.length === 0) {
    renderInlineMessage(wrap, "CLIP", "Keine Segmente (mind. 1 Marker nötig)." );
    return;
  }

  for (let i = 0; i < segs.length; i++) {
    const s = segs[i];
    const row = document.createElement("div");
    row.className = "clip-row";
    row.innerHTML = `
      <div class="clip-row-left">
        <span class="badge">C</span>
        <div class="name">${escapeHtml(formatTime(s.start))} → ${escapeHtml(formatTime(s.end))}</div>
      </div>
      <div class="clip-actions">
        <button class="btn btn-sm btn-primary" type="button">Erstellen</button>
      </div>
    `;
    row.querySelector("button").addEventListener("click", async () => {
      await createClipsForSegments([s]);
    });
    wrap.appendChild(row);
  }
}

function refreshClipEditorForCurrentVideo() {
  if (!state.currentVideo || state.currentVideo.kind !== "source") {
    state.clipMarkers = [];
    setClipEditorHint("");
    setClipEditorEnabled(false);
    renderMarkerList();
    renderSegmentList();
    return;
  }

  setClipEditorHint(state.currentVideo.relpath);
  setClipEditorEnabled(!state.clipBusy);
  renderMarkerList();
  renderSegmentList();
}

async function createClipsForSegments(segments) {
  if (!state.currentVideo || state.currentVideo.kind !== "source") return;
  if (state.clipBusy) return;

  const segs = (segments || [])
    .map((s) => ({ start: Number(s.start), end: Number(s.end) }))
    .filter((s) => Number.isFinite(s.start) && Number.isFinite(s.end) && s.end > s.start);
  if (segs.length === 0) {
    setStatus("Keine gültigen Segmente.", "error");
    return;
  }

  state.clipBusy = true;
  refreshClipEditorForCurrentVideo();
  try {
    const resp = await apiPost("/api/clips/create", { relpath: state.currentVideo.relpath, segments: segs });
    const created = (resp && resp.created) ? resp.created : [];
    if (created.length > 0) {
      setStatus(`Clips erstellt: ${created.length}`, "ok");
    } else {
      setStatus("Keine Clips erstellt.", "error");
    }

    await loadList(state.currentPath);
    await loadTagIndex({ refresh: true });
    await waitForTagIndexReady({ maxMs: 60000 });
    updateTagSuggestions();

    if (created.length > 0) {
      playSource(created[0].relpath);
    }
  } catch (e) {
    setStatus(e.message, "error");
  } finally {
    state.clipBusy = false;
    refreshClipEditorForCurrentVideo();
  }
}

function renderVideoTagChips(tags) {
  const wrap = $("videoTags");
  if (!wrap) return;
  wrap.innerHTML = "";
  const arr = tags || [];
  if (arr.length === 0) {
    const span = document.createElement("div");
    span.className = "panel-subtitle";
    span.textContent = "Keine Tags.";
    wrap.appendChild(span);
    return;
  }
  for (const t of arr) {
    const chip = document.createElement("div");
    chip.className = "tag-chip";
    chip.innerHTML = `
      <span>${escapeHtml(t)}</span>
      <button type="button" aria-label="Tag entfernen">×</button>
    `;
    chip.querySelector("button").addEventListener("click", () => removeTagFromCurrentVideo(t));
    wrap.appendChild(chip);
  }
}

function updateTagSuggestions() {
  const input = $("tagEditInput");
  const dl = $("tagSuggestions");
  if (!input || !dl) return;
  const q = String(input.value || "").trim().toLowerCase();

  const existing = new Set((state.currentFileTags || []).map((x) => String(x).toLowerCase()));
  const all = (state.tagIndex && state.tagIndex.tags) ? state.tagIndex.tags : [];

  const filtered = [];
  for (const it of all) {
    const tag = String(it.tag || "");
    const tl = tag.toLowerCase();
    if (existing.has(tl)) continue;
    if (q && !tl.includes(q)) continue;
    filtered.push(tag);
    if (filtered.length >= 40) break;
  }

  dl.innerHTML = "";
  for (const t of filtered) {
    const opt = document.createElement("option");
    opt.value = t;
    dl.appendChild(opt);
  }
}

function refreshTagEditorForCurrentVideo() {
  if (!state.currentVideo || state.currentVideo.kind !== "source") {
    state.currentFileTags = [];
    setTagEditorHint("");
    renderVideoTagChips([]);
    setTagEditorEnabled(false);
    return;
  }

  const filename = filenameFromRelpath(state.currentVideo.relpath);
  state.currentFileTags = extractTagsFromFilename(filename);
  setTagEditorHint(state.currentVideo.relpath);
  renderVideoTagChips(state.currentFileTags);
  setTagEditorEnabled(true);
  updateTagSuggestions();
}

async function applyTagEdit(action, tag) {
  if (!state.currentVideo || state.currentVideo.kind !== "source") return;
  const relpath = state.currentVideo.relpath;
  const oldRelpath = relpath;
  try {
    const resp = await apiPost("/api/tags/edit", { relpath, action, tag });
    if (resp && resp.changed) {
      state.currentVideo.relpath = resp.relpath;
      const player = $("videoPlayer");
      if (player) {
        player.src = `/media/source/${encodePath(resp.relpath)}`;
        player.load();
        player.play().catch(() => {});
      }

      if (Array.isArray(state.tagResults) && state.tagResults.length > 0) {
        let anyChanged = false;
        for (const r of state.tagResults) {
          if (r && r.relpath === oldRelpath) {
            r.relpath = resp.relpath;
            if (resp.name) r.name = resp.name;
            anyChanged = true;
          }
        }
        if (anyChanged) {
          renderResults(state.lastResultsKind || "tag", state.tagResults);
        }
      }
    }
    state.currentFileTags = resp.tags || [];
    renderVideoTagChips(state.currentFileTags);

    await loadList(state.currentPath);
    await loadTagIndex({ refresh: true });
    await waitForTagIndexReady({ maxMs: 60000 });
    updateTagSuggestions();

    setStatus("Tags aktualisiert.", "ok");
  } catch (e) {
    setStatus(e.message, "error");
  }
}

async function addTagToCurrentVideo(tag) {
  const t = String(tag || "").trim();
  if (!t) return;
  await applyTagEdit("add", t);
}

async function removeTagFromCurrentVideo(tag) {
  const t = String(tag || "").trim();
  if (!t) return;
  await applyTagEdit("remove", t);
}

async function runNameSearch() {
  const qEl = $("nameQuery");
  const query = qEl ? (qEl.value || "") : "";
  try {
    const resp = await apiPost("/api/name/search", { query, limit: 200 });
    state.tagResults = resp.results || [];
    renderNameResults(state.tagResults);
    if (query.trim() !== "") {
      setStatus(`Name-Suche: ${resp.count} Treffer.`, "ok");
    }
  } catch (e) {
    setStatus(e.message, "error");
  }
}

function setTagIndexStatus(text) {
  const el = $("tagIndexStatus");
  if (!el) return;
  el.textContent = text || "";
}

function renderTagDropdown(tags) {
  const sel = $("tagDropdown");
  if (!sel) return;
  sel.innerHTML = "";
  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = "Tag auswählen…";
  sel.appendChild(opt0);

  const items = tags || [];
  for (const it of items) {
    const opt = document.createElement("option");
    opt.value = it.tag;
    opt.textContent = `${it.tag} (${it.count})`;
    sel.appendChild(opt);
  }
}

async function loadTagIndex({ refresh = false } = {}) {
  try {
    const url = refresh ? "/api/tags/list?refresh=1" : "/api/tags/list";
    const data = await apiGet(url);
    state.tagIndex = data;

    if (data.error) {
      setTagIndexStatus(`Tag-Scan Fehler: ${data.error}`);
    } else if (data.building) {
      setTagIndexStatus("Tag-Scan läuft…");
    } else {
      setTagIndexStatus(`Tags: ${data.count}`);
    }

    if (!data.building && !data.error) {
      renderTagDropdown(data.tags || []);
    }
    return data;
  } catch (e) {
    setTagIndexStatus(`Tag-Scan Fehler: ${e.message}`);
    return null;
  }
}

async function waitForTagIndexReady({ maxMs = 30000 } = {}) {
  const start = Date.now();
  while (Date.now() - start < maxMs) {
    const data = await loadTagIndex({ refresh: false });
    if (data && !data.building) {
      return data;
    }
    await new Promise((r) => setTimeout(r, 700));
  }
  return null;
}

function renderResults(kind, results) {
  const el = $("tagResultsList");
  if (!el) return;
  state.lastResultsKind = kind;
  el.innerHTML = "";

  const items = results || [];
  if (items.length === 0) {
    const qEl = (kind === "name") ? $("nameQuery") : $("tagQuery");
    const q = qEl ? String(qEl.value || "").trim() : "";
    if (q === "") {
      renderInlineMessage(el, "INFO", kind === "name"
        ? "Dateinamen eingeben (z.B. canayer) und auf 'Name suchen' klicken."
        : "Tags eingeben (z.B. Abwehr) und auf 'Suchen' klicken."
      );
    } else {
      renderInlineMessage(el, "INFO", kind === "name"
        ? "Keine Treffer im Dateinamen."
        : "Keine Treffer. Tags sind im Dateinamen in [..]."
      );
    }
    return;
  }

  for (const r of items) {
    const row = document.createElement("div");
    row.className = "item";
    row.draggable = true;
    row.dataset.relpath = r.relpath;
    row.innerHTML = `
      <div class="item-left">
        <span class="badge">${kind === "name" ? "NAME" : "TAG"}</span>
        <div class="name">${escapeHtml(r.name)}</div>
      </div>
      <div class="mono" style="font-size:11px; color: var(--muted); max-width: 45%; overflow:hidden; text-overflow: ellipsis; white-space: nowrap;">
        ${escapeHtml(r.relpath)}
      </div>
    `;

    row.addEventListener("dragstart", (e) => {
      e.dataTransfer.setData("text/plain", r.relpath);
      e.dataTransfer.setData("application/x-video-drag", JSON.stringify({ kind: "source", relpath: r.relpath }));
      e.dataTransfer.effectAllowed = "copyMove";
    });

    const nameEl = row.querySelector(".name");
    if (nameEl) {
      nameEl.title = r.name || "";
      nameEl.addEventListener("dblclick", () => {
        playSource(r.relpath);
      });
    }

    row.title = r.relpath || "";

    el.appendChild(row);
  }

  updateActivePlayingHighlights();
}

function renderTagResults(results) {
  renderResults("tag", results);
}

function renderNameResults(results) {
  renderResults("name", results);
}

async function runTagSearch({ refresh = false } = {}) {
  const qEl = $("tagQuery");
  const query = qEl ? (qEl.value || "") : "";
  const mode = getTagMode();

  try {
    const resp = await apiPost("/api/tags/search", {
      query,
      mode,
      refresh,
      limit: 200,
    });
    state.tagResults = resp.results || [];
    renderTagResults(state.tagResults);
    if (query.trim() !== "") {
      setStatus(`Tag-Suche: ${resp.count} Treffer.`, "ok");
    }
  } catch (e) {
    setStatus(e.message, "error");
  }
}

function setupTagSearchUI() {
  const q = $("tagQuery");
  const nq = $("nameQuery");
  const nbtn = $("nameSearchBtn");
  const btn = $("tagSearchBtn");
  const rescan = $("tagRescanBtn");
  const dropdown = $("tagDropdown");

  if (btn) {
    btn.addEventListener("click", () => runTagSearch({ refresh: false }));
  }
  if (rescan) {
    rescan.addEventListener("click", async () => {
      await loadTagIndex({ refresh: true });
      await waitForTagIndexReady({ maxMs: 60000 });
      await runTagSearch({ refresh: true });
    });
  }
  if (q) {
    q.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        runTagSearch({ refresh: false });
      }
    });
  }

  if (nbtn) {
    nbtn.addEventListener("click", () => runNameSearch());
  }
  if (nq) {
    nq.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        runNameSearch();
      }
    });
  }

  if (dropdown) {
    dropdown.addEventListener("change", async () => {
      const tag = dropdown.value;
      if (!tag) return;
      if (q) {
        const existing = String(q.value || "").trim();
        q.value = existing ? `${existing} ${tag}` : tag;
      }
      dropdown.value = "";
      await runTagSearch({ refresh: false });
    });
  }
}

function setupTagEditorUI() {
  const input = $("tagEditInput");
  const btn = $("tagAddBtn");

  if (input) {
    input.addEventListener("input", () => {
      updateTagSuggestions();
    });

    input.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        const v = String(input.value || "").trim();
        if (!v) return;
        input.value = "";
        addTagToCurrentVideo(v);
      }
    });
  }

  if (btn) {
    btn.addEventListener("click", () => {
      const v = input ? String(input.value || "").trim() : "";
      if (!v) return;
      if (input) input.value = "";
      addTagToCurrentVideo(v);
    });
  }

  refreshTagEditorForCurrentVideo();
}

function setupClipEditorUI() {
  const add = $("markerAddBtn");
  const clear = $("markerClearBtn");
  const all = $("createAllClipsBtn");
  const player = $("videoPlayer");

  if (add) {
    add.addEventListener("click", () => {
      if (!player) return;
      const t = Number(player.currentTime || 0);
      if (!Number.isFinite(t) || t < 0) return;
      state.clipMarkers = _sortedUniqueMarkers([...(state.clipMarkers || []), t]);
      refreshClipEditorForCurrentVideo();
    });
  }

  if (clear) {
    clear.addEventListener("click", () => {
      state.clipMarkers = [];
      refreshClipEditorForCurrentVideo();
    });
  }

  if (all) {
    all.addEventListener("click", async () => {
      const segs = computeSegmentsFromMarkers(state.clipMarkers || []);
      if (segs.length === 0) {
        setStatus("Keine Segmente zum Erstellen.", "error");
        return;
      }
      const ok = window.confirm(`Alle Segmente als Clips erstellen? (${segs.length})`);
      if (!ok) return;
      await createClipsForSegments(segs);
    });
  }

  if (player) {
    player.addEventListener("loadedmetadata", () => {
      refreshClipEditorForCurrentVideo();
    });
  }

  refreshClipEditorForCurrentVideo();
}

function setupQueueControls() {
  const btn = $("queueClearAll");
  if (!btn) return;
  btn.addEventListener("click", async () => {
    if (!window.confirm("Queue wirklich komplett löschen?")) return;
    try {
      await apiPost("/api/queue/clear", {});
      await loadQueue();
      setStatus("Queue geleert.", "ok");
    } catch (e) {
      setStatus(e.message, "error");
    }
  });
}

function setStatus(msg, kind = "") {
  const el = $("status");
  el.textContent = msg || "";
  el.className = "status" + (kind ? ` ${kind}` : "");
  if (msg) {
    window.clearTimeout(setStatus._t);
    setStatus._t = window.setTimeout(() => {
      el.textContent = "";
      el.className = "status";
    }, 4500);
  }
}

function encodePath(relpath) {
  if (!relpath) return "";
  return relpath
    .split("/")
    .map((s) => encodeURIComponent(s))
    .join("/");
}

function getInitialBrowserPath() {
  return "";
}

function urlWithBrowserPath(path) {
  const u = new URL(window.location.href);
  if (path && String(path).trim() !== "") {
    u.searchParams.set("path", path);
  } else {
    u.searchParams.delete("path");
  }
  return u.pathname + u.search;
}

function navigateList(path, { replace = false } = {}) {
  const p = path || "";
  const url = urlWithBrowserPath(p);
  const st = { path: p };
  if (replace) {
    window.history.replaceState(st, "", url);
  } else {
    window.history.pushState(st, "", url);
  }
  loadList(p);
}

function getTransferMode() {
  const radios = document.querySelectorAll('input[name="transferMode"]');
  for (const r of radios) {
    if (r.checked) return r.value;
  }
  return "copy";
}

function getTagMode() {
  const radios = document.querySelectorAll('input[name="tagMode"]');
  for (const r of radios) {
    if (r.checked) return r.value;
  }
  return "and";
}

async function apiGet(url) {
  const res = await fetch(url, { headers: { "Accept": "application/json" } });
  const data = await res.json().catch(() => null);
  if (!res.ok) {
    const msg = (data && data.error) ? data.error : `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

async function apiPost(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const data = await res.json().catch(() => null);
  if (!res.ok) {
    const msg = (data && data.error) ? data.error : `HTTP ${res.status}`;
    const err = new Error(msg);
    err.data = data;
    err.status = res.status;
    throw err;
  }
  return data;
}

async function apiDelete(url) {
  const res = await fetch(url, { method: "DELETE", headers: { "Accept": "application/json" } });
  const data = await res.json().catch(() => null);
  if (!res.ok) {
    const msg = (data && data.error) ? data.error : `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderInlineMessage(containerEl, badge, message) {
  containerEl.innerHTML = "";
  const row = document.createElement("div");
  row.className = "item";
  row.innerHTML = `
    <div class="item-left">
      <span class="badge">${escapeHtml(badge)}</span>
      <div class="name">${escapeHtml(message)}</div>
    </div>
    <div></div>
  `;
  containerEl.appendChild(row);
}

function updateActivePlayingHighlights() {
  const current = state.currentVideo;

  const tagResults = $("tagResultsList");
  if (tagResults) {
    for (const row of tagResults.querySelectorAll('.item[data-relpath]')) {
      const rp = row.dataset.relpath || "";
      const on = Boolean(current && current.kind === "source" && rp === current.relpath);
      row.classList.toggle("active-playing", on);
    }
  }

  const folderList = $("folderList");
  if (folderList) {
    for (const row of folderList.querySelectorAll('.item[data-relpath]')) {
      const rp = row.dataset.relpath || "";
      const on = Boolean(current && current.kind === "source" && rp === current.relpath);
      row.classList.toggle("active-playing", on);
    }
  }

  const queueList = $("queueList");
  if (queueList) {
    for (const row of queueList.querySelectorAll('.queue-item')) {
      const rp = row.dataset.targetRelpath || "";
      const on = Boolean(current && current.kind === "target" && rp === current.relpath);
      row.classList.toggle("active-playing", on);
    }
  }
}

function renderFolders(listData) {
  const folderList = $("folderList");
  folderList.innerHTML = "";

  const folders = listData.folders || [];
  const videos = listData.videos || [];

  if (listData.parent_path !== null && listData.parent_path !== undefined) {
    const rowUp = document.createElement("div");
    rowUp.className = "item";
    rowUp.innerHTML = `
      <div class="item-left">
        <span class="badge">UP</span>
        <div class="name">..</div>
      </div>
      <div></div>
    `;
    rowUp.addEventListener("dblclick", () => {
      navigateList(listData.parent_path || "");
    });
    folderList.appendChild(rowUp);
  }

  if (folders.length === 0 && videos.length === 0) {
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div class="item-left">
        <span class="badge">INFO</span>
        <div class="name">Keine Inhalte im aktuellen Ordner. Prüfe, ob VIDEO_ROOT korrekt gesetzt ist.</div>
      </div>
      <div></div>
    `;
    folderList.appendChild(row);
    return;
  }

  for (const f of folders) {
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div class="item-left">
        <span class="badge">DIR</span>
        <div class="name">${escapeHtml(f.name)}</div>
      </div>
      <div></div>
    `;
    row.addEventListener("dblclick", () => {
      navigateList(f.relpath);
    });
    folderList.appendChild(row);
  }

  for (const v of videos) {
    const row = document.createElement("div");
    row.className = "item";
    row.draggable = true;
    row.dataset.relpath = v.relpath;
    row.innerHTML = `
      <div class="item-left">
        <span class="badge">VID</span>
        <div class="name">${escapeHtml(v.name)}</div>
      </div>
      <div></div>
    `;

    row.addEventListener("dragstart", (e) => {
      e.dataTransfer.setData("text/plain", v.relpath);
      e.dataTransfer.effectAllowed = "copyMove";
    });

    const nameEl = row.querySelector(".name");
    if (nameEl) {
      nameEl.title = v.name || "";
      nameEl.addEventListener("dblclick", () => {
        playSource(v.relpath);
      });
    }

    row.title = v.relpath || "";

    folderList.appendChild(row);
  }

  updateActivePlayingHighlights();
}

function renderVideosTop(listData) {
  const el = $("videoList");
  el.innerHTML = "";

  const videos = listData.videos || [];

  if (videos.length === 0) {
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div class="item-left">
        <span class="badge">INFO</span>
        <div class="name">Keine Videos in diesem Ordner.</div>
      </div>
      <div></div>
    `;
    el.appendChild(row);
    return;
  }

  for (const v of videos) {
    const row = document.createElement("div");
    row.className = "item";
    row.draggable = true;
    row.dataset.relpath = v.relpath;
    row.innerHTML = `
      <div class="item-left">
        <span class="badge">VID</span>
        <div class="name">${escapeHtml(v.name)}</div>
      </div>
      <div style="display:flex; gap:8px;">
        <button class="btn" type="button">Preview</button>
      </div>
    `;

    row.addEventListener("dragstart", (e) => {
      e.dataTransfer.setData("text/plain", v.relpath);
      e.dataTransfer.effectAllowed = "copyMove";
    });

    row.querySelector("button").addEventListener("click", () => {
      playSource(v.relpath);
    });

    const nameEl = row.querySelector(".name");
    if (nameEl) {
      nameEl.title = v.name || "";
      nameEl.addEventListener("dblclick", () => {
        playSource(v.relpath);
      });
    }

    row.title = v.relpath || "";

    el.appendChild(row);
  }

  updateActivePlayingHighlights();
}

function renderBreadcrumb(currentPath) {
  const el = $("breadcrumb");
  if (!el) return;
  el.textContent = currentPath || "/";
}

function playSource(relpath) {
  const player = $("videoPlayer");
  hidePlayerMsg();
  state.currentVideo = { kind: "source", relpath };
  player.src = `/media/source/${encodePath(relpath)}`;
  player.load();
  player.play().catch(() => {});
  refreshTagEditorForCurrentVideo();
  refreshClipEditorForCurrentVideo();
  updateActivePlayingHighlights();
}

function playTarget(relpath) {
  const player = $("videoPlayer");
  hidePlayerMsg();
  state.currentVideo = { kind: "target", relpath };
  player.src = `/media/target/${encodePath(relpath)}`;
  player.load();
  player.play().catch(() => {});
  refreshTagEditorForCurrentVideo();
  refreshClipEditorForCurrentVideo();
  updateActivePlayingHighlights();
}

function computeQueueInsertIndex(containerEl, clientY) {
  const items = Array.from(containerEl.querySelectorAll(".queue-item"));
  if (items.length === 0) return 0;
  for (let i = 0; i < items.length; i++) {
    const r = items[i].getBoundingClientRect();
    const mid = r.top + r.height / 2;
    if (clientY < mid) return i;
  }
  return items.length;
}

async function saveQueueOrder(orderedIds) {
  await apiPost("/api/queue/reorder", { ordered_ids: orderedIds });
  await loadQueue();
}

function isRelpathAlreadyInQueue(relpath, kind) {
  const norm = String(relpath || "");
  if (norm === "") return false;
  if (kind === "target") {
    return state.queue.some((it) => it && it.target_relpath === norm);
  }
  return false;
}

async function ensureQueueInsertedAt(itemId, insertIndex) {
  await loadQueue();
  const ids = state.queue.map((x) => Number(x.id));
  const idNum = Number(itemId);
  const existingIdx = ids.indexOf(idNum);
  if (existingIdx === -1) return;
  ids.splice(existingIdx, 1);
  const idx = Math.max(0, Math.min(insertIndex, ids.length));
  ids.splice(idx, 0, idNum);
  await saveQueueOrder(ids);
}

async function handleQueueDrop(relpath, insertIndex, kind) {
  const mode = "copy";
  if (kind === "target" && isRelpathAlreadyInQueue(relpath, kind)) {
    setStatus("Bereits in der Queue.", "error");
    return;
  }
  try {
    if (kind === "target") {
      const resp = await apiPost("/api/queue/add", { target_relpath: relpath });
      const item = resp && resp.item;
      if (item && insertIndex !== null && insertIndex !== undefined) {
        await ensureQueueInsertedAt(item.id, insertIndex);
      } else {
        await loadQueue();
      }
      setStatus("In Queue aufgenommen.", "ok");
      return;
    }

    const resp = await apiPost("/api/transfer", {
      source_path: relpath,
      target_subdir: "",
      mode: mode,
    });

    if (resp && resp.ok) {
      const itemId = resp.queue_item && resp.queue_item.id;
      if (itemId && insertIndex !== null && insertIndex !== undefined) {
        await ensureQueueInsertedAt(itemId, insertIndex);
      } else {
        await loadQueue();
      }
      setStatus("In Queue aufgenommen.", "ok");
    } else {
      setStatus("Transfer fehlgeschlagen.", "error");
    }
  } catch (e2) {
    if (e2.status === 409) {
      setStatus("Bereits in der Queue.", "error");
    } else {
      setStatus(e2.message, "error");
    }
  }
}

function showPlayerMsg(text) {
  const el = $("playerMsg");
  if (!el) return;
  el.textContent = text;
  el.classList.add("visible");
}

function hidePlayerMsg() {
  const el = $("playerMsg");
  if (!el) return;
  el.textContent = "";
  el.classList.remove("visible");
}

function setupVideoPlayerDiagnostics() {
  const player = $("videoPlayer");
  if (!player) return;

  player.addEventListener("dragstart", (e) => {
    if (!state.currentVideo || !state.currentVideo.relpath) {
      e.preventDefault();
      return;
    }
    e.dataTransfer.setData("text/plain", state.currentVideo.relpath);
    e.dataTransfer.setData("application/x-video-drag", JSON.stringify(state.currentVideo));
    e.dataTransfer.effectAllowed = "copy";
  });

  player.addEventListener("loadedmetadata", () => {
    if (player.videoWidth === 0 || player.videoHeight === 0) {
      showPlayerMsg(
        "Keine Videospur/Codec nicht unterstützt (Chromium). Teste .mp4 (H.264/AAC) oder installiere passende Codecs."
      );
    }
  });

  player.addEventListener("error", () => {
    const err = player.error;
    const code = err ? err.code : 0;
    showPlayerMsg(`Video kann nicht angezeigt werden (Decoder/MIME/Codec). Fehlercode: ${code}`);
  });
}

async function loadList(path) {
  const rel = path || "";
  try {
    const data = await apiGet(`/api/list?path=${encodeURIComponent(rel)}`);
    state.currentPath = data.current_path || "";
    state.lastList = data;

    renderBreadcrumb(state.currentPath);
    renderFolders(data);

  } catch (e) {
    setStatus(e.message, "error");

    state.lastList = null;
    renderBreadcrumb(state.currentPath || "");
    renderInlineMessage($("folderList"), "ERROR", `Dateibrowser-Fehler: ${e.message}`);
  }
}

function renderQueue(items) {
  state.queue = items || [];
  const el = $("queueList");
  el.innerHTML = "";

  for (const it of state.queue) {
    const row = document.createElement("div");
    row.className = "queue-item";
    row.dataset.id = it.id;
    row.dataset.targetRelpath = it.target_relpath;

    row.innerHTML = `
      <div class="item-left" style="min-width:0;">
        <span class="badge">#${it.position}</span>
        <div class="name">${escapeHtml(it.filename)}</div>
      </div>
      <div class="queue-actions">
        <button class="btn" type="button">Play</button>
        <button class="btn" type="button" data-action="remove">Entfernen</button>
      </div>
    `;

    row.querySelector("button").addEventListener("click", () => {
      playTarget(it.target_relpath);
    });

    row.title = it.target_relpath || "";
    const nm = row.querySelector(".name");
    if (nm) nm.title = it.filename || "";

    row.querySelector('[data-action="remove"]').addEventListener("click", async () => {
      try {
        await apiDelete(`/api/queue/item?id=${encodeURIComponent(it.id)}`);
        await loadQueue();
      } catch (e) {
        setStatus(e.message, "error");
      }
    });

    el.appendChild(row);
  }

  updateActivePlayingHighlights();

  if (!state.sortable) {
    state.sortable = new Sortable(el, {
      animation: 150,
      onEnd: async () => {
        await persistQueueOrder();
      },
    });
  }
}

async function loadQueue() {
  try {
    const data = await apiGet("/api/queue");
    renderQueue(data.items || []);
  } catch (e) {
    setStatus(e.message, "error");
  }
}

async function persistQueueOrder() {
  const el = $("queueList");
  const ids = Array.from(el.querySelectorAll(".queue-item")).map((n) => Number(n.dataset.id));
  try {
    await apiPost("/api/queue/reorder", { ordered_ids: ids });
    await loadQueue();
    setStatus("Reihenfolge gespeichert.", "ok");
  } catch (e) {
    setStatus(e.message, "error");
  }
}

function setupDropZone() {
  const dz = $("dropZone");
  const ql = $("queueList");

  dz.addEventListener("dragover", (e) => {
    e.preventDefault();
    dz.classList.add("dragover");
  });

  dz.addEventListener("dragleave", () => {
    dz.classList.remove("dragover");
  });

  async function onDropToQueue(e) {
    e.preventDefault();
    dz.classList.remove("dragover");

    let kind = "source";
    let relpath = e.dataTransfer.getData("text/plain");
    const payload = e.dataTransfer.getData("application/x-video-drag");
    if (payload) {
      try {
        const obj = JSON.parse(payload);
        if (obj && obj.relpath) {
          relpath = obj.relpath;
          kind = obj.kind || "source";
        }
      } catch {
      }
    }

    if (!relpath) return;
    const insertIndex = (e.currentTarget === ql) ? computeQueueInsertIndex(ql, e.clientY) : null;
    await handleQueueDrop(relpath, insertIndex, kind);
  }

  dz.addEventListener("drop", onDropToQueue);
  ql.addEventListener("dragover", (e) => {
    e.preventDefault();
  });
  ql.addEventListener("drop", onDropToQueue);
}

function renderExportBreadcrumb(p) {
  $("exportBreadcrumb").textContent = p || "/";
}

function renderExportFolders(listData) {
  const el = $("exportFolderList");
  el.innerHTML = "";

  const folders = listData.folders || [];

  if (listData.parent_path !== null && listData.parent_path !== undefined) {
    const rowUp = document.createElement("div");
    rowUp.className = "item";
    rowUp.innerHTML = `
      <div class="item-left">
        <span class="badge">UP</span>
        <div class="name">..</div>
      </div>
      <div></div>
    `;
    rowUp.addEventListener("dblclick", () => {
      loadExportList(listData.parent_path || "");
    });
    el.appendChild(rowUp);
  }

  for (const f of folders) {
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div class="item-left">
        <span class="badge">DIR</span>
        <div class="name">${escapeHtml(f.name)}</div>
      </div>
      <div></div>
    `;

    row.addEventListener("dblclick", () => {
      loadExportList(f.relpath);
    });

    el.appendChild(row);
  }
}

async function loadExportList(path) {
  const rel = path || "";
  try {
    const data = await apiGet(`/api/export/list?path=${encodeURIComponent(rel)}`);
    state.exportPath = data.current_path || "";
    state.exportList = data;
    renderExportBreadcrumb(state.exportPath);
    renderExportFolders(data);

  } catch (e) {
    setStatus(e.message, "error");
  }
}

function setupExportUI() {
  $("exportMkdir").addEventListener("click", async () => {
    const name = $("newExportFolder").value || "";
    if (name.trim() === "") {
      setStatus("Bitte Ordnername eingeben.", "error");
      return;
    }

    try {
      await apiPost("/api/export/mkdir", { parent_path: state.exportPath, folder_name: name.trim() });
      $("newExportFolder").value = "";
      await loadExportList(state.exportPath);
      setStatus("Ordner angelegt.", "ok");
    } catch (e) {
      setStatus(e.message, "error");
    }
  });

  $("exportRun").addEventListener("click", async () => {
    try {
      let resp;
      try {
        resp = await apiPost("/api/export/run", { destination_subdir: state.exportPath, clear_destination: false });
      } catch (e) {
        if (e.status === 409 && e.data && e.data.code === "destination_not_empty") {
          const ok = window.confirm("Export-Zielordner ist nicht leer. Vorher leeren?");
          if (!ok) return;
          resp = await apiPost("/api/export/run", { destination_subdir: state.exportPath, clear_destination: true });
        } else {
          throw e;
        }
      }
      if (resp.skipped_count > 0) {
        setStatus(`Export fertig: ${resp.exported_count} kopiert, ${resp.skipped_count} übersprungen.`, "error");
      } else {
        setStatus(`Export fertig: ${resp.exported_count} kopiert.`, "ok");
      }
    } catch (e) {
      setStatus(e.message, "error");
    }
  });
}

async function main() {
  setupDropZone();
  setupQueueControls();
  setupTagSearchUI();
  setupTagEditorUI();
  setupClipEditorUI();
  setupExportUI();
  setupVideoPlayerDiagnostics();

  const initialPath = getInitialBrowserPath();
  window.history.replaceState({ path: initialPath }, "", urlWithBrowserPath(initialPath));
  window.addEventListener("popstate", (e) => {
    const st = e.state || {};
    const p = (st && typeof st.path === "string") ? st.path : getInitialBrowserPath();
    loadList(p);
  });

  await loadList(initialPath);
  await loadQueue();
  await loadExportList("");
  renderTagResults([]);
  await loadTagIndex({ refresh: false });
  await waitForTagIndexReady({ maxMs: 60000 });
  updateTagSuggestions();
}

main();
