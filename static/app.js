let state = {
  currentPath: "",
  lastList: null,
  lastResultsKind: "tag",
  queue: [],
  sortable: null,
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

function isTypingTarget(el) {
  if (!el) return false;
  const tag = String(el.tagName || "").toUpperCase();
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return true;
  if (el.isContentEditable) return true;
  return false;
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

async function renameSourceVideoFile(relpath, displayName) {
  const rp = String(relpath || "");
  if (!rp) return;
  const newName = String(displayName || "").trim();
  if (!newName) return;

  try {
    const resp = await apiPost("/api/files/rename", { relpath: rp, new_name: newName });
    if (!resp || !resp.ok) {
      setStatus("Umbenennen fehlgeschlagen.", "error");
      return;
    }

    const newRelpath = resp.relpath || rp;
    const newFilename = resp.name || filenameFromRelpath(newRelpath) || newName;

    if (resp.changed && state.currentVideo && state.currentVideo.kind === "source" && state.currentVideo.relpath === rp) {
      state.currentVideo.relpath = newRelpath;
      const player = $("videoPlayer");
      if (player) {
        player.src = `/media/source/${encodePath(newRelpath)}`;
        player.load();
        player.play().catch(() => {});
      }
      refreshTagEditorForCurrentVideo();
      refreshClipEditorForCurrentVideo();
    }

    if (Array.isArray(state.tagResults) && state.tagResults.length > 0) {
      let anyChanged = false;
      for (const r of state.tagResults) {
        if (r && r.relpath === rp) {
          r.relpath = newRelpath;
          r.name = newFilename;
          anyChanged = true;
        }
      }
      if (anyChanged) {
        renderResults(state.lastResultsKind || "tag", state.tagResults);
      }
    }

    await loadList(state.currentPath);
    await loadTagIndex({ refresh: true });
    await waitForTagIndexReady({ maxMs: 60000 });
    updateTagSuggestions();
    updateActivePlayingHighlights();

    setStatus("Datei umbenannt.", "ok");
  } catch (e) {
    setStatus(e.message, "error");
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
    renderMarkerTimeline();
    return;
  }

  setClipEditorHint(state.currentVideo.relpath);
  setClipEditorEnabled(!state.clipBusy);
  renderMarkerList();
  renderSegmentList();
  renderMarkerTimeline();
}

function addMarkerAtTime(t) {
  const tt = Number(t);
  if (!Number.isFinite(tt) || tt < 0) return;
  state.clipMarkers = _sortedUniqueMarkers([...(state.clipMarkers || []), tt]);
  refreshClipEditorForCurrentVideo();
}

function bindFileContextMenu(rowEl, relpath, displayName) {
  if (!rowEl) return;
  const rp = String(relpath || "");
  if (!rp) return;
  const dn = String(displayName || "");

  rowEl.addEventListener("contextmenu", (e) => {
    if (e.defaultPrevented) return;
    e.preventDefault();
    e.stopPropagation();
    _showFileMenuAt(e.clientX, e.clientY, rp, dn);
  });

  rowEl.addEventListener("mousedown", (e) => {
    if (e.button !== 2) return;
    if (e.defaultPrevented) return;
    e.preventDefault();
    e.stopPropagation();
    _showFileMenuAt(e.clientX, e.clientY, rp, dn);
  });
}

function setupGlobalFileContextMenu() {
  const handle = (e) => {
    const t = e.target;
    if (!t || !(t instanceof Element)) return;
    const row = t.closest('.item[data-relpath]');
    if (!row) return;

    const inFolder = row.closest('#folderList');
    const inTags = row.closest('#tagResultsList');
    if (!inFolder && !inTags) return;

    const rp = row.getAttribute('data-relpath') || "";
    if (!rp) return;

    const nameEl = row.querySelector('.name');
    const name = nameEl ? String(nameEl.textContent || "").trim() : filenameFromRelpath(rp);

    e.preventDefault();
    e.stopPropagation();
    _showFileMenuAt(e.clientX, e.clientY, rp, name);
  };

  document.addEventListener(
    "contextmenu",
    (e) => {
      handle(e);
    },
    true
  );

  document.addEventListener(
    "pointerdown",
    (e) => {
      if (e.button !== 2) return;
      handle(e);
    },
    true
  );
}

function setupBrowserListSplitter() {
  const splitter = document.getElementById("browserHSplitter");
  const container = document.querySelector(".browser-lists");
  const tagWrap = document.querySelector(".tag-results-wrap");
  const folderWrap = document.querySelector(".folder-wrap");
  if (!splitter || !container || !tagWrap || !folderWrap) return;

  const storageKey = "browser.tagResultsHeightPx";
  const saved = Number(window.localStorage.getItem(storageKey));
  if (Number.isFinite(saved) && saved > 0) {
    tagWrap.style.flex = `0 0 ${Math.round(saved)}px`;
  }

  let dragging = false;
  let startY = 0;
  let startH = 0;

  const minTag = 120;
  const minFolder = 120;

  const clampAndApply = (newHeightPx) => {
    const containerH = container.getBoundingClientRect().height;
    const splitterH = splitter.getBoundingClientRect().height || 8;
    const maxTag = Math.max(minTag, containerH - splitterH - minFolder);
    const h = Math.max(minTag, Math.min(newHeightPx, maxTag));
    tagWrap.style.flex = `0 0 ${Math.round(h)}px`;
  };

  splitter.addEventListener("dblclick", (e) => {
    e.preventDefault();
    try {
      window.localStorage.removeItem(storageKey);
    } catch (_) {}
    tagWrap.style.flex = "0 0 38%";
  });

  splitter.addEventListener("pointerdown", (e) => {
    dragging = true;
    startY = e.clientY;
    startH = tagWrap.getBoundingClientRect().height;
    try {
      splitter.setPointerCapture(e.pointerId);
    } catch (_) {}
    e.preventDefault();
  });

  splitter.addEventListener("pointermove", (e) => {
    if (!dragging) return;
    const delta = e.clientY - startY;
    clampAndApply(startH + delta);
    e.preventDefault();
  });

  const endDrag = (e) => {
    if (!dragging) return;
    dragging = false;
    try {
      splitter.releasePointerCapture(e.pointerId);
    } catch (_) {}
    const h = tagWrap.getBoundingClientRect().height;
    try {
      window.localStorage.setItem(storageKey, String(Math.round(h)));
    } catch (_) {}
  };

  splitter.addEventListener("pointerup", endDrag);
  splitter.addEventListener("pointercancel", endDrag);

  window.addEventListener("resize", () => {
    const flex = String(tagWrap.style.flex || "");
    const m = flex.match(/0\s+0\s+(\d+)px/);
    if (!m) return;
    const px = Number(m[1]);
    if (!Number.isFinite(px)) return;
    clampAndApply(px);
  });
}

let _segmentMenuEl = null;
let _fileMenuEl = null;
let _renameMenuEl = null;
let _fileMenuSuppressCloseUntil = 0;

function _hideSegmentMenu() {
  if (!_segmentMenuEl) return;
  _segmentMenuEl.style.display = "none";
}

function _hideFileMenu() {
  if (!_fileMenuEl) return;
  _fileMenuEl.style.display = "none";
}

function _hideRenameMenu() {
  if (!_renameMenuEl) return;
  _renameMenuEl.style.display = "none";
}

function _ensureSegmentMenuEl() {
  if (_segmentMenuEl) return _segmentMenuEl;
  const el = document.createElement("div");
  el.id = "segmentContextMenu";
  el.className = "context-menu";
  el.style.display = "none";
  document.body.appendChild(el);
  _segmentMenuEl = el;

  document.addEventListener(
    "click",
    (e) => {
      if (!_segmentMenuEl || _segmentMenuEl.style.display === "none") return;
      if (_segmentMenuEl.contains(e.target)) return;
      _hideSegmentMenu();
    },
    true
  );

  document.addEventListener(
    "keydown",
    (e) => {
      if (e.key === "Escape") {
        _hideSegmentMenu();
      }
    },
    true
  );

  window.addEventListener(
    "scroll",
    () => {
      _hideSegmentMenu();
    },
    true
  );

  window.addEventListener(
    "resize",
    () => {
      _hideSegmentMenu();
    },
    true
  );

  return el;
}

function _ensureRenameMenuEl() {
  if (_renameMenuEl) return _renameMenuEl;
  const el = document.createElement("div");
  el.id = "renameContextMenu";
  el.className = "context-menu";
  el.style.display = "none";
  document.body.appendChild(el);
  _renameMenuEl = el;

  document.addEventListener(
    "click",
    (e) => {
      if (!_renameMenuEl || _renameMenuEl.style.display === "none") return;
      if (_renameMenuEl.contains(e.target)) return;
      _hideRenameMenu();
    },
    true
  );

  document.addEventListener(
    "keydown",
    (e) => {
      if (e.key === "Escape") {
        _hideRenameMenu();
      }
    },
    true
  );

  window.addEventListener(
    "scroll",
    () => {
      _hideRenameMenu();
    },
    true
  );

  window.addEventListener(
    "resize",
    () => {
      _hideRenameMenu();
    },
    true
  );

  return el;
}

function _placeMenuAt(el, clientX, clientY) {
  if (!el) return;
  el.style.left = `${clientX}px`;
  el.style.top = `${clientY}px`;
  el.style.display = "block";

  window.requestAnimationFrame(() => {
    if (el.style.display === "none") return;
    const w = el.offsetWidth || 0;
    const h = el.offsetHeight || 0;
    const pad = 8;
    const maxX = Math.max(pad, window.innerWidth - w - pad);
    const maxY = Math.max(pad, window.innerHeight - h - pad);
    const x = Math.max(pad, Math.min(clientX, maxX));
    const y = Math.max(pad, Math.min(clientY, maxY));
    el.style.left = `${x}px`;
    el.style.top = `${y}px`;
  });
}

function _ensureFileMenuEl() {
  if (_fileMenuEl) return _fileMenuEl;
  const el = document.createElement("div");
  el.id = "fileContextMenu";
  el.className = "context-menu";
  el.style.display = "none";
  document.body.appendChild(el);
  _fileMenuEl = el;

  el.addEventListener("mousedown", (e) => {
    e.stopPropagation();
  });

  el.addEventListener("click", (e) => {
    e.stopPropagation();
  });

  document.addEventListener(
    "click",
    (e) => {
      if (!_fileMenuEl || _fileMenuEl.style.display === "none") return;
      if (Date.now() < _fileMenuSuppressCloseUntil) return;
      if (_fileMenuEl.contains(e.target)) return;
      _hideFileMenu();
    },
    true
  );

  document.addEventListener(
    "keydown",
    (e) => {
      if (e.key === "Escape") {
        _hideFileMenu();
      }
    },
    true
  );

  window.addEventListener(
    "scroll",
    () => {
      _hideFileMenu();
    },
    true
  );

  window.addEventListener(
    "resize",
    () => {
      _hideFileMenu();
    },
    true
  );

  return el;
}

function _showFileMenuAt(clientX, clientY, relpath, displayName) {
  const rp = String(relpath || "");
  if (!rp) return;

  _hideRenameMenu();
  _hideSegmentMenu();

  _fileMenuSuppressCloseUntil = Date.now() + 250;

  const el = _ensureFileMenuEl();
  const label = String(displayName || filenameFromRelpath(rp) || rp);
  el.innerHTML = "";

  const title = document.createElement("div");
  title.className = "context-menu-title";
  title.textContent = label;
  el.appendChild(title);

  const btnRename = document.createElement("button");
  btnRename.type = "button";
  btnRename.className = "context-menu-item";
  btnRename.textContent = "Umbenennen";
  const doRename = (e) => {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    const input = window.prompt("Neuer Dateiname (ohne Pfad):", label);
    if (input === null) return;
    const newName = String(input || "").trim();
    if (!newName) return;
    _hideFileMenu();
    renameSourceVideoFile(rp, newName);
  };
  btnRename.addEventListener("mousedown", (e) => {
    if (e.button !== 0) return;
    doRename(e);
  });
  btnRename.addEventListener("click", doRename);
  el.appendChild(btnRename);

  const btnDelete = document.createElement("button");
  btnDelete.type = "button";
  btnDelete.className = "context-menu-item";
  btnDelete.textContent = "Löschen";
  const doDelete = async (e) => {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    _hideFileMenu();
    await deleteSourceVideoFile(rp, label);
  };
  btnDelete.addEventListener("mousedown", (e) => {
    if (e.button !== 0) return;
    doDelete(e);
  });
  btnDelete.addEventListener("click", doDelete);
  el.appendChild(btnDelete);

  _placeMenuAt(el, clientX, clientY);
}

function _renderFileRenameInline(menuEl, clientX, clientY, relpath, displayName) {
  const rp = String(relpath || "");
  if (!rp) return;
  const el = menuEl || _ensureFileMenuEl();
  const currentName = String(displayName || filenameFromRelpath(rp) || rp);

  _fileMenuSuppressCloseUntil = Date.now() + 250;

  el.innerHTML = "";

  const title = document.createElement("div");
  title.className = "context-menu-title";
  title.textContent = "Neuer Dateiname";
  el.appendChild(title);

  const input = document.createElement("input");
  input.type = "text";
  input.id = "renameInput";
  input.name = "rename";
  input.autocomplete = "off";
  input.className = "input";
  input.value = currentName;
  input.style.width = "280px";
  el.appendChild(input);

  const btnOk = document.createElement("button");
  btnOk.type = "button";
  btnOk.className = "context-menu-item";
  btnOk.textContent = "OK";
  const doOk = async (e) => {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    const newName = String(input.value || "").trim();
    if (!newName) return;
    _hideFileMenu();
    await renameSourceVideoFile(rp, newName);
  };
  btnOk.addEventListener("mousedown", (e) => {
    if (e.button !== 0) return;
    doOk(e);
  });
  btnOk.addEventListener("click", doOk);
  el.appendChild(btnOk);

  const btnCancel = document.createElement("button");
  btnCancel.type = "button";
  btnCancel.className = "context-menu-item";
  btnCancel.textContent = "Abbrechen";
  const doCancel = (e) => {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    _hideFileMenu();
  };
  btnCancel.addEventListener("mousedown", (e) => {
    if (e.button !== 0) return;
    doCancel(e);
  });
  btnCancel.addEventListener("click", doCancel);
  el.appendChild(btnCancel);

  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      btnOk.click();
    } else if (e.key === "Escape") {
      _hideFileMenu();
    }
  });

  _placeMenuAt(el, clientX, clientY);
  window.requestAnimationFrame(() => {
    try {
      input.focus();
      input.select();
    } catch (_) {}
  });
}

function _showRenameMenuAt(clientX, clientY, relpath, displayName) {
  const rp = String(relpath || "");
  if (!rp) return;
  const el = _ensureRenameMenuEl();
  const currentName = String(displayName || filenameFromRelpath(rp) || rp);

  el.innerHTML = "";
  const title = document.createElement("div");
  title.className = "context-menu-title";
  title.textContent = "Neuer Dateiname";
  el.appendChild(title);

  const input = document.createElement("input");
  input.type = "text";
  input.id = "renameInput";
  input.name = "rename";
  input.autocomplete = "off";
  input.className = "input";
  input.value = currentName;
  input.style.width = "280px";
  el.appendChild(input);

  const btnOk = document.createElement("button");
  btnOk.type = "button";
  btnOk.className = "context-menu-item";
  btnOk.textContent = "OK";
  btnOk.addEventListener("click", async () => {
    const newName = String(input.value || "").trim();
    if (!newName) return;
    _hideRenameMenu();
    await renameSourceVideoFile(rp, newName);
  });
  el.appendChild(btnOk);

  const btnCancel = document.createElement("button");
  btnCancel.type = "button";
  btnCancel.className = "context-menu-item";
  btnCancel.textContent = "Abbrechen";
  btnCancel.addEventListener("click", () => {
    _hideRenameMenu();
  });
  el.appendChild(btnCancel);

  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      btnOk.click();
    } else if (e.key === "Escape") {
      _hideRenameMenu();
    }
  });

  _placeMenuAt(el, clientX, clientY);
  window.requestAnimationFrame(() => {
    try {
      input.focus();
      input.select();
    } catch (_) {}
  });
}

function _deleteSegmentByEndMarker(endTime) {
  const end = Number(endTime);
  if (!Number.isFinite(end)) return;
  state.clipMarkers = (state.clipMarkers || []).filter((m) => Math.abs(Number(m) - end) >= 0.05);
  refreshClipEditorForCurrentVideo();
}

function _showSegmentMenuAt(clientX, clientY, segment) {
  const player = $("videoPlayer");
  if (!player) return;
  if (!segment || !Number.isFinite(segment.start) || !Number.isFinite(segment.end)) return;

  const el = _ensureSegmentMenuEl();
  const label = `${formatTime(segment.start)} → ${formatTime(segment.end)}`;
  el.innerHTML = "";

  const title = document.createElement("div");
  title.className = "context-menu-title";
  title.textContent = label;
  el.appendChild(title);

  const btnCreate = document.createElement("button");
  btnCreate.type = "button";
  btnCreate.className = "context-menu-item";
  btnCreate.textContent = "Segment erstellen";
  btnCreate.addEventListener("click", async () => {
    _hideSegmentMenu();
    await createClipsForSegments([{ start: segment.start, end: segment.end }]);
  });
  el.appendChild(btnCreate);

  const btnDelete = document.createElement("button");
  btnDelete.type = "button";
  btnDelete.className = "context-menu-item";
  btnDelete.textContent = "Segment löschen";
  btnDelete.addEventListener("click", () => {
    _hideSegmentMenu();
    _deleteSegmentByEndMarker(segment.end);
  });
  el.appendChild(btnDelete);

  el.style.display = "block";

  const padding = 8;
  const w = el.offsetWidth || 200;
  const h = el.offsetHeight || 90;
  let x = clientX;
  let y = clientY;
  if (x + w + padding > window.innerWidth) x = window.innerWidth - w - padding;
  if (y + h + padding > window.innerHeight) y = window.innerHeight - h - padding;
  x = Math.max(padding, x);
  y = Math.max(padding, y);
  el.style.left = `${x}px`;
  el.style.top = `${y}px`;
}

function renderMarkerTimeline() {
  const tl = $("markerTimeline");
  const player = $("videoPlayer");
  if (!tl || !player) return;

  const duration = Number(player.duration);
  tl.innerHTML = "";

  if (!state.currentVideo || state.currentVideo.kind !== "source") {
    const hint = document.createElement("div");
    hint.className = "hint";
    hint.textContent = "Kein Video gewählt.";
    tl.appendChild(hint);
    return;
  }

  if (!Number.isFinite(duration) || duration <= 0) {
    const hint = document.createElement("div");
    hint.className = "hint";
    hint.textContent = "Lade Metadaten…";
    tl.appendChild(hint);
    return;
  }

  const segs = computeSegmentsFromMarkers(state.clipMarkers || []);
  const colors = [
    "rgba(74, 222, 128, 0.55)",
    "rgba(110, 168, 254, 0.55)",
    "rgba(255, 107, 107, 0.55)",
    "rgba(246, 193, 71, 0.55)",
  ];

  for (let i = 0; i < segs.length; i++) {
    const s = segs[i];
    const left = (s.start / duration) * 100;
    const width = ((s.end - s.start) / duration) * 100;
    const seg = document.createElement("div");
    seg.className = "seg";
    seg.style.left = `${left}%`;
    seg.style.width = `${width}%`;
    seg.style.background = colors[i % colors.length];
    const label = `${formatTime(s.start)} → ${formatTime(s.end)}`;
    seg.title = label;
    const span = document.createElement("span");
    span.className = "seg-label";
    span.textContent = label;
    seg.appendChild(span);

    seg.addEventListener("contextmenu", (e) => {
      e.preventDefault();
      e.stopPropagation();
      _showSegmentMenuAt(e.clientX, e.clientY, s);
    });

    tl.appendChild(seg);
  }

  const markers = _sortedUniqueMarkers(state.clipMarkers || []);
  for (const t of markers) {
    const x = (t / duration) * 100;
    const tick = document.createElement("div");
    tick.className = "tick";
    tick.style.left = `calc(${x}% - 1px)`;
    tick.title = `Marker ${formatTime(t)} (Alt+Klick: löschen)`;
    tick.dataset.t = String(t);

    tick.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      const tt = Number(tick.dataset.t);
      if (!Number.isFinite(tt)) return;

      if (e.altKey) {
        state.clipMarkers = (state.clipMarkers || []).filter((m) => Math.abs(Number(m) - tt) >= 0.05);
        refreshClipEditorForCurrentVideo();
        return;
      }

      player.currentTime = tt;
      player.play().catch(() => {});
      renderMarkerTimeline();
    });

    tl.appendChild(tick);
  }

  const playhead = document.createElement("div");
  playhead.className = "playhead";
  const ct = Math.max(0, Math.min(duration, Number(player.currentTime || 0)));
  playhead.style.left = `calc(${(ct / duration) * 100}% - 1px)`;
  tl.appendChild(playhead);

  tl.title = "Klick: springen | Shift+Klick: Marker bei aktueller Zeit | Alt+Klick Marker: löschen";
}

function setupMarkerTimelineUI() {
  const tl = $("markerTimeline");
  const player = $("videoPlayer");
  if (!tl || !player) return;

  tl.addEventListener("click", (e) => {
    if (!state.currentVideo || state.currentVideo.kind !== "source") return;

    const duration = Number(player.duration);
    if (!Number.isFinite(duration) || duration <= 0) return;

    if (e.shiftKey) {
      addMarkerAtTime(Number(player.currentTime || 0));
      return;
    }

    const r = tl.getBoundingClientRect();
    const x = Math.max(0, Math.min(r.width, e.clientX - r.left));
    const t = (x / r.width) * duration;

    player.currentTime = t;
    player.play().catch(() => {});
    renderMarkerTimeline();
  });

  player.addEventListener("timeupdate", () => {
    renderMarkerTimeline();
  });

  player.addEventListener("loadedmetadata", () => {
    renderMarkerTimeline();
  });
}

function setupMarkerShortcut() {
  document.addEventListener("keydown", (e) => {
    if (e.key !== "m" && e.key !== "M") return;
    if (e.ctrlKey || e.altKey || e.metaKey) return;
    if (isTypingTarget(e.target)) return;
    if (!state.currentVideo || state.currentVideo.kind !== "source") return;

    const player = $("videoPlayer");
    if (!player) return;
    e.preventDefault();
    addMarkerAtTime(Number(player.currentTime || 0));
  });
}

function setupVideoShiftClickMarker() {
  const player = $("videoPlayer");
  if (!player) return;

  player.addEventListener("pointerdown", (e) => {
    if (!e.shiftKey) return;
    if (e.button !== 0) return;
    if (!state.currentVideo || state.currentVideo.kind !== "source") return;
    if (isTypingTarget(e.target)) return;

    e.preventDefault();
    e.stopPropagation();
    addMarkerAtTime(Number(player.currentTime || 0));
  });
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

  const items = [...(tags || [])].sort((a, b) => {
    const aa = String((a && a.tag) || "").toLowerCase();
    const bb = String((b && b.tag) || "").toLowerCase();
    return aa.localeCompare(bb, "de");
  });
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
      <div class="mono" style="font-size:11px; color: var(--muted); max-width: 55%; overflow:hidden; text-overflow: ellipsis; white-space: nowrap;">
        ${escapeHtml(r.relpath)}
      </div>
    `;

    row.addEventListener("dragstart", (e) => {
      e.dataTransfer.setData("text/plain", r.relpath);
      e.dataTransfer.setData("application/x-video-drag", JSON.stringify({ kind: "source", relpath: r.relpath }));
      e.dataTransfer.effectAllowed = "copyMove";
    });

    bindFileContextMenu(row, r.relpath, r.name);

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
      addMarkerAtTime(t);
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

function setMergeProgress({ visible, pct = 0, text = "" }) {
  const wrap = $("mergeProgressWrap");
  const bar = $("mergeProgressBar");
  const t = $("mergeProgressText");
  if (wrap) wrap.style.display = visible ? "block" : "none";
  if (bar) bar.style.width = `${Math.max(0, Math.min(100, Number(pct) || 0))}%`;
  if (t) t.textContent = text || "";
}

async function startMergeDownload() {
  const btn = $("mergeDownloadBtn");
  if (btn) btn.disabled = true;
  setMergeProgress({ visible: true, pct: 0, text: "Starte Merge…" });

  try {
    const resp = await apiPost("/api/merge/start", {});
    const jobId = resp && resp.job_id;
    if (!jobId) {
      throw new Error("Merge-Job konnte nicht gestartet werden.");
    }

    const start = Date.now();
    while (true) {
      const st = await apiGet(`/api/merge/status?job_id=${encodeURIComponent(jobId)}`);
      const pct = Number(st.progress_pct || 0);
      const phase = st.phase || "";
      const msg = st.message || "";
      setMergeProgress({ visible: true, pct, text: `${phase}${msg ? ": " + msg : ""}` });

      if (st.status === "done") {
        if (st.download_ready) {
          setMergeProgress({ visible: true, pct: 100, text: "Fertig. Download startet…" });
          window.location.href = `/api/merge/download/${encodeURIComponent(jobId)}`;
          break;
        }
        setMergeProgress({ visible: true, pct: 99, text: "Finalisiere Ausgabe…" });
      }
      if (st.status === "error") {
        throw new Error(st.error || "Merge fehlgeschlagen.");
      }
      if (Date.now() - start > 1000 * 60 * 60) {
        throw new Error("Timeout beim Merge.");
      }
      await new Promise((r) => setTimeout(r, 700));
    }
  } finally {
    if (btn) btn.disabled = false;
    window.setTimeout(() => setMergeProgress({ visible: false, pct: 0, text: "" }), 2500);
  }
}

function setupMergeDownloadUI() {
  const btn = $("mergeDownloadBtn");
  if (!btn) return;
  btn.addEventListener("click", async () => {
    try {
      await loadQueue();
      if (!state.queue || state.queue.length === 0) {
        setStatus("Queue ist leer.", "error");
        return;
      }
      if (!window.confirm(`Alle Videos in der Queue (${state.queue.length}) zu einem Video mergen und downloaden?`)) {
        return;
      }
      await startMergeDownload();
    } catch (e) {
      setStatus(e.message, "error");
      setMergeProgress({ visible: false, pct: 0, text: "" });
      if (btn) btn.disabled = false;
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

async function deleteSourceVideoFile(relpath, displayName) {
  const rp = String(relpath || "");
  if (!rp) return;
  const name = String(displayName || filenameFromRelpath(rp) || rp);
  const ok = window.confirm(`Datei wirklich löschen?\n\n${name}`);
  if (!ok) return;

  try {
    await apiPost("/api/files/delete", { relpath: rp });

    if (state.currentVideo && state.currentVideo.kind === "source" && state.currentVideo.relpath === rp) {
      const player = $("videoPlayer");
      if (player) {
        try {
          player.pause();
        } catch (_) {}
        player.removeAttribute("src");
        player.load();
      }
      state.currentVideo = null;
      refreshTagEditorForCurrentVideo();
      refreshClipEditorForCurrentVideo();
    }

    if (Array.isArray(state.tagResults) && state.tagResults.length > 0) {
      const before = state.tagResults.length;
      state.tagResults = state.tagResults.filter((r) => r && r.relpath !== rp);
      if (state.tagResults.length !== before) {
        renderResults(state.lastResultsKind || "tag", state.tagResults);
      }
    }

    await loadList(state.currentPath);
    await loadTagIndex({ refresh: true });
    await waitForTagIndexReady({ maxMs: 60000 });
    updateTagSuggestions();
    updateActivePlayingHighlights();

    setStatus("Datei gelöscht.", "ok");
  } catch (e) {
    setStatus(e.message, "error");
  }
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

    bindFileContextMenu(row, v.relpath, v.name);

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

async function main() {
  setupGlobalFileContextMenu();
  setupBrowserListSplitter();
  setupDropZone();
  setupQueueControls();
  setupMergeDownloadUI();
  setupTagSearchUI();
  setupTagEditorUI();
  setupClipEditorUI();
  setupMarkerTimelineUI();
  setupMarkerShortcut();
  setupVideoShiftClickMarker();
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
  renderTagResults([]);
  await loadTagIndex({ refresh: false });
  await waitForTagIndexReady({ maxMs: 60000 });
  updateTagSuggestions();
}

main();
