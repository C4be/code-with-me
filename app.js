let isPermissionGranted = async () => false;
let requestPermission = async () => "denied";
let sendNotification = () => {};
const notificationsReady = window.__TAURI__
  ? import("@tauri-apps/plugin-notification").then((plugin) => {
    ({ isPermissionGranted, requestPermission, sendNotification } = plugin);
  }).catch(() => {})
  : Promise.resolve();

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));
const CODE_EXTENSIONS = new Set(["py", "go", "cpp", "cc", "cxx", "java"]);
const LANGUAGE_EXTENSIONS = { python: "py", go: "go", cpp: "cpp", java: "java" };
const LANGUAGE_NAMES = { py: "Python", go: "Go", cpp: "C++", cc: "C++", cxx: "C++", java: "Java" };
const CURSOR_COLORS = ["#6d59d9", "#d85b83", "#1a8f7a", "#c5782b", "#3d7bc8", "#aa5ca8"];
const INSTALL_URLS = { python: "https://www.python.org/downloads/", go: "https://go.dev/dl/", cpp: "https://clang.llvm.org/get_started.html", java: "https://adoptium.net/temurin/releases/" };
const EDITOR_FONT_SIZES = [10, 11, 12, 13, 14, 16, 18, 20, 22, 24];

const initialTask = `---
type: challenge
title: Пара с заданной суммой
theme: Массивы
subtopic: Два указателя
category: Массивы · Два указателя
difficulty: medium
time_limit: 25
language: python
entrypoint: 0.solution-0.py
---

Дан **отсортированный** массив целых чисел \`numbers\`. Найдите два числа, сумма которых равна \`target\`.

Верните **индексы** этих чисел (нумерация с 1) в виде массива \`[i, j]\`, где \`i < j\`.

### Примеры

- \`numbers = [2, 7, 11, 15], target = 9\` → \`[1, 2]\`
- \`numbers = [2, 3, 4], target = 6\` → \`[1, 3]\`

### Подсказка

Используйте два указателя: один в начале массива, другой в конце.`;

const initialSolution = `def two_sum(numbers, target):
    left = 0
    right = len(numbers) - 1

    while left < right:
        total = numbers[left] + numbers[right]
        if total == target:
            return [left + 1, right + 1]
        if total < target:
            left += 1
        else:
            right -= 1


if __name__ == "__main__":
    print(two_sum([2, 7, 11, 15], 9))
`;

let files = { "task.cwm.md": initialTask, "0.solution-0.py": initialSolution };
let currentTaskFile = "task.cwm.md";
let activeFile = "0.solution-0.py";
let taskMode = "preview";
let pendingRoomInfo = null;
let renameTarget = null;
let deleteTarget = null;
let toastTimer = 0;
let editorFontSize = Number(localStorage.getItem("code-with-me-editor-font-size")) || 12;
const saveTimers = new Map();
const presence = new Map();
const roomState = {
  base: "",
  code: "",
  inviteUrl: "",
  host: false,
  local: true,
  socket: null,
  participantId: "",
  selfName: "",
  participants: [],
};

const codeInput = $("#code-input");
const output = $("#output-text");

function escapeHTML(value = "") {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function showToast(message, duration = 2400) {
  const toast = $("#toast");
  toast.textContent = message;
  toast.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.remove("show"), duration);
}

function updateThemeControl() {
  const isDark = document.documentElement.dataset.theme === "dark";
  const control = $("#theme-toggle");
  control.setAttribute("aria-checked", String(isDark));
  control.setAttribute("aria-label", isDark ? "Тёмная тема включена" : "Светлая тема включена");
}

function savedName() {
  return localStorage.getItem("code-with-me-name") || "Участник";
}

function setProfileLabel(name) {
  $("#profile-name").textContent = name;
  $("#profile-button").title = `Изменить имя (${name})`;
}

function applyEditorFontSize() {
  document.documentElement.style.setProperty("--editor-font-size", `${editorFontSize}px`);
  $("#font-size-value").textContent = `${editorFontSize} px`;
  localStorage.setItem("code-with-me-editor-font-size", String(editorFontSize));
  refreshEditor(true);
}

function editorLineHeight() {
  return Number.parseFloat(getComputedStyle(codeInput).lineHeight) || editorFontSize * 1.75;
}

function toggleTheme() {
  const nextTheme = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = nextTheme;
  localStorage.setItem("code-with-me-theme", nextTheme);
  updateThemeControl();
}

function transformCursorOffset(previous, next, offset) {
  let prefix = 0;
  const sharedLength = Math.min(previous.length, next.length);
  while (prefix < sharedLength && previous[prefix] === next[prefix]) prefix += 1;

  let suffix = 0;
  while (
    suffix < previous.length - prefix
    && suffix < next.length - prefix
    && previous[previous.length - suffix - 1] === next[next.length - suffix - 1]
  ) suffix += 1;

  const previousChangedEnd = previous.length - suffix;
  const nextChangedEnd = next.length - suffix;
  if (offset < prefix) return offset;
  if (offset >= previousChangedEnd) return Math.max(0, offset + nextChangedEnd - previousChangedEnd);
  return prefix + Math.min(offset - prefix, Math.max(0, nextChangedEnd - prefix));
}

function syncEditorFromRemote(content) {
  const previous = codeInput.value;
  const start = transformCursorOffset(previous, content, codeInput.selectionStart);
  const end = transformCursorOffset(previous, content, codeInput.selectionEnd);
  const direction = codeInput.selectionDirection;
  codeInput.value = content;
  $("#highlight-code").innerHTML = `${highlight(content)}\n`;
  const lineCount = Math.max(1, content.split("\n").length);
  $("#line-numbers").innerHTML = Array.from({ length: lineCount }, (_, index) => index + 1).join("<br>");
  codeInput.style.height = `${Math.max($("#code-wrap").clientHeight - 35, lineCount * editorLineHeight())}px`;
  codeInput.setSelectionRange(Math.min(start, content.length), Math.min(end, content.length), direction);
  updateCursorStatus();
  renderRemoteCursors();
  sendPresence();
}

async function notifyDisconnected() {
  const message = "Вас отключили от комнаты. Не забудьте сохранить проект.";
  showToast(message, 6500);
  if (!window.__TAURI__?.core?.invoke) return;
  try {
    await notificationsReady;
    let granted = await isPermissionGranted();
    if (!granted) granted = (await requestPermission()) === "granted";
    if (granted) sendNotification({ title: "Code with me", body: message });
  } catch (error) {
    console.warn("Не удалось показать системное уведомление:", error);
  }
}

function setEditorFullscreen(fullscreen) {
  const panel = $(".editor-panel");
  const button = $("#editor-fullscreen-button");
  panel.classList.toggle("is-fullscreen", fullscreen);
  button.setAttribute("aria-pressed", String(fullscreen));
  button.setAttribute("aria-label", fullscreen ? "Выйти из полноэкранного редактора" : "Развернуть редактор на весь экран");
  button.title = fullscreen ? "Выйти из полноэкранного редактора (Esc)" : "Развернуть редактор на весь экран";
  button.textContent = fullscreen ? "⤢" : "⛶";
  requestAnimationFrame(() => {
    refreshEditor(true);
    if (fullscreen) codeInput.focus();
  });
}

function taskIndexFromPath(path) {
  if (path === "task.cwm.md") return 0;
  const match = path.match(/^task-(\d+)\.cwm\.md$/);
  return match ? Number(match[1]) : null;
}

function taskPathForIndex(index) {
  return index === 0 ? "task.cwm.md" : `task-${index}.cwm.md`;
}

function taskFiles() {
  return Object.keys(files)
    .map((path) => ({ path, index: taskIndexFromPath(path) }))
    .filter((item) => item.index !== null)
    .sort((a, b) => a.index - b.index);
}

function extensionOf(path) {
  return path.split(".").pop().toLowerCase();
}

function isCodeFile(path) {
  return CODE_EXTENSIONS.has(extensionOf(path));
}

function parseChallenge(source) {
  const match = String(source || "").match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/);
  if (!match) {
    return {
      type: "challenge",
      title: "Задание без названия",
      theme: "Практика",
      subtopic: "",
      category: "Практика",
      difficulty: "medium",
      time_limit: "20",
      language: "python",
      entrypoint: "",
      body: String(source || ""),
    };
  }
  const metadata = {};
  for (const line of match[1].split(/\r?\n/)) {
    const pair = line.match(/^([\w-]+):\s*(.*)$/);
    if (pair) metadata[pair[1]] = pair[2].trim();
  }
  if (!metadata.theme && metadata.category) {
    const [theme, ...rest] = metadata.category.split("·");
    metadata.theme = theme.trim();
    metadata.subtopic = rest.join("·").trim();
  }
  return { ...metadata, body: match[2].trim() };
}

function composeChallenge(challenge) {
  const theme = challenge.theme?.trim() || "Практика";
  const subtopic = challenge.subtopic?.trim() || "";
  const category = [theme, subtopic].filter(Boolean).join(" · ");
  return `---
type: challenge
title: ${challenge.title?.trim() || "Новое задание"}
theme: ${theme}
subtopic: ${subtopic}
category: ${category}
difficulty: ${challenge.difficulty || "medium"}
time_limit: ${challenge.time_limit || "20"}
language: ${challenge.language || "python"}
entrypoint: ${challenge.entrypoint || ""}
---

${challenge.body?.trim() || "Опишите условие задания."}
`;
}

function solutions() {
  const result = [];
  const seen = new Set();
  for (const path of Object.keys(files)) {
    if (!isCodeFile(path)) continue;
    const match = path.match(/^(\d+)\.([^.]+)\.(py|go|cpp|cc|cxx|java)$/i);
    if (!match) continue;
    result.push({ path, task: Number(match[1]), label: match[2], extension: match[3].toLowerCase() });
    seen.add(path);
  }
  for (const task of taskFiles()) {
    const challenge = parseChallenge(files[task.path]);
    const entrypoint = challenge.entrypoint;
    if (entrypoint && files[entrypoint] !== undefined && isCodeFile(entrypoint) && !seen.has(entrypoint)) {
      result.push({ path: entrypoint, task: task.index, label: "solution-0", extension: extensionOf(entrypoint) });
      seen.add(entrypoint);
    }
  }
  if (!result.some((item) => item.task === 0) && files["main.py"] !== undefined) {
    result.push({ path: "main.py", task: 0, label: "solution-0", extension: "py" });
  }
  return result.sort((a, b) => a.task - b.task || a.label.localeCompare(b.label, "ru", { numeric: true }));
}

function solutionForPath(path) {
  return solutions().find((item) => item.path === path) || null;
}

function solutionDisplay(solution) {
  return solution ? `${solution.task}.${solution.label}` : "Нет решения";
}

function currentTaskIndex() {
  return taskIndexFromPath(currentTaskFile) ?? 0;
}

function colorForId(id) {
  let hash = 0;
  for (const character of id || "") hash = (hash * 31 + character.charCodeAt(0)) >>> 0;
  return CURSOR_COLORS[hash % CURSOR_COLORS.length];
}

function initials(name) {
  return Array.from((name || "?").trim())[0]?.toUpperCase() || "?";
}

function peopleForPath(path) {
  return roomState.participants.filter((person) => {
    if (person.id === roomState.participantId) return activeFile === path;
    return presence.get(person.id)?.file === path;
  });
}

function renderRoomParticipants(participants = roomState.participants) {
  roomState.participants = participants;
  const liveIds = new Set(participants.map((person) => person.id));
  for (const id of presence.keys()) if (!liveIds.has(id)) presence.delete(id);
  const container = $("#participants");
  container.innerHTML = participants
    .map((person) => `<span class="avatar" style="background:${colorForId(person.id)};color:#fff" title="${escapeHTML(person.name)}${person.host ? " · ведущий" : ""}">${escapeHTML(initials(person.name))}</span>`)
    .join("");
  container.insertAdjacentHTML("beforeend", `<span class="people-count">${participants.length} / 10</span>`);
  renderSolutionTabs();
  renderRemoteCursors();
}

function renderTaskRail() {
  const rail = $("#task-rail");
  const add = $("#add-task-button");
  rail.querySelectorAll(".task-index").forEach((element) => element.remove());
  for (const task of taskFiles()) {
    const challenge = parseChallenge(files[task.path]);
    const button = document.createElement("button");
    button.className = `task-index${task.path === currentTaskFile ? " active" : ""}`;
    button.dataset.taskFile = task.path;
    button.title = challenge.title || `Задание ${task.index}`;
    button.textContent = String(task.index);
    rail.insertBefore(button, add);
  }
}

function renderSolutionTabs() {
  const items = solutions().filter((solution) => solution.task === currentTaskIndex());
  const container = $("#solution-tabs");
  container.innerHTML = "";
  for (const solution of items) {
    const tab = document.createElement("button");
    tab.className = `solution-tab${solution.path === activeFile ? " active" : ""}`;
    tab.dataset.solutionPath = solution.path;
    const people = peopleForPath(solution.path);
    const badges = people
      .slice(0, 4)
      .map((person) => `<span class="tab-person" style="background:${colorForId(person.id)}" title="${escapeHTML(person.name)}">${escapeHTML(initials(person.name))}</span>`)
      .join("");
    tab.innerHTML = `<span class="solution-tab-name">${escapeHTML(solutionDisplay(solution))}</span><span class="tab-people">${badges}</span><span class="solution-tab-tools"><span class="rename-tab" role="button" title="Переименовать" aria-label="Переименовать">✎</span><span class="delete-tab" role="button" title="Удалить решение" aria-label="Удалить решение">×</span></span>`;
    container.append(tab);
  }
  const add = document.createElement("button");
  add.className = "add-solution";
  add.id = "add-solution-button";
  add.textContent = "+ Решение";
  container.append(add);
  const taskSolutions = items.filter((item) => item.task === currentTaskIndex()).length;
  $("#solution-count").textContent = `${taskSolutions} ${taskSolutions === 1 ? "РЕШЕНИЕ" : "РЕШЕНИЯ"}`;
  renderActiveEditors();
  const selected = solutionForPath(activeFile);
  $("#code-language-select").value = selected?.extension || "py";
  $("#code-language-select").disabled = !selected;
  $("#delete-task-button").disabled = taskFiles().length <= 1;
}

function renderActiveEditors() {
  const people = peopleForPath(activeFile);
  const container = $("#active-editors");
  if (!people.length) {
    container.textContent = roomState.code ? "На этой вкладке пока никого" : "Локальное редактирование";
    return;
  }
  container.innerHTML = people
    .map((person) => `<span class="tab-person" style="background:${colorForId(person.id)}">${escapeHTML(initials(person.name))}</span><span>${escapeHTML(person.name)}</span>`)
    .join("");
}

function inlineMarkdown(value) {
  return escapeHTML(value)
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/\*([^*]+)\*/g, "<em>$1</em>");
}

function markdownToHTML(markdown) {
  const lines = String(markdown || "").split(/\r?\n/);
  let html = "";
  let paragraph = [];
  let list = "";
  let inCode = false;
  let code = [];
  const flushParagraph = () => {
    if (paragraph.length) html += `<p>${inlineMarkdown(paragraph.join(" "))}</p>`;
    paragraph = [];
  };
  const closeList = () => {
    if (list) html += `</${list}>`;
    list = "";
  };
  for (const line of lines) {
    if (line.trim().startsWith("```")) {
      flushParagraph();
      closeList();
      if (inCode) {
        html += `<pre><code>${escapeHTML(code.join("\n"))}</code></pre>`;
        code = [];
      }
      inCode = !inCode;
      continue;
    }
    if (inCode) {
      code.push(line);
      continue;
    }
    const heading = line.match(/^(#{2,3})\s+(.+)$/);
    if (heading) {
      flushParagraph();
      closeList();
      html += `<h${heading[1].length}>${inlineMarkdown(heading[2])}</h${heading[1].length}>`;
      continue;
    }
    const bullet = line.match(/^[-*]\s+(.+)$/);
    if (bullet) {
      flushParagraph();
      if (list !== "ul") {
        closeList();
        list = "ul";
        html += "<ul>";
      }
      html += `<li>${inlineMarkdown(bullet[1])}</li>`;
      continue;
    }
    const numbered = line.match(/^\d+[.)]\s+(.+)$/);
    if (numbered) {
      flushParagraph();
      if (list !== "ol") {
        closeList();
        list = "ol";
        html += "<ol>";
      }
      html += `<li>${inlineMarkdown(numbered[1])}</li>`;
      continue;
    }
    if (!line.trim()) {
      flushParagraph();
      closeList();
    } else {
      paragraph.push(line.trim());
    }
  }
  flushParagraph();
  closeList();
  if (code.length) html += `<pre><code>${escapeHTML(code.join("\n"))}</code></pre>`;
  return html || "<p>Условие пока не заполнено.</p>";
}

function renderTaskPreview(challenge) {
  const category = [challenge.theme, challenge.subtopic].filter(Boolean).join(" · ") || challenge.category || "Практика";
  $("#task-category").textContent = category;
  $("#task-title").textContent = challenge.title || "Новое задание";
  $("#task-difficulty").textContent = ({ easy: "ЛЁГКАЯ", medium: "СРЕДНЯЯ", hard: "СЛОЖНАЯ" })[challenge.difficulty] || "ЗАДАНИЕ";
  $("#task-meta").textContent = `⏱ ${challenge.time_limit || "—"} минут`;
  $("#task-description").innerHTML = markdownToHTML(challenge.body);
}

function fillTaskForm(challenge) {
  $("#task-field-title").value = challenge.title || "";
  $("#task-field-theme").value = challenge.theme || "";
  $("#task-field-subtopic").value = challenge.subtopic || "";
  $("#task-field-difficulty").value = challenge.difficulty || "medium";
  $("#task-field-time").value = challenge.time_limit || "20";
  $("#task-field-language").value = challenge.language || "python";
  $("#task-field-body").value = challenge.body || "";
}

function renderTask() {
  const source = files[currentTaskFile] || initialTask;
  const challenge = parseChallenge(source);
  $("#task-number").textContent = `ЗАДАНИЕ ${currentTaskIndex()}`;
  $("#task-source-input").value = source;
  fillTaskForm(challenge);
  renderTaskPreview(challenge);
  renderTaskRail();
}

function switchTaskMode(mode) {
  taskMode = mode;
  $$("[data-task-mode]").forEach((button) => button.classList.toggle("active", button.dataset.taskMode === mode));
  $("#task-form-view").classList.toggle("hidden", mode !== "form");
  $("#task-source-view").classList.toggle("hidden", mode !== "source");
  $("#task-preview-view").classList.toggle("hidden", mode !== "preview");
  if (mode === "source") $("#task-source-input").value = files[currentTaskFile] || "";
  if (mode === "preview") renderTaskPreview(parseChallenge(files[currentTaskFile]));
}

function formChallenge() {
  const previous = parseChallenge(files[currentTaskFile]);
  const taskSolutions = solutions().filter((item) => item.task === currentTaskIndex());
  return {
    title: $("#task-field-title").value,
    theme: $("#task-field-theme").value,
    subtopic: $("#task-field-subtopic").value,
    difficulty: $("#task-field-difficulty").value,
    time_limit: $("#task-field-time").value,
    language: $("#task-field-language").value,
    entrypoint: previous.entrypoint || taskSolutions[0]?.path || "",
    body: $("#task-field-body").value,
  };
}

function handleTaskFormInput() {
  const challenge = formChallenge();
  const source = composeChallenge(challenge);
  files[currentTaskFile] = source;
  $("#task-source-input").value = source;
  renderTaskPreview(challenge);
  renderTaskRail();
  schedulePersist(currentTaskFile, source, true);
}

function handleTaskSourceInput() {
  const source = $("#task-source-input").value;
  files[currentTaskFile] = source;
  const challenge = parseChallenge(source);
  fillTaskForm(challenge);
  renderTaskPreview(challenge);
  renderTaskRail();
  schedulePersist(currentTaskFile, source, true);
}

function highlight(source) {
  const extension = extensionOf(activeFile || "txt");
  const language = LANGUAGE_NAMES[extension] || "Text";
  const words = language === "Python"
    ? "and as assert async await break class continue def del elif else except False finally for from global if import in is lambda None nonlocal not or pass raise return True try while with yield"
    : language === "Go"
      ? "break case chan const continue default defer else fallthrough for func go goto if import interface map package range return select struct switch type var"
      : language === "Java"
        ? "abstract assert boolean break byte case catch char class const continue default do double else enum extends final finally float for if implements import instanceof int interface long native new package private protected public return short static super switch synchronized this throw throws transient try void volatile while"
        : "auto bool break case char class const continue default do double else enum extern float for if include int long namespace new private protected public return short signed sizeof static std string struct switch template this throw try typedef typename union unsigned using virtual void while";
  const keywords = new Set(words.split(" "));
  const pattern = /("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|`(?:[^`\\]|\\.)*`|#[^\n]*|\/\/[^\n]*|\b\d+(?:\.\d+)?\b|\b[A-Za-z_]\w*\b)/g;
  let html = "";
  let last = 0;
  let match;
  while ((match = pattern.exec(source))) {
    html += escapeHTML(source.slice(last, match.index));
    const token = match[0];
    const next = source.slice(match.index + token.length);
    let type = "";
    if (/^["'`]/.test(token)) type = "tok-string";
    else if ((language === "Python" && token.startsWith("#")) || (language !== "Python" && token.startsWith("//"))) type = "tok-comment";
    else if (language === "C++" && token.startsWith("#")) type = "tok-key";
    else if (/^\d/.test(token)) type = "tok-number";
    else if (keywords.has(token)) type = "tok-key";
    else if (/^\s*\(/.test(next)) type = "tok-func";
    html += type ? `<span class="${type}">${escapeHTML(token)}</span>` : escapeHTML(token);
    last = match.index + token.length;
  }
  return html + escapeHTML(source.slice(last));
}

function updateCursorStatus() {
  const before = codeInput.value.slice(0, codeInput.selectionStart);
  const lines = before.split("\n");
  $("#cursor-position").textContent = `Строка ${lines.length}, столбец ${lines.at(-1).length + 1}`;
}

function refreshEditor(keepCursor = false) {
  const solution = solutionForPath(activeFile);
  const content = files[activeFile] || "";
  const cursor = keepCursor ? codeInput.selectionStart : 0;
  codeInput.disabled = !solution;
  codeInput.value = content;
  $("#highlight-code").innerHTML = `${highlight(content)}\n`;
  const lineCount = Math.max(1, content.split("\n").length);
  $("#line-numbers").innerHTML = Array.from({ length: lineCount }, (_, index) => index + 1).join("<br>");
  codeInput.style.height = `${Math.max($("#code-wrap").clientHeight - 35, lineCount * editorLineHeight())}px`;
  $("#solution-breadcrumb").textContent = `Задание ${solution?.task ?? currentTaskIndex()}`;
  $("#active-solution-name").textContent = solutionDisplay(solution);
  $("#status-language").textContent = LANGUAGE_NAMES[solution?.extension] || "Text";
  if (keepCursor) codeInput.setSelectionRange(Math.min(cursor, content.length), Math.min(cursor, content.length));
  updateCursorStatus();
  renderSolutionTabs();
  renderRemoteCursors();
}

function renderRemoteCursors() {
  const container = $("#remote-cursors");
  container.innerHTML = "";
  if (!activeFile) return;
  for (const [participantId, state] of presence) {
    if (participantId === roomState.participantId || state.file !== activeFile) continue;
    const participant = roomState.participants.find((item) => item.id === participantId);
    if (!participant) continue;
    const position = Math.max(0, Math.min(Number(state.cursorStart) || 0, (files[activeFile] || "").length));
    const before = (files[activeFile] || "").slice(0, position).split("\n");
    const caret = document.createElement("div");
    caret.className = "remote-caret";
    caret.style.setProperty("--cursor-color", colorForId(participantId));
    caret.style.top = `${(before.length - 1) * editorLineHeight()}px`;
    caret.style.left = `${before.at(-1).length * editorFontSize * 0.602}px`;
    caret.innerHTML = `<span>${escapeHTML(participant.name)}</span>`;
    container.append(caret);
  }
}

function sendPresence() {
  if (!roomState.socket || roomState.socket.readyState !== WebSocket.OPEN || !activeFile) return;
  roomState.socket.send(JSON.stringify({
    type: "presence",
    file: activeFile,
    cursorStart: codeInput.selectionStart,
    cursorEnd: codeInput.selectionEnd,
  }));
  if (roomState.participantId) {
    presence.set(roomState.participantId, { file: activeFile, cursorStart: codeInput.selectionStart, cursorEnd: codeInput.selectionEnd });
  }
  renderSolutionTabs();
}

function activateSolution(path, selectTask = true) {
  const solution = solutionForPath(path);
  if (!solution) return;
  activeFile = path;
  if (selectTask) currentTaskFile = taskPathForIndex(solution.task);
  renderTask();
  renderSolutionTabs();
  refreshEditor(false);
  sendPresence();
}

function selectTask(path) {
  currentTaskFile = path;
  const index = currentTaskIndex();
  const preferred = solutions().find((solution) => solution.task === index);
  if (preferred) activeFile = preferred.path;
  renderTask();
  renderSolutionTabs();
  refreshEditor(false);
  sendPresence();
}

function roomApiUrl(path, query = {}) {
  const url = new URL(path, roomState.base);
  url.searchParams.set("code", roomState.code);
  for (const [key, value] of Object.entries(query)) url.searchParams.set(key, value);
  return url;
}

async function roomFetch(path, options = {}, query = {}) {
  const response = await fetch(roomApiUrl(path, query), options);
  if (!response.ok) throw new Error((await response.text()) || `Ошибка комнаты (${response.status})`);
  if (response.status === 204) return null;
  return response.json();
}

async function loadWorkspace() {
  const previousTask = currentTaskFile;
  const previousFile = activeFile;
  const next = {};
  if (roomState.code) {
    const entries = await roomFetch("/api/files");
    for (const entry of entries) {
      if (entry.isDirectory || !entry.isText) continue;
      if (!entry.path.endsWith(".cwm.md") && !isCodeFile(entry.path)) continue;
      const result = await roomFetch("/api/file", {}, { path: entry.path });
      next[entry.path] = result.content;
    }
  } else if (window.__TAURI__?.core?.invoke) {
    const invoke = window.__TAURI__.core.invoke;
    const entries = await invoke("list_project_files");
    for (const entry of entries) {
      if (entry.isDirectory || !entry.isText) continue;
      if (!entry.path.endsWith(".cwm.md") && !isCodeFile(entry.path)) continue;
      next[entry.path] = await invoke("read_project_file", { path: entry.path });
    }
  } else {
    Object.assign(next, files);
  }
  files = Object.keys(next).length ? next : { "task.cwm.md": initialTask, "0.solution-0.py": initialSolution };
  const tasks = taskFiles();
  currentTaskFile = tasks.some((task) => task.path === previousTask) ? previousTask : tasks[0]?.path || "task.cwm.md";
  const availableSolutions = solutions();
  activeFile = availableSolutions.some((solution) => solution.path === previousFile)
    ? previousFile
    : availableSolutions.find((solution) => solution.task === currentTaskIndex())?.path || availableSolutions[0]?.path || "";
  renderTask();
  renderSolutionTabs();
  refreshEditor(false);
}

async function persistFile(path, content) {
  if (roomState.code) {
    return roomFetch("/api/file", { method: "PUT", headers: { "Content-Type": "text/plain" }, body: content }, { path });
  }
  const invoke = window.__TAURI__?.core?.invoke;
  if (invoke) return invoke("save_project_file", { path, content });
}

function schedulePersist(path, content, isTask = false) {
  clearTimeout(saveTimers.get(path));
  if (isTask) $("#task-save-state").textContent = "● Сохраняется…";
  saveTimers.set(path, setTimeout(async () => {
    try {
      await persistFile(path, content);
      if (isTask && path === currentTaskFile) $("#task-save-state").textContent = "● Сохранено";
    } catch (error) {
      showToast(`Не удалось сохранить: ${error.message || error}`);
    }
  }, 260));
}

async function persistWorkspace() {
  for (const timer of saveTimers.values()) clearTimeout(timer);
  saveTimers.clear();
  await Promise.all(Object.entries(files).map(([path, content]) => persistFile(path, content)));
  $("#task-save-state").textContent = "● Сохранено";
}

async function createEntry(path, content) {
  if (roomState.code) {
    await roomFetch("/api/files", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code: roomState.code, path, kind: "file", content, encoding: null }),
    });
  } else if (window.__TAURI__?.core?.invoke) {
    await window.__TAURI__.core.invoke("create_project_entry", { path, kind: "file", content, encoding: null });
  }
  files[path] = content;
}

async function renameFile(oldPath, newPath) {
  if (roomState.code) {
    await roomFetch("/api/rename", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code: roomState.code, oldPath, newPath }),
    });
  } else if (window.__TAURI__?.core?.invoke) {
    await window.__TAURI__.core.invoke("rename_project_entry", { oldPath, newPath });
  }
  files[newPath] = files[oldPath];
  delete files[oldPath];
}

async function deleteFile(path) {
  if (roomState.code) {
    await roomFetch("/api/file", { method: "DELETE" }, { path });
  } else if (window.__TAURI__?.core?.invoke) {
    await window.__TAURI__.core.invoke("delete_project_entry", { path });
  }
  delete files[path];
}

function askDelete(target) {
  deleteTarget = target;
  const task = target.kind === "task";
  const contest = target.kind === "contest";
  $("#delete-title").textContent = contest ? `Удалить контест «${target.name}»?` : task ? `Удалить задание ${target.index}?` : `Удалить ${solutionDisplay(target.solution)}?`;
  $("#delete-description").textContent = contest
    ? "Контест и все его задания с решениями будут удалены с этого компьютера. Это действие нельзя отменить."
    : task ? "Вместе с условием удалятся все его решения и написанный в них код."
      : "Вкладка решения и весь написанный в ней код будут удалены.";
  $("#delete-modal").classList.remove("hidden");
}

async function confirmDelete() {
  if (!deleteTarget) return;
  const target = deleteTarget;
  $("#delete-modal").classList.add("hidden");
  deleteTarget = null;
  try {
    if (target.kind === "solution") {
      const remaining = solutions().filter((solution) => solution.task === target.solution.task && solution.path !== target.solution.path);
      if (!remaining.length) { showToast("У задания должно остаться хотя бы одно решение"); return; }
      await deleteFile(target.solution.path);
      if (activeFile === target.solution.path) activeFile = remaining[0].path;
      const taskPath = taskPathForIndex(target.solution.task);
      const challenge = parseChallenge(files[taskPath]);
      if (challenge.entrypoint === target.solution.path) {
        files[taskPath] = composeChallenge({ ...challenge, entrypoint: remaining[0].path });
        await persistFile(taskPath, files[taskPath]);
      }
      if (roomState.participantId) presence.set(roomState.participantId, { file: activeFile, cursorStart: 0, cursorEnd: 0 });
      showToast("Решение удалено");
    } else if (target.kind === "contest") {
      await window.__TAURI__.core.invoke("delete_local_contest", { folder: target.folder });
      files = {};
      currentTaskFile = "task.cwm.md";
      activeFile = "";
      await loadWorkspace();
      await refreshContestList();
      showToast(`Контест «${target.name}» удалён`);
      return;
    } else {
      const owned = solutions().filter((solution) => solution.task === target.index).map((solution) => solution.path);
      for (const path of owned) await deleteFile(path);
      await deleteFile(target.path);
      const remainingTasks = taskFiles();
      currentTaskFile = remainingTasks[0]?.path || "task.cwm.md";
      activeFile = solutions().find((solution) => solution.task === currentTaskIndex())?.path || "";
      showToast(`Задание ${target.index} удалено`);
    }
    renderTask();
    renderSolutionTabs();
    refreshEditor(false);
    sendPresence();
  } catch (error) {
    showToast(`Не удалось удалить: ${error.message || error}`);
  }
}

async function changeSolutionLanguage(extension) {
  const solution = solutionForPath(activeFile);
  if (!solution || solution.extension === extension) return;
  const newPath = `${solution.task}.${solution.label}.${extension}`;
  if (files[newPath] !== undefined) { showToast("Вкладка с таким именем уже существует"); $("#code-language-select").value = solution.extension; return; }
  try {
    const oldPath = solution.path;
    const currentContent = codeInput.value;
    files[oldPath] = currentContent;
    await persistFile(oldPath, files[oldPath]);
    await renameFile(oldPath, newPath);
    if (!currentContent.trim()) {
      const language = ({ py: "python", go: "go", cpp: "cpp", java: "java" })[extension];
      files[newPath] = starterCode(language, solution.task);
      await persistFile(newPath, files[newPath]);
    }
    if (activeFile === oldPath) activeFile = newPath;
    const taskPath = taskPathForIndex(solution.task);
    const challenge = parseChallenge(files[taskPath]);
    if (challenge.entrypoint === oldPath) {
      const language = ({ py: "python", go: "go", cpp: "cpp", java: "java" })[extension];
      files[taskPath] = composeChallenge({ ...challenge, language, entrypoint: newPath });
      await persistFile(taskPath, files[taskPath]);
      fillTaskForm(parseChallenge(files[taskPath]));
    }
    renderSolutionTabs();
    refreshEditor(false);
    sendPresence();
    showToast(`Язык решения: ${LANGUAGE_NAMES[extension]}`);
  } catch (error) {
    showToast(`Не удалось сменить язык: ${error.message || error}`);
    renderSolutionTabs();
  }
}

function starterCode(language, taskIndex) {
  if (language === "go") return `package main\n\nimport "fmt"\n\nfunc main() {\n\tfmt.Println("Задание ${taskIndex}")\n}\n`;
  if (language === "cpp") return `#include <iostream>\n\nint main() {\n    std::cout << "Задание ${taskIndex}" << std::endl;\n    return 0;\n}\n`;
  if (language === "java") return `public class Main {\n    public static void main(String[] args) {\n        System.out.println("Задание ${taskIndex}");\n    }\n}\n`;
  return `def solution():\n    # Напишите решение задачи ${taskIndex}\n    pass\n\n\nif __name__ == "__main__":\n    solution()\n`;
}

async function createTask() {
  const indexes = taskFiles().map((task) => task.index);
  const index = indexes.length ? Math.max(...indexes) + 1 : 0;
  const taskPath = taskPathForIndex(index);
  const solutionPath = `${index}.solution-0.py`;
  const source = composeChallenge({
    title: `Новое задание ${index}`,
    theme: "Практика",
    subtopic: "",
    difficulty: "medium",
    time_limit: "20",
    language: "python",
    entrypoint: solutionPath,
    body: "Опишите условие задания.\n\n### Примеры\n\n- `входные данные` → `ожидаемый результат`",
  });
  try {
    await createEntry(taskPath, source);
    await createEntry(solutionPath, starterCode("python", index));
    currentTaskFile = taskPath;
    activeFile = solutionPath;
    switchTaskMode("form");
    renderTask();
    refreshEditor(false);
    sendPresence();
    showToast(`Задание ${index} создано`);
  } catch (error) {
    showToast(`Не удалось создать задание: ${error.message || error}`);
  }
}

async function createSolution() {
  const task = currentTaskIndex();
  const challenge = parseChallenge(files[currentTaskFile]);
  const language = challenge.language || "python";
  const extension = LANGUAGE_EXTENSIONS[language] || "py";
  const existing = solutions().filter((solution) => solution.task === task);
  let index = 0;
  while (existing.some((solution) => solution.label === `solution-${index}`)) index += 1;
  const path = `${task}.solution-${index}.${extension}`;
  try {
    await createEntry(path, starterCode(language, task));
    activeFile = path;
    if (!challenge.entrypoint) {
      files[currentTaskFile] = composeChallenge({ ...challenge, entrypoint: path });
      await persistFile(currentTaskFile, files[currentTaskFile]);
    }
    renderTask();
    refreshEditor(false);
    sendPresence();
    showToast(`${task}.solution-${index} создано`);
  } catch (error) {
    showToast(`Не удалось создать решение: ${error.message || error}`);
  }
}

function openRename(path) {
  const solution = solutionForPath(path);
  if (!solution) return;
  renameTarget = solution;
  $("#rename-prefix").textContent = `${solution.task}.`;
  $("#rename-input").value = solution.label;
  $("#rename-modal").classList.remove("hidden");
  setTimeout(() => $("#rename-input").select(), 20);
}

async function submitRename(event) {
  event.preventDefault();
  if (!renameTarget) return;
  const label = $("#rename-input").value.trim();
  if (!/^[\p{L}\p{N}_-]+$/u.test(label)) {
    showToast("Используйте буквы, цифры, дефис или подчёркивание");
    return;
  }
  const newPath = `${renameTarget.task}.${label}.${renameTarget.extension}`;
  if (newPath === renameTarget.path) {
    $("#rename-modal").classList.add("hidden");
    return;
  }
  try {
    const oldPath = renameTarget.path;
    await renameFile(oldPath, newPath);
    if (activeFile === oldPath) activeFile = newPath;
    const challenge = parseChallenge(files[currentTaskFile]);
    if (challenge.entrypoint === oldPath) {
      files[currentTaskFile] = composeChallenge({ ...challenge, entrypoint: newPath });
      await persistFile(currentTaskFile, files[currentTaskFile]);
    }
    $("#rename-modal").classList.add("hidden");
    renderTask();
    refreshEditor(true);
    sendPresence();
    showToast(`Вкладка переименована в ${renameTarget.task}.${label}`);
  } catch (error) {
    showToast(`Не удалось переименовать: ${error.message || error}`);
  }
}

async function runCode() {
  if (!activeFile) return;
  files[activeFile] = codeInput.value;
  $("#run-result").textContent = "ВЫПОЛНЯЕТСЯ";
  output.textContent = `$ ${solutionDisplay(solutionForPath(activeFile))}\nЗапуск…`;
  try {
    await persistFile(activeFile, codeInput.value);
    const invoke = window.__TAURI__?.core?.invoke;
    if (!invoke) throw new Error("Для локального запуска откройте настольное приложение");
    const result = await invoke("run_local_source", { extension: extensionOf(activeFile), source: codeInput.value });
    output.textContent = `$ ${solutionDisplay(solutionForPath(activeFile))}\n${result.stdout || ""}${result.stderr ? `\n${result.stderr}` : ""}`;
    $("#run-result").textContent = result.timedOut ? "ТАЙМАУТ" : result.success ? "УСПЕШНО" : "ОШИБКА";
  } catch (error) {
    output.textContent = `$ ${solutionDisplay(solutionForPath(activeFile))}\n${error.message || error}`;
    $("#run-result").textContent = "ОШИБКА";
  }
}

function setRoomChrome(label, online, local = false) {
  const state = $("#room-state");
  state.classList.toggle("room-idle", !online);
  state.querySelector("span").textContent = label;
  $("#invite-button").disabled = !online;
  $("#start-room-button").classList.toggle("hidden", online);
  $("#stop-room-button").classList.toggle("hidden", !online || !roomState.host);
  $("#close-contest-button").classList.remove("hidden");
  roomState.local = local;
}

async function connectRoom(info, host = false, guestName = "") {
  roomState.local = false;
  roomState.base = new URL(info.inviteUrl).origin;
  roomState.code = info.inviteCode;
  roomState.inviteUrl = info.inviteUrl;
  roomState.host = host;
  roomState.selfName = host ? savedName() : guestName.trim().slice(0, 40) || savedName();
  roomState.participants = info.participants || [];
  setRoomChrome(host ? "Комната запущена" : "Вы в комнате", true);
  renderRoomParticipants();
  roomState.socket?.close();
  const socketUrl = new URL("/ws", roomState.base);
  socketUrl.protocol = socketUrl.protocol === "https:" ? "wss:" : "ws:";
  socketUrl.searchParams.set("code", roomState.code);
  socketUrl.searchParams.set("name", roomState.selfName);
  socketUrl.searchParams.set("host", String(host));
  const socket = new WebSocket(socketUrl);
  roomState.socket = socket;
  await new Promise((resolve, reject) => {
    let ready = false;
    socket.onmessage = async (event) => {
      let message;
      try { message = JSON.parse(event.data); } catch { return; }
      if (message.type === "room:ready") {
        roomState.participantId = message.id;
        roomState.selfName = message.name;
        renderRoomParticipants(message.room.participants || []);
        try {
          await loadWorkspace();
          if (!host) showWorkspace();
          ready = true;
          sendPresence();
          resolve();
        } catch (error) {
          reject(error);
        }
        return;
      }
      if (message.type === "participants") renderRoomParticipants(message.participants || []);
      if (message.type === "presence" && message.participantId) {
        presence.set(message.participantId, message);
        renderSolutionTabs();
        renderRemoteCursors();
      }
      if (message.type === "file:saved") {
        files[message.path] = message.content;
        if (message.path === activeFile && codeInput.value !== message.content) {
          syncEditorFromRemote(message.content);
        }
        renderRemoteCursors();
        if (message.path === currentTaskFile && !$("#task-form").contains(document.activeElement) && document.activeElement !== $("#task-source-input")) renderTask();
      }
      if (message.type === "file:created" || message.type === "file:renamed" || message.type === "file:deleted") await loadWorkspace();
    };
    socket.onerror = () => {
      if (!ready) reject(new Error("Комната недоступна или уже заполнена"));
    };
    socket.onclose = () => {
      if (!ready) {
        reject(new Error("Комната недоступна или уже заполнена"));
        return;
      }
      if (!host && roomState.socket === socket) {
        resetRoomConnection();
        setRoomChrome("Соединение прервано", false);
        notifyDisconnected();
      }
    };
  });
}

async function startRoom() {
  const invoke = window.__TAURI__?.core?.invoke;
  if (!invoke) {
    showToast("Комната запускается из desktop-приложения");
    return;
  }
  const button = $("#start-room-button");
  const buttonLabel = button.textContent;
  button.disabled = true;
  button.textContent = "Подключаем…";
  showToast("Подготавливаем публичную ссылку…", 90000);
  try {
    await persistWorkspace();
    const info = await invoke("start_room");
    await connectRoom(info, true);
    showToast("Комната запущена");
  } catch (error) {
    showToast(`Не удалось создать комнату: ${error}`);
  } finally {
    button.disabled = false;
    button.textContent = buttonLabel;
  }
}

function resetRoomConnection() {
  const socket = roomState.socket;
  roomState.base = "";
  roomState.code = "";
  roomState.inviteUrl = "";
  roomState.host = false;
  roomState.participantId = "";
  roomState.participants = [];
  roomState.socket = null;
  presence.clear();
  socket?.close();
  renderRoomParticipants([]);
}

async function stopRoom() {
  try {
    await window.__TAURI__?.core?.invoke("stop_room");
    resetRoomConnection();
    setRoomChrome("Локальная комната", false, true);
    showToast("Комната переведена в локальный режим");
  } catch (error) {
    showToast(`Не удалось остановить комнату: ${error}`);
  }
}

async function prepareInvite(urlText) {
  const invite = new URL(urlText);
  if (!new Set(["https:", "http:"]).has(invite.protocol) || invite.username || invite.password) {
    throw new Error("Нужна HTTP или HTTPS ссылка-приглашение");
  }
  const code = invite.searchParams.get("code");
  if (!code || code.length > 128) throw new Error("В ссылке не найден код комнаты");
  roomState.base = invite.origin;
  roomState.code = code;
  const info = await roomFetch("/api/room");
  if (!info?.inviteCode || info.inviteCode !== code) throw new Error("Ссылка недействительна");
  if (info.participantCount >= info.maxParticipants) throw new Error("В комнате уже 10 участников");
  pendingRoomInfo = info;
  $("#join-name").value = localStorage.getItem("code-with-me-name") || "";
  $("#connect-room-modal").classList.add("hidden");
  $("#join-modal").classList.remove("hidden");
  setTimeout(() => $("#join-name").focus(), 20);
}

async function connectFromInvite(event) {
  event.preventDefault();
  try {
    await prepareInvite($("#connect-room-url").value.trim());
  } catch (error) {
    pendingRoomInfo = null;
    resetRoomConnection();
    showToast(`Не удалось открыть приглашение: ${error.message || error}`);
  }
}

async function joinFromLink() {
  const code = new URLSearchParams(location.search).get("code");
  if (!code) return false;
  try {
    await prepareInvite(location.href);
  } catch (error) {
    resetRoomConnection();
    setRoomChrome("Не удалось подключиться", false);
    showToast(`Не удалось подключиться: ${error.message || error}`);
  }
  return true;
}

async function submitJoin(event) {
  event.preventDefault();
  const name = $("#join-name").value.trim();
  if (!name || !pendingRoomInfo) return;
  const info = pendingRoomInfo;
  pendingRoomInfo = null;
  localStorage.setItem("code-with-me-name", name.slice(0, 40));
  setProfileLabel(name.slice(0, 40));
  $("#join-modal").classList.add("hidden");
  try {
    await connectRoom(info, false, name);
  } catch (error) {
    resetRoomConnection();
    setRoomChrome("Не удалось подключиться", false);
    pendingRoomInfo = info;
    $("#join-modal").classList.remove("hidden");
    showToast(`Не удалось подключиться: ${error.message || error}`);
  }
}

function showInvite() {
  if (!roomState.inviteUrl) return;
  $("#invite-link").value = roomState.inviteUrl;
  $("#invite-modal").classList.remove("hidden");
  setTimeout(() => $("#invite-link").select(), 20);
}

async function copyInvite() {
  try {
    await navigator.clipboard.writeText(roomState.inviteUrl);
    showToast("Ссылка скопирована");
  } catch {
    $("#invite-link").select();
    showToast("Ссылка выделена — скопируйте её вручную");
  }
}

function downloadText(contents, name) {
  const url = URL.createObjectURL(new Blob([contents], { type: "application/vnd.code-with-me.room+json" }));
  const link = document.createElement("a");
  link.href = url;
  link.download = name;
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function exportLesson(mode) {
  try {
    if (activeFile) files[activeFile] = codeInput.value;
    await persistWorkspace();
    const name = `code-with-me-${mode === "template" ? "tasks" : "snapshot"}.cwmroom`;
    if (roomState.code) {
      const link = document.createElement("a");
      link.href = roomApiUrl("/api/export", { mode });
      link.download = name;
      document.body.append(link);
      link.click();
      link.remove();
    } else if (window.__TAURI__?.core?.invoke) {
      downloadText(await window.__TAURI__.core.invoke("export_room_archive", { mode }), name);
    } else {
      const archive = {
        format: "code-with-me-room",
        version: 1,
        mode,
        name: "code-with-me",
        files: Object.entries(files).map(([path, content]) => ({ path, kind: "file", encoding: "utf8", content: mode === "template" && isCodeFile(path) ? "" : content })),
      };
      downloadText(JSON.stringify(archive, null, 2), name);
    }
    $("#lesson-modal").classList.add("hidden");
    showToast(mode === "template" ? "Задачи сохранены без решений" : "Состояние занятия сохранено");
  } catch (error) {
    showToast(`Не удалось сохранить занятие: ${error.message || error}`);
  }
}

async function switchToLocalRoom(name) {
  resetRoomConnection();
  await loadWorkspace();
  setRoomChrome("Локальная комната", false, true);
  $("#lesson-modal").classList.add("hidden");
  showWorkspace();
  await refreshContestList();
  showToast(`Открыто: ${name}`);
}

async function createLocalCopy() {
  const invoke = window.__TAURI__?.core?.invoke;
  if (!invoke) return;
  try {
    if (activeFile) files[activeFile] = codeInput.value;
    await persistWorkspace();
    if (roomState.host) await invoke("stop_room");
    const name = await invoke("create_local_room", { mode: "snapshot" });
    await switchToLocalRoom(name);
  } catch (error) {
    showToast(`Не удалось создать копию: ${error.message || error}`);
  }
}

async function importRoomFile(event) {
  const selected = event.target.files?.[0];
  const invoke = window.__TAURI__?.core?.invoke;
  if (!selected || !invoke) return;
  try {
    await persistWorkspace();
    if (roomState.host) await invoke("stop_room");
    const name = await invoke("import_room_archive", { archive: await selected.text() });
    await switchToLocalRoom(name);
  } catch (error) {
    showToast(`Не удалось открыть занятие: ${error.message || error}`);
  } finally {
    event.target.value = "";
  }
}

function showWorkspace() {
  $("#home-view").classList.add("hidden");
  $(".workspace").classList.remove("hidden");
  $(".course-title").textContent = parseChallenge(files[currentTaskFile]).title || "Контест";
  $("#close-contest-button").classList.remove("hidden");
}

async function refreshContestList() {
  const mine = $("#my-contest-list");
  const saved = $("#saved-contest-list");
  if (!mine || !saved) return;
  let rooms = [];
  if (window.__TAURI__?.core?.invoke) {
    try { rooms = await window.__TAURI__.core.invoke("list_local_rooms"); } catch (error) { console.warn("Не удалось загрузить контесты", error); }
  }
  if (!rooms.length) rooms = [{ name: parseChallenge(files["task.cwm.md"] || initialTask).title || "Моё занятие", folder: "workspace", taskCount: taskFiles().length, category: "my" }];
  const card = (room) => `<article class="contest-card"><span class="contest-card-icon">⌘</span><span class="contest-card-copy"><strong>${escapeHTML(room.name)}</strong><small>${room.taskCount} ${room.taskCount === 1 ? "задание" : "заданий"}</small></span><div class="contest-card-actions"><button class="contest-card-action open" data-contest-folder="${escapeHTML(room.folder)}">Открыть</button><button class="contest-card-action" data-export-folder="${escapeHTML(room.folder)}" data-export-name="${escapeHTML(room.name)}">Скачать</button>${room.folder === "workspace" ? "" : `<button class="contest-card-action delete" data-delete-folder="${escapeHTML(room.folder)}" data-delete-name="${escapeHTML(room.name)}">Удалить</button>`}</div></article>`;
  const ownRooms = rooms.filter((room) => room.category === "my");
  const savedRooms = rooms.filter((room) => room.category !== "my");
  mine.innerHTML = ownRooms.length ? ownRooms.map(card).join("") : `<p class="empty-state">Пока нет своих контестов. Создайте новый кнопкой выше.</p>`;
  saved.innerHTML = savedRooms.length ? savedRooms.map(card).join("") : `<p class="empty-state">Здесь появятся импортированные файлы и сохранённые занятия.</p>`;
  await refreshDependencies();
}

async function refreshDependencies() {
  const container = $("#dependency-list");
  if (!container) return;
  const invoke = window.__TAURI__?.core?.invoke;
  if (!invoke) {
    container.innerHTML = `<p class="empty-state">Проверка зависимостей доступна в настольном приложении.</p>`;
    return;
  }
  try {
    const dependencies = await invoke("check_dependencies");
    container.innerHTML = dependencies.map((item) => `<article class="dependency-card"><span class="dependency-status ${item.installed ? "installed" : "missing"}">${item.installed ? "✓" : "!"}</span><span class="dependency-copy"><strong>${escapeHTML(item.name)}</strong><small>${escapeHTML(item.installed ? (item.version || "Найдено в системе") : item.description)}</small></span><span class="dependency-result ${item.installed ? "installed" : "missing"}">${item.installed ? "Установлено" : "Не найдено"}</span>${item.installed ? "" : `<button class="button secondary dependency-install" data-dependency="${escapeHTML(item.id)}">Установить</button>`}</article>`).join("");
  } catch (error) {
    container.innerHTML = `<p class="empty-state">Не удалось проверить зависимости: ${escapeHTML(error.message || error)}</p>`;
  }
}

async function createContest(event) {
  event.preventDefault();
  const name = $("#contest-name-input").value.trim();
  if (!name) return;
  try {
    await window.__TAURI__.core.invoke("create_contest", { name });
    files = {};
    currentTaskFile = "task.cwm.md";
    activeFile = "";
    await loadWorkspace();
    $("#create-contest-modal").classList.add("hidden");
    $("#create-contest-form").reset();
    showWorkspace();
    setRoomChrome("Локальная комната", false, true);
    await refreshContestList();
    showToast(`Контест «${name}» создан`);
  } catch (error) { showToast(`Не удалось создать контест: ${error.message || error}`); }
}

function openProfile() {
  $("#profile-name-input").value = savedName();
  $("#profile-modal").classList.remove("hidden");
  setTimeout(() => $("#profile-name-input").select(), 20);
}

async function renameSelf(event) {
  event.preventDefault();
  const name = $("#profile-name-input").value.trim().slice(0, 40);
  if (!name) return;
  localStorage.setItem("code-with-me-name", name);
  roomState.selfName = name;
  setProfileLabel(name);
  if (roomState.socket?.readyState === WebSocket.OPEN) roomState.socket.send(JSON.stringify({ type: "rename", name }));
  const ownIndex = roomState.participants.findIndex((person) => person.id === roomState.participantId);
  if (ownIndex >= 0) {
    roomState.participants[ownIndex] = { ...roomState.participants[ownIndex], name };
    renderRoomParticipants();
  }
  $("#profile-modal").classList.add("hidden");
  showToast("Имя обновлено");
}

async function openDependencyPage(id) {
  try {
    if (window.__TAURI__?.core?.invoke) await window.__TAURI__.core.invoke("open_dependency_page", { id });
    else window.open(INSTALL_URLS[id], "_blank", "noopener");
  } catch (error) { showToast(`Не удалось открыть установщик: ${error.message || error}`); }
}

async function exportLocalContest(folder, name) {
  try {
    const archive = await window.__TAURI__.core.invoke("export_local_contest", { folder });
    const filename = `${(name || "контест").replace(/[^\p{L}\p{N}_-]+/gu, "-")}.cwmroom`;
    downloadText(archive, filename);
    showToast(`Контест «${name}» скачан`);
  } catch (error) { showToast(`Не удалось скачать контест: ${error.message || error}`); }
}

async function showHome() {
  if (activeFile && codeInput.value !== files[activeFile]) files[activeFile] = codeInput.value;
  try { await persistWorkspace(); } catch (error) { showToast(`Не удалось сохранить: ${error.message || error}`); return; }
  const stopHostedRoom = roomState.host;
  if (roomState.socket) resetRoomConnection();
  if (stopHostedRoom && window.__TAURI__?.core?.invoke) {
    try { await window.__TAURI__.core.invoke("stop_room"); } catch { /* already stopped */ }
  }
  roomState.host = false;
  roomState.local = true;
  setRoomChrome("Главное меню", false, true);
  $(".workspace").classList.add("hidden");
  $("#home-view").classList.remove("hidden");
  $(".course-title").textContent = "";
  $("#close-contest-button").classList.add("hidden");
  await refreshContestList();
}

async function openContest(folder) {
  const invoke = window.__TAURI__?.core?.invoke;
  try {
    if (invoke) {
      await invoke("open_local_room", { folder });
      files = {};
      currentTaskFile = "task.cwm.md";
      activeFile = "";
      await loadWorkspace();
    }
    showWorkspace();
    renderTask();
    renderSolutionTabs();
    refreshEditor(false);
    setRoomChrome("Локальная комната", false, true);
  } catch (error) { showToast(`Не удалось открыть контест: ${error.message || error}`); }
}

function updateCompletion() {
  const language = LANGUAGE_NAMES[extensionOf(activeFile || "")];
  const words = language === "Python" ? ["def", "return", "range", "print", "while", "for", "import"]
    : language === "Go" ? ["func", "package", "import", "return", "range"]
      : language === "Java" ? ["class", "public", "static", "return", "new"]
        : ["include", "int", "return", "vector", "string", "auto"];
  const before = codeInput.value.slice(0, codeInput.selectionStart);
  const partial = before.match(/[A-Za-z_]\w*$/)?.[0] || "";
  const suggestion = partial ? words.find((word) => word.startsWith(partial) && word !== partial) : "";
  $("#suggestion-word").textContent = suggestion || "";
  $("#suggestion").classList.toggle("hidden", !suggestion);
}

async function restore() {
  if (await joinFromLink()) return;
  if (window.__TAURI__?.core?.invoke) {
    try {
      await loadWorkspace();
      const current = await window.__TAURI__.core.invoke("current_room");
      if (current) await connectRoom(current, true);
      else { setRoomChrome("Главное меню", false, true); $(".workspace").classList.add("hidden"); $("#home-view").classList.remove("hidden"); $("#close-contest-button").classList.add("hidden"); await refreshContestList(); }
    } catch (error) {
      showToast(`Не удалось открыть занятие: ${error}`);
    }
  } else {
    renderTask();
    refreshEditor(false);
    setRoomChrome("Главное меню", false, true);
    $(".workspace").classList.add("hidden");
    $("#home-view").classList.remove("hidden");
    await refreshContestList();
  }
}

function init() {
  updateThemeControl();
  setProfileLabel(savedName());
  applyEditorFontSize();
  const systemTheme = window.matchMedia("(prefers-color-scheme: dark)");
  systemTheme.addEventListener("change", (event) => {
    if (localStorage.getItem("code-with-me-theme")) return;
    document.documentElement.dataset.theme = event.matches ? "dark" : "light";
    updateThemeControl();
  });
  $("#theme-toggle").addEventListener("click", toggleTheme);
  $("#profile-button").addEventListener("click", openProfile);
  $("#profile-form").addEventListener("submit", renameSelf);
  $("#create-contest-button").addEventListener("click", () => {
    $("#contest-name-input").value = "";
    $("#create-contest-modal").classList.remove("hidden");
    setTimeout(() => $("#contest-name-input").focus(), 20);
  });
  $("#create-contest-form").addEventListener("submit", createContest);
  $("#refresh-dependencies-button").addEventListener("click", refreshDependencies);
  $("#dependency-list").addEventListener("click", (event) => {
    const button = event.target.closest("[data-dependency]");
    if (button) openDependencyPage(button.dataset.dependency);
  });
  $("#font-smaller").addEventListener("click", () => {
    const index = EDITOR_FONT_SIZES.indexOf(editorFontSize);
    if (index > 0) { editorFontSize = EDITOR_FONT_SIZES[index - 1]; applyEditorFontSize(); }
  });
  $("#font-larger").addEventListener("click", () => {
    const index = EDITOR_FONT_SIZES.indexOf(editorFontSize);
    if (index < EDITOR_FONT_SIZES.length - 1) { editorFontSize = EDITOR_FONT_SIZES[index + 1]; applyEditorFontSize(); }
  });
  $("#editor-fullscreen-button").addEventListener("click", () => {
    setEditorFullscreen(!$(".editor-panel").classList.contains("is-fullscreen"));
  });

  $("#task-mode-switch").addEventListener("click", (event) => {
    const button = event.target.closest("[data-task-mode]");
    if (button) switchTaskMode(button.dataset.taskMode);
  });
  $("#task-form").addEventListener("input", handleTaskFormInput);
  $("#task-form").addEventListener("change", handleTaskFormInput);
  $("#task-source-input").addEventListener("input", handleTaskSourceInput);
  $("#task-rail").addEventListener("click", (event) => {
    const task = event.target.closest("[data-task-file]");
    if (task) selectTask(task.dataset.taskFile);
    if (event.target.closest("#add-task-button")) createTask();
  });
  $("#solution-tabs").addEventListener("click", (event) => {
    if (event.target.closest("#add-solution-button")) return createSolution();
    const tab = event.target.closest("[data-solution-path]");
    if (!tab) return;
    if (event.target.closest(".rename-tab")) openRename(tab.dataset.solutionPath);
    else if (event.target.closest(".delete-tab")) { const solution = solutionForPath(tab.dataset.solutionPath); if (solution) askDelete({ kind: "solution", solution }); }
    else activateSolution(tab.dataset.solutionPath);
  });
  $("#solution-tabs").addEventListener("dblclick", (event) => {
    const tab = event.target.closest("[data-solution-path]");
    if (tab) openRename(tab.dataset.solutionPath);
  });
  $("#rename-form").addEventListener("submit", submitRename);
  $("#delete-task-button").addEventListener("click", () => {
    if (taskFiles().length <= 1) { showToast("В контесте должно остаться хотя бы одно задание"); return; }
    askDelete({ kind: "task", path: currentTaskFile, index: currentTaskIndex() });
  });
  $("#confirm-delete-button").addEventListener("click", confirmDelete);
  $("#code-language-select").addEventListener("change", (event) => changeSolutionLanguage(event.target.value));
  $$('[data-close-modal]').forEach((button) => button.addEventListener("click", () => $(`#${button.dataset.closeModal}`).classList.add("hidden")));
  $$(".modal-backdrop").forEach((modal) => modal.addEventListener("click", (event) => {
    if (event.target === modal && modal.id !== "join-modal") modal.classList.add("hidden");
  }));

  codeInput.addEventListener("input", () => {
    if (!activeFile) return;
    files[activeFile] = codeInput.value;
    $("#highlight-code").innerHTML = `${highlight(codeInput.value)}\n`;
    const count = Math.max(1, codeInput.value.split("\n").length);
    $("#line-numbers").innerHTML = Array.from({ length: count }, (_, index) => index + 1).join("<br>");
    codeInput.style.height = `${Math.max($("#code-wrap").clientHeight - 35, count * editorLineHeight())}px`;
    schedulePersist(activeFile, codeInput.value);
    updateCursorStatus();
    updateCompletion();
    sendPresence();
  });
  codeInput.addEventListener("click", () => { updateCursorStatus(); updateCompletion(); sendPresence(); });
  codeInput.addEventListener("keyup", () => { updateCursorStatus(); updateCompletion(); sendPresence(); });
  codeInput.addEventListener("select", sendPresence);
  codeInput.addEventListener("keydown", (event) => {
    if (event.key === "Tab") {
      event.preventDefault();
      const suggestion = $("#suggestion-word").textContent;
      const before = codeInput.value.slice(0, codeInput.selectionStart);
      const partial = before.match(/[A-Za-z_]\w*$/)?.[0] || "";
      if (suggestion && partial) {
        codeInput.setRangeText(suggestion, codeInput.selectionStart - partial.length, codeInput.selectionStart, "end");
      } else {
        codeInput.setRangeText("    ", codeInput.selectionStart, codeInput.selectionEnd, "end");
      }
      codeInput.dispatchEvent(new Event("input"));
    }
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      runCode();
    }
  });

  $("#run-button").addEventListener("click", runCode);
  $("#clear-output").addEventListener("click", () => { output.textContent = "Вывод очищен."; $("#run-result").textContent = "ГОТОВО"; });
  $("#start-room-button").addEventListener("click", startRoom);
  $("#stop-room-button").addEventListener("click", stopRoom);
  $("#invite-button").addEventListener("click", showInvite);
  $("#copy-invite-button").addEventListener("click", copyInvite);
  $("#join-form").addEventListener("submit", submitJoin);
  $("#lesson-button").addEventListener("click", () => $("#lesson-modal").classList.remove("hidden"));
  $$("[data-export-mode]").forEach((button) => button.addEventListener("click", () => exportLesson(button.dataset.exportMode)));
  $("#open-room-file").addEventListener("click", () => $("#room-file-input").click());
  $("#room-file-input").addEventListener("change", importRoomFile);
  $("#local-copy-button").addEventListener("click", createLocalCopy);
  $("#close-contest-button").addEventListener("click", showHome);
  $(".brand").addEventListener("click", (event) => { event.preventDefault(); showHome(); });
  $("#home-import-button").addEventListener("click", () => $("#home-file-input").click());
  $("#home-connect-button").addEventListener("click", () => {
    $("#connect-room-url").value = "";
    $("#connect-room-modal").classList.remove("hidden");
    setTimeout(() => $("#connect-room-url").focus(), 20);
  });
  $("#connect-room-form").addEventListener("submit", connectFromInvite);
  $("#home-file-input").addEventListener("change", importRoomFile);
  $("#home-view").addEventListener("click", (event) => {
    const exportButton = event.target.closest("[data-export-folder]");
    if (exportButton) { exportLocalContest(exportButton.dataset.exportFolder, exportButton.dataset.exportName); return; }
    const deleteButton = event.target.closest("[data-delete-folder]");
    if (deleteButton) { askDelete({ kind: "contest", folder: deleteButton.dataset.deleteFolder, name: deleteButton.dataset.deleteName }); return; }
    const openButton = event.target.closest("[data-contest-folder]");
    if (openButton) openContest(openButton.dataset.contestFolder);
  });
  $("#lesson-local-actions").classList.toggle("hidden", !window.__TAURI__?.core?.invoke);

  document.addEventListener("keydown", (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
      event.preventDefault();
      showInvite();
    }
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "s") {
      event.preventDefault();
      if (activeFile) files[activeFile] = codeInput.value;
      persistWorkspace().then(() => showToast("Изменения сохранены")).catch((error) => showToast(`Не удалось сохранить: ${error}`));
    }
    if (event.key === "Escape") {
      if ($(".editor-panel").classList.contains("is-fullscreen")) {
        setEditorFullscreen(false);
        return;
      }
      $$(".modal-backdrop:not(#join-modal)").forEach((modal) => modal.classList.add("hidden"));
    }
  });
  window.addEventListener("resize", () => { refreshEditor(true); renderRemoteCursors(); });
  restore();
}

init();
