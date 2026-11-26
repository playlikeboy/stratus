// The panel's one dropdown. The server renders it (see ui/fields.html):
//
//   <div class="select" data-select>
//     <input type="hidden" name="…" value="…">
//     <button class="control select-button" role="combobox" aria-expanded="false" aria-controls="…">…</button>
//     <ul class="select-menu" role="listbox" id="…" hidden><li role="option" data-value="…" aria-selected="…">…</li>…</ul>
//   </div>
//
// This script makes it work: open and close, choose with the mouse or the
// keyboard, type to jump, and keep the hidden input's value for the form.
(function () {
  "use strict";
  let open = null; // the dropdown whose menu is open

  function parts(root) {
    return {
      input: root.querySelector("input[type=hidden]"),
      button: root.querySelector(".select-button"),
      label: root.querySelector(".select-value"),
      menu: root.querySelector(".select-menu"),
      options: Array.from(root.querySelectorAll("[role=option]")),
    };
  }

  function show(root) {
    if (open && open !== root) hide(open, false);
    const p = parts(root);
    p.menu.hidden = false;
    p.button.setAttribute("aria-expanded", "true");
    root.classList.add("open");
    const current = p.options.find((o) => o.getAttribute("aria-selected") === "true") || p.options[0];
    focusOption(root, current);
    open = root;
  }

  function hide(root, refocus) {
    const p = parts(root);
    p.menu.hidden = true;
    p.button.setAttribute("aria-expanded", "false");
    p.button.removeAttribute("aria-activedescendant");
    root.classList.remove("open");
    if (refocus) p.button.focus();
    if (open === root) open = null;
  }

  function focusOption(root, option) {
    if (!option) return;
    const p = parts(root);
    p.options.forEach((o) => o.classList.toggle("active", o === option));
    p.button.setAttribute("aria-activedescendant", option.id);
    option.scrollIntoView({ block: "nearest" });
  }

  function active(root) {
    return parts(root).options.find((o) => o.classList.contains("active"));
  }

  function choose(root, option) {
    const p = parts(root);
    p.options.forEach((o) => o.setAttribute("aria-selected", String(o === option)));
    p.label.textContent = option.textContent;
    root.classList.toggle("empty", option.dataset.value === "");
    if (p.input.value !== option.dataset.value) {
      p.input.value = option.dataset.value;
      p.input.dispatchEvent(new Event("change", { bubbles: true }));
    }
    hide(root, true);
  }

  // Typing letters jumps to the next option that starts with them.
  let typed = "", typedAt = 0;
  function typeahead(root, key) {
    const now = Date.now();
    typed = now - typedAt > 700 ? key : typed + key;
    typedAt = now;
    const opts = parts(root).options;
    const from = opts.indexOf(active(root)) + (typed.length === 1 ? 1 : 0);
    const ordered = opts.slice(from).concat(opts.slice(0, from));
    const hit = ordered.find((o) => o.textContent.trim().toLowerCase().startsWith(typed.toLowerCase()));
    if (hit) focusOption(root, hit);
  }

  function onKey(event) {
    const root = event.target.closest("[data-select]");
    if (!root) return;
    const p = parts(root);
    const isOpen = !p.menu.hidden;
    const i = p.options.indexOf(active(root));
    switch (event.key) {
      case "ArrowDown":
      case "ArrowUp":
        event.preventDefault();
        if (!isOpen) return show(root);
        focusOption(root, p.options[Math.max(0, Math.min(p.options.length - 1, i + (event.key === "ArrowDown" ? 1 : -1)))]);
        return;
      case "Home":
      case "End":
        if (!isOpen) return;
        event.preventDefault();
        focusOption(root, event.key === "Home" ? p.options[0] : p.options[p.options.length - 1]);
        return;
      case "Enter":
      case " ":
        event.preventDefault();
        if (isOpen && active(root)) choose(root, active(root));
        else show(root);
        return;
      case "Escape":
        if (isOpen) {
          event.preventDefault();
          hide(root, true);
        }
        return;
      case "Tab":
        if (isOpen) hide(root, false);
        return;
    }
    if (event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey) {
      if (!isOpen) show(root);
      typeahead(root, event.key);
    }
  }

  document.addEventListener("click", (event) => {
    const option = event.target.closest("[data-select] [role=option]");
    if (option) return choose(option.closest("[data-select]"), option);
    const button = event.target.closest("[data-select] .select-button");
    if (button) {
      const root = button.closest("[data-select]");
      return parts(root).menu.hidden ? show(root) : hide(root, true);
    }
    if (open && !open.contains(event.target)) hide(open, false);
  });
  document.addEventListener("mousemove", (event) => {
    const option = event.target.closest("[data-select] [role=option]");
    if (option) focusOption(option.closest("[data-select]"), option);
  });
  document.addEventListener("keydown", onKey);
})();
