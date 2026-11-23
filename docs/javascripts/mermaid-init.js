// Renders Mermaid diagrams with Mermaid's own themes, as GitHub does,
// instead of Material for MkDocs' built-in theming. The fences use the
// "mermaid-raw" class (see mkdocs.yml) so Material leaves them alone.
//
// Each diagram sits in a small viewer, like GitHub's: buttons to zoom and
// move, drag with the mouse to move, Ctrl + wheel to zoom, and a button to
// open it in a fullscreen overlay (Escape closes it).
//
// Flowcharts and state diagrams use the ELK layout, which crosses fewer lines
// than Mermaid's default layout.
import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11.17.2/dist/mermaid.esm.min.mjs";
import elkLayouts from "https://cdn.jsdelivr.net/npm/@mermaid-js/layout-elk@0.2.3/dist/mermaid-layout-elk.esm.min.mjs";

mermaid.registerLayoutLoaders(elkLayouts);

{
  var ZOOM_STEP = 1.25;
  var MIN_SCALE = 0.25;
  var MAX_SCALE = 10;
  var MOVE_STEP = 60;

  var BUTTONS = [
    ["up", "Move up", "▲"],
    ["zoom-in", "Zoom in", "+"],
    ["left", "Move left", "◀"],
    ["reset", "Reset view", "⟲"],
    ["right", "Move right", "▶"],
    ["down", "Move down", "▼"],
    ["zoom-out", "Zoom out", "−"],
    ["fullscreen", "Show fullscreen", "⤢"],
  ];

  function currentTheme() {
    return document.body.getAttribute("data-md-color-scheme") === "slate" ? "dark" : "default";
  }

  function collect() {
    // Turn each <pre class="mermaid-raw"><code>…</code></pre> into a container
    // that keeps the diagram source, so it can be redrawn when the theme changes.
    document.querySelectorAll("pre.mermaid-raw").forEach(function (pre) {
      var div = document.createElement("div");
      div.className = "mermaid-diagram";
      div.dataset.source = pre.textContent;
      pre.replaceWith(div);
    });
    return Array.from(document.querySelectorAll("div.mermaid-diagram"));
  }

  function setUpViewer(div, svg) {
    var pan = document.createElement("div");
    pan.className = "mermaid-pan";
    pan.innerHTML = svg;

    var controls = document.createElement("div");
    controls.className = "mermaid-controls";
    BUTTONS.forEach(function (b) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "mermaid-" + b[0];
      button.dataset.action = b[0];
      button.title = b[1];
      button.setAttribute("aria-label", b[1]);
      button.textContent = b[2];
      controls.appendChild(button);
    });

    div.replaceChildren(pan, controls);
    // A theme change redraws the diagram; keep the overlay open if it was.
    setFullscreenButton(div, div.classList.contains("is-fullscreen"));

    // x, y: translation of the diagram's centre from the viewer's centre.
    var view = { x: 0, y: 0, scale: 1 };
    div._view = view;
    div._pan = pan;
    apply(div);
  }

  function apply(div) {
    var v = div._view;
    div._pan.style.transform = "translate(" + v.x + "px, " + v.y + "px) scale(" + v.scale + ")";
  }

  // Zoom by `factor`, keeping the point (cx, cy) fixed; it is relative to the
  // viewer's centre, so (0, 0) zooms on the middle of the viewer.
  function zoom(div, factor, cx, cy) {
    var v = div._view;
    var scale = Math.min(MAX_SCALE, Math.max(MIN_SCALE, v.scale * factor));
    var r = scale / v.scale;
    v.x = cx - (cx - v.x) * r;
    v.y = cy - (cy - v.y) * r;
    v.scale = scale;
    apply(div);
  }

  function move(div, dx, dy) {
    div._view.x += dx;
    div._view.y += dy;
    apply(div);
  }

  var fullscreen = null;

  function setFullscreenButton(div, open) {
    var button = div.querySelector("button[data-action=fullscreen]");
    if (!button) return;
    var label = open ? "Close fullscreen" : "Show fullscreen";
    button.textContent = open ? "✕" : "⤢";
    button.title = label;
    button.setAttribute("aria-label", label);
  }

  function toggleFullscreen(div) {
    var open = fullscreen !== div;
    if (fullscreen && open) toggleFullscreen(fullscreen);
    div.classList.toggle("is-fullscreen", open);
    document.documentElement.classList.toggle("mermaid-fullscreen-open", open);
    setFullscreenButton(div, open);
    fullscreen = open ? div : null;
    div._view = { x: 0, y: 0, scale: 1 };
    apply(div);
  }

  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape" && fullscreen) toggleFullscreen(fullscreen);
  });

  function attachEvents(div) {
    div.addEventListener("click", function (e) {
      var button = e.target.closest("button[data-action]");
      if (!button || !div._view) return;
      switch (button.dataset.action) {
        case "zoom-in": zoom(div, ZOOM_STEP, 0, 0); break;
        case "zoom-out": zoom(div, 1 / ZOOM_STEP, 0, 0); break;
        case "reset": div._view = { x: 0, y: 0, scale: 1 }; apply(div); break;
        case "fullscreen": toggleFullscreen(div); break;
        case "up": move(div, 0, MOVE_STEP); break;
        case "down": move(div, 0, -MOVE_STEP); break;
        case "left": move(div, MOVE_STEP, 0); break;
        case "right": move(div, -MOVE_STEP, 0); break;
      }
    });

    // Ctrl + wheel zooms on the cursor; a plain wheel still scrolls the page.
    div.addEventListener("wheel", function (e) {
      if (!e.ctrlKey || !div._view) return;
      e.preventDefault();
      var rect = div.getBoundingClientRect();
      var cx = e.clientX - rect.left - rect.width / 2;
      var cy = e.clientY - rect.top - rect.height / 2;
      zoom(div, e.deltaY < 0 ? ZOOM_STEP : 1 / ZOOM_STEP, cx, cy);
    }, { passive: false });

    // Drag with the mouse to move. Touch keeps scrolling the page; the
    // buttons work on touch screens.
    var drag = null;
    div.addEventListener("pointerdown", function (e) {
      if (e.pointerType !== "mouse" || e.button !== 0 || !div._view) return;
      if (e.target.closest(".mermaid-controls")) return;
      drag = { x: e.clientX, y: e.clientY };
      div.setPointerCapture(e.pointerId);
      div.classList.add("is-dragging");
      e.preventDefault();
    });
    div.addEventListener("pointermove", function (e) {
      if (!drag) return;
      move(div, e.clientX - drag.x, e.clientY - drag.y);
      drag = { x: e.clientX, y: e.clientY };
    });
    function endDrag() {
      drag = null;
      div.classList.remove("is-dragging");
    }
    div.addEventListener("pointerup", endDrag);
    div.addEventListener("pointercancel", endDrag);
  }

  var counter = 0;

  async function render() {
    var diagrams = collect();
    if (diagrams.length === 0) return;
    mermaid.initialize({ startOnLoad: false, theme: currentTheme(), layout: "elk" });
    for (var div of diagrams) {
      if (!div.dataset.events) {
        attachEvents(div);
        div.dataset.events = "true";
      }
      try {
        var result = await mermaid.render("mermaid-svg-" + counter++, div.dataset.source);
        setUpViewer(div, result.svg);
      } catch (e) {
        div._view = null;
        div.textContent = div.dataset.source;
        console.error(e);
      }
    }
  }

  function start() {
    render();
    new MutationObserver(render).observe(document.body, {
      attributes: true,
      attributeFilter: ["data-md-color-scheme"],
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
}
