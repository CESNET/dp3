const MERMAID_PALETTES = {
  default: {
    actionNode: {
      fill: "transparent",
      stroke: "#6b7280",
      strokeWidth: "1.5px",
      strokeDasharray: "5 3",
      text: "#111827",
    },
    hookNode: {
      fill: "#eef2ff",
      stroke: "#4f46e5",
      strokeWidth: "1.5px",
      text: "#111827",
    },
    taskProducer: {
      fill: "#fff3cd",
      stroke: "#d97706",
      strokeWidth: "2px",
      text: "#111827",
    },
    edgeLabelBackground: "#ffffff",
    edgeLabelText: "#111827",
  },
  dark: {
    actionNode: {
      fill: "#2f3340",
      stroke: "#9ca3af",
      strokeWidth: "1.5px",
      strokeDasharray: "5 3",
      text: "#e5e7eb",
    },
    hookNode: {
      fill: "#1f2540",
      stroke: "#a5b4fc",
      strokeWidth: "1.5px",
      text: "#f8fafc",
    },
    taskProducer: {
      fill: "#4a3410",
      stroke: "#fbbf24",
      strokeWidth: "2px",
      text: "#fff7ed",
    },
    edgeLabelBackground: "#252632",
    edgeLabelText: "#e5e7eb",
  },
};

const mermaidRoots = new Set();
let themeObserverInstalled = false;
let stylesScheduled = false;

const currentTheme = () =>
  document.body?.getAttribute("data-md-color-scheme") === "slate" ? "dark" : "default";

const setImportant = (element, property, value) => {
  element.style.setProperty(property, value, "important");
};

const styleNodeGroup = (root, className, palette) => {
  for (const node of root.querySelectorAll(`svg .node.${className}`)) {
    for (const shape of node.querySelectorAll("rect, circle, ellipse, polygon, path")) {
      setImportant(shape, "fill", palette.fill);
      setImportant(shape, "stroke", palette.stroke);
      setImportant(shape, "stroke-width", palette.strokeWidth);
      if (palette.strokeDasharray) {
        setImportant(shape, "stroke-dasharray", palette.strokeDasharray);
      } else {
        shape.style.removeProperty("stroke-dasharray");
      }
    }

    for (const label of node.querySelectorAll(
      "text, tspan, .nodeLabel, foreignObject div, foreignObject span"
    )) {
      setImportant(label, "fill", palette.text);
      setImportant(label, "color", palette.text);
    }

    for (const bg of node.querySelectorAll(".labelBkg")) {
      setImportant(bg, "fill", "transparent");
    }
  }
};

const styleEdgeLabels = (root, palette) => {
  for (const element of root.querySelectorAll(
    ".edgeLabel .label, .edgeLabel .labelBkg, .edgeLabel rect, .edgeLabel foreignObject div, .edgeLabel foreignObject span"
  )) {
    setImportant(element, "background", palette.edgeLabelBackground);
    setImportant(element, "fill", palette.edgeLabelBackground);
    setImportant(element, "stroke", "none");
    setImportant(element, "box-shadow", "none");
  }

  for (const label of root.querySelectorAll(
    ".edgeLabel text, .edgeLabel tspan, .edgeLabel foreignObject div, .edgeLabel foreignObject span"
  )) {
    setImportant(label, "fill", palette.edgeLabelText);
    setImportant(label, "color", palette.edgeLabelText);
  }
};

const applyMermaidStyles = () => {
  const palette = MERMAID_PALETTES[currentTheme()];
  for (const root of Array.from(mermaidRoots)) {
    if (!root.isConnected) {
      mermaidRoots.delete(root);
      continue;
    }
    styleNodeGroup(root, "actionNode", palette.actionNode);
    styleNodeGroup(root, "hookNode", palette.hookNode);
    styleNodeGroup(root, "taskProducer", palette.taskProducer);
    styleEdgeLabels(root, palette);
  }
};

const scheduleMermaidStyles = () => {
  if (stylesScheduled) {
    return;
  }
  stylesScheduled = true;
  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      stylesScheduled = false;
      applyMermaidStyles();
    });
  });
};

const patchAttachShadow = () => {
  if (Element.prototype.__dp3MermaidShadowPatched) {
    return;
  }

  const originalAttachShadow = Element.prototype.attachShadow;
  Element.prototype.attachShadow = function patchedAttachShadow(init) {
    const root = originalAttachShadow.call(this, init);
    if (this.classList?.contains("mermaid")) {
      mermaidRoots.add(root);
      scheduleMermaidStyles();
    }
    return root;
  };

  Element.prototype.__dp3MermaidShadowPatched = true;
};

const installThemeObserver = () => {
  if (themeObserverInstalled || !document.body) {
    return;
  }
  themeObserverInstalled = true;

  new MutationObserver((mutations) => {
    for (const mutation of mutations) {
      if (mutation.attributeName === "data-md-color-scheme") {
        scheduleMermaidStyles();
        break;
      }
    }
  }).observe(document.body, {
    attributes: true,
    attributeFilter: ["data-md-color-scheme"],
  });
};

const initMermaidStyling = () => {
  installThemeObserver();
  scheduleMermaidStyles();
};

patchAttachShadow();

if (typeof document$ !== "undefined") {
  document$.subscribe(initMermaidStyling);
} else {
  window.addEventListener("DOMContentLoaded", initMermaidStyling);
}
