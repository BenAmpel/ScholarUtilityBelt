// Custom Node.js loader hook that forces .js files under src/ to be treated as ES modules.
// This is needed because package.json has "type": "commonjs" but the source uses ESM export syntax.

export function resolve(specifier, context, nextResolve) {
  return nextResolve(specifier, context);
}

export function load(url, context, nextLoad) {
  // Force .js files under src/ to load as ESM
  if (url.includes("/src/") && url.endsWith(".js")) {
    return nextLoad(url, { ...context, format: "module" });
  }
  return nextLoad(url, context);
}
