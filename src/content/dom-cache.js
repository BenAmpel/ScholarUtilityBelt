// DOM query caching utilities using WeakMap to avoid memory leaks.
// Cache is automatically invalidated when elements are removed from DOM.

const domCache = new WeakMap();

function hasConnectedElements(elements) {
  return Array.isArray(elements) && elements.length > 0 && elements.every((element) => element?.isConnected);
}

/**
 * Get a cached element or query and cache it.
 * @param {Element} container - Container element to query within
 * @param {string} selector - CSS selector
 * @returns {Element|null} - Cached or newly queried element
 */
export function getCachedElement(container, selector) {
  if (!container || !selector) return null;
  
  // Get or create cache for this container
  let cache = domCache.get(container);
  if (!cache) {
    cache = new Map();
    domCache.set(container, cache);
  }
  
  // Return cached element if available and still in DOM
  if (cache.has(selector)) {
    const element = cache.get(selector);
    if (element && element.isConnected) {
      return element;
    }
    // Element was removed, clear from cache
    cache.delete(selector);
  }
  
  // Query and cache
  const element = container.querySelector(selector);
  if (element) {
    cache.set(selector, element);
  }
  return element;
}

/**
 * Get cached elements or query and cache them.
 * @param {Element} container - Container element to query within
 * @param {string} selector - CSS selector
 * @returns {NodeList|Array} - Cached or newly queried elements
 */
export function getCachedElements(container, selector) {
  if (!container || !selector) return [];
  
  // Get or create cache for this container
  let cache = domCache.get(container);
  if (!cache) {
    cache = new Map();
    domCache.set(container, cache);
  }
  
  const cacheKey = `all:${selector}`;
  
  // Return cached elements if available and still in DOM
  if (cache.has(cacheKey)) {
    const elements = cache.get(cacheKey);
    if (hasConnectedElements(elements)) {
      return elements;
    }
    // Elements were removed, clear from cache
    cache.delete(cacheKey);
  }
  
  // Query and cache
  const elements = Array.from(container.querySelectorAll(selector));
  if (elements.length > 0) {
    cache.set(cacheKey, elements);
  }
  return elements;
}

/**
 * Clear cache for a specific container (useful when container is replaced).
 * @param {Element} container - Container element
 */
export function clearCache(container) {
  if (container) {
    domCache.delete(container);
  }
}

/**
 * Clear all caches (use sparingly, mainly for testing).
 */
export function clearAllCaches() {
  // WeakMap doesn't support iteration, so we can't clear all
  // This is intentional - caches will be garbage collected when containers are removed
}

/**
 * True when every node a batch of mutation records added or removed is one of the extension's own floating
 * tooltips. Those writes come from the extension itself, so they must not wake the tooltip sweep again.
 * @param {Array} records - MutationObserver records
 * @returns {boolean}
 */
export function isOnlyOwnTooltipMutation(records) {
  if (!Array.isArray(records) || records.length === 0) return false;
  const own = (n) => n?.nodeType === 1 && n.classList?.contains?.("su-floating-tooltip");
  return records.every((r) => {
    const nodes = [...(r.addedNodes || []), ...(r.removedNodes || [])];
    return nodes.length > 0 && nodes.every(own);
  });
}

/**
 * A "run once soon" scheduler for observer-driven cleanup. Repeated requests share one timer. While the tab is
 * hidden no timer runs: the sweep is held until the tab is visible again, so a background tab does no work.
 * @param {Function} sweep - the work
 * @param {number} delayMs - wait after the last request before sweeping
 * @param {object} [env] - injectable for tests: isHidden, setTimer, clearTimer
 * @returns {{request: Function, resume: Function}} call resume() from a visibilitychange handler
 */
export function createDeferredSweep(sweep, delayMs, env = {}) {
  const isHidden = env.isHidden || (() => typeof document !== "undefined" && document.hidden);
  const setTimer = env.setTimer || ((fn, ms) => setTimeout(fn, ms));
  let timer = null;
  let held = false;
  const start = () => {
    if (timer) return;
    timer = setTimer(() => { timer = null; sweep(); }, delayMs);
  };
  return {
    request() {
      if (isHidden()) { held = true; return; }
      start();
    },
    resume() {
      if (!held || isHidden()) return;
      held = false;
      start();
    },
  };
}
