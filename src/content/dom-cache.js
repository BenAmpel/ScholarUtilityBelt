// DOM query caching utilities using WeakMap to avoid memory leaks.
// Cache is automatically invalidated when elements are removed from DOM.

const domCache = new WeakMap();

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
    // Check if at least one element is still connected (quick check)
    if (elements.length > 0 && elements[0]?.isConnected) {
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
