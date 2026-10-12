// Options card for "Ask my shortlist": provider, the user's own API key, model and editable prices.
// The key is kept only in chrome.storage.local under ASK_CONFIG_KEY. The provider's host is requested as an
// optional host permission when the user turns the feature on (a click is the user gesture), and removed
// when they turn it off. Nothing is requested in the manifest's required permissions.
import { ASK_CONFIG_KEY, DEFAULT_PRICES, normalizeAskConfig } from "../common/ask-verify.js";
import { getProvider } from "../common/ask-provider.js";
import { canUseFeature, getEntitlementStatus } from "../common/entitlement.js";

const $ = (id) => document.getElementById(id);

async function readConfig() {
  const stored = await chrome.storage.local.get(ASK_CONFIG_KEY);
  return normalizeAskConfig(stored[ASK_CONFIG_KEY]);
}

function formValues() {
  return normalizeAskConfig({
    providerId: $("askProvider").value,
    apiKey: $("askApiKey").value,
    model: $("askModel").value,
    inPerM: $("askInPrice").value,
    outPerM: $("askOutPrice").value,
    enabled: true,
  });
}

function fillForm(cfg) {
  $("askProvider").value = cfg.providerId;
  $("askApiKey").value = cfg.apiKey;
  $("askModel").value = cfg.model;
  $("askInPrice").value = String(cfg.inPerM);
  $("askOutPrice").value = String(cfg.outPerM);
}

function say(msg) {
  $("askStatus").textContent = msg;
}

export async function initAskSettings() {
  if (!$("askAiCard")) return;
  let cfg = await readConfig();
  let allowed;
  try {
    allowed = canUseFeature("askShortlist", await getEntitlementStatus({ needPurchased: true }));
  } catch {
    allowed = false;
  }
  fillForm(cfg);
  say(cfg.enabled ? "On." : "Off.");
  $("askProNote").hidden = allowed;
  $("askEnable").disabled = !allowed;
  $("askUnlock").addEventListener("click", () => chrome.runtime.sendMessage({ action: "openUpsellModal" }));

  $("askProvider").addEventListener("change", () => {
    // Switching provider loads that provider's starting model and prices; the key is not carried across.
    const id = $("askProvider").value;
    const d = DEFAULT_PRICES[id];
    $("askModel").value = d.model;
    $("askInPrice").value = String(d.inPerM);
    $("askOutPrice").value = String(d.outPerM);
    $("askApiKey").value = "";
    say("Provider changed. Paste a key for it and turn the feature on.");
  });

  $("askEnable").addEventListener("click", async () => {
    const next = formValues();
    if (next.apiKey.length < 8) {
      say("Paste your API key first.");
      return;
    }
    const provider = getProvider(next.providerId);
    // First await in the handler, so the click still counts as the user gesture.
    let granted;
    try {
      granted = await chrome.permissions.request({ origins: [provider.origin] });
    } catch (e) {
      say("Could not ask for permission: " + (e && e.message ? e.message : "unknown error"));
      return;
    }
    if (!granted) {
      say("Permission for " + provider.host + " was not granted, so the feature stays off.");
      return;
    }
    if (cfg.enabled && cfg.providerId !== next.providerId) {
      try { await chrome.permissions.remove({ origins: [getProvider(cfg.providerId).origin] }); } catch { /* ignore */ }
    }
    cfg = { ...next, enabled: true };
    await chrome.storage.local.set({ [ASK_CONFIG_KEY]: cfg });
    say("On. Questions go to " + provider.host + " only after you confirm each one in the Library.");
  });

  $("askSave").addEventListener("click", async () => {
    const next = formValues();
    if (!cfg.enabled || next.providerId !== cfg.providerId) {
      say("Use “Turn on” to enable this provider.");
      return;
    }
    cfg = { ...next, enabled: true };
    await chrome.storage.local.set({ [ASK_CONFIG_KEY]: cfg });
    say("Saved.");
  });

  $("askDisable").addEventListener("click", async () => {
    const provider = getProvider(cfg.providerId);
    await chrome.storage.local.remove(ASK_CONFIG_KEY);
    if (provider) {
      try { await chrome.permissions.remove({ origins: [provider.origin] }); } catch { /* ignore */ }
    }
    cfg = normalizeAskConfig(null);
    fillForm(cfg);
    say("Off. The key was removed from this browser. The host permission was released unless you had also granted access to all sites.");
  });
}
