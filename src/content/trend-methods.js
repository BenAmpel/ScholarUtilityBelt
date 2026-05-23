export const METHOD_KEYWORDS = [
  "meta-analysis",
  "systematic review",
  "randomized controlled trial",
  "survey",
  "case study",
  "qualitative",
  "longitudinal",
  "cross-sectional",
  "machine learning",
  "deep learning",
  "nlp",
  "llm"
];

const patterns = METHOD_KEYWORDS.map(kw => ({
  keyword: kw,
  regex: new RegExp("\\b" + kw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\b", "i")
}));

export function detectMethods(elements) {
  const counts = new Map();
  for (const el of elements) {
    const text = String(el?.textContent || "");
    if (!text) continue;
    for (const { keyword, regex } of patterns) {
      if (regex.test(text)) {
        counts.set(keyword, (counts.get(keyword) || 0) + 1);
      }
    }
  }
  return counts;
}
