// Web Worker for heavy statistical computations
// Processes citation arrays and computes metrics like h-index, g-index, Gini coefficient, etc.

self.onmessage = function(e) {
  const { type, data } = e.data;
  
  if (type === 'computeStats') {
    const { citations, years, authorCounts, yearCitations } = data;
    
    // Sort citations descending
    const sortedCitations = citations.slice().sort((a, b) => b - a);
    const n = sortedCitations.length;
    const totalCite = sortedCitations.reduce((a, b) => a + b, 0);
    
    // h-index: largest h such that h papers have >= h citations each
    let hIndex = 0;
    for (let i = 0; i < sortedCitations.length; i++) {
      if (sortedCitations[i] >= i + 1) hIndex = i + 1;
      else break;
    }
    
    // g-index: largest g such that top g papers have ≥ g² citations (cumulative sum)
    let gIndex = 0;
    let cumSum = 0;
    for (let g = 1; g <= sortedCitations.length; g++) {
      cumSum += sortedCitations[g - 1];
      if (cumSum >= g * g) gIndex = g;
    }
    
    // Gini coefficient over citation counts (0 = equal, 1 = maximally unequal)
    let citationGini = null;
    if (n >= 1 && totalCite > 0) {
      const asc = sortedCitations.slice().sort((a, b) => a - b);
      let B = 0;
      for (let i = 0; i < n; i++) B += (i + 1) * asc[i];
      citationGini = Math.round(((2 * B) / (n * totalCite) - (n + 1) / n) * 100) / 100;
      citationGini = Math.max(0, Math.min(1, citationGini));
    }
    
    // e-index: excess citations beyond h-core. e = √(Σ_{i=1..h} (c_i - h))
    let eIndex = null;
    if (hIndex >= 1 && sortedCitations.length >= hIndex) {
      let excessSum = 0;
      for (let i = 0; i < hIndex; i++) excessSum += Math.max(0, sortedCitations[i] - hIndex);
      eIndex = excessSum > 0 ? Math.round(Math.sqrt(excessSum) * 100) / 100 : null;
    }
    
    // h-core share: citations in top h papers / total citations
    let hCoreShare = null;
    if (hIndex >= 1 && totalCite > 0 && sortedCitations.length >= hIndex) {
      let hCoreCite = 0;
      for (let i = 0; i < hIndex; i++) hCoreCite += sortedCitations[i];
      hCoreShare = Math.round((hCoreCite / totalCite) * 1000) / 1000;
    }
    
    // Median citations per paper
    let medianCitations = null;
    if (n >= 1) {
      const sorted = sortedCitations.slice().sort((a, b) => a - b);
      const mid = Math.floor(n / 2);
      medianCitations = n % 2 === 1 ? sorted[mid] : Math.round((sorted[mid - 1] + sorted[mid]) / 2);
    }
    
    // Mean citations per paper
    const meanCitations = n >= 1 && totalCite >= 0 ? Math.round((totalCite / n) * 10) / 10 : null;
    
    // Consistency index: coefficient of variation σ/μ
    let consistencyIndex = null;
    if (n >= 2 && totalCite > 0) {
      const mean = totalCite / n;
      let variance = 0;
      for (let i = 0; i < n; i++) variance += (sortedCitations[i] - mean) ** 2;
      variance /= n;
      const sigma = Math.sqrt(variance);
      consistencyIndex = mean > 0 ? Math.round((sigma / mean) * 100) / 100 : null;
    }
    
    // Average team size: mean authors per paper
    let avgTeamSize = null;
    if (authorCounts && authorCounts.length > 0) {
      avgTeamSize = Math.round((authorCounts.reduce((a, b) => a + b, 0) / authorCounts.length) * 10) / 10;
    }
    
    // Citation half-life: median publication year weighted by citations
    let citationHalfLife = null;
    if (yearCitations && yearCitations.length > 0 && totalCite > 0) {
      const sorted = yearCitations.slice().sort((a, b) => a.year - b.year);
      let cumCite = 0;
      const target = totalCite / 2;
      for (const item of sorted) {
        cumCite += item.citations;
        if (cumCite >= target) {
          citationHalfLife = item.year;
          break;
        }
      }
    }
    
    self.postMessage({
      type: 'statsResult',
      data: {
        hIndex,
        gIndex: gIndex > 0 ? gIndex : null,
        citationGini,
        eIndex,
        hCoreShare,
        medianCitations,
        meanCitations,
        consistencyIndex,
        avgTeamSize,
        citationHalfLife
      }
    });
  } else if (type === 'computeCSS') {
    // Contribution Signal Score computation
    const { velocity, venueWeight, artifactCount } = data;
    
    // Normalize velocity (log scale)
    const V_norm = velocity > 0 ? Math.log(1 + velocity) / Math.log(1 + 100) : 0;
    
    // Artifact count normalized (0-1 scale, assuming max 2 artifacts)
    const N = Math.min(artifactCount || 0, 2) / 2;
    
    // CSS = 0.4×V + 0.4×W + 0.2×N (E omitted - no reference entropy data)
    const score = Math.round((0.4 * V_norm + 0.4 * venueWeight + 0.2 * N) * 100);
    
    self.postMessage({
      type: 'cssResult',
      data: {
        score,
        V_norm: Math.round(V_norm * 100) / 100,
        W: Math.round(venueWeight * 100) / 100,
        N
      }
    });
  }
};
