export interface TagFilter {
  tags?: string[];
  onlyTagged?: boolean;
}

export function normalizeTags(tags?: string[]): string[] | undefined {
  if (!tags || tags.length === 0) return undefined;
  const set = new Set(tags.map(t => t.trim().toLowerCase()).filter(Boolean));
  return Array.from(set);
}

export function matchesFilter(fileTags: string[] | undefined, filter?: TagFilter): boolean {
  if (!filter || (!filter.tags && !filter.onlyTagged)) return true;
  const ft = normalizeTags(fileTags) ?? [];
  const want = normalizeTags(filter.tags) ?? [];

  if (want.length > 0) {
    // any match
    return ft.some(t => want.includes(t));
  }

  // onlyTagged without specific tags
  if (filter.onlyTagged) {
    return ft.length > 0;
  }

  return true;
}

