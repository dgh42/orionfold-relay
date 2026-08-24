/**
 * Path builders for primitives whose id may contain a slash.
 *
 * A primitive shipped by a plugin bundle is registered under a plugin-scoped
 * namespace, `<plugin-id>/<primitive-id>` — see the `options.namespace` branch in
 * `src/lib/agents/profiles/registry.ts` for profiles and `scanBundleBlueprints`
 * in `src/lib/plugins/registry.ts` for blueprints. So `reading-radar/reader-coach`
 * is one id, not two path segments.
 *
 * Interpolating such an id straight into a template literal produces a request
 * path with an extra segment, which no longer matches the single dynamic segment
 * the route declares (`src/app/api/agents/[id]/...`). Next then serves its HTML
 * 404 page. Callers that expect JSON fail on the HTML body rather than on the
 * status, so the symptom reads as a server error instead of a bad URL.
 *
 * Encoding the id keeps it in one segment. Next decodes route params, so the
 * handler still receives the original `reading-radar/reader-coach`.
 */

function primitivePath(
  base: string,
  id: string,
  segments: readonly string[]
): string {
  const tail = segments.length ? `/${segments.join("/")}` : "";
  return `${base}/${encodeURIComponent(id)}${tail}`;
}

/** A profile's page path — `/agents/<id>`, plus any trailing segments. */
export function agentPath(profileId: string, ...segments: string[]): string {
  return primitivePath("/agents", profileId, segments);
}

/** A profile's API path — `/api/agents/<id>`, plus any trailing segments. */
export function agentApiPath(profileId: string, ...segments: string[]): string {
  return primitivePath("/api/agents", profileId, segments);
}

/** A blueprint's API path — `/api/blueprints/<id>`, plus any trailing segments. */
export function blueprintApiPath(
  blueprintId: string,
  ...segments: string[]
): string {
  return primitivePath("/api/blueprints", blueprintId, segments);
}
