import { describe, expect, it } from "vitest";

import {
  agentApiPath,
  agentPath,
  blueprintApiPath,
} from "@/lib/routing/primitive-paths";

/**
 * A plugin bundle registers its primitives under `<plugin-id>/<primitive-id>`,
 * so an id can contain a slash. These paths must keep such an id inside one
 * path segment: a route declares a single dynamic segment, and an id that spans
 * two of them matches no route at all.
 */
const NAMESPACED = "reading-radar/reader-coach";
const PLAIN = "reader-coach";

/** Segments a request path has, the way route matching counts them. */
function segmentCount(path: string): number {
  return path.split("/").length;
}

describe("primitive paths", () => {
  it("leaves an id without a slash untouched", () => {
    expect(agentApiPath(PLAIN)).toBe("/api/agents/reader-coach");
    expect(agentPath(PLAIN)).toBe("/agents/reader-coach");
  });

  it("keeps a namespaced id in a single segment", () => {
    expect(agentApiPath(NAMESPACED)).toBe(
      "/api/agents/reading-radar%2Freader-coach"
    );
    expect(segmentCount(agentApiPath(NAMESPACED))).toBe(
      segmentCount(agentApiPath(PLAIN))
    );
    expect(segmentCount(agentPath(NAMESPACED))).toBe(
      segmentCount(agentPath(PLAIN))
    );
  });

  it("round-trips the id the way route params are decoded", () => {
    const segments = agentApiPath(NAMESPACED).split("/");
    expect(decodeURIComponent(segments[3])).toBe(NAMESPACED);
  });

  it("appends trailing segments after the id", () => {
    expect(agentApiPath(NAMESPACED, "context")).toBe(
      "/api/agents/reading-radar%2Freader-coach/context"
    );
    expect(agentApiPath(NAMESPACED, "test-results")).toBe(
      "/api/agents/reading-radar%2Freader-coach/test-results"
    );
    expect(agentPath(NAMESPACED, "edit")).toBe(
      "/agents/reading-radar%2Freader-coach/edit"
    );
  });

  it("builds blueprint paths the same way", () => {
    expect(blueprintApiPath("finance-pack/monthly-close", "instantiate")).toBe(
      "/api/blueprints/finance-pack%2Fmonthly-close/instantiate"
    );
    expect(
      segmentCount(blueprintApiPath("finance-pack/monthly-close", "start"))
    ).toBe(segmentCount(blueprintApiPath("monthly-close", "start")));
  });

  it("encodes other characters that would break a segment", () => {
    expect(agentApiPath("a?b#c")).toBe("/api/agents/a%3Fb%23c");
  });
});
