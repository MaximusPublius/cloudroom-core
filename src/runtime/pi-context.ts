// Loaded only by the Pi adapter. This adds context, never a user task or an agent loop.
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  pi.registerCommand("cloudroom_context", {
    description: "Cloudroom context-only notice (v1)",
    handler: async (args) => {
      const { text } = JSON.parse(args);
      if (typeof text !== "string" || text.length > 32768) throw new Error("Invalid notice");
      pi.sendMessage({ customType: "cloudroom_notice", content: text, display: true }, { triggerTurn: false });
    },
  });
}
