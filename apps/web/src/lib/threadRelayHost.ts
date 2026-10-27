import * as ThreadRelayHost from "@t3tools/client-runtime/thread-relay/host";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import { randomUUID } from "./utils";

// Settings state imports the connection runtime that mounts this layer, so load it lazily.
const threadRelayEnabled = Stream.unwrap(
  Effect.promise(() => import("../hooks/useSettings")).pipe(
    Effect.map(({ getClientSettings, subscribeClientSettings }) =>
      Stream.callback<boolean>((queue) =>
        Effect.acquireRelease(
          Effect.sync(() => {
            const emit = () => Queue.offerUnsafe(queue, getClientSettings().threadRelayEnabled);
            emit();
            return subscribeClientSettings(emit);
          }),
          (unsubscribe) => Effect.sync(unsubscribe),
        ),
      ),
    ),
  ),
);

/** One host per page load: each open app is its own relay host. */
export const layer = Layer.unwrap(
  Effect.sync(() => ThreadRelayHost.layer({ clientId: randomUUID(), enabled: threadRelayEnabled })),
);
