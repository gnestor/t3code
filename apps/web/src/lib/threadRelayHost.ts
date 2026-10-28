import * as ThreadRelayHost from "@t3tools/client-runtime/thread-relay/host";
import type { EnvironmentId } from "@t3tools/contracts";
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

const relayConnectedEnvironmentIds = new Set<EnvironmentId>();

/** Whether the environment accepted this app's relay connection; a server without the relay never does. */
export const isThreadRelayConnected = (environmentId: EnvironmentId) =>
  relayConnectedEnvironmentIds.has(environmentId);

/** One host per page load: each open app is its own relay host. */
export const layer = Layer.unwrap(
  Effect.sync(() =>
    ThreadRelayHost.layer({
      clientId: randomUUID(),
      enabled: threadRelayEnabled,
      onConnectedChange: (environmentId, connected) => {
        if (connected) relayConnectedEnvironmentIds.add(environmentId);
        else relayConnectedEnvironmentIds.delete(environmentId);
      },
    }),
  ),
);
