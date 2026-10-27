import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";

export const TEST_SERVER_ENVIRONMENT_ID = EnvironmentId.make("environment-local-test");

/** The receiving server's identity for orchestration tests that run the turn services. */
export const layerTestServerEnvironmentIdentity = Layer.succeed(
  ServerEnvironment.ServerEnvironmentIdentity,
  ServerEnvironment.ServerEnvironmentIdentity.of({
    getEnvironmentId: Effect.succeed(TEST_SERVER_ENVIRONMENT_ID),
  }),
);
