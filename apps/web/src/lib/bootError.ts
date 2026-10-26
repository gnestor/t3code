import type { DesktopBridge } from "@t3tools/contracts";

const FALLBACK_TRIAGE_COMMAND = "npx t3 triage";

/** The Windows launcher is a `.cmd` run from PowerShell, which needs `&` to run a quoted path. */
function launcherCommand(launcherPath: string) {
  if (launcherPath.endsWith(".cmd")) {
    return /^[\w:\\.-]+$/.test(launcherPath)
      ? launcherPath
      : `& '${launcherPath.replace(/['\u2018\u2019\u201A\u201B]/g, "$&$&")}'`;
  }
  return /^[\w@%+=:,./-]+$/.test(launcherPath)
    ? launcherPath
    : `'${launcherPath.replaceAll("'", `'"'"'`)}'`;
}

/** Desktop can run its bundled launcher without Node; everything else falls back to npx. */
export async function resolveTriageCommand(bridge: DesktopBridge | undefined) {
  try {
    const state = await bridge?.cliCommand?.getState();
    if (state?.onPath) return "t3 triage";
    if (state?.launcherPath) return `${launcherCommand(state.launcherPath)} triage`;
  } catch (error) {
    console.warn("Could not read the desktop CLI state.", error);
  }
  return FALLBACK_TRIAGE_COMMAND;
}

export function showBootError(error: unknown) {
  console.error("T3 Code failed to start.", error);
  const bootShell = document.getElementById("boot-shell");
  if (!bootShell) return;

  const content = document.createElement("div");
  content.id = "boot-error";
  content.setAttribute("role", "alert");

  const message = document.createElement("p");
  message.textContent = "T3 Code could not load.";
  content.append(message);

  if (import.meta.env.DEV && error instanceof Error) {
    const detail = document.createElement("p");
    detail.textContent = error.message;
    content.append(detail);
  }

  const reload = document.createElement("button");
  reload.type = "button";
  reload.textContent = "Reload";
  reload.addEventListener("click", () => window.location.reload());
  content.append(reload);

  const hint = document.createElement("p");
  hint.textContent = "If Reload does not fix the problem, run this command in a terminal:";
  const command = document.createElement("code");
  command.textContent = FALLBACK_TRIAGE_COMMAND;
  const commandRow = document.createElement("div");
  commandRow.id = "boot-error-command";
  commandRow.append(command);
  // The clipboard API exists only in secure contexts, such as HTTPS and the desktop app.
  if (window.isSecureContext) {
    const copy = document.createElement("button");
    copy.type = "button";
    copy.textContent = "Copy";
    copy.addEventListener("click", () => {
      navigator.clipboard.writeText(command.textContent ?? "").then(
        () => (copy.textContent = "Copied"),
        (copyError) => console.warn("Could not copy the triage command.", copyError),
      );
    });
    commandRow.append(copy);
  }
  content.append(hint, commandRow);
  bootShell.replaceChildren(content);

  void resolveTriageCommand(window.desktopBridge).then((value) => (command.textContent = value));
}
