import {
  useClientSettings,
  useClientSettingsHydrated,
  updateClientSettings,
} from "~/hooks/useSettings";
import { Switch } from "../ui/switch";
import { SettingsRow, SettingsSection } from "./settingsLayout";
import { searchableSetting } from "./settingsSearch";

export function ThreadRelaySettings({ environmentCount }: { environmentCount: number }) {
  const threadRelayEnabled = useClientSettings((current) => current.threadRelayEnabled);
  const settingsHydrated = useClientSettingsHydrated();

  if (environmentCount < 2) return null;

  return (
    <SettingsSection {...searchableSetting("thread-relay")}>
      <SettingsRow
        title="Let agents on my environments reach each other through this app"
        description="While T3 Code is open, agents on your environments can read, search, and message threads on your other environments with this app's access to them. Turn this off to keep each environment's agents isolated."
        control={
          <Switch
            aria-label="Let agents on my environments reach each other through this app"
            checked={threadRelayEnabled}
            disabled={!settingsHydrated}
            onCheckedChange={(checked) => updateClientSettings({ threadRelayEnabled: checked })}
          />
        }
      />
    </SettingsSection>
  );
}
