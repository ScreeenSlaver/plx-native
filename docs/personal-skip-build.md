# Personal debug-flavour build

This guide builds a personal, shipping-feature PlxNative package from the `ScreeenSlaver/plx-native`
fork. It does not publish an official release. The package uses the separate debug app identity so
it can sit beside the stable PlxNative installation.

## Run the build

Once the fork's **CI** workflow is available in GitHub Actions, open the fork's **Actions** tab,
choose **CI**, select **Run workflow**, and choose branch `codex/personal-skip-controls`. The workflow has no
inputs: it runs the existing host lint, unit-test and Python gates alongside the webOS ARM build,
package checks and firmware compatibility gates. Wait for the run to finish and use the artifact
from a fully green run.

If GitHub has not enabled Actions for the new fork, enable the fork's workflows first, then run only
**CI**. Do not run the release, nightly, release-candidate or Pages workflows for a personal
package. The CI run uploads a GitHub Actions artifact named `plxnative-<commit SHA>`; download it
from that run's **Artifacts** section and extract the `.ipk` inside.

The hosted runner supplies the arm64 Linux environment and webOS NDK required for this product
build. Both Make invocations set `RELEASE=1`, and packaging explicitly sets `FLAVOR=debug`. The
former compiles the shipping feature set; the latter keeps the package at app ID
`com.beb.plxnative.debug`, with its own launcher tile and local app data. It can be installed beside
the stable `com.beb.plxnative` app. Keep stable installed as your fallback.

Install the extracted `.ipk` from your computer with webOS Dev Manager using **Apps → Install**.
Open **PlxNative debug** from the TV launcher and sign in. The stable and debug installs have
separate sign-ins and settings. To remove the personal copy later, uninstall the debug app in Dev
Manager; this does not uninstall stable.

## Skip Intro and Skip Recap

During playback, PlxNative can look up community-submitted segment timings from TheIntroDB and
IntroDB. Existing Plex markers take precedence. Community timings fill missing marker kinds when
the item has a supported external ID and usable episode coordinates; the lookup is asynchronous,
so playback continues if a service is slow or has no matching data. Coverage varies, and community
timings may not match a different cut or edition. Reliable Plex IDs and season/episode ordering are
needed; title guessing is not used.

Episodes require verified TMDB aired ordering. When a show inherits its library's ordering,
the shared Plex token must be able to read that preference. If the server refuses it, or uses
DVD, absolute, TVDB or unknown ordering, community timings are left unavailable. Server markers
and More → Next Episode remain usable.

When the playhead reaches a known intro or recap, the player offers a **Skip Intro** or **Skip
Recap** control. Pressing the control performs the skip; it is not automatic. No Plex marker needs
to be created manually, and the app does not submit timings or media.

The persistent **Next episode** preference is in **Settings → Playback → Next episode**:
**Countdown**, **After credits**, or **Off**. The **Next Episode** item in the More menu is a
one-time action that starts the queued episode immediately; it does not change that saved
preference.

See [the privacy policy](../PRIVACY.md#community-skip-segment-lookups) for the exact lookup data and
network disclosure. On-device playback, display and performance have not been verified by the
hosted build; a green CI run proves its stated build and host gates only.
