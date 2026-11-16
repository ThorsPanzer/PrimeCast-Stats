# Public release checklist

- [ ] Confirm that the repository owner has the rights to publish all original PrimeCast code under GPL-3.0.
- [ ] Confirm Riot Developer Portal registration, review and key-type requirements for a publicly shared community application; do **not** distribute a personal API key. A public product may require Riot production approval.
- [ ] Review Riot Games policies and the required disclaimer. Check permission before redistributing any Riot/third-party imagery or logos.
- [ ] Remove personal `config.json`, SQLite caches, private team logos, caches of third-party graphics, logs and temporary files. Review all files in `git status --short` before pushing.
- [ ] Run a clean Windows installation and an end-to-end OBS test (pregame, postgame, head-to-head, settings, network errors and cache migration).
- [ ] Verify operational behavior and browser UI in Firefox and OBS Chromium Browser Source.
- [ ] Provide an accessible project page, terms and privacy policy if Riot requires them for approval.

Important: GPL-3.0 is an OSI-recognized open-source license, but **it does not prohibit reselling** the software or modified versions. To prohibit sales, you would need a restrictive source-available license instead, and it should not be represented as open source.
