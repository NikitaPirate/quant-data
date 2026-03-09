# quant-data Agents

- Do not write absolute paths in repository documentation. If an absolute path is truly necessary, ask the user for explicit confirmation first.
- Keep `skills/quant-data/SKILL.md` and `skills/quant-data/agents/openai.yaml` aligned with the real `quant-data` CLI and Python API.
- Update the skill in the same change whenever command names, flags, JSON schemas, config discovery, supported data flows, market types, exchange support rules, or the `Candles` API change.
- Maintain both output modes for every CLI command:
  default output must stay human-readable;
  `--json` output must stay machine-readable and current.
- Treat `qd capabilities` and `qd config show` as the introspection surface for agents. Keep them accurate whenever support boundaries or config behavior change.
