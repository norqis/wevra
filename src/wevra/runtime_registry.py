from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List

from wevra.models import ApprovalMode, RuntimeBackend


@dataclass(frozen=True)
class StructuredRuntimeAdapter:
    backend: RuntimeBackend
    label: str
    command_builder: Callable[[str, dict, str, ApprovalMode, str | None, str | None], List[str]]
    output_loader: Callable[[str, str | None], dict]
    timeout_builder: Callable[[int], str]
    schema_preparer: Callable[[dict], dict]


def _identity_schema(schema: dict) -> dict:
    return copy.deepcopy(schema)


def _prepare_openai_strict_schema(schema: dict) -> dict:
    def transform(node):
        if isinstance(node, dict):
            cleaned = {}
            for key, value in node.items():
                if key == "properties" and isinstance(value, dict):
                    cleaned["properties"] = {
                        property_name: transform(property_schema)
                        for property_name, property_schema in value.items()
                    }
                    continue
                if key in {"default", "title"}:
                    continue
                cleaned[key] = transform(value)
            if cleaned.get("type") == "object" or "properties" in cleaned:
                properties = cleaned.get("properties")
                if not isinstance(properties, dict):
                    properties = {}
                cleaned["properties"] = {
                    property_name: transform(property_schema)
                    for property_name, property_schema in properties.items()
                }
                cleaned["required"] = list(cleaned["properties"].keys())
                cleaned["additionalProperties"] = False
            return cleaned
        if isinstance(node, list):
            return [transform(item) for item in node]
        return node

    return transform(copy.deepcopy(schema))


def _build_codex_command(
    prompt: str,
    schema: dict,
    model: str,
    approval_mode: ApprovalMode,
    schema_path: str | None,
    output_path: str | None,
) -> List[str]:
    if not schema_path or not output_path:
        raise ValueError("codex requires schema_path and output_path")
    command = [
        "codex",
        "exec",
        "--skip-git-repo-check",
        "--output-schema",
        schema_path,
        "--output-last-message",
        output_path,
    ]
    if model:
        command.extend(["--model", model])
    if approval_mode == ApprovalMode.AUTO:
        command.append("--dangerously-bypass-approvals-and-sandbox")
    else:
        command.append("--full-auto")
    command.append(prompt)
    return command


def _load_codex_output(stdout_text: str, output_path: str | None) -> dict:
    if not output_path:
        raise ValueError("codex requires output_path")
    return json.loads(Path(output_path).read_text())


def _build_claude_command(
    prompt: str,
    schema: dict,
    model: str,
    approval_mode: ApprovalMode,
    schema_path: str | None,
    output_path: str | None,
) -> List[str]:
    command = [
        "claude",
        "-p",
        "--json-schema",
        json.dumps(schema, sort_keys=True),
    ]
    if model:
        command.extend(["--model", model])
    if approval_mode == ApprovalMode.AUTO:
        command.append("--dangerously-skip-permissions")
    else:
        command.extend(["--permission-mode", "default"])
    command.append(prompt)
    return command


def _load_claude_output(stdout_text: str, output_path: str | None) -> dict:
    return json.loads(stdout_text)


STRUCTURED_RUNTIME_ADAPTERS: Dict[RuntimeBackend, StructuredRuntimeAdapter] = {
    RuntimeBackend.CODEX: StructuredRuntimeAdapter(
        backend=RuntimeBackend.CODEX,
        label="Codex",
        command_builder=_build_codex_command,
        output_loader=_load_codex_output,
        timeout_builder=lambda seconds: (
            f"Codex timed out after {seconds}s while waiting for a structured response."
        ),
        schema_preparer=_prepare_openai_strict_schema,
    ),
    RuntimeBackend.CLAUDE: StructuredRuntimeAdapter(
        backend=RuntimeBackend.CLAUDE,
        label="Claude Code",
        command_builder=_build_claude_command,
        output_loader=_load_claude_output,
        timeout_builder=lambda seconds: (
            f"Claude Code timed out after {seconds}s while waiting for a structured response."
        ),
        schema_preparer=_identity_schema,
    ),
}


RUNTIME_LABELS: Dict[RuntimeBackend, str] = {
    RuntimeBackend.INHERIT: "Per-role defaults",
    RuntimeBackend.MOCK: "Mock",
    RuntimeBackend.CODEX: "Codex",
    RuntimeBackend.CLAUDE: "Claude Code",
}


def runtime_label(runtime: RuntimeBackend | str) -> str:
    if isinstance(runtime, str):
        runtime = RuntimeBackend(runtime)
    return RUNTIME_LABELS.get(runtime, runtime.value)


def runtime_option_payload(
    *,
    include_mock: bool = True,
    include_inherit: bool = False,
) -> list[dict[str, str]]:
    options: list[RuntimeBackend] = []
    if include_inherit:
        options.append(RuntimeBackend.INHERIT)
    if include_mock:
        options.append(RuntimeBackend.MOCK)
    options.extend([RuntimeBackend.CODEX, RuntimeBackend.CLAUDE])
    return [{"value": runtime.value, "label": runtime_label(runtime)} for runtime in options]


def structured_runtime_adapter(runtime: RuntimeBackend) -> StructuredRuntimeAdapter:
    try:
        return STRUCTURED_RUNTIME_ADAPTERS[runtime]
    except KeyError as exc:
        raise RuntimeError(f"Unsupported backend: {runtime.value}") from exc
