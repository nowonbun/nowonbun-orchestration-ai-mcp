from __future__ import annotations

ALLOWED_ROLES = {"system", "user", "assistant", "tool"}


def normalize_messages(messages: list[dict] | None) -> list[dict[str, str]]:
    if messages is None:
        return []
    if not isinstance(messages, list):
        raise ValueError("messages must be a list")

    normalized: list[dict[str, str]] = []
    for message in messages:
        role = str(message.get("role", "")).strip()
        content = str(message.get("content", ""))
        if not role:
            continue
        if role not in ALLOWED_ROLES:
            raise ValueError(f"unsupported role: {role}")
        if content:
            normalized.append({"role": role, "content": content})
    return normalized


def compile_prompt(system_prompt: str | None, messages: list[dict] | None, prompt: str | None) -> str:
    parts: list[str] = []

    if system_prompt:
        parts.extend(["[SYSTEM]", system_prompt, ""])

    for message in normalize_messages(messages):
        parts.extend([f"[{message['role'].upper()}]", message["content"], ""])

    if prompt:
        parts.extend(["[USER]", str(prompt)])

    return "\n".join(parts).strip()
