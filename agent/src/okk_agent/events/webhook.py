from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Callable

from fastapi import FastAPI, Request, HTTPException

from okk_agent.agent import Event

logger = logging.getLogger(__name__)


def create_webhook_app(on_event: Callable[[Event], None], webhook_secret: str | None = None) -> FastAPI:
    app = FastAPI(title="okk-agent webhook")

    @app.post("/webhook/github")
    async def github_webhook(request: Request):
        body = await request.body()

        # Verify signature if secret is configured
        if webhook_secret:
            signature = request.headers.get("X-Hub-Signature-256", "")
            expected = "sha256=" + hmac.new(
                webhook_secret.encode(), body, hashlib.sha256,
            ).hexdigest()
            if not hmac.compare_digest(signature, expected):
                raise HTTPException(status_code=403, detail="Invalid signature")

        event_type = request.headers.get("X-GitHub-Event", "")
        payload = json.loads(body)

        if event_type == "push":
            ref = payload.get("ref", "")
            if ref.startswith("refs/tags/v"):
                tag = ref.removeprefix("refs/tags/")
                on_event(Event(
                    type="new_tag",
                    summary=f"New tag pushed: {tag}",
                    details={
                        "tag": tag,
                        "ref": ref,
                        "commits": len(payload.get("commits", [])),
                        "pusher": payload.get("pusher", {}).get("name"),
                    },
                ))

        elif event_type == "pull_request":
            action = payload.get("action")
            pr = payload.get("pull_request", {})
            if action == "closed" and pr.get("merged"):
                on_event(Event(
                    type="pr_merged",
                    summary=f"PR #{pr['number']} merged: {pr['title']}",
                    details={
                        "number": pr["number"],
                        "title": pr["title"],
                        "body": pr.get("body", "")[:500],
                        "changed_files": pr.get("changed_files"),
                        "merged_by": pr.get("merged_by", {}).get("login"),
                    },
                ))

        elif event_type == "issue_comment":
            action = payload.get("action")
            comment = payload.get("comment", {})
            issue = payload.get("issue", {})
            commenter = comment.get("user", {}).get("login", "")
            org_member = payload.get("organization", {}).get("login") == "oxia-db" if payload.get("organization") else False
            sender_association = comment.get("author_association", "")
            is_trusted = org_member or sender_association in ("OWNER", "MEMBER", "COLLABORATOR")

            # Only react to new comments from trusted users that mention @okk-agent
            if action == "created" and is_trusted and "🤖" not in comment.get("body", "") and "@okk-agent" in comment.get("body", "").lower():
                on_event(Event(
                    type="github_comment",
                    summary=f"Comment on #{issue['number']} by @{commenter}: {comment.get('body', '')[:100]}",
                    details={
                        "issue_number": issue["number"],
                        "issue_title": issue.get("title", ""),
                        "comment_body": comment.get("body", ""),
                        "commenter": commenter,
                        "issue_labels": [l["name"] for l in issue.get("labels", [])],
                    },
                ))

        return {"status": "ok"}

    @app.get("/healthz")
    async def health():
        return {"status": "healthy"}

    @app.post("/trigger/{event_type}")
    async def trigger_event(event_type: str):
        """Manually trigger an event (for testing)."""
        summaries = {
            "health_check": "Manual health check: verify testcase status and check Oxia logs for anomalies.",
            "periodic_summary": "Manual periodic summary. Check metrics, testcase status, and post a brief update on the daily issue.",
            "daily_report": "Manual daily report trigger.",
            "chaos_round": "Manual chaos round: inject a fault and verify recovery.",
            "scale_event": "Manual scale test: scale down, verify, scale back up.",
        }
        if event_type not in summaries:
            return {"error": f"Unknown event type: {event_type}"}
        on_event(Event(type=event_type, summary=summaries[event_type], details={}))
        return {"status": "triggered", "type": event_type}

    return app
