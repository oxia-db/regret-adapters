from __future__ import annotations

import base64
import json
import logging
from datetime import datetime, timezone

from github import Github, GithubException

from okk_agent.config import Config

logger = logging.getLogger(__name__)


class ReportTools:
    def __init__(self, config: Config):
        self.config = config
        self._gh = Github(config.github_token)
        self._repo = self._gh.get_repo(config.github_repo)
        self._daily_issue_cache: dict[str, int] = {}  # date_str -> issue_number

    def get_or_create_daily_issue(self) -> str:
        """Get today's tracking issue or create one. Returns issue number and URL."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        title = f"🤖 okk-agent daily report — {today}"

        # Check cache first
        if today in self._daily_issue_cache:
            number = self._daily_issue_cache[today]
            try:
                issue = self._repo.get_issue(number)
                if issue.state == "open":
                    return json.dumps({"number": number, "url": issue.html_url, "existed": True})
            except GithubException:
                pass

        # Search for existing issue
        try:
            query = f"repo:{self.config.github_repo} is:open label:daily-report \"{today}\" in:title"
            results = self._gh.search_issues(query)
            for issue in results:
                if today in issue.title:
                    self._daily_issue_cache[today] = issue.number
                    return json.dumps({"number": issue.number, "url": issue.html_url, "existed": True})
        except GithubException:
            pass

        # Create new daily issue
        try:
            body = f"# Daily Verification Report — {today}\n\nEvents and observations from okk-agent.\n\n---\n🤖 Managed by okk-agent"
            label_objects = []
            for label_name in ["daily-report"]:
                try:
                    label_objects.append(self._repo.get_label(label_name))
                except GithubException:
                    self._repo.create_label(name=label_name, color="0075ca")
                    label_objects.append(self._repo.get_label(label_name))

            issue = self._repo.create_issue(title=title, body=body, labels=label_objects)
            self._daily_issue_cache[today] = issue.number
            logger.info("Created daily issue #%d for %s", issue.number, today)
            return json.dumps({"number": issue.number, "url": issue.html_url, "existed": False})
        except GithubException as e:
            return json.dumps({"error": f"Failed to create daily issue: {e}"})

    def comment_on_issue(self, issue_number: int, body: str) -> str:
        """Add a comment to an existing GitHub issue."""
        try:
            if "🤖" not in body:
                body += "\n\n🤖 okk-agent"
            issue = self._repo.get_issue(issue_number)
            comment = issue.create_comment(body)
            logger.info("Commented on issue #%d (comment %d)", issue_number, comment.id)
            return json.dumps({"status": "commented", "issue_number": issue_number, "comment_id": comment.id})
        except GithubException as e:
            return json.dumps({"error": f"Failed to comment on issue #{issue_number}: {e}"})

    def search_github_issues(self, query: str, state: str = "open") -> str:
        try:
            full_query = f"repo:{self.config.github_repo} is:{state} {query}"
            results = self._gh.search_issues(full_query)
            issues = []
            for i, issue in enumerate(results):
                if i >= 10:
                    break
                issues.append({
                    "number": issue.number,
                    "title": issue.title,
                    "state": issue.state,
                    "labels": [l.name for l in issue.labels],
                    "created_at": issue.created_at.isoformat(),
                    "url": issue.html_url,
                })
            return json.dumps(issues, indent=2)
        except GithubException as e:
            return json.dumps({"error": f"GitHub search failed: {e}"})

    def create_github_issue(self, title: str, body: str, labels: list[str] | None = None) -> str:
        try:
            # Ensure agent marker
            if "\U0001f916" not in body:
                body += "\n\n---\n\U0001f916 Filed by okk-agent"

            label_objects = []
            if labels:
                for label_name in labels:
                    try:
                        label_objects.append(self._repo.get_label(label_name))
                    except GithubException:
                        # Create label if it doesn't exist
                        self._repo.create_label(name=label_name, color="0075ca")
                        label_objects.append(self._repo.get_label(label_name))

            issue = self._repo.create_issue(
                title=title,
                body=body,
                labels=label_objects,
            )
            logger.info("Created issue #%d: %s", issue.number, title)
            return json.dumps({
                "status": "created",
                "number": issue.number,
                "url": issue.html_url,
            })
        except GithubException as e:
            return json.dumps({"error": f"Failed to create issue: {e}"})

    def create_pull_request(self, branch: str, title: str, body: str, files: dict[str, str]) -> str:
        try:
            # Ensure branch naming
            if not branch.startswith("agent/"):
                branch = f"agent/{branch}"

            # Ensure agent marker
            if "\U0001f916" not in body:
                body += "\n\n---\n\U0001f916 Generated by okk-agent"

            # Get default branch SHA
            default_branch = self._repo.default_branch
            ref = self._repo.get_git_ref(f"heads/{default_branch}")
            base_sha = ref.object.sha

            # Create branch
            try:
                self._repo.create_git_ref(f"refs/heads/{branch}", base_sha)
            except GithubException as e:
                if e.status == 422:  # Branch already exists
                    pass
                else:
                    raise

            # Commit files
            for path, content in files.items():
                try:
                    existing = self._repo.get_contents(path, ref=branch)
                    self._repo.update_file(
                        path=path, message=f"agent: update {path}",
                        content=content, sha=existing.sha, branch=branch,
                    )
                except GithubException:
                    self._repo.create_file(
                        path=path, message=f"agent: add {path}",
                        content=content, branch=branch,
                    )

            # Create PR
            pr = self._repo.create_pull(
                title=title, body=body, head=branch, base=default_branch,
            )

            # Add label
            try:
                pr.add_to_labels("agent-improvement")
            except GithubException:
                self._repo.create_label(name="agent-improvement", color="1d76db")
                pr.add_to_labels("agent-improvement")

            logger.info("Created PR #%d: %s", pr.number, title)
            return json.dumps({
                "status": "created",
                "number": pr.number,
                "url": pr.html_url,
            })
        except GithubException as e:
            return json.dumps({"error": f"Failed to create PR: {e}"})

    def read_okk_source(self, path: str) -> str:
        try:
            content = self._repo.get_contents(path)
            if content.encoding == "base64":
                return base64.b64decode(content.content).decode("utf-8")
            return content.decoded_content.decode("utf-8")
        except GithubException as e:
            return f"Error reading {path}: {e}"
