import importlib.util
import unittest
from pathlib import Path
from unittest.mock import call, patch


SPEC = importlib.util.spec_from_file_location(
    "check_submodule_pointers",
    Path(__file__).resolve().parents[1] / "check_submodule_pointers.py",
)
HOOK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HOOK)


class OpenForkPrTest(unittest.TestCase):
    repo = "serenedb/duckdb"
    sha = "08958fe028b3e45a21bc8b90ea3167a2d550a800"
    base = "v2026.10.06"

    def pr(self, **values):
        return {
            "number": 223,
            "state": "open",
            "head": {"sha": self.sha},
            "base": {"ref": self.base},
            **values,
        }

    def associated(self):
        return f"repos/{self.repo}/commits/{self.sha}/pulls"

    def page(self, number):
        return f"repos/{self.repo}/pulls?state=open&per_page=100&page={number}"

    def test_empty_commit_association_finds_exact_open_head(self):
        with patch.object(
            HOOK, "api_get", side_effect=[(200, []), (200, [self.pr()])]
        ) as api:
            self.assertEqual(
                HOOK.open_fork_pr(self.repo, self.sha),
                (f"{self.repo}#223 into {self.base}", self.base),
            )
        self.assertEqual(
            api.call_args_list, [call(self.associated()), call(self.page(1))]
        )

    def test_associated_open_head_needs_no_fallback(self):
        with patch.object(HOOK, "api_get", return_value=(200, [self.pr()])) as api:
            self.assertIsNotNone(HOOK.open_fork_pr(self.repo, self.sha))
        api.assert_called_once_with(self.associated())

    def test_unknown_commit_association_uses_open_pull_requests(self):
        for code in (404, 422):
            with self.subTest(code=code), patch.object(
                HOOK, "api_get", side_effect=[(code, None), (200, [self.pr()])]
            ):
                self.assertIsNotNone(HOOK.open_fork_pr(self.repo, self.sha))

    def test_fallback_paginates_and_filters_candidates(self):
        rejected = [
            self.pr(state="closed"),
            self.pr(head={"sha": "another-commit"}),
            self.pr(base={"ref": "main"}),
        ]
        with patch.object(
            HOOK,
            "api_get",
            side_effect=[
                (200, []),
                (200, [rejected[0]] * 100),
                (200, [*rejected, self.pr()]),
            ],
        ) as api:
            self.assertIsNotNone(HOOK.open_fork_pr(self.repo, self.sha))
        self.assertEqual(
            api.call_args_list,
            [call(self.associated()), call(self.page(1)), call(self.page(2))],
        )

    def test_unmatched_candidates_do_not_pass(self):
        for pr in (
            self.pr(state="closed"),
            self.pr(head={"sha": "another-commit"}),
            self.pr(base={"ref": "main"}),
        ):
            with self.subTest(pr=pr), patch.object(
                HOOK, "api_get", side_effect=[(200, []), (200, [pr])]
            ):
                self.assertIsNone(HOOK.open_fork_pr(self.repo, self.sha))

    def test_older_open_base_remains_a_failure(self):
        with patch.object(
            HOOK,
            "api_get",
            side_effect=[(200, []), (200, [self.pr(base={"ref": "v2026.09.01"})])],
        ):
            status, _ = HOOK.verdict(
                {self.base: "main-commit"},
                self.sha,
                "main-commit",
                lambda branch, sha: sha == "main-commit",
                lambda: True,
                lambda: HOOK.open_fork_pr(self.repo, self.sha),
            )
        self.assertEqual(status, "fail")

    def test_unavailable_commit_association_fails_closed(self):
        for response in ((0, None), (403, None), (500, None), (200, {})):
            with self.subTest(response=response), patch.object(
                HOOK, "api_get", return_value=response
            ) as api:
                with self.assertRaises(HOOK.Unreachable):
                    HOOK.open_fork_pr(self.repo, self.sha)
                api.assert_called_once_with(self.associated())

    def test_unavailable_fallback_page_fails_closed(self):
        for response in (
            (0, None),
            (403, None),
            (404, None),
            (422, None),
            (500, None),
            (200, {}),
        ):
            with self.subTest(response=response), patch.object(
                HOOK,
                "api_get",
                side_effect=[
                    (200, []),
                    (200, [self.pr(state="closed")] * 100),
                    response,
                ],
            ):
                with self.assertRaises(HOOK.Unreachable):
                    HOOK.open_fork_pr(self.repo, self.sha)


if __name__ == "__main__":
    unittest.main()
