import importlib.util
import subprocess
from pathlib import Path

import pytest

CI = Path(__file__).resolve().parents[1] / "ci.py"


def load_ci():
    spec = importlib.util.spec_from_file_location("ci_under_test", CI)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def repo(tmp_path, monkeypatch):
    ci = load_ci()

    def write(rel, text):
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    write(ci.IMAGE_DOCKERFILE,
          "FROM ubuntu\n"
          "COPY --from=rust /usr/local/cargo /usr/local/cargo\n"
          "COPY test-data-requirements.txt /tmp/\n"
          "COPY --from=drivers java /tmp/drivers/java\n")
    write("scripts/ci/test-data-requirements.txt", "a==1\n")
    write("scripts/ci/other.txt", "not copied\n")
    write("tests/drivers/java/pom.xml", "<project/>\n")
    write(".gitignore", "target/\n")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    monkeypatch.setattr(ci, "ROOT", tmp_path)
    return ci, write


def test_the_tag_is_a_hash_of_the_inputs(repo):
    ci, _ = repo
    tag = ci.image_name()
    assert tag.startswith(f"{ci.IMAGE_REPO}:")
    assert len(tag.rpartition(":")[2]) == 12
    assert ci.image_name() == tag


def test_a_copied_file_changes_the_tag_and_another_file_does_not(repo):
    ci, write = repo
    tag = ci.image_name()
    write("scripts/ci/other.txt", "still not copied\n")
    assert ci.image_name() == tag
    write("scripts/ci/test-data-requirements.txt", "a==2\n")
    assert ci.image_name() != tag


def test_an_untracked_file_in_a_copied_directory_counts_unless_ignored(repo):
    ci, write = repo
    tag = ci.image_name()
    write("tests/drivers/java/target/build.log", "ignored\n")
    assert ci.image_name() == tag
    write("tests/drivers/java/Extra.java", "class Extra {}\n")
    assert ci.image_name() != tag


def test_the_dockerfile_is_an_input(repo):
    ci, write = repo
    tag = ci.image_name()
    write(ci.IMAGE_DOCKERFILE,
          "FROM ubuntu\n"
          "COPY --from=rust /usr/local/cargo /usr/local/cargo\n"
          "COPY test-data-requirements.txt /tmp/\n"
          "COPY --from=drivers java /tmp/drivers/java\n"
          "RUN true\n")
    assert ci.image_name() != tag
