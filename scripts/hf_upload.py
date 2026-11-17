"""Apply a staged Fieldhouse run to Hugging Face as ONE atomic commit.

R does all the data work and stages files in build/out plus a list of repo
paths to delete in build/delete_paths.txt. This script uploads the adds and
deletes together, so the dataset is never half-updated: either the new
verified season file AND the removal of its merged delta files land, or
nothing does. Every commit is also kept in the dataset's git history, so any
bad run can be rolled back.

Env: HF_REPO (e.g. your-org/fieldhouse-data), HF_TOKEN (write token).
"""
import os
import sys
from pathlib import Path

from huggingface_hub import CommitOperationAdd, CommitOperationDelete, HfApi
from huggingface_hub.utils import HfHubHTTPError, RepositoryNotFoundError

work = Path(os.environ.get("FIELDHOUSE_WORK", "build"))
out = work / "out"
repo = os.environ["HF_REPO"]
api = HfApi(token=os.environ["HF_TOKEN"])

adds = [p for p in out.rglob("*") if p.is_file()] if out.exists() else []
dels_file = work / "delete_paths.txt"
dels = [l.strip() for l in dels_file.read_text().splitlines() if l.strip()] if dels_file.exists() else []
msg_file = work / "commit_message.txt"
message = msg_file.read_text().strip() if msg_file.exists() else "fieldhouse sync"

if not adds and not dels:
    print("nothing new; no commit")
    sys.exit(0)

# A fine-grained token scoped to one dataset can't create repos, so only
# create it if it really doesn't exist yet (normally you create it by hand).
try:
    api.dataset_info(repo)
except RepositoryNotFoundError:
    api.create_repo(repo, repo_type="dataset", private=True)
# Hard stop if the dataset is (or was switched to) public. Nothing uploads.
if not api.dataset_info(repo).private:
    sys.exit(f"REFUSING TO UPLOAD: dataset {repo} is PUBLIC. Make it private in its Settings on Hugging Face.")
existing = set(api.list_repo_files(repo, repo_type="dataset"))
ops = [CommitOperationAdd(path_in_repo=p.relative_to(out).as_posix(), path_or_fileobj=str(p)) for p in adds]
ops += [CommitOperationDelete(path_in_repo=d) for d in dels if d in existing]
sha_file = work / "base_sha.txt"
parent = sha_file.read_text().strip() if sha_file.exists() else ""
try:
    # parent_commit = the revision this run started from. If another job
    # committed since, Hugging Face rejects this instead of overwriting it.
    info = api.create_commit(repo, repo_type="dataset", operations=ops, commit_message=message,
                             parent_commit=parent or None)
except HfHubHTTPError as e:
    status = getattr(e.response, "status_code", None)
    if status in (409, 412):
        print("CONFLICT: the dataset changed while this run was working; nothing was written.")
        sys.exit(3)
    raise
print(f"committed {len(adds)} files, deleted {len(ops) - len(adds)}")
