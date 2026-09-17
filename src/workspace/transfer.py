"""Portable first-copy snapshots. Uses only Python's standard library and Git."""
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile

LIMIT = 4 * 1024**3
MAX_ENTRIES = 200000
EXCLUDED = {"node_modules", ".cache", "__pycache__", ".venv", "venv", ".next", ".turbo", "target", ".DS_Store", ".cloudroom-imported"}


def git(root, *args, check=True):
    result = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-C", str(root), *args],
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=600)
    if check and result.returncode:
        raise ValueError("Could not preserve the project's Git state")
    return result.stdout if result.returncode == 0 else None


def pack(source, output):
    source = source.resolve(strict=True)
    if not source.is_dir():
        raise ValueError("Choose a project folder")
    total, count = 0, 0
    with tempfile.TemporaryDirectory(prefix="cloudroom-git-") as temporary, tarfile.open(output, "w:gz", compresslevel=1) as archive:
        repositories = []

        def add(path, name):
            nonlocal total, count
            archive.inodes.clear()
            info = archive.gettarinfo(str(path), arcname=name)
            if not (info.isfile() or info.isdir() or info.issym()):
                raise ValueError("Project contains an unsupported special file")
            if info.issym():
                target = (path.parent / os.readlink(path)).resolve()
                if not target.is_relative_to(source):
                    raise ValueError("Project contains a symlink outside its folder; copy its contents into the project first")
                info.linkname = os.path.relpath(target, path.parent)
            total += info.size
            count += 1
            if total > LIMIT or count > MAX_ENTRIES:
                raise ValueError("Project exceeds the 4 GiB or 200,000 file import limit")
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mode = 0o755 if info.isdir() or info.mode & 0o111 else 0o644
            if info.isfile():
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, "rb") as data:
                    before = os.fstat(data.fileno())
                    if before.st_size != info.size or not stat.S_ISREG(before.st_mode):
                        raise ValueError("Project changed while copying; retry")
                    archive.addfile(info, data)
                    after = os.fstat(data.fileno())
                    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                        raise ValueError("Project changed while copying; retry")
            else:
                archive.addfile(info)

        def walk(folder):
            if (folder / ".git").exists():
                metadata = Path(temporary) / str(len(repositories))
                metadata.mkdir()
                head = git(folder, "rev-parse", "--verify", "HEAD", check=False)
                branch = git(folder, "symbolic-ref", "--quiet", "--short", "HEAD", check=False)
                origin = git(folder, "config", "--get", "remote.origin.url", check=False)
                state = {"path": folder.relative_to(source).as_posix(), "head": head.decode().strip() if head else None,
                         "branch": branch.decode().strip() if branch else None, "origin": origin.decode().strip() if origin else None}
                if head:
                    git(folder, "bundle", "create", str(metadata / "bundle"), "--all", "HEAD")
                patch = git(folder, "diff", "--cached", "--binary", "--no-ext-diff", "--no-textconv")
                (metadata / "index.patch").write_bytes(patch)
                repositories.append(state)
                for file in metadata.iterdir():
                    add(file, "git/" + metadata.name + "/" + file.name)
            for path in sorted(folder.iterdir()):
                if path.name == ".git" or path.name in EXCLUDED:
                    continue
                add(path, "files/" + path.relative_to(source).as_posix())
                if path.is_dir() and not path.is_symlink():
                    walk(path)

        walk(source)
        manifest = Path(temporary) / "manifest.json"
        manifest.write_text(json.dumps({"version": 1, "repositories": repositories}))
        add(manifest, "manifest.json")
    print(json.dumps({"bytes": os.path.getsize(output)}))


def unpack(root, destination, identity=None):
    root = root.resolve(strict=True)
    if destination.parent.resolve() != root or destination.exists() or destination.is_symlink():
        raise ValueError("Cloud workspace destination is already occupied")
    destination = root / destination.name
    with tempfile.TemporaryDirectory(prefix=".cloudroom-import-", dir=root) as temporary:
        staging = Path(temporary)
        (staging / "files").mkdir()
        total, count, seen = 0, 0, set()
        with tarfile.open(fileobj=sys.stdin.buffer, mode="r|gz") as archive:
            for member in archive:
                name = PurePosixPath(member.name)
                if name.is_absolute() or ".." in name.parts or not name.parts or "\\" in member.name:
                    raise ValueError("Invalid archive path")
                if name.parts[0] not in {"files", "git", "manifest.json"} or ".git" in name.parts or name == PurePosixPath('files/.cloudroom-imported') or member.name in seen:
                    raise ValueError("Invalid archive entry")
                seen.add(member.name)
                total += member.size
                count += 1
                if total > LIMIT or count > MAX_ENTRIES or member.size < 0:
                    raise ValueError("Project import exceeds its size limit")
                target = staging.joinpath(*name.parts)
                for parent in target.parents:
                    if parent == staging:
                        break
                    if parent.is_symlink():
                        raise ValueError("Archive writes through a symlink")
                target.parent.mkdir(parents=True, exist_ok=True)
                if member.isdir():
                    target.mkdir(exist_ok=True)
                elif member.isfile():
                    with target.open("xb") as output, archive.extractfile(member) as data:
                        shutil.copyfileobj(data, output, 1024 * 1024)
                    target.chmod(0o755 if member.mode & 0o111 else 0o644)
                elif member.issym() and name.parts[0] == "files":
                    link = PurePosixPath(member.linkname)
                    if link.is_absolute() or not (target.parent / member.linkname).resolve().is_relative_to(staging / "files"):
                        raise ValueError("Archive symlink leaves the project")
                    target.symlink_to(member.linkname)
                else:
                    raise ValueError("Unsupported archive entry")
        manifest_path = staging / "manifest.json"
        if manifest_path.stat().st_size > 1024 * 1024:
            raise ValueError("Project manifest is too large")
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("version") != 1:
            raise ValueError("Unsupported project snapshot")
        for index, repository in enumerate(manifest["repositories"]):
            relative = PurePosixPath(repository["path"])
            folder = (staging / "files" / relative).resolve()
            if relative.is_absolute() or ".." in relative.parts or not folder.is_relative_to(staging / "files"):
                raise ValueError("Invalid Git root")
            folder.mkdir(parents=True, exist_ok=True)
            metadata = staging / "git" / str(index)
            if repository["head"]:
                git(folder, "clone", "--bare", str(metadata / "bundle"), str(folder / ".git"))
                git(folder, "config", "core.bare", "false")
                git(folder, "config", "--unset-all", "remote.origin.url", check=False)
                branch = repository["branch"]
                if branch:
                    git(folder, "check-ref-format", "--branch", branch)
                    git(folder, "symbolic-ref", "HEAD", "refs/heads/" + branch)
                else:
                    git(folder, "update-ref", "--no-deref", "HEAD", repository["head"])
                git(folder, "read-tree", repository["head"])
            else:
                git(folder, "init", "--quiet")
                if repository["branch"]:
                    git(folder, "check-ref-format", "--branch", repository["branch"])
                    git(folder, "symbolic-ref", "HEAD", "refs/heads/" + repository["branch"])
            if repository["origin"]:
                git(folder, "config", "remote.origin.url", repository["origin"])
            patch = metadata / "index.patch"
            if patch.stat().st_size:
                git(folder, "apply", "--cached", "--binary", str(patch))
        if destination.exists() or destination.is_symlink():
            raise ValueError("Cloud workspace destination is already occupied")
        if identity:
            (staging / "files/.cloudroom-imported").write_text(identity)
        (staging / "files").rename(destination)


if __name__ == "__main__":
    def terminate(*_):
        raise SystemExit(1)
    signal.signal(signal.SIGTERM, terminate)
    try:
        if sys.argv[1] == "pack":
            pack(Path(sys.argv[2]), Path(sys.argv[3]))
        elif sys.argv[1] == "unpack":
            unpack(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4] if len(sys.argv) > 4 else None)
        else:
            raise ValueError("Unknown workspace operation")
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, tarfile.TarError):
        print("Could not copy the project. Check its size, Git state, symlinks, and available disk space.", file=sys.stderr)
        sys.exit(1)
